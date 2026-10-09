// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.logging.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

/** Process entry point: startup sequence and the wiring of the replication pipeline. */
public final class Main {

    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    /** One row of {@code paimon.table_registry}, as much of it as startup needs. */
    record RegistryRow(long relid, String schema, String table, List<String> pk,
                       List<DdlMessage.Col> cols, boolean backfillDue,
                       String changelogProducer) {}

    private static volatile boolean exiting;

    public static void main(String[] args) throws Exception {
        // 1. configuration (fails with "PGPAIMON_JDBC_URL is required" and so on)
        WriterConfig config = WriterConfig.fromEnv(System.getenv());

        Catalog catalog = CatalogFactory.createCatalog(
            CatalogContext.create(Options.fromMap(config.catalogOptions())));
        SchemaRegistry registry = new SchemaRegistry(catalog, config);

        boolean slotExists;
        List<RegistryRow> rows;
        try (Connection conn = plainConnection(config)) {
            // 2. CHECK that the slot exists. Main never creates it: the initial backfill must
            //    read the snapshot exported by the slot's own creation (Backfiller, Task 13).
            slotExists = slotExists(conn, config.slotName());
            // 3. rebuild the registry from paimon.table_registry BEFORE the stream opens. A
            //    'create' message is emitted once and consumed once; without this a restarted
            //    writer knows no tables and silently discards every replicated row.
            rows = readRegistry(conn);
        }
        for (RegistryRow r : rows) {
            registry.createTable(r.relid(), r.schema(), r.table(), r.pk(), r.cols(),
                                 r.changelogProducer());
        }
        LOG.info("registry rebuilt: " + rows.size() + " table(s), slot "
                 + (slotExists ? "exists" : "absent"));

        // 4. where to resume; 0 means "the slot's own position", an ordinary value
        List<Long> relids = new ArrayList<>();
        for (RegistryRow r : rows) {
            relids.add(r.relid());
        }
        long resume = new OffsetStore(registry, config).highestCommittedLsn(relids);

        PaimonSink sink = new PaimonSink(registry, config);

        // 5. initial backfill (and slot creation) for anything due. A backfill moves the
        //    committed offsets (each loaded table now stands at the slot's consistent point), so
        //    resume is read again afterwards.
        backfillIfDue(config, registry, sink, rows, slotExists);
        resume = new OffsetStore(registry, config).highestCommittedLsn(relids);

        // 6. the pipeline
        PgOutputDecoder decoder = new PgOutputDecoder();
        SlotConsumer[] consumerRef = new SlotConsumer[1];
        TxnBatcher batcher = new TxnBatcher(new ToastResolver(), registry,
            confirmingFlusher(sink, lsn -> consumerRef[0].confirm(lsn)));
        DdlMessageHandler ddl = new DdlMessageHandler(registry, config.messagePrefix());
        SlotConsumer.Handler handler = new PipelineHandler(registry::tracked, ddl::handle,
            new PipelineHandler.Target() {
                @Override public void setRelation(long relid, RelationMeta meta) {
                    sink.setRelation(relid, meta);
                }
                @Override public void truncate(long relid, long commitLsn) throws Exception {
                    sink.truncate(relid, commitLsn);
                }
            }, batcher);
        SlotConsumer consumer = new SlotConsumer(config, decoder, handler);
        consumerRef[0] = consumer;

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.stop();
            try {
                if (!exiting) {
                    mainThread.join(30_000);   // let an in-flight flush finish before closing
                }
                sink.close();
                catalog.close();
            } catch (Exception e) {
                LOG.warning("error while closing the sink: " + e);
            }
        }, "shutdown"));

        try {
            consumer.run(resume);
        } catch (Throwable t) {
            // Fatal by design: resume from the last confirmed LSN on restart.
            LOG.severe("fatal: " + t);
            t.printStackTrace();
            exiting = true;
            System.exit(1);
        }
    }

    /** What step 5 has to do, decided from the slot and the registry alone. */
    enum BackfillAction { NONE, CREATE_SLOT_AND_BACKFILL, RECREATE_SLOT_AND_BACKFILL }

    /**
     * The slot and the due tables determine what is safe.
     * <ul>
     *   <li>slot present, nothing due: stream.</li>
     *   <li>slot absent: every tracked table must be due. A table that is not due has Paimon
     *       data whose stream position was the lost slot; a new slot cannot reconcile it, so
     *       this is a refusal, not a guess.</li>
     *   <li>slot present, every table due: no table has ever streamed (a table leaves the due
     *       state only after its load, and the stream starts after all loads), so the slot
     *       holds nothing of value. An interrupted first load lands here and its exported
     *       snapshot is gone, so the slot is dropped and made again.</li>
     *   <li>slot present, some due and some not: a table added to a running system. Its
     *       snapshot cannot be made consistent with the existing slot.</li>
     * </ul>
     */
    static BackfillAction decideBackfill(boolean slotExists, int dueCount, int totalCount,
                                         String slotName) {
        if (!slotExists) {
            if (dueCount != totalCount) {
                throw new IllegalStateException("slot '" + slotName + "' does not exist but "
                    + (totalCount - dueCount) + " tracked table(s) were already backfilled; "
                    + "their stream position is lost and a new slot cannot reconcile them."
                    + " Re-register them (paimon.remove_table, delete the Paimon tables,"
                    + " paimon.add_table) to load them afresh");
            }
            return BackfillAction.CREATE_SLOT_AND_BACKFILL;
        }
        if (dueCount == 0) {
            return BackfillAction.NONE;
        }
        if (dueCount == totalCount) {
            return BackfillAction.RECREATE_SLOT_AND_BACKFILL;
        }
        throw new IllegalStateException("slot '" + slotName + "' exists and " + dueCount
            + " of " + totalCount + " tracked table(s) are awaiting their initial backfill:"
            + " a table added while the writer's slot exists cannot be loaded consistently."
            + " Remove and re-add it while no slot exists, or recreate the slot and reload all");
    }

    /**
     * Step 5. The only place the slot is created, because the backfill must read the snapshot
     * that the slot's creation exports. The replication connection holding that snapshot is
     * kept open until every due table is loaded.
     */
    private static void backfillIfDue(WriterConfig config, SchemaRegistry registry,
                                      PaimonSink sink, List<RegistryRow> rows,
                                      boolean slotExists) throws Exception {
        List<RegistryRow> due = new ArrayList<>();
        for (RegistryRow r : rows) {
            if (r.backfillDue()) {
                due.add(r);
            }
        }
        BackfillAction action = decideBackfill(slotExists, due.size(), rows.size(),
                                               config.slotName());
        if (action == BackfillAction.NONE) {
            return;
        }
        if (action == BackfillAction.RECREATE_SLOT_AND_BACKFILL) {
            LOG.warning("slot '" + config.slotName() + "' exists but every table awaits its"
                + " initial backfill (an interrupted first load): dropping and recreating it");
            try (Connection conn = plainConnection(config);
                 PreparedStatement ps = conn.prepareStatement(
                     "SELECT pg_drop_replication_slot(?)")) {
                ps.setString(1, config.slotName());
                ps.execute();
            }
        }
        try (Backfiller backfiller = new Backfiller(config, registry, sink)) {
            Backfiller.BackfillPlan plan = backfiller.createSlotAndExportSnapshot();
            for (RegistryRow r : due) {
                List<String> names = new ArrayList<>();
                List<Boolean> isKey = new ArrayList<>();
                for (DdlMessage.Col c : r.cols()) {
                    names.add(c.name());
                    isKey.add(r.pk().contains(c.name()));
                }
                sink.setRelation(r.relid(),
                    new RelationMeta(r.relid(), r.schema(), r.table(), names, isKey));
                backfiller.backfill(plan, r.relid(), r.schema(), r.table(), names);
            }
        }
    }

    static Connection plainConnection(WriterConfig config) throws java.sql.SQLException {
        Properties p = new Properties();
        p.setProperty("user", config.user());
        p.setProperty("password", config.password());
        return DriverManager.getConnection(config.jdbcUrl(), p);
    }

    static boolean slotExists(Connection conn, String slotName) throws java.sql.SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    static List<RegistryRow> readRegistry(Connection conn) throws java.sql.SQLException {
        List<RegistryRow> rows = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                 "SELECT relid::bigint, schema_name, table_name, pk_cols, cols::text,"
                 + " backfill_due, changelog_producer FROM paimon.table_registry"
                 + " ORDER BY relid");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Array pk = rs.getArray(4);
                rows.add(new RegistryRow(rs.getLong(1), rs.getString(2), rs.getString(3),
                    Arrays.asList((String[]) pk.getArray()),
                    DdlMessage.parseCols(rs.getString(5)), rs.getBoolean(6), rs.getString(7)));
            }
        }
        return rows;
    }

    /** Confirms to the server only after the Paimon commit has returned. */
    static TxnBatcher.Flusher confirmingFlusher(TxnBatcher.Flusher inner, LongConsumer confirm) {
        return (records, commitLsn, endLsn) -> {
            inner.flush(records, commitLsn, endLsn);
            confirm.accept(endLsn);     // only after the Paimon commit returned
        };
    }

    /** The relid of an Insert, Update or Delete; -1 for any other event. */
    static long relidOf(PgEvent e) {
        if (e instanceof PgEvent.Insert i) return i.relid();
        if (e instanceof PgEvent.Update u) return u.relid();
        if (e instanceof PgEvent.Delete d) return d.relid();
        return -1L;
    }

    /** Routes decoded events to the sink, the DDL handler and the batcher. */
    static final class PipelineHandler implements SlotConsumer.Handler {

        interface Target {
            void setRelation(long relid, RelationMeta meta);
            void truncate(long relid, long commitLsn) throws Exception;
        }

        private final LongPredicate tracked;
        private final Consumer<PgEvent.Message> ddl;
        private final Target target;
        private final TxnBatcher batcher;
        private final Map<Long, RelationMeta> relations = new HashMap<>();
        private final Set<Long> pendingTruncates = new LinkedHashSet<>();

        PipelineHandler(LongPredicate tracked, Consumer<PgEvent.Message> ddl, Target target,
                        TxnBatcher batcher) {
            this.tracked = tracked;
            this.ddl = ddl;
            this.target = target;
            this.batcher = batcher;
        }

        @Override
        public void onEvent(PgEvent event, long changeLsn) throws Exception {
            if (event instanceof PgEvent.Relation r) {
                target.setRelation(r.meta().relid(), r.meta());
                relations.put(r.meta().relid(), r.meta());
            } else if (event instanceof PgEvent.Message m) {
                ddl.accept(m);
            } else if (event instanceof PgEvent.Truncate t) {
                // Buffered, not applied: sink.truncate needs this transaction's commit LSN as
                // its commitIdentifier and that only arrives with the Commit. The batcher still
                // sees the event for its truncate-versus-DML conflict check.
                for (long relid : t.relids()) {
                    if (tracked.test(relid)) {
                        pendingTruncates.add(relid);
                    }
                }
            } else if (event instanceof PgEvent.Commit c) {
                // Before the batcher flushes (and so confirms): were the slot confirmed first,
                // a crash here would lose the truncate. A replay is filtered by commit LSN.
                for (long relid : pendingTruncates) {
                    target.truncate(relid, c.commitLsn());
                }
                pendingTruncates.clear();
            }
            batcher.onEvent(event, changeLsn, relations.get(relidOf(event)));
        }

        @Override
        public void onDisconnect() {
            batcher.discardOpenTransaction();
            pendingTruncates.clear();   // that transaction never committed
        }
    }
}
