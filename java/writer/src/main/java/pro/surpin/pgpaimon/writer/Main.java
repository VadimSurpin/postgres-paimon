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
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
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
                       String changelogProducer, int buckets, String histTable,
                       String registration, Integer histBuckets, boolean histBucketsUser) {
        RegistryRow(long relid, String schema, String table, List<String> pk,
                    List<DdlMessage.Col> cols, boolean backfillDue,
                    String changelogProducer, int buckets) {
            this(relid, schema, table, pk, cols, backfillDue, changelogProducer, buckets, null,
                 null, null, false);
        }
        SchemaRegistry.HistSpec hist() {
            return new SchemaRegistry.HistSpec(histTable, registration, histBuckets,
                                               histBucketsUser);
        }
    }

    private static volatile boolean exiting;

    public static void main(String[] args) throws Exception {
        // 1. configuration (fails with "PGPAIMON_JDBC_URL is required" and so on)
        WriterConfig config = WriterConfig.fromEnv(System.getenv());

        Catalog catalog = CatalogFactory.createCatalog(
            CatalogContext.create(Options.fromMap(config.catalogOptions())));
        SchemaRegistry registry = new SchemaRegistry(catalog, config);

        boolean slotExists;
        List<RegistryRow> rows;
        String token;
        try (Connection conn = plainConnection(config)) {
            // 1a. the guardrails are still in force: refuse to start if any was bypassed
            //     (HealthCheck says why) -- before anything is read or written.
            // 1b. the credential every DDL message carries (DdlMessageHandler says why).
            token = preflight(conn);
            // 2. CHECK that the slot exists. Main never creates it: the initial backfill must
            //    read the snapshot exported by the slot's own creation (Backfiller, Task 13).
            slotExists = slotExists(conn, config.slotName());
            // 3. rebuild the registry from paimon.table_registry BEFORE the stream opens. A
            //    'create' message is emitted once and consumed once; without this a restarted
            //    writer knows no tables and silently discards every replicated row.
            rows = readRegistry(conn);
        } catch (java.sql.SQLException e) {
            throw missingGrant(e);
        }
        // 3a. the relid to Paimon identity mapping only. No catalog call, no warehouse write:
        //     step 3b may refuse to start, and "the writer refused to start, so nothing
        //     happened" has to be TRUE. It was not: the bootstrap used to create tables and add
        //     reconciled columns first and throw afterwards, so a failed start had already
        //     bumped schema versions in the warehouse.
        for (RegistryRow r : rows) {
            registry.register(r.relid(), r.schema(), r.table());
        }

        // 3b. decide what startup will do, which is where a refusal belongs.
        OffsetStore offsets = new OffsetStore(registry, config);
        List<RegistryRow> due = new ArrayList<>();
        List<String> stranded = new ArrayList<>();
        for (RegistryRow r : rows) {
            switch (classify(r.backfillDue(), offsets.hasCommitted(r.relid()), slotExists)) {
                case DUE -> due.add(r);
                case STRANDED -> stranded.add(r.schema() + "." + r.table());
                case STREAMING -> { }
            }
        }
        BackfillAction action = decideBackfill(slotExists, due.size(), rows.size(), stranded,
                                               config.slotName());

        // 3c. only now does anything in the warehouse change.
        for (RegistryRow r : rows) {
            // reconcileLostDdl only when the slot is absent: see SchemaRegistry.bootstrapTable.
            registry.bootstrapTable(r.relid(), r.schema(), r.table(), r.pk(), r.cols(),
                                    r.changelogProducer(), r.buckets(), !slotExists,
                                    r.hist());
        }
        LOG.info("registry rebuilt: " + rows.size() + " table(s), slot "
                 + (slotExists ? "exists" : "absent"));

        // 4. where to resume; 0 means "the slot's own position", an ordinary value. The MIN is
        //    over every Paimon table the writer commits to: each main table AND each history
        //    table, since the two are committed separately and a crash can leave either behind.
        List<Long> relids = resumeKeys(rows);
        long resume = new OffsetStore(registry, config).highestCommittedLsn(relids);

        PaimonSink sink = new PaimonSink(registry, config);

        // 5. initial backfill (and slot creation) for anything due. A backfill moves the
        //    committed offsets (each loaded table now stands at the slot's consistent point), so
        //    resume is read again afterwards.
        runBackfill(config, registry, sink, rows, due, action);
        resume = new OffsetStore(registry, config).highestCommittedLsn(relids);

        warnOnStaleSkipLsn(config.skipLsn(), resume);

        // 6. the pipeline
        PgOutputDecoder decoder = new PgOutputDecoder();
        SlotConsumer[] consumerRef = new SlotConsumer[1];
        // TxnBatcher -> MiniBatcher -> confirming flusher -> PaimonSink: a transaction is complete
        // at TxnBatcher, several are committed together at MiniBatcher, and the slot is confirmed
        // only after that commit returns.
        OffsetStore liveOffsets = new OffsetStore(registry, config);
        MiniBatcher.Limits limits = new MiniBatcher.Limits(config.batchMaxRows(),
            config.batchMaxBytes(), config.batchMaxIntervalMs());
        MiniBatcher mini = new MiniBatcher(
            confirmingFlusher(sink, lsn -> consumerRef[0].confirm(lsn)),
            liveOffsets::highestCommitted, limits);
        LOG.info("commit batching: up to " + limits.maxRows() + " change records, "
            + limits.maxBytes() + " estimated bytes or " + limits.maxIntervalMs()
            + " ms since the batch's first transaction, whichever comes first (PGPAIMON_BATCH_*)");
        TxnBatcher batcher = new TxnBatcher(new ToastResolver(), registry, mini,
            config.skipLsn(),
            (record, meta) -> registry.rowBuilder(record.relid(), meta).build(record));
        DdlMessageHandler ddl = new DdlMessageHandler(registry, config.messagePrefix(), token,
            config.skipLsn(), () -> {
                try (Connection conn = plainConnection(config)) {
                    return readToken(conn);
                } catch (java.sql.SQLException e) {
                    throw new IllegalStateException(e);
                }
            });
        HealthCheck health = new HealthCheck(() -> plainConnection(config), config.healthIntervalMs());
        SlotConsumer.Handler handler = new PipelineHandler(registry::tracked, registry::primaryKey, ddl::handle, ddl::isDdl,
            new PipelineHandler.Target() {
                @Override public void setRelation(long relid, RelationMeta meta) {
                    sink.setRelation(relid, meta);
                }
                @Override public void truncate(long relid, long commitLsn) throws Exception {
                    sink.truncate(relid, commitLsn);
                }
                @Override public boolean overwrites(long relid) {
                    // A key-less table's main table is an operation log: a TRUNCATE is a T row
                    // in it (TxnBatcher), never an overwrite. The log is an audit trail.
                    return !registry.keyless(relid);
                }
            }, batcher, mini, health::maybeCheck);
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

    /**
     * The tables whose committed identifiers decide where the stream resumes (the MIN over them):
     * each tracked relation's main table AND its current history table. The two are committed
     * separately, so a crash can leave either behind, and leaving the history out lost changes
     * for good: measured (review of the history tables, I2), five one-row transactions committed
     * in one batch, a halt right after the main table's commit, and after the restart the history
     * held only the last of them. Archives are not included: nothing is written to an archive
     * after its rotation, and the writer commits a flush's archives first (PaimonSink).
     */
    static List<Long> resumeKeys(List<RegistryRow> rows) {
        List<Long> keys = new ArrayList<>();
        for (RegistryRow r : rows) {
            keys.add(r.relid());
            keys.add(HistoryKeys.histKey(r.relid()));
        }
        return keys;
    }

    /**
     * A {@code PGPAIMON_SKIP_LSN} at or below where the stream resumes can never fire: that
     * record is behind us. Said out loud so a setting left behind after a successful skip is
     * visible, rather than sitting armed and forgotten. Not a refusal to start -- the setting is
     * harmless, and refusing would turn a stale variable into an outage.
     */
    static void warnOnStaleSkipLsn(long skipLsn, long resume) {
        if (skipLsn == 0) {
            return;
        }
        if (resume != 0 && Long.compareUnsigned(skipLsn, resume) <= 0) {
            LOG.warning("PGPAIMON_SKIP_LSN=" + SlotConsumer.formatLsn(skipLsn)
                + " is at or below the resume position " + SlotConsumer.formatLsn(resume)
                + ", so it can never fire: that record is already behind the stream. If its skip"
                + " has already happened, unset the variable; if it has not, the LSN is wrong.");
            return;
        }
        LOG.warning("PGPAIMON_SKIP_LSN=" + SlotConsumer.formatLsn(skipLsn) + " is armed: the ONE"
            + " record at that change LSN will be dropped, and logged in full, if and only if it"
            + " fails because a value has no Paimon representation; or the ONE DDL message at that"
            + " LSN, if and only if it is rejected for its credential. Every other failure there,"
            + " and every other LSN, stays fatal. Unset it once the skip has happened.");
    }

    /** What step 5 has to do, decided from the slot and the registry alone. */
    enum BackfillAction { NONE, CREATE_SLOT_AND_BACKFILL, RECREATE_SLOT_AND_BACKFILL }

    /** What one {@code paimon.table_registry} row means for startup. */
    enum RowRole {
        /** Load it from the slot's exported snapshot. */
        DUE,
        /** It holds data we wrote whose stream position is the lost slot: refuse. */
        STRANDED,
        /** Nothing to do: the stream covers it. */
        STREAMING
    }

    /**
     * The three-line truth table that decides it. Two flags, and they mean different things:
     * {@code backfillDue} is "the table held rows at add_table() that an initial load must copy",
     * {@code hasCommitted} is "this writer has committed something for it to Paimon".
     *
     * <p>The last line is the one that matters and the one that was wrong twice. A table that is
     * not due and has never been written is a table <em>registered while empty</em>.
     * <ul>
     *   <li>Slot present: the stream covers it. Its {@code create} message travelled through the
     *       slot and every row it has ever had arrives as DML.</li>
     *   <li>Slot absent: this is the first start, so <strong>load it</strong>. Treating it as
     *       merely "not stranded" and skipping it lost data silently: rows inserted between
     *       {@code add_table()} and the first start are in neither the snapshot's past nor the new
     *       slot's future, so they never reached Paimon and never would — on the documented order
     *       of operations ("register tables, then start the writer"), with no warning. Loading it
     *       is right either way: still empty, the load is a no-op; grown, the load is exactly what
     *       the documentation promises.</li>
     * </ul>
     */
    static RowRole classify(boolean backfillDue, boolean hasCommitted, boolean slotExists) {
        if (backfillDue) {
            return RowRole.DUE;
        }
        if (hasCommitted) {
            return RowRole.STRANDED;
        }
        return slotExists ? RowRole.STREAMING : RowRole.DUE;
    }

    /**
     * The slot and the due tables determine what is safe.
     * <ul>
     *   <li>slot present, nothing due: stream.</li>
     *   <li>slot absent: no tracked table may already hold data this writer committed. Such a
     *       table's stream position was the lost slot and a new slot cannot reconcile it, so
     *       this is a refusal, not a guess. {@code stranded} names those tables.</li>
     *   <li>slot present, every table due: no table has ever streamed (a table leaves the due
     *       state only after its load, and the stream starts after all loads), so the slot
     *       holds nothing of value. An interrupted first load lands here and its exported
     *       snapshot is gone, so the slot is dropped and made again.</li>
     *   <li>slot present, some due and some not: a table added to a running system. Its
     *       snapshot cannot be made consistent with the existing slot.</li>
     * </ul>
     *
     * <p>Note what the slot-absent test is NOT: it is not {@code dueCount != totalCount}.
     * {@code backfill_due} means "the table held rows at registration that the initial load
     * must copy", not "the table has never been loaded". A table registered while empty is
     * therefore not due although nothing has ever been written for it, and keying the refusal
     * on that flag made the documented first start impossible, with an error whose own remedy
     * (remove_table + add_table on an empty table) looped. "Has this system ever run for this
     * table" is answered by Paimon's own snapshots, through
     * {@link OffsetStore#hasCommitted(long)}; the caller passes the result as {@code stranded}.
     */
    static BackfillAction decideBackfill(boolean slotExists, int dueCount, int totalCount,
                                         List<String> stranded, String slotName) {
        if (!slotExists) {
            if (!stranded.isEmpty()) {
                throw new IllegalStateException("slot '" + slotName + "' does not exist but "
                    + stranded.size() + " tracked table(s) already hold data this writer"
                    + " committed " + stranded + "; their stream position is lost and a new slot"
                    + " cannot reconcile them. Re-bootstrap: UPDATE paimon.table_registry SET"
                    + " backfill_due = true; then start the writer again (it creates a slot and"
                    + " reloads every tracked table from a fresh snapshot, losing Paimon-side"
                    + " changelog history)");
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
            + " To reload everything: stop the writer, then run"
            + " SELECT pg_drop_replication_slot('" + slotName + "');"
            + " UPDATE paimon.table_registry SET backfill_due = true;"
            + " and start the writer again (it creates the slot, truncates and reloads every"
            + " table)");
    }

    /**
     * Step 5. The only place the slot is created, because the backfill must read the snapshot
     * that the slot's creation exports. The replication connection holding that snapshot is
     * kept open until every due table is loaded.
     */
    private static void runBackfill(WriterConfig config, SchemaRegistry registry,
                                    PaimonSink sink, List<RegistryRow> rows,
                                    List<RegistryRow> due, BackfillAction action)
            throws Exception {
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
            // The slot is gone NOW, so anything that was still in it is LOST, not pending --
            // and the bootstrap above ran while it existed, so it declined to reconcile any
            // missing column and deferred to a DDL message that this drop has just destroyed.
            // Re-running it with the slot genuinely absent is what keeps that deferral honest.
            // Without this the Paimon schema stayed permanently behind the registry while
            // PostgreSQL held the column's value, exit 0, two misleading warnings: whole-branch
            // finding 4, reopened on this one path.
            //
            // Before the load, deliberately: the reloaded rows must carry the added column.
            LOG.info("re-running the registry bootstrap now the slot is gone: a DDL message that"
                + " was pending in it is lost, so a column missing from Paimon is reconciled");
            for (RegistryRow r : rows) {
                registry.bootstrapTable(r.relid(), r.schema(), r.table(), r.pk(), r.cols(),
                                        r.changelogProducer(), r.buckets(), true,
                                        r.hist());
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

    /**
     * A plain (non-replication) connection, with {@code search_path} pinned to
     * {@code pg_catalog, pg_temp}. Every name the writer uses is schema-qualified, and the writer
     * usually runs as a superuser: with the default path, an operator or function planted in
     * {@code public} by any role with CREATE there could win resolution inside the writer's own
     * queries, which it runs unattended (the periodic {@code paimon.health()}). The extension's
     * functions pin their own path as well (paimon_slot--1.0.sql, top).
     */
    static Connection plainConnection(WriterConfig config) throws java.sql.SQLException {
        return DriverManager.getConnection(config.jdbcUrl(), plainProperties(config));
    }

    static Properties plainProperties(WriterConfig config) {
        Properties p = new Properties();
        p.setProperty("user", config.user());
        p.setProperty("password", config.password());
        p.setProperty("options", "-c search_path=pg_catalog,pg_temp");
        return p;
    }

    /**
     * Steps 1a and 1b: refuses to start unless {@code paimon.health()} reports nothing, then
     * returns the DDL message credential. In this order, deliberately: nothing is read from the
     * registry or written anywhere while a guardrail is known to be bypassed.
     */
    static String preflight(Connection conn) throws java.sql.SQLException {
        List<String> problems;
        try {
            problems = HealthCheck.problemsAtStartup(conn);
        } catch (java.sql.SQLException e) {
            if (HealthCheck.transientFailure(e) || "42501".equals(e.getSQLState())) {
                throw e;    // not reached, or a grant: missingGrant() names them all
            }
            throw HealthCheck.checkFailed(e, "at startup");
        }
        HealthCheck.requireHealthy(problems, "at startup");
        return readToken(conn);
    }

    /** What the writer's role needs when it is not a superuser (docs, section 2). */
    static final String WRITER_GRANTS = "GRANT USAGE ON SCHEMA paimon TO <role>;"
        + " GRANT SELECT ON paimon.secret, paimon.retained_history TO <role>;"
        + " GRANT SELECT, UPDATE ON paimon.table_registry TO <role>;"
        + " and SELECT on every replicated table, besides the REPLICATION attribute";

    /**
     * A permission error at startup names every grant the writer needs, not only the object the
     * first failing query happened to touch (it used to be a raw "permission denied for schema
     * paimon" from the health check).
     */
    static RuntimeException missingGrant(java.sql.SQLException e) {
        if ("42501".equals(e.getSQLState())) {
            return new IllegalStateException("the writer's role lacks a privilege it needs ("
                + e.getMessage() + "). A writer that does not connect as a superuser needs: "
                + WRITER_GRANTS, e);
        }
        return new IllegalStateException("startup query failed: " + e.getMessage(), e);
    }

    /** The installation's DDL message credential; the writer's role must be able to read it. */
    static String readToken(Connection conn) throws java.sql.SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT token FROM paimon.secret");
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException("paimon.secret is empty: the extension's DDL"
                    + " message credential is missing; recreate the extension");
            }
            return rs.getString(1);
        } catch (java.sql.SQLException e) {
            if ("42501".equals(e.getSQLState())) {
                throw new IllegalStateException("the writer's role cannot read paimon.secret, the"
                    + " credential every DDL message carries. A writer that does not connect as a"
                    + " superuser needs: " + WRITER_GRANTS, e);
            }
            throw e;
        }
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
                 + " backfill_due, changelog_producer, buckets, hist_table, registration::text,"
                 + " hist_buckets, hist_buckets_user IS NOT NULL"
                 + " FROM paimon.table_registry ORDER BY relid");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Array pk = rs.getArray(4);
                rows.add(new RegistryRow(rs.getLong(1), rs.getString(2), rs.getString(3),
                    Arrays.asList((String[]) pk.getArray()),
                    DdlMessage.parseCols(rs.getString(5)), rs.getBoolean(6), rs.getString(7),
                    rs.getInt(8), rs.getString(9), rs.getString(10), rs.getInt(11),
                    rs.getBoolean(12)));
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

            /** Whether a TRUNCATE empties the relation's main table (false: an operation log). */
            default boolean overwrites(long relid) {
                return true;
            }
        }

        private final LongPredicate tracked;
        private final LongFunction<List<String>> primaryKey;
        private final Consumer<PgEvent.Message> ddl;
        private final Predicate<PgEvent.Message> isDdl;
        private final Target target;
        private final TxnBatcher batcher;
        private final MiniBatcher mini;
        private final Map<Long, RelationMeta> relations = new HashMap<>();
        private final Set<Long> pendingTruncates = new LinkedHashSet<>();
        private final Runnable periodic;

        PipelineHandler(LongPredicate tracked, LongFunction<List<String>> primaryKey,
                        Consumer<PgEvent.Message> ddl, Predicate<PgEvent.Message> isDdl,
                        Target target, TxnBatcher batcher, MiniBatcher mini) {
            this(tracked, primaryKey, ddl, isDdl, target, batcher, mini, () -> { });
        }

        /** {@code periodic} runs on every event and idle tick (the health check). */
        PipelineHandler(LongPredicate tracked, LongFunction<List<String>> primaryKey,
                        Consumer<PgEvent.Message> ddl, Predicate<PgEvent.Message> isDdl,
                        Target target, TxnBatcher batcher, MiniBatcher mini, Runnable periodic) {
            this.periodic = periodic;
            this.tracked = tracked;
            this.primaryKey = primaryKey;
            this.ddl = ddl;
            this.isDdl = isDdl;
            this.target = target;
            this.batcher = batcher;
            this.mini = mini;
        }

        @Override
        public void onEvent(PgEvent event, long changeLsn) throws Exception {
            mini.tick();    // the time limit also fires while events are flowing
            periodic.run();
            if (event instanceof PgEvent.Relation r) {
                RelationMeta meta = withRegistryKey(r.meta());
                RelationMeta before = relations.get(meta.relid());
                if (before != null && !before.equals(meta)) {
                    // Buffered records were shaped by the old description; the sink builds them
                    // at flush time from whatever it holds then.
                    mini.flushBuffered();
                }
                target.setRelation(meta.relid(), meta);
                relations.put(meta.relid(), meta);
            } else if (event instanceof PgEvent.Message m) {
                if (isDdl.test(m)) {
                    // A batch never spans a schema change: records are built against the schema
                    // current at flush time. Commit and confirm what is buffered, then change it.
                    mini.flushBuffered();
                    ddl.accept(m);
                    // A relation id may now name a different Paimon table.
                    mini.forgetCommitted();
                } else {
                    ddl.accept(m);
                }
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
                //
                // The buffer is flushed first, so the overwrite lands after every earlier append
                // and before every later one; the rest of this transaction then starts a new
                // buffer. Without that, an earlier transaction's rows would be committed after
                // the truncate that is meant to remove them.
                boolean barrier = false;
                for (long relid : pendingTruncates) {
                    // BEGIN; TRUNCATE t; DROP TABLE t; COMMIT: the drop_table message arrives
                    // between the Truncate and the Commit and untracks the relation, so there
                    // is no Paimon table left to empty. Truncating a table that is about to be
                    // dropped is pointless anyway.
                    if (!tracked.test(relid)) {
                        LOG.info("skipping the buffered truncate of relation " + relid
                            + ": it stopped being tracked before this transaction committed"
                            + " (dropped in the same transaction)");
                        continue;
                    }
                    if (!barrier) {
                        mini.flushBuffered();
                        barrier = true;
                    }
                    if (!target.overwrites(relid)) {
                        continue;   // an operation log: the T row travels with the records
                    }
                    // Under commitLsn - 1, not commitLsn: the records this transaction made
                    // AFTER the truncate (TxnBatcher dropped the ones before it) are committed
                    // next, under commitLsn, and the per-table rule-4 filter drops a record whose
                    // transaction's commit LSN is at or below the table's committed identifier.
                    // One below is still above every earlier transaction (each commit record
                    // has its own, lower, LSN), and a replay converges: the overwrite is
                    // filtered at commitLsn - 1, and the records only once commitLsn is in.
                    target.truncate(relid, c.commitLsn() - 1);
                    mini.noteCommitted(relid, c.commitLsn() - 1);
                }
                pendingTruncates.clear();
            }
            batcher.onEvent(event, changeLsn, relations.get(relidOf(event)));
        }

        /**
         * Under REPLICA IDENTITY FULL pgoutput marks every column as a key column, which would
         * make every UPDATE look like a key change (-D/+I instead of -U/+U). The key is the
         * registry's primary key. An untracked relation is passed through unchanged: its
         * events are skipped anyway.
         */
        private RelationMeta withRegistryKey(RelationMeta m) {
            if (!tracked.test(m.relid())) {
                return m;
            }
            Set<String> pk = new java.util.HashSet<>(primaryKey.apply(m.relid()));
            List<Boolean> isKey = new ArrayList<>(m.columnNames().size());
            for (String c : m.columnNames()) {
                isKey.add(pk.contains(c));
            }
            return new RelationMeta(m.relid(), m.namespace(), m.relName(), m.columnNames(), isKey);
        }

        @Override
        public void onIdle() throws Exception {
            mini.tick();
            periodic.run();
        }

        @Override
        public void onShutdown() throws Exception {
            mini.drain();
        }

        @Override
        public boolean hasUnconfirmed() {
            return mini.hasBuffered();
        }

        /**
         * The keepalive guard's fallback (see {@link SlotConsumer#guardBeforeNextRead}): after a
         * Relation message, which carries LSN 0, the next read could auto-advance the flushed
         * position past the buffer, so the buffered transactions are committed and confirmed now.
         * The open transaction is not touched: it is incomplete, and an auto-advance cannot pass
         * its commit record.
         */
        @Override
        public void commitUnconfirmed() throws Exception {
            if (mini.hasBuffered()) {
                LOG.fine("keepalive guard: committing " + mini.bufferedRows()
                    + " buffered record(s) before an auto-advance could confirm past them");
                mini.flushBuffered();
            }
        }

        @Override
        public void onDisconnect() {
            batcher.discardOpenTransaction();
            pendingTruncates.clear();   // that transaction never committed
            mini.discard();             // complete but unconfirmed: replays from the last confirmed LSN
        }
    }
}
