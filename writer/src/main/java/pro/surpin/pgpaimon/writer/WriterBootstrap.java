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
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;
import pro.surpin.pgpaimon.writer.dto.BackfillPlan;
import pro.surpin.pgpaimon.writer.dto.DdlMessage;
import pro.surpin.pgpaimon.writer.dto.PgEvent;
import pro.surpin.pgpaimon.writer.dto.RegistryRow;
import pro.surpin.pgpaimon.writer.dto.RelationMeta;

/** Restores the writer state and preserves the replication pipeline ordering. */
public class WriterBootstrap {

  private static final @NotNull Logger LOG = Logger.getLogger(WriterBootstrap.class.getName());

  private final @NotNull WriterConfig config;
  private final @NotNull SchemaRegistry registry;
  private final @NotNull PaimonSink sink;
  private final @NotNull OffsetStore offsets;

  public WriterBootstrap(
      @NotNull WriterConfig config,
      @NotNull SchemaRegistry registry,
      @NotNull PaimonSink sink,
      @NotNull OffsetStore offsets
  ) {
    this.config = config;
    this.registry = registry;
    this.sink = sink;
    this.offsets = offsets;
  }

  /** Restore the registry and complete the initial load before reading the stream. */
  public long initialize() throws Exception {
    boolean slotExists;
    List<RegistryRow> rows;
    try (Connection conn = plainConnection(config)) {
      // Check whether the slot exists. Bootstrap never creates it directly: the initial backfill
      // must
      // read the snapshot exported by the slot's own creation (Backfiller, Task 13).
      slotExists = slotExists(conn, config.slotName());
      // 3. rebuild the registry from paimon.table_registry BEFORE the stream opens. A
      // 'create' message is emitted once and consumed once; without this a restarted
      // writer knows no tables and silently discards every replicated row.
      rows = readRegistry(conn);
    }
    // 3a. the relid to Paimon identity mapping only. No catalog call, no warehouse write:
    // step 3b may refuse to start, and "the writer refused to start, so nothing
    // happened" has to be TRUE. It was not: the bootstrap used to create tables and add
    // reconciled columns first and throw afterwards, so a failed start had already
    // bumped schema versions in the warehouse.
    for (RegistryRow r : rows) {
      registry.register(r.relid(), r.table());
    }

    // 3b. decide what startup will do, which is where a refusal belongs.
    List<RegistryRow> due = new ArrayList<>();
    List<String> stranded = new ArrayList<>();
    for (RegistryRow r : rows) {
      switch (classify(r.backfillDue(), offsets.hasCommitted(r.relid()), slotExists)) {
        case DUE -> due.add(r);
        case STRANDED -> stranded.add(r.schema() + "." + r.table());
        case STREAMING -> {
        }
      }
    }
    BackfillAction action =
        decideBackfill(slotExists, due.size(), rows.size(), stranded, config.slotName());

    // 3c. only now does anything in the warehouse change.
    for (RegistryRow r : rows) {
      // reconcileLostDdl only when the slot is absent: see SchemaRegistry.bootstrapTable.
      registry.bootstrapTable(r.relid(), r.schema(), r.table(), r.pk(), r.cols(),
          r.changelogProducer(), !slotExists);
    }
    LOG.info("registry rebuilt: " + rows.size() + " table(s), slot "
        + (slotExists ? "exists" : "absent"));

    // Collect the tracked relations for the resume position after backfill.
    List<Long> relids = new ArrayList<>();
    for (RegistryRow r : rows) {
      relids.add(r.relid());
    }
    // Initial backfill moves the committed offsets, so read the resume position afterward.
    runBackfill(config, registry, sink, rows, due, action);
    long resume = offsets.highestCommittedLsn(relids);

    warnOnStaleSkipLsn(config.skipLsn(), resume);

    return resume;
  }

  /**
   * A {@code PGPAIMON_SKIP_LSN} at or below where the stream resumes can never fire: that record is
   * behind us. Said out loud so a setting left behind after a successful skip is visible, rather
   * than sitting armed and forgotten. Not a refusal to start -- the setting is harmless, and
   * refusing would turn a stale variable into an outage.
   */
  static void warnOnStaleSkipLsn(
      long skipLsn,
      long resume
  ) {
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
        + " fails because a value has no Paimon representation. Every other failure there,"
        + " and every other LSN, stays fatal. Unset it once the skip has happened.");
  }

  /** What step 5 has to do, decided from the slot and the registry alone. */
  enum BackfillAction {
    NONE,
    CREATE_SLOT_AND_BACKFILL,
    RECREATE_SLOT_AND_BACKFILL
  }

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
   * The three-line truth table that decides it. Two flags, and they mean different things: {@code
   * backfillDue} is "the table held rows at add_table() that an initial load must copy", {@code
   * hasCommitted} is "this writer has committed something for it to Paimon".
   *
   * <p>
   * The last line is the one that matters and the one that was wrong twice. A table that is not due
   * and has never been written is a table <em>registered while empty</em>.
   *
   * <ul>
   * <li>Slot present: the stream covers it. Its {@code create} message travelled through the slot
   * and every row it has ever had arrives as DML.
   * <li>Slot absent: this is the first start, so <strong>load it</strong>. Treating it as merely
   * "not stranded" and skipping it lost data silently: rows inserted between {@code
   *       add_table()} and the first start are in neither the snapshot's past nor the new slot's
   * future, so they never reached Paimon and never would — on the documented order of operations
   * ("register tables, then start the writer"), with no warning. Loading it is right either way:
   * still empty, the load is a no-op; grown, the load is exactly what the documentation promises.
   * </ul>
   */
  static @NotNull RowRole classify(
      boolean backfillDue,
      boolean hasCommitted,
      boolean slotExists
  ) {
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
   *
   * <ul>
   * <li>slot present, nothing due: stream.
   * <li>slot absent: no tracked table may already hold data this writer committed. Such a table's
   * stream position was the lost slot and a new slot cannot reconcile it, so this is a refusal, not
   * a guess. {@code stranded} names those tables.
   * <li>slot present, every table due: no table has ever streamed (a table leaves the due state
   * only after its load, and the stream starts after all loads), so the slot holds nothing of
   * value. An interrupted first load lands here and its exported snapshot is gone, so the slot is
   * dropped and made again.
   * <li>slot present, some due and some not: a table added to a running system. Its snapshot cannot
   * be made consistent with the existing slot.
   * </ul>
   *
   * <p>
   * Note what the slot-absent test is NOT: it is not {@code dueCount != totalCount}. {@code
   * backfill_due} means "the table held rows at registration that the initial load must copy", not
   * "the table has never been loaded". A table registered while empty is therefore not due although
   * nothing has ever been written for it, and keying the refusal on that flag made the documented
   * first start impossible, with an error whose own remedy (remove_table + add_table on an empty
   * table) looped. "Has this system ever run for this table" is answered by Paimon's own snapshots,
   * through {@link OffsetStore#hasCommitted(long)}; the caller passes the result as {@code
   * stranded}.
   */
  static @NotNull BackfillAction decideBackfill(
      boolean slotExists,
      int dueCount,
      int totalCount,
      @NotNull List<String> stranded,
      @NotNull String slotName
  ) {
    if (!slotExists) {
      if (!stranded.isEmpty()) {
        throw new IllegalStateException("slot '" + slotName + "' does not exist but "
            + stranded.size() + " tracked table(s) already hold data this writer" + " committed "
            + stranded + "; their stream position is lost and a new slot"
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
    throw new IllegalStateException("slot '" + slotName + "' exists and " + dueCount + " of "
        + totalCount + " tracked table(s) are awaiting their initial backfill:"
        + " a table added while the writer's slot exists cannot be loaded consistently."
        + " To reload everything: stop the writer, then run" + " SELECT pg_drop_replication_slot('"
        + slotName + "');" + " UPDATE paimon.table_registry SET backfill_due = true;"
        + " and start the writer again (it creates the slot, truncates and reloads every"
        + " table)");
  }

  /**
   * Step 5. The only place the slot is created, because the backfill must read the snapshot that
   * the slot's creation exports. The replication connection holding that snapshot is kept open
   * until every due table is loaded.
   */
  private static void runBackfill(
      @NotNull WriterConfig config,
      @NotNull SchemaRegistry registry,
      @NotNull PaimonSink sink,
      @NotNull List<RegistryRow> rows,
      @NotNull List<RegistryRow> due,
      @NotNull BackfillAction action
  ) throws Exception {
    if (action == BackfillAction.NONE) {
      return;
    }
    if (action == BackfillAction.RECREATE_SLOT_AND_BACKFILL) {
      LOG.warning("slot '" + config.slotName() + "' exists but every table awaits its"
          + " initial backfill (an interrupted first load): dropping and recreating it");
      try (Connection conn = plainConnection(config);
          PreparedStatement ps = conn.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
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
            r.changelogProducer(), true);
      }
    }
    try (Backfiller backfiller = new Backfiller(config, registry, sink)) {
      BackfillPlan plan = backfiller.createSlotAndExportSnapshot();
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

  static @NotNull Connection plainConnection(
      @NotNull WriterConfig config
  ) throws java.sql.SQLException {
    Properties p = new Properties();
    p.setProperty("user", config.user());
    p.setProperty("password", config.password());
    return DriverManager.getConnection(config.jdbcUrl(), p);
  }

  static boolean slotExists(
      @NotNull Connection conn,
      @NotNull String slotName
  ) throws java.sql.SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
      ps.setString(1, slotName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  static @NotNull List<RegistryRow> readRegistry(
      @NotNull Connection conn
  ) throws java.sql.SQLException {
    List<RegistryRow> rows = new ArrayList<>();
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT relid::bigint, schema_name, table_name, pk_cols, cols::text,"
            + " backfill_due, changelog_producer FROM paimon.table_registry" + " ORDER BY relid");
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        Array pk = rs.getArray(4);
        rows.add(new RegistryRow(rs.getLong(1), rs.getString(2), rs.getString(3),
            Arrays.asList((String[]) pk.getArray()), DdlMessage.parseCols(rs.getString(5)),
            rs.getBoolean(6), rs.getString(7)));
      }
    }
    return rows;
  }

  /** Confirms to the server only after the Paimon commit has returned. */
  static @NotNull TxnBatcher.Flusher confirmingFlusher(
      @NotNull TxnBatcher.Flusher inner,
      @NotNull LongConsumer confirm
  ) {
    return (
        records,
        commitLsn,
        endLsn) -> {
      inner.flush(records, commitLsn, endLsn);
      confirm.accept(endLsn); // only after the Paimon commit returned
    };
  }

  /** The relid of an Insert, Update or Delete; -1 for any other event. */
  static long relidOf(
      @NotNull PgEvent e
  ) {
    if (e instanceof PgEvent.Insert i)
      return i.relid();
    if (e instanceof PgEvent.Update u)
      return u.relid();
    if (e instanceof PgEvent.Delete d)
      return d.relid();
    return -1L;
  }

  /** Routes decoded events to the sink, the DDL handler and the batcher. */
  static final class PipelineHandler implements SlotConsumer.Handler {

    interface Target {
      void setRelation(
          long relid,
          @NotNull RelationMeta meta
      );

      void truncate(
          long relid,
          long commitLsn
      ) throws Exception;
    }

    private final @NotNull LongPredicate tracked;
    private final @NotNull LongFunction<List<String>> primaryKey;
    private final @NotNull Consumer<PgEvent.Message> ddl;
    private final @NotNull Target target;
    private final @NotNull TxnBatcher batcher;
    private final @NotNull Map<Long, RelationMeta> relations = new HashMap<>();
    private final @NotNull Set<Long> pendingTruncates = new LinkedHashSet<>();

    PipelineHandler(
        @NotNull LongPredicate tracked,
        @NotNull LongFunction<List<String>> primaryKey,
        @NotNull Consumer<PgEvent.Message> ddl,
        @NotNull Target target,
        @NotNull TxnBatcher batcher
    ) {
      this.tracked = tracked;
      this.primaryKey = primaryKey;
      this.ddl = ddl;
      this.target = target;
      this.batcher = batcher;
    }

    @Override
    public void onEvent(
        @NotNull PgEvent event,
        long changeLsn
    ) throws Exception {
      if (event instanceof PgEvent.Relation r) {
        RelationMeta meta = withRegistryKey(r.meta());
        target.setRelation(meta.relid(), meta);
        relations.put(meta.relid(), meta);
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
          target.truncate(relid, c.commitLsn());
        }
        pendingTruncates.clear();
      }
      batcher.onEvent(event, changeLsn, relations.get(relidOf(event)));
    }

    /**
     * Under REPLICA IDENTITY FULL pgoutput marks every column as a key column, which would make
     * every UPDATE look like a key change (-D/+I instead of -U/+U). The key is the registry's
     * primary key. An untracked relation is passed through unchanged: its events are skipped
     * anyway.
     */
    private @NotNull RelationMeta withRegistryKey(
        @NotNull RelationMeta m
    ) {
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
    public void onDisconnect() {
      batcher.discardOpenTransaction();
      pendingTruncates.clear(); // that transaction never committed
    }
  }
}
