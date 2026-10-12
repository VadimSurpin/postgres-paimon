// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeJsonParser;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.DdlMessage;
import pro.surpin.pgpaimon.writer.dto.RelationMeta;


/**
 * The only class in the writer that talks to the Paimon {@link Catalog}. Everything else asks it
 * for a {@link FileStoreTable} or a {@link RowBuilder}.
 *
 * <p>
 * Tracking is by PostgreSQL relation oid (a {@code long}, see package-info). The mapping relid to
 * Paimon identifier is learned from {@code create} messages, which the writer re-issues for every
 * row of {@code paimon.table_registry} at startup; a relid never created in this process is
 * untracked.
 *
 * <p>
 * Every tracked relation has two Paimon tables in the same database: its main table, keyed by the
 * relid, and its history table ({@code
 *
<table>
 * _hist}), keyed by {@link HistoryKeys#histKey}. The main table is a primary-key table, or, for a
 * relation with no primary key, an append-only operation log; the history table is always
 * append-only. Every lookup below takes either key. Spec:
 * docs/superpowers/specs/2026-10-14-operation-history-design.md.
 */
public final class SchemaRegistry implements TableTracker {

  private static final @NotNull Logger LOG = Logger.getLogger(SchemaRegistry.class.getName());

  private final Catalog catalog;
  private final WriterConfig config;
  private final Map<Long, Identifier> identifiers = new ConcurrentHashMap<>();
  private final Map<Long, FileStoreTable> tables = new ConcurrentHashMap<>();
  private final Map<Long, CachedBuilder> builders = new ConcurrentHashMap<>();
  private final Set<Long> keyless = ConcurrentHashMap.newKeySet();
  /** Per relid: its current history table's identifier (the archives hang off it). */
  private final Map<Long, HistState> history = new ConcurrentHashMap<>();
  private final CrashHook hook;

  /**
   * What the writer knows about one relation's history: the current table's identifier and baseline
   * (the LSN of its last rotation, 0 if never rotated), the archives by rotation LSN, and the
   * bucket count to give a new history table (the registry's estimate; the user's
   * {@code hist_buckets} when {@code userBuckets}).
   */
  private static final class HistState {
    final Identifier current;
    volatile long baseline;
    final java.util.concurrent.ConcurrentSkipListMap<Long, Identifier> archives =
        new java.util.concurrent.ConcurrentSkipListMap<>(Long::compareUnsigned);
    volatile int buckets;
    volatile boolean userBuckets;

    HistState(
        Identifier current,
        int buckets,
        boolean userBuckets
    ) {
      this.current = current;
      this.buckets = buckets;
      this.userBuckets = userBuckets;
    }
  }

  /** How a create message (or a registry row) describes the relation's history table. */
  public record HistSpec(
      String table,
      String registration,
      Integer buckets,
      boolean userBuckets
  ) {
    static final HistSpec DEFAULT = new HistSpec(null, null, null, false);
  }

  private record CachedBuilder(
      List<String> columns,
      FileStoreTable table,
      RowMaker builder
  ) {
  }

  /**
   * Schema option on a history table: the {@code paimon.table_registry.registration} it was created
   * for. A history table is an audit trail; a different registration -- a table dropped and
   * re-created under the same name, or one registered again after remove_table() -- must not
   * continue it, and a table carrying none was not made by this writer.
   */
  static final String REGISTRATION_OPTION = "pgpaimon.registration";

  /** Schema option on a history table naming the table it records, for people reading it. */
  static final String HISTORY_OF_OPTION = "pgpaimon.history-of";

  /**
   * Schema option on a history table created by a rotation: the DDL LSN it started at. Every change
   * at or below it belongs to an archive (see {@link #historyKey}).
   */
  static final String BASELINE_OPTION = "pgpaimon.history-baseline-lsn";

  /** Options every table this writer creates carries: the cluster identity, see ClusterIdentity. */
  private Map<String, String> identityOptions = Map.of();

  /** Sets what {@link #identityOptions} every table created from now on carries. */
  public void identityOptions(
      Map<String, String> options
  ) {
    identityOptions = Map.copyOf(options);
  }

  /** {@code s} with {@link #identityOptions} added. */
  private Schema stamped(
      Schema s
  ) {
    if (identityOptions.isEmpty()) {
      return s;
    }
    Map<String, String> o = new java.util.HashMap<>(s.options());
    o.putAll(identityOptions);
    return new Schema(s.fields(), s.partitionKeys(), s.primaryKeys(), o, s.comment());
  }

  /** The options of an existing Paimon table, or null when there is no such table. */
  public Map<String, String> optionsOf(
      String schema,
      String table
  ) {
    try {
      return catalog.getTable(identifierOf(schema, table)).options();
    } catch (Catalog.TableNotExistException e) {
      return null;
    }
  }

  /**
   * Sets {@code options} on an existing Paimon table where they differ (one schema change), and
   * drops every cached table and row builder, which may now be stale. Returns whether anything
   * changed; a missing table is not an error.
   */
  public boolean stampExisting(
      String schema,
      String table,
      Map<String, String> options
  ) {
    Map<String, String> have = optionsOf(schema, table);
    if (have == null) {
      return false;
    }
    List<SchemaChange> changes = new ArrayList<>();
    options.forEach((
        k,
        v) -> {
      if (!v.equals(have.get(k))) {
        changes.add(SchemaChange.setOption(k, v));
      }
    });
    if (changes.isEmpty()) {
      return false;
    }
    try {
      catalog.alterTable(identifierOf(schema, table), changes, false);
    } catch (Exception e) {
      throw new IllegalStateException(
          "cannot set " + options.keySet() + " on " + identifierOf(schema, table) + ": " + e, e);
    }
    tables.clear();
    builders.clear();
    return true;
  }

  public SchemaRegistry(
      @NotNull Catalog catalog,
      @NotNull WriterConfig config
  ) {
    this(catalog, config, CrashHook.fromEnv(System.getenv()));
  }

  SchemaRegistry(
      Catalog catalog,
      WriterConfig config,
      CrashHook hook
  ) {
    this.catalog = catalog;
    this.config = config;
    this.hook = hook;
  }

  /**
   * Idempotent: an existing Paimon table is left untouched.
   *
   * <p>
   * The {@code id} of each column (the PostgreSQL attnum) is advisory. Paimon assigns field ids
   * itself and {@code Schema.Builder.column} cannot set them. This is a deliberate divergence from
   * main, which writes attnum as the field id (legacy:extension/paimon_schema.cpp:131). Column
   * names are the contract, which is why RowBuilder maps by name.
   *
   * <p>
   * The {@code bucket} option IS set, from the count the create message carries, which
   * {@code paimon.add_table()} resolved (its {@code buckets => N} argument, or
   * {@code paimon.default_buckets()}). That puts the table in fixed-bucket mode, where Paimon
   * hashes the primary key to pick the bucket and keeps no key index. Leaving it unset, as this did
   * for layout parity with main (legacy:extension/paimon_schema.cpp:219-223), costs that index: 1.2
   * GB of a 1.8 GB warehouse on a measured TPC-C run, for 88 MB of data. Paimon does not
   * redistribute existing data when the count changes, so a wrong guess costs a rewrite rather than
   * being permanent.
   */
  public void createTable(
      long relid,
      @NotNull String schema,
      @NotNull String table,
      @NotNull List<String> pk,
      @NotNull List<DdlMessage.Col> cols
  ) {
    createTable(relid, schema, table, pk, cols, "input");
  }

  /**
   * Records the relid to Paimon identity mapping and NOTHING else: no catalog call, no warehouse
   * write.
   *
   * <p>
   * Startup needs the mapping before it can decide whether to refuse to start -- the decision reads
   * each table's committed offsets -- and it must be able to refuse without having touched the
   * warehouse first, or "the writer refused to start, so nothing happened" is false. So the mapping
   * is established here, the decision is taken, and only then does {@link #bootstrapTable} create
   * and reconcile.
   */
  public void register(
      long relid,
      String schema,
      String table
  ) {
    identifiers.put(relid, identifierOf(schema, table));
  }

  /**
   * The Paimon identity of a PostgreSQL table: its <strong>PostgreSQL schema</strong> as the Paimon
   * database, and its name as the Paimon table. On disk that is {@code <warehouse>/<schema>.db/
   *
  <table>
   * }.
   *
   * <p>
   * This used to be a single configured database for everything, which meant the PostgreSQL schema
   * was dropped from the identity entirely: {@code public.f} and {@code s2.f} both resolved to one
   * Paimon table and interleaved into it, two tables' rows merging on a shared primary key.
   * Carrying the schema is what makes the identity faithful; it also gives each table its own
   * {@code pgpaimon.applied-ddl-lsn}, where same-named tables previously shared one.
   */
  private static Identifier identifierOf(
      String schema,
      String table
  ) {
    return Identifier.create(schema, table);
  }

  /** As above with an explicit changelog-producer (taken from the message's options). */
  public void createTable(
      long relid,
      @NotNull String schema,
      @NotNull String table,
      @NotNull List<String> pk,
      @NotNull List<DdlMessage.Col> cols,
      @Nullable String changelogProducer
  ) {
    createTable(relid, schema, table, pk, cols, changelogProducer, 0);
  }

  /** As above, recording {@code ddlLsn} as the table's applied-DDL identifier. */
  public void createTable(
      long relid,
      @NotNull String schema,
      @NotNull String table,
      @NotNull List<String> pk,
      @NotNull List<DdlMessage.Col> cols,
      @Nullable String changelogProducer,
      long ddlLsn
  ) {
    createTable(relid, schema, table, pk, cols, changelogProducer, null, ddlLsn);
  }

  /** As above with the bucket count the create message carried. */
  public void createTable(
      long relid,
      String schema,
      String table,
      List<String> pk,
      List<DdlMessage.Col> cols,
      String changelogProducer,
      Integer buckets,
      long ddlLsn
  ) {
    createTable(relid, schema, table, pk, cols, changelogProducer, buckets, ddlLsn, null, null);
  }

  /**
   * As above with the history table's name and the registration id the create message carries
   * ({@code null}: {@code
   *
  <table>
   * _hist}, and no ownership check).
   */
  public void createTable(
      long relid,
      String schema,
      String table,
      List<String> pk,
      List<DdlMessage.Col> cols,
      String changelogProducer,
      Integer buckets,
      long ddlLsn,
      String histTable,
      String registration
  ) {
    createTable(relid, schema, table, pk, cols, changelogProducer, buckets, ddlLsn,
        new HistSpec(histTable, registration, null, false));
  }

  /** As above with everything the create message says about the history table. */
  public void createTable(
      long relid,
      String schema,
      String table,
      List<String> pk,
      List<DdlMessage.Col> cols,
      String changelogProducer,
      Integer buckets,
      long ddlLsn,
      HistSpec hist
  ) {
    create(relid, schema, table, pk, cols, changelogProducer, buckets, false, ddlLsn, hist);
  }

  /**
   * The startup bootstrap's replay of one {@code paimon.table_registry} row.
   *
   * <p>
   * {@code reconcileLostDdl} must be true <strong>only when the replication slot is
   * absent</strong>. A DDL message can be lost only along with the slot; while the slot lives, a
   * column the registry has and Paimon lacks is <em>pending</em> in the unconsumed stream, not lost
   * — and at bootstrap the registry is always ahead of the stream by everything unconsumed.
   * Treating that as loss fabricated the column as a new empty one and the still-pending {@code
   * rename_col}/{@code add_col} then collided, wedging the writer on an ordinary {@code RENAME
   * COLUMN} performed while it was stopped.
   */
  public void bootstrapTable(
      long relid,
      @NotNull String schema,
      @NotNull String table,
      @NotNull List<String> pk,
      @NotNull List<DdlMessage.Col> cols,
      @Nullable String changelogProducer,
      boolean reconcileLostDdl
  ) {
    bootstrapTable(relid, schema, table, pk, cols, changelogProducer, null, reconcileLostDdl);
  }

  /** As above with the bucket count {@code paimon.table_registry} recorded. */
  public void bootstrapTable(
      long relid,
      String schema,
      String table,
      List<String> pk,
      List<DdlMessage.Col> cols,
      String changelogProducer,
      Integer buckets,
      boolean reconcileLostDdl
  ) {
    bootstrapTable(relid, schema, table, pk, cols, changelogProducer, buckets, reconcileLostDdl,
        HistSpec.DEFAULT);
  }

  /** As above with the registry row's history table name and registration id. */
  public void bootstrapTable(
      long relid,
      String schema,
      String table,
      List<String> pk,
      List<DdlMessage.Col> cols,
      String changelogProducer,
      Integer buckets,
      boolean reconcileLostDdl,
      HistSpec hist
  ) {
    // No LSN: the bootstrap replays a registry row, not a message, so it has none to record
    // and must not disturb whatever the table already carries.
    create(relid, schema, table, pk, cols, changelogProducer, buckets, reconcileLostDdl, 0, hist);
  }

  /**
   * The main table of a relation with no primary key: an append-only operation log. The user
   * columns come first, unprefixed and all nullable (a {@code T} row is NULL in every one, and a
   * log never rewrites a column's nullability), then the operation metadata.
   *
   * <p>
   * Bucketing, measured on Paimon 1.4.1 (60 commits of 20 rows each): an unaware-bucket table
   * ({@code bucket = -1}) gets no compaction from the writer and held 60 live files; a fixed-bucket
   * table compacts in the writer and held 16. The bucket key must be a column no DDL can touch:
   * with a user column in {@code bucket-key}, {@code DROP COLUMN} of it fails ("Field names [...]
   * should contains all bucket keys"), which would be a DDL message the writer can never apply. So:
   * the registry's fixed bucket count, keyed on {@code _lsn}, which every row has, differs per
   * change, and is never altered.
   */
  private Schema logSchema(
      List<DdlMessage.Col> cols,
      Integer buckets,
      long ddlLsn
  ) {
    Schema.Builder b = Schema.newBuilder();
    for (DdlMessage.Col c : cols) {
      b.column(c.name(), parseType(c.type()).copy(true));
    }
    appendMeta(b, LogRowBuilder.META);
    appendOptions(b, buckets, ddlLsn);
    return b.build();
  }

  /**
   * A history table: the table's columns (all nullable: a {@code T} row is NULL in every one), then
   * {@code _op, _commit_lsn, _lsn, _op_seq, _xid, _ts, _dt}. Append-only, partitioned by
   * {@code _dt} (the UTC date of the commit), {@code buckets} buckets in every partition, keyed on
   * {@code _lsn} like {@link #logSchema}.
   */
  private static Schema histSchema(
      List<DataField> userColumns,
      int buckets,
      long ddlLsn,
      long baseline,
      String historyOf,
      String registration
  ) {
    return histSchema(userColumns, buckets, ddlLsn, baseline, historyOf, registration, Map.of());
  }

  /**
   * The options an operator set on a history table that its successor at a rotation keeps: every
   * one except the writer's own ({@code pgpaimon.*}), the layout it decides ({@code bucket}, which
   * a rotation re-measures, {@code bucket-key}, {@code partition.legacy-name}) and {@code path}.
   * Retention above all: the guide tells operators to set {@code partition.expiration-time} on
   * {@code T_hist}, and a rotation built the new table's options from scratch, so after the next
   * DDL the history grew without bound again, without a word (review of the history tables, I5).
   */
  static Map<String, String> carriedOptions(
      Map<String, String> archived
  ) {
    Map<String, String> out = new java.util.TreeMap<>();
    for (Map.Entry<String, String> e : archived.entrySet()) {
      String k = e.getKey();
      if (k.startsWith("pgpaimon.") || k.equals("bucket") || k.equals("bucket-key")
          || k.equals("partition.legacy-name") || k.equals("path")) {
        continue;
      }
      out.put(k, e.getValue());
    }
    return out;
  }

  private static Schema histSchema(
      List<DataField> userColumns,
      int buckets,
      long ddlLsn,
      long baseline,
      String historyOf,
      String registration,
      Map<String, String> carried
  ) {
    Schema.Builder b = Schema.newBuilder();
    carried.forEach(b::option); // first: the writer's own options below win
    for (DataField f : userColumns) {
      b.column(f.name(), f.type().copy(true));
    }
    appendMeta(b, LogRowBuilder.HIST_META);
    b.partitionKeys(LogRowBuilder.DT);
    // Partition directories named _dt=2026-01-01, not by the DATE's internal day number
    // (_dt=20454), which is what Paimon's legacy naming would give.
    b.option("partition.legacy-name", "false");
    appendOptions(b, buckets, ddlLsn);
    b.option(HISTORY_OF_OPTION, historyOf);
    if (registration != null) {
      b.option(REGISTRATION_OPTION, registration);
    }
    if (baseline != 0) {
      b.option(BASELINE_OPTION, Long.toUnsignedString(baseline));
    }
    return b.build();
  }

  private static void appendMeta(
      Schema.Builder b,
      List<String> meta
  ) {
    for (String m : meta) {
      switch (m) {
        case LogRowBuilder.OP -> b.column(m, DataTypes.STRING().notNull());
        case LogRowBuilder.OP_SEQ -> b.column(m, DataTypes.INT().notNull());
        case LogRowBuilder.TS -> b.column(m, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull());
        case LogRowBuilder.DT -> b.column(m, DataTypes.DATE().notNull());
        default -> b.column(m, DataTypes.BIGINT().notNull());
      }
    }
  }

  private static void appendOptions(
      Schema.Builder b,
      Integer buckets,
      long ddlLsn
  ) {
    b.option("bucket", String.valueOf(buckets == null ? DEFAULT_BUCKETS : buckets));
    b.option("bucket-key", LogRowBuilder.LSN);
    if (ddlLsn != 0) {
      b.option(DDL_LSN_OPTION, Long.toUnsignedString(ddlLsn));
    }
  }

  private void create(
      long relid,
      String schema,
      String table,
      List<String> pk,
      List<DdlMessage.Col> cols,
      String changelogProducer,
      Integer buckets,
      boolean reconcileLostDdl,
      long ddlLsn,
      HistSpec hist
  ) {
    boolean isLog = pk.isEmpty();
    Schema wanted;
    if (isLog) {
      wanted = logSchema(cols, buckets, ddlLsn);
    } else {
      Schema.Builder b = Schema.newBuilder();
      for (DdlMessage.Col c : cols) {
        b.column(c.name(), parseType(c.type()));
      }
      b.column(config.seqField(), DataTypes.BIGINT().notNull());
      b.column(config.opSeqField(), DataTypes.INT().notNull());
      if (config.xidField() != null) {
        b.column(config.xidField(), DataTypes.BIGINT());
      }
      b.primaryKey(pk);
      b.option("merge-engine", "deduplicate");
      b.option("changelog-producer", changelogProducer == null ? "input" : changelogProducer);
      b.option("sequence.field", config.seqField() + "," + config.opSeqField());
      b.option("ignore-update-before", "false");
      // Fixed-bucket mode. Never left unset: dynamic mode would reject the sink's
      // write(row) and bring back the key index this exists to remove. A create message
      // always carries a count, so the fallback is only reached by a caller that has none.
      b.option("bucket", String.valueOf(buckets == null ? DEFAULT_BUCKETS : buckets));
      if (ddlLsn != 0) {
        // On the table's own creation, so a create and its identifier are one atomic
        // write.
        b.option(DDL_LSN_OPTION, Long.toUnsignedString(ddlLsn));
      }
      wanted = b.build();
    }

    Identifier id = identifierOf(schema, table);
    Table existing = lookup(id);
    if (existing != null && !existing.primaryKeys().equals(pk)) {
      // remove_table() keeps the main Paimon table, and a table re-registered after its key
      // was added, dropped or changed in between cannot continue it: a primary-key table
      // and an operation log are different layouts (measured: "Paimon schema has no
      // metadata column '_seq'" on every restart, replication down for every table), and a
      // table merged on another key would merge the wrong rows. add_table() refuses this
      // when it can see it (paimon.kept_tables); this is the backstop.
      throw new IllegalStateException("Paimon table " + id + " exists as "
          + (existing.primaryKeys().isEmpty() ? "an operation log (no primary key)"
              : "a primary-key table (key " + existing.primaryKeys() + ")")
          + " but " + schema + "." + table + " is now registered "
          + (isLog ? "without a primary key (an operation log)" : "with primary key " + pk)
          + ": the two cannot continue one another. It is the main table kept by an"
          + " earlier paimon.remove_table(). Rename or drop " + id + " from a compute engine"
          + " (or move its directory out of the warehouse), then restart the writer: it"
          + " creates the table afresh. Nothing has been written.");
    }
    try {
      // One Paimon database per PostgreSQL schema, created on demand. Idempotent.
      catalog.createDatabase(schema, true);
      catalog.createTable(id, stamped(wanted), true);
    } catch (Exception e) {
      throw new IllegalStateException("cannot create Paimon table " + id, e);
    }
    identifiers.put(relid, id);
    if (isLog) {
      keyless.add(relid);
    } else {
      keyless.remove(relid);
    }
    forget0(relid);
    if (existing != null) {
      // Deliberately NOT carrying the identifier forward onto a table that already
      // existed. The option is written only on the table's own creation, which is the case
      // the drop gate needs: a drop purges the Paimon table, so the create that follows it
      // is a genuine creation and records the newer LSN. Writing it here instead would make
      // a create ALTER an existing table -- an options-only schema version bump -- and
      // "a create never alters an existing table" is worth more than carrying the
      // identifier forward for a table that was never dropped. Leaving the older LSN in
      // place also fails safe: the gate is then less likely to skip, not more.
      reconcileOrWarn(relid, id, existing, pk, cols, reconcileLostDdl);
    }
    createHistory(relid, schema, table, hist.table() == null ? table + "_hist" : hist.table(), cols,
        hist.buckets() == null ? DEFAULT_HIST_BUCKETS : hist.buckets(), hist.userBuckets(), ddlLsn,
        hist.registration(), reconcileLostDdl);
  }

  /**
   * Creates the relation's history table, or adopts the existing one if it is this registration's.
   * Refuses one that belongs to another registration, or to none: appending a different table's
   * changes to an audit trail is the corruption the history exists to prevent, and
   * {@code paimon.add_table()} already refuses every collision it can see
   * (paimon.retained_history). Reaching the refusal here means a history table it could not see --
   * made by hand, or kept when paimon.forget_history() was run without renaming it. The writer
   * stops on it; renaming that Paimon table and restarting resolves it, so it is an outage with an
   * exact remedy, never silent mixing.
   */
  private void createHistory(
      long relid,
      String schema,
      String table,
      String histTable,
      List<DdlMessage.Col> cols,
      int histBuckets,
      boolean userBuckets,
      long ddlLsn,
      String registration,
      boolean reconcileLostDdl
  ) {
    long hk = HistoryKeys.histKey(relid);
    Identifier hid = identifierOf(schema, histTable);
    requireOwnArchives(hid, registration, schema + "." + table);
    Table existing = null;
    try {
      // Paimon's catalog caches tables (cache-enabled is its default). The remedy for a
      // refusal below is to rename or drop this table by hand, out of band: without
      // dropping the cached entry the writer would keep seeing the table that is gone.
      catalog.invalidateTable(hid);
      try {
        existing = catalog.getTable(hid);
      } catch (Catalog.TableNotExistException e) {
        // normal first-time case
      }
      if (existing == null && interruptedRotation(hid)) {
        // A rotation renamed this table away and stopped before creating its successor.
        // Creating it here, from the registry, would make a table with no baseline that
        // the replayed DDL message would then take for a half-copied rename -- and drop
        // the real history. The replayed message creates it; until then every change
        // routes to the archive (see the baseline set below).
        LOG.warning("history table " + hid + " is missing and an archive of it exists:"
            + " a rotation was interrupted after its rename; the replayed DDL message"
            + " completes it");
      } else if (existing == null) {
        List<DataField> user = new ArrayList<>();
        for (DdlMessage.Col c : cols) {
          user.add(new DataField(user.size(), c.name(), parseType(c.type())));
        }
        // No baseline: a table's first history holds everything from its creation on.
        catalog.createTable(hid,
            stamped(histSchema(user, histBuckets, ddlLsn, 0, schema + "." + table, registration)),
            true);
      }
    } catch (Exception e) {
      throw new IllegalStateException("cannot create Paimon history table " + hid, e);
    }
    if (existing != null && registration != null) {
      String owner = existing.options().get(REGISTRATION_OPTION);
      if (!registration.equals(owner)) {
        throw new IllegalStateException("Paimon table " + hid + " exists but is not the"
            + " history table of this registration of " + schema + "." + table
            + (owner == null
                ? " (it carries no " + REGISTRATION_OPTION + "; this writer did not create it)"
                : " (it belongs to registration " + owner + ", an earlier table of the same name)")
            + ". A history table is an audit trail and is never continued by a"
            + " different table. Rename or drop " + hid + " and its archives " + histTable
            + "_<lsn> from a compute engine (or move their directories out of"
            + " the warehouse), then restart the writer: it creates a new, empty history"
            + " table. Nothing has been written to it.");
      }
    }
    HistState st = new HistState(hid, histBuckets, userBuckets);
    identifiers.put(hk, hid);
    history.put(relid, st);
    forget0(hk);
    discoverArchives(relid, st);
    Table now = lookup(hid);
    st.baseline = now != null ? baselineOf(now) : st.archives.isEmpty() ? 0 : st.archives.lastKey();
    if (existing != null) {
      reconcileHistory(hk, hid, existing, cols, reconcileLostDdl);
    }
  }

  /**
   * Refuses archives {@code <hist>_<lsn>} that belong to another registration (or to none).
   *
   * <p>
   * An archive is recognised by its name, and a name says nothing about whose it is. The archives
   * of a table dropped earlier stay (they are its audit trail), and an operator who moved aside
   * only {@code <hist>} -- the archives have distinct names and look harmless -- then ran
   * forget_history() and add_table() for a new table of the same name, made this writer take them
   * for the new table's: measured, it read the absent {@code <hist>} as an interrupted rotation
   * ("the replayed DDL message completes it"), never created it, and stopped on every restart with
   * "cannot load Paimon table", replication down for every table. And a reader who unions
   * {@code <hist>} with its archives, as the guide says to, would get the earlier table's rows as
   * this one's history. Archives carry the registration of the history they were rotated out of
   * ({@code pgpaimon.registration}), so ownership is checked, and a foreign one is refused by name,
   * with the remedy, before anything is written.
   */
  private void requireOwnArchives(
      Identifier hid,
      String registration,
      String table
  ) {
    if (registration == null) {
      return; // a create without a registration (older messages): nothing to compare
    }
    Table current = lookup(hid);
    if (current != null && !registration.equals(current.options().get(REGISTRATION_OPTION))) {
      return; // <hist> itself is foreign: createHistory refuses that, more specifically
    }
    List<String> foreign = new ArrayList<>();
    try {
      for (String n : catalog.listTables(hid.getDatabaseName())) {
        if (HistoryKeys.archiveLsn(n, hid.getObjectName()) < 0) {
          continue;
        }
        Identifier aid = Identifier.create(hid.getDatabaseName(), n);
        catalog.invalidateTable(aid);
        String owner = catalog.getTable(aid).options().get(REGISTRATION_OPTION);
        if (!registration.equals(owner)) {
          foreign.add(n + (owner == null ? " (no registration)" : " (registration " + owner + ")"));
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("cannot list the history archives of " + hid, e);
    }
    if (!foreign.isEmpty()) {
      throw new IllegalStateException("Paimon table(s) " + foreign + " in database "
          + hid.getDatabaseName() + " are named like archives of " + hid.getObjectName()
          + " but belong to another registration than this one of " + table + " (" + registration
          + "): they are the retained history of an earlier table of the same"
          + " name. A history and its archives are an audit trail and are never continued by a"
          + " different table. Rename or drop them (and " + hid.getObjectName() + ", if it"
          + " is also an earlier table's) from a compute engine, or move their directories out"
          + " of the warehouse, then restart the writer. Nothing has been written.");
    }
  }

  /** Whether {@code hid} is absent because a rotation archived it and then stopped. */
  private boolean interruptedRotation(
      Identifier hid
  ) {
    try {
      for (String n : catalog.listTables(hid.getDatabaseName())) {
        if (HistoryKeys.archiveLsn(n, hid.getObjectName()) >= 0) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      throw new IllegalStateException(
          "cannot list the tables of Paimon database " + hid.getDatabaseName(), e);
    }
  }

  private static long baselineOf(
      Table t
  ) {
    String v = t.options().get(BASELINE_OPTION);
    return v == null ? 0 : Long.parseUnsignedLong(v);
  }

  /** Registers every archive {@code <hist>_<16 hex>} of a relation that the catalog lists. */
  private void discoverArchives(
      long relid,
      HistState st
  ) {
    List<String> names;
    try {
      names = catalog.listTables(st.current.getDatabaseName());
    } catch (Exception e) {
      throw new IllegalStateException("cannot list the tables of Paimon database "
          + st.current.getDatabaseName() + " to find the history archives of " + st.current, e);
    }
    for (String n : names) {
      long lsn = HistoryKeys.archiveLsn(n, st.current.getObjectName());
      if (lsn >= 0) {
        registerArchive(relid, st, lsn, Identifier.create(st.current.getDatabaseName(), n));
      }
    }
  }

  private void registerArchive(
      long relid,
      HistState st,
      long lsn,
      Identifier id
  ) {
    st.archives.put(lsn, id);
    long key = HistoryKeys.archiveKey(relid, lsn);
    identifiers.put(key, id);
    forget0(key);
  }

  /**
   * The history-table counterpart of {@link #reconcileOrWarn}: a column the registry has and Paimon
   * lacks is added only when the slot is absent (its message is then lost, not pending; a pending
   * one rotates the table when it is applied); a type difference is a warning. A column Paimon has
   * and the registry does not is a lost drop, and stays (history is never rewritten).
   */
  private void reconcileHistory(
      long hk,
      Identifier hid,
      Table existing,
      List<DdlMessage.Col> cols,
      boolean reconcileLostDdl
  ) {
    Map<String, DataType> have = new java.util.LinkedHashMap<>();
    for (DataField f : existing.rowType().getFields()) {
      have.put(f.name(), f.type());
    }
    List<String> diffs = new ArrayList<>();
    List<SchemaChange> add = new ArrayList<>();
    for (DdlMessage.Col c : cols) {
      DataType t = parseType(c.type()).copy(true);
      DataType got = have.get(c.name());
      if (got == null) {
        if (reconcileLostDdl) {
          add.add(addUserColumn(true, c.name(), t));
        } else {
          diffs.add("column '" + c.name() + "' is missing; its DDL message should still"
              + " be in the slot and rotates the table when applied");
        }
      } else if (!got.equals(t)) {
        diffs.add("column '" + c.name() + "' is " + got.asSQLString() + " in Paimon but "
            + t.asSQLString() + " in the registry");
      }
    }
    if (!add.isEmpty()) {
      try {
        catalog.alterTable(hid, add, false);
        forget0(hk);
        LOG.warning("history table " + hid + ": added " + add.size() + " column(s) the"
            + " registry declares and Paimon lacked (the slot is absent, so their DDL"
            + " messages are lost, not pending; earlier rows hold NULL there)");
      } catch (Exception e) {
        diffs.add("missing columns could not be added: " + e);
      }
    }
    if (!diffs.isEmpty()) {
      LOG.warning(
          "history table " + hid + " differs from the registry: " + String.join("; ", diffs));
    }
  }

  /**
   * A repeated create for an existing table (the restart bootstrap replays every registry row).
   *
   * <p>
   * Columns the create message declares and Paimon lacks are <strong>applied</strong>, one
   * {@code addColumn} each, with a WARNING naming the column — but <strong>only when {@code
   * reconcileLostDdl}</strong>, which the caller sets only if the slot is absent. Warning alone was
   * not enough: a DDL message lost with the slot (the re-bootstrap procedure in the operations
   * guide drops it) leaves the Paimon schema permanently behind, and {@link RowBuilder} then
   * discards that column's value from every row for ever, with nothing but two warnings to show for
   * it. The registry is rebuilt from {@code paimon.table_registry}, which is updated in the same
   * transaction as the DDL, so on this one difference the create message is the authority and
   * Paimon is simply behind.
   *
   * <p>
   * With the slot present the same difference means the opposite: the DDL message is still in the
   * stream, waiting. Reconciling then fabricates the column and the pending message collides with
   * it, so it is only reported.
   *
   * <p>
   * Everything else is a WARNING and nothing more, because there is no way to tell which side is
   * right: a column present in both with different types, a different primary key, a column Paimon
   * has that the message does not.
   *
   * <p>
   * Never throws, and a reconciliation that fails degrades to a warning: drift must not block a
   * restart.
   */
  private void reconcileOrWarn(
      long relid,
      @NotNull Identifier id,
      @NotNull Table existing,
      @NotNull List<String> pk,
      @NotNull List<DdlMessage.Col> cols,
      boolean reconcileLostDdl
  ) {
    List<String> diffs = new ArrayList<>();
    if (!existing.primaryKeys().equals(pk)) {
      diffs.add("primary key is " + existing.primaryKeys() + " in Paimon but " + pk
          + " in the create message");
    }
    Map<String, DataType> have = new java.util.LinkedHashMap<>();
    for (DataField f : existing.rowType().getFields()) {
      have.put(f.name(), f.type());
    }
    List<DdlMessage.Col> missing = new ArrayList<>();
    for (DdlMessage.Col c : cols) {
      DataType t = have.remove(c.name());
      if (t == null) {
        missing.add(c);
      } else if (!t.equals(mainType(relid, c.type()))) {
        diffs.add("column '" + c.name() + "' is " + t.asSQLString() + " in Paimon but "
            + mainType(relid, c.type()).asSQLString() + " in the create message");
      }
    }
    if (keyless(relid)) {
      LogRowBuilder.META.forEach(have::remove);
    } else {
      have.remove(config.seqField());
      have.remove(config.opSeqField());
      if (config.xidField() != null) {
        have.remove(config.xidField());
      }
    }
    for (String extra : have.keySet()) {
      // Its own WARNING, not a clause in a list. This column is NULL for every row, and
      // every SELECT * downstream returns it; the commonest way to get one is a rename
      // whose DDL message was lost, where the pre-rename column survives beside the
      // correctly reloaded new one. Nothing here can safely drop it -- the registry not
      // naming it is not proof that no reader wants it -- so say exactly what it is.
      LOG.warning("table " + id + " has a column '" + extra + "' that"
          + " paimon.table_registry does not: it is NOT maintained, reads NULL for every"
          + " row, and any SELECT * will return it. The usual cause is a rename whose DDL"
          + " message was lost, leaving the pre-rename column beside the new one. It is"
          + " not dropped automatically, because the registry not naming it is not proof"
          + " that no reader wants it. Check it holds nothing you need, then remove it"
          + " from a compute engine: ALTER TABLE `" + id.getDatabaseName() + "`.`"
          + id.getObjectName() + "` DROP COLUMN `" + extra + "`");
    }
    for (DdlMessage.Col c : missing) {
      if (!reconcileLostDdl) {
        // Careful not to promise: the slot existing is why this is left alone, and the
        // slot can be dropped moments later (the RECREATE_SLOT_AND_BACKFILL path), in
        // which case Main re-runs this with reconciliation on.
        diffs.add("column '" + c.name() + "' is missing from Paimon; the slot still"
            + " exists, so its DDL message should still be in the stream and is left to"
            + " that message rather than guessed at here. If the column is still missing"
            + " once the writer has caught up, the message was lost: re-register the" + " table");
        continue;
      }
      DataType t = mainType(relid, c.type());
      try {
        catalog.alterTable(id, List.of(addUserColumn(keyless(relid), c.name(), t)), false);
        forget0(relid);
        LOG.warning("create for existing table " + id + ": column '" + c.name() + "' "
            + t.asSQLString() + " was missing from its Paimon schema and has been added"
            + " (the slot is absent, so its DDL message is lost, not pending; rows"
            + " written before now hold NULL there)");
      } catch (Exception e) {
        diffs.add(
            "column '" + c.name() + "' is missing from Paimon and could not be" + " added: " + e);
      }
    }
    if (!diffs.isEmpty()) {
      LOG.warning("create for existing table " + id + " differs from its Paimon schema: "
          + String.join("; ", diffs));
    }
  }

  /*
   * Why every DDL operation below tolerates "already in the desired state".
   *
   * A DDL-only transaction produces no records, so nothing about it reaches Paimon and
   * filterAndCommit has no identifier to filter on. The slot position is confirmed to pgjdbc
   * immediately but reaches the server only at the next status tick, and the resume position is the
   * highest Paimon commit identifier, which a DDL-only transaction never moves -- so a DDL that is
   * the last thing in the stream replays on EVERY restart, clean or not. Paimon's own errors for
   * that replay (ColumnAlreadyExistException, ColumnNotExistException) are not retryable, and the
   * slot only advances after a successful commit, so the writer stopped permanently.
   *
   * The fix is the mechanism data already had, applied to the thing that lacked it. Data replay is
   * safe because filterAndCommit compares a commit IDENTIFIER; DDL had no equivalent. Each DDL
   * message carries its own WAL LSN, which is stable across replays, so the LSN of the last applied
   * DDL message is recorded in the Paimon table's own schema options ({@value #DDL_LSN_OPTION}) --
   * written in the SAME alterTable call as the column change, so the two cannot diverge (measured
   * against Paimon 1.4.1: a list of SchemaChanges becomes one atomic schema commit, and a failing
   * change in the list leaves the option unwritten too). Any DDL message at or below the recorded
   * LSN is skipped.
   *
   * This is what makes replaying a SEQUENCE safe, which per-op state inspection cannot be:
   * "ADD COLUMN num int" then "ALTER COLUMN num TYPE bigint" leaves a replayed add_col looking at a
   * BIGINT column, indistinguishable from a genuine conflict, and "ADD COLUMN x" then
   * "RENAME x TO y" leaves a replayed add_col re-creating a spurious x for the replayed rename to
   * collide with. The invariant the LSN gate gives by construction: replaying any suffix of an
   * already-applied DDL sequence converges to the same schema and never fails.
   *
   * The state-based tolerances below remain as a second line, but ONLY for a table with no recorded
   * LSN -- one whose DDL was applied before this mechanism existed. A table that has an identifier
   * cannot reach them by replay, because the gate filters a replay first, so reaching them there
   * means genuine drift and must still fail loudly. Keying the tolerance on {@code
   * appliedDdlLsn(relid) == 0} is what keeps a tolerance needed for one case from silencing a real
   * fault in the other.
   */

  /**
   * The bucket count for a table whose caller supplied none. It matches
   * {@code paimon.default_buckets()}'s floor, so such a table is bucketed as an empty table
   * registered normally would be -- and never left in dynamic-bucket mode, which the sink's
   * {@code write(row)} cannot use.
   */
  static final int DEFAULT_BUCKETS = 4;

  /** A history table's bucket count per partition when its caller supplied none. */
  static final int DEFAULT_HIST_BUCKETS = HistBuckets.MIN;

  /**
   * Schema option holding the WAL LSN of the last DDL message applied to this table.
   *
   * <p>
   * Caveat worth knowing: the value carries no cluster or timeline identity. A warehouse pointed at
   * a different server, or at one restored by PITR or re-{@code initdb}'d, can therefore meet DDL
   * messages whose LSNs sit below the recorded value and skip them as replays. Nothing here detects
   * that; the operations guide says so where operators look.
   */
  static final String DDL_LSN_OPTION = "pgpaimon.applied-ddl-lsn";

  /*
   * The history table rotates on DDL (spec 2026-10-14, rule 3; Amendment A2). Each DDL message
   * applied to a tracked relation -- add_col, drop_col, rename_col, alter_col -- first archives the
   * current <t>_hist as <t>_hist_<lsn> and creates a new, empty <t>_hist with the new schema, then
   * alters the main table as before. Every row of an archive was written under one schema. Each
   * step is gated so that a replay after a crash between any two converges; see rotate().
   */
  public void addColumn(
      long relid,
      @NotNull DdlMessage.Col col,
      long ddlLsn
  ) {
    rotate(relid, ddlLsn, "add_col '" + col.name() + "'", user -> {
      if (user.stream().noneMatch(f -> f.name().equals(col.name()))) {
        user.add(new DataField(user.size(), col.name(), parseType(col.type()).copy(true)));
      }
    });
    addColumnMain(relid, col, ddlLsn);
  }

  private void addColumnMain(
      long relid,
      DdlMessage.Col col,
      long ddlLsn
  ) {
    if (replayed(relid, ddlLsn, "add_col '" + col.name() + "'")) {
      return;
    }
    DataType wanted = mainType(relid, col.type());
    DataField existing = field(relid, col.name());
    if (existing != null) {
      if (existing.type().equals(wanted)) {
        LOG.info("add_col '" + col.name() + "' on " + identifiers.get(relid)
            + " is already applied as " + wanted.asSQLString() + ": no-op");
        return;
      }
      // Not a throw when there is no recorded LSN: a replayed add superseded by a replayed
      // alter_col looks exactly like this, and the stored type is then the later, correct
      // one. But with an identifier recorded, a replay was already filtered by the gate,
      // so getting here is genuine drift and must be loud.
      String what = "add_col '" + col.name() + "' on " + identifiers.get(relid)
          + ": the column already exists as " + existing.type().asSQLString()
          + " but the message declares " + wanted.asSQLString();
      requireNoRecordedLsn(relid, what);
      LOG.warning(what + "; this table has no recorded DDL LSN, so a superseded replay"
          + " cannot be ruled out: leaving the stored type alone");
      return;
    }
    alter(relid, List.of(addUserColumn(keyless(relid), col.name(), wanted)), ddlLsn);
  }

  public void dropColumn(
      long relid,
      @NotNull String name,
      long ddlLsn
  ) {
    noteNameChange(relid, ddlLsn, name, DROPPED);
    rotate(relid, ddlLsn, "drop_col '" + name + "'",
        user -> user.removeIf(f -> f.name().equals(name)));
    dropColumnMain(relid, name, ddlLsn);
  }

  private void dropColumnMain(
      long relid,
      String name,
      long ddlLsn
  ) {
    if (replayed(relid, ddlLsn, "drop_col '" + name + "'")) {
      return;
    }
    if (field(relid, name) == null) {
      LOG.info("drop_col '" + name + "' on " + identifiers.get(relid)
          + " is already applied (no such column): no-op");
      return;
    }
    alter(relid, List.of(SchemaChange.dropColumn(name)), ddlLsn);
  }

  /** A rename is already applied when the source is gone and the target is there. */
  public void renameColumn(
      long relid,
      @NotNull String from,
      @NotNull String to,
      long ddlLsn
  ) {
    noteNameChange(relid, ddlLsn, from, to);
    rotate(relid, ddlLsn, "rename_col '" + from + "' -> '" + to + "'", user -> {
      if (user.stream().noneMatch(f -> f.name().equals(to))) {
        user.replaceAll(f -> f.name().equals(from) ? f.newName(to) : f);
      }
    });
    renameColumnMain(relid, from, to, ddlLsn);
  }

  private void renameColumnMain(
      long relid,
      String from,
      String to,
      long ddlLsn
  ) {
    if (replayed(relid, ddlLsn, "rename_col '" + from + "' -> '" + to + "'")) {
      return;
    }
    if (field(relid, from) == null && field(relid, to) != null) {
      LOG.info("rename_col '" + from + "' -> '" + to + "' on " + identifiers.get(relid)
          + " is already applied: no-op");
      return;
    }
    alter(relid, List.of(SchemaChange.renameColumn(from, to)), ddlLsn);
  }

  /**
   * The name {@link #asOfNow} gives a column dropped since: empty, which no column can be called
   * (the extension refuses a name with no visible character, and so does Paimon), so no field
   * matches it and the builders skip it without a warning.
   */
  static final String DROPPED = "";

  /**
   * A column renamed (to {@code to}) or dropped ({@code to} is {@link #DROPPED}) by the DDL message
   * at {@code lsn}.
   */
  private record NameChange(
      long lsn,
      String from,
      String to
  ) {
  }

  /** Per relation, in stream order: every rename and drop this process has seen. */
  private final Map<Long, List<NameChange>> nameChanges = new ConcurrentHashMap<>();

  /**
   * Remembers a rename or drop, also when the message itself is a replay that the Paimon side
   * skips: a record decoded before it may still be waiting to be built (see {@link #asOfNow}). A
   * message seen twice (an in-process reconnect replays from the last confirmed LSN) is recorded
   * once.
   */
  private void noteNameChange(
      long relid,
      long lsn,
      String from,
      String to
  ) {
    if (lsn == 0) {
      return;
    }
    List<NameChange> l = nameChanges.computeIfAbsent(relid,
        k -> java.util.Collections.synchronizedList(new ArrayList<>()));
    NameChange c = new NameChange(lsn, from, to);
    if (!l.contains(c)) {
      l.add(c);
    }
  }

  /**
   * The column names a record decoded under {@code meta} at change LSN {@code seq} has in the main
   * table's schema NOW: every rename and drop at a later LSN applied, in stream order; a dropped
   * column's name becomes {@link #DROPPED} (its value is discarded, as PostgreSQL discarded it).
   *
   * <p>
   * Why it exists. Records are built when their batch is committed, against the Paimon schema
   * current then, by column NAME. A transaction that writes a row and then renames a column --
   * {@code BEGIN; INSERT ...; ALTER TABLE s RENAME COLUMN v TO w; INSERT ...; COMMIT}, the shape of
   * many reload and migration scripts -- had its first row built against a schema with no
   * {@code v}: the value was discarded and Paimon held NULL where PostgreSQL held the value
   * (measured: {@code 20 | null} against {@code 20 | before rename}). With a drop and a re-add of
   * the same name the old value would even have landed in the NEW column. Translating the names to
   * the schema's current ones is exact, because the stream orders every DDL message against every
   * change.
   *
   * <p>
   * Only main tables need it: a history row decoded before a DDL goes to that DDL's archive, whose
   * schema is the one it was decoded under.
   */
  public RelationMeta asOfNow(
      long key,
      RelationMeta meta,
      long seq
  ) {
    if (meta == null || HistoryKeys.isHist(key)) {
      return meta;
    }
    List<NameChange> l = nameChanges.get(key);
    if (l == null || l.isEmpty()) {
      return meta;
    }
    List<String> names = null;
    synchronized (l) {
      for (NameChange c : l) {
        if (Long.compareUnsigned(c.lsn(), seq) <= 0) {
          continue;
        }
        if (names == null) {
          names = new ArrayList<>(meta.columnNames());
        }
        for (int i = 0; i < names.size(); i++) {
          if (c.from().equals(names.get(i))) {
            names.set(i, c.to());
          }
        }
      }
    }
    if (names == null || names.equals(meta.columnNames())) {
      return meta;
    }
    return new RelationMeta(meta.relid(), meta.namespace(), meta.relName(), names, meta.isKey());
  }

  /**
   * Adds a user column. In an operation log or a history table it goes before the metadata
   * ({@code _op} onwards), where the layout puts the user's columns: appended at the end it landed
   * after them (review of the history tables, m2), contradicting the documented layout and the
   * TPC-C comparator's schema check.
   */
  static SchemaChange addUserColumn(
      boolean logLayout,
      String name,
      DataType type
  ) {
    return logLayout
        ? SchemaChange.addColumn(name, type, null, SchemaChange.Move.before(name, LogRowBuilder.OP))
        : SchemaChange.addColumn(name, type);
  }

  /** One field of the table's Paimon schema by name, or null if there is none. */
  private DataField field(
      long key,
      String name
  ) {
    for (DataField f : table(key).schema().fields()) {
      if (f.name().equals(name)) {
        return f;
      }
    }
    return null;
  }

  /**
   * A no-op when the stored type already equals the new one (varchar vs text are both STRING). A
   * column the message names and Paimon does not have is logged and skipped rather than thrown: in
   * a replayed sequence the column may since have been renamed or dropped.
   */
  public void alterColumn(
      long relid,
      @NotNull String name,
      @NotNull String type,
      long ddlLsn
  ) {
    DataType wanted = parseType(type).copy(true);
    rotate(relid, ddlLsn, "alter_col '" + name + "'",
        user -> user.replaceAll(f -> f.name().equals(name) ? f.newType(wanted) : f));
    alterColumnMain(relid, name, type, ddlLsn);
  }

  /**
   * Rotates a relation's history table for the DDL message at {@code ddlLsn}:
   * <ol>
   * <li>rename {@code <t>_hist} to {@code <t>_hist_<16 hex digits of ddlLsn>};</li>
   * <li>create a new {@code <t>_hist} with the user columns {@code change} makes of the old ones,
   * recording {@code ddlLsn} as its applied-DDL LSN and as its baseline;</li>
   * <li>(the caller) alter the main table as before.</li>
   * </ol>
   * Replay-safe at every point, because each step's outcome is visible and checked first:
   * <ul>
   * <li>the current table already records {@code ddlLsn} (or later): steps 1-2 are done;</li>
   * <li>no current table but the archive exists: step 1 is done, do step 2 from the archive's
   * schema;</li>
   * <li>both exist and the current table is older: a rename was interrupted half-way, which only a
   * non-atomic store (S3: copy, then delete) can leave. The source is kept if it is still readable
   * and the partial target dropped; otherwise the target is kept;</li>
   * <li>neither: impossible for a tracked relation, and loud.</li>
   * </ul>
   * Measured on Paimon 1.4.1: {@code renameTable} on a filesystem catalog is a directory
   * {@code Files.move}, atomic on a local disk (and HDFS), refuses an existing target
   * ({@code TableAlreadyExistException}), and the renamed table keeps its snapshots, so the
   * commit-identifier filter keeps working on the archive.
   */
  private void rotate(
      long relid,
      long ddlLsn,
      String what,
      java.util.function.Consumer<List<DataField>> change
  ) {
    HistState st = history.get(relid);
    if (st == null || ddlLsn == 0) {
      return;
    }
    long hk = HistoryKeys.histKey(relid);
    Identifier cur = st.current;
    Identifier arch = Identifier.create(cur.getDatabaseName(),
        HistoryKeys.archiveName(cur.getObjectName(), ddlLsn));
    for (Identifier tracked : identifiers.values()) {
      if (tracked.equals(arch) && !st.archives.containsValue(arch)) {
        throw new IllegalStateException(what + ": cannot archive history table " + cur + " as "
            + arch + ", which is a replicated table's Paimon table. Rename one of"
            + " them; paimon.add_table() refuses such names, so a guardrail was bypassed");
      }
    }
    Table current = lookup(cur);
    Table archived = lookup(arch);
    if (current != null && Long.compareUnsigned(appliedDdlLsn(current), ddlLsn) >= 0) {
      LOG.info(what + ": history table " + cur + " already rotated at "
          + SlotConsumer.formatLsn(ddlLsn) + ": replay, no-op");
      if (archived != null) {
        registerArchive(relid, st, ddlLsn, arch);
      }
      return;
    }
    try {
      if (current != null && archived != null) {
        if (readable(current)) {
          LOG.warning(what + ": both " + cur + " and " + arch + " exist: a rename was"
              + " interrupted half-way. " + cur + " is intact, so the partial " + arch
              + " is dropped and the rename repeated");
          catalog.dropTable(arch, false);
          archived = null;
        } else {
          LOG.warning(what + ": both " + cur + " and " + arch + " exist and " + cur
              + " is no longer readable: the rename had copied it; keeping " + arch);
          catalog.dropTable(cur, false);
          current = null;
        }
      }
      if (current != null) {
        catalog.renameTable(cur, arch, false);
        archived = lookup(arch);
        hook.step("rotate-renamed");
      }
      if (archived == null) {
        throw new IllegalStateException(what + ": neither " + cur + " nor " + arch
            + " exists, so the history of this table up to here cannot be found");
      }
      List<DataField> user = new ArrayList<>();
      for (DataField f : archived.rowType().getFields()) {
        if (!LogRowBuilder.HIST_META.contains(f.name())) {
          user.add(f);
        }
      }
      change.accept(user);
      registerArchive(relid, st, ddlLsn, arch);
      int buckets = bucketsAfterRotation(st);
      Map<String, String> carried = carriedOptions(archived.options());
      if (!carried.isEmpty()) {
        LOG.info(
            what + ": the new " + cur + " keeps the options set on the archived one: " + carried);
      }
      catalog.createTable(cur,
          stamped(histSchema(user, buckets, ddlLsn, ddlLsn,
              archived.options().getOrDefault(HISTORY_OF_OPTION, ""),
              archived.options().get(REGISTRATION_OPTION), carried)),
          false);
      hook.step("rotate-created");
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(what + ": cannot rotate history table " + cur, e);
    }
    forget0(hk);
    st.baseline = ddlLsn;
    LOG.info(what + ": history table " + cur + " archived as " + arch + "; a new " + cur
        + " starts at " + SlotConsumer.formatLsn(ddlLsn));
  }

  /**
   * The bucket count for a new history table at a rotation: the user's {@code hist_buckets} always;
   * else measured from the archives' daily partitions when at least {@link HistBuckets#MIN_DAYS}
   * days exist; else what the registry estimated at add_table. The count of an existing table is
   * never changed: only the new table gets it.
   */
  private int bucketsAfterRotation(
      HistState st
  ) {
    if (st.userBuckets) {
      return st.buckets;
    }
    Map<Integer, Long> bytesByDay = new java.util.HashMap<>();
    for (Identifier a : st.archives.values()) {
      Table t = lookup(a);
      if (t instanceof FileStoreTable f) {
        for (var e : f.newSnapshotReader().partitionEntries()) {
          bytesByDay.merge(e.partition().getInt(0), e.fileSizeInBytes(), Long::sum);
        }
      }
    }
    java.util.OptionalInt measured = HistBuckets.measured(bytesByDay);
    if (measured.isPresent()) {
      LOG.info("history of " + st.current + ": " + bytesByDay.size() + " day(s) measured,"
          + " the new history table gets " + measured.getAsInt() + " bucket(s) per day");
      st.buckets = measured.getAsInt();
    }
    return st.buckets;
  }

  private Table lookup(
      Identifier id
  ) {
    catalog.invalidateTable(id);
    try {
      return catalog.getTable(id);
    } catch (Catalog.TableNotExistException e) {
      return null;
    }
  }

  private static boolean readable(
      Table t
  ) {
    try {
      if (t instanceof FileStoreTable f) {
        f.snapshotManager().latestSnapshot();
      }
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private long appliedDdlLsn(
      Table t
  ) {
    String v = t.options().get(DDL_LSN_OPTION);
    return v == null ? 0 : Long.parseUnsignedLong(v);
  }

  /** The current history table's baseline: changes at or below it belong to archives. */
  long historyBaseline(
      long relid
  ) {
    HistState st = history.get(relid);
    return st == null ? 0 : st.baseline;
  }

  @Override
  public long historyKey(
      long relid,
      long changeLsn
  ) {
    HistState st = history.get(relid);
    if (st == null) {
      return 0;
    }
    if (Long.compareUnsigned(changeLsn, st.baseline) > 0) {
      return HistoryKeys.histKey(relid);
    }
    var archive = st.archives.higherEntry(changeLsn);
    if (archive == null) {
      LOG.warning("a change of " + st.current + " at " + SlotConsumer.formatLsn(changeLsn)
          + " predates the history table's baseline " + SlotConsumer.formatLsn(st.baseline)
          + " and no archive covers it (removed by hand?): it is not written to the" + " history");
      return 0;
    }
    return HistoryKeys.archiveKey(relid, archive.getKey());
  }

  private void alterColumnMain(
      long relid,
      String name,
      String type,
      long ddlLsn
  ) {
    if (replayed(relid, ddlLsn, "alter_col '" + name + "'")) {
      return;
    }
    DataType wanted = mainType(relid, type);
    DataField current = field(relid, name);
    if (current == null) {
      String what = "alter_col '" + name + "' on " + identifiers.get(relid)
          + ": no such column in the Paimon schema";
      requireNoRecordedLsn(relid, what);
      LOG.warning(what + "; this table has no recorded DDL LSN, so the column may have"
          + " been renamed or dropped by a later message already applied: skipped");
      return;
    }
    if (current.type().equals(wanted)) {
      return;
    }
    alter(relid, List.of(SchemaChange.updateColumnType(name, wanted)), ddlLsn);
  }

  /**
   * A column's Paimon type in the relation's main table: as declared for a primary-key table;
   * always nullable in an operation log, whose {@code T} rows are NULL in every user column.
   */
  private DataType mainType(
      long relid,
      String sql
  ) {
    DataType t = parseType(sql);
    return keyless(relid) ? t.copy(true) : t;
  }

  /**
   * Whether this DDL message is at or below the last DDL LSN applied to the table, and so has
   * already been applied. LSNs are compared unsigned. An {@code ddlLsn} of 0 means the caller has
   * no LSN (the startup bootstrap, which replays registry rows rather than messages) and disables
   * the gate.
   */
  private boolean replayed(
      long relid,
      long ddlLsn,
      @NotNull String what
  ) {
    if (ddlLsn == 0) {
      return false;
    }
    long applied = appliedDdlLsn(relid);
    if (applied != 0 && Long.compareUnsigned(ddlLsn, applied) <= 0) {
      // Both forms: the X/XXXXXXXX one to compare with pg_replication_slots and the
      // writer's own messages, the decimal one because that is what the schema option
      // holds and what an operator reading it back will see.
      LOG.info(what + " on " + identifiers.get(relid) + " at " + SlotConsumer.formatLsn(ddlLsn)
          + " (" + Long.toUnsignedString(ddlLsn) + ") is at or below the last applied DDL LSN "
          + SlotConsumer.formatLsn(applied) + " (" + Long.toUnsignedString(applied)
          + "): replay, no-op");
      return true;
    }
    return false;
  }

  /**
   * Throws unless the table has no recorded DDL LSN. The state-based tolerances exist for a table
   * that predates the identifier; a table that HAS one cannot reach them by replay, because
   * {@link #replayed} filters first, so reaching them means the registry and the Paimon schema
   * genuinely disagree and the writer must say so rather than carry on.
   */
  private void requireNoRecordedLsn(
      long relid,
      @NotNull String what
  ) {
    long applied = appliedDdlLsn(relid);
    if (applied != 0) {
      throw new IllegalStateException(what + ". This is not a replay: the table's last"
          + " applied DDL LSN is " + SlotConsumer.formatLsn(applied)
          + " and this message is above it, so the Paimon schema and"
          + " paimon.table_registry genuinely disagree. Compare them; re-register the"
          + " table (paimon.remove_table, delete the Paimon table, paimon.add_table) if"
          + " they do");
    }
  }

  /** The recorded last-applied DDL LSN of a table, or 0 if it has none. */
  long appliedDdlLsn(
      long relid
  ) {
    String v = table(relid).schema().options().get(DDL_LSN_OPTION);
    if (v == null) {
      return 0;
    }
    try {
      return Long.parseUnsignedLong(v);
    } catch (NumberFormatException e) {
      LOG.warning(
          "ignoring unparseable " + DDL_LSN_OPTION + " '" + v + "' on " + identifiers.get(relid));
      return 0;
    }
  }

  /** Purges the warehouse data. Untracked relids are ignored. */
  public void dropTable(
      long relid,
      @Nullable String schema,
      @Nullable String table
  ) {
    dropTable(relid, schema, table, 0);
  }

  /**
   * Purges the warehouse data. Untracked relids are ignored.
   *
   * <p>
   * Gated on the applied-DDL identifier like the column ops, for one reason: a Paimon table is
   * identified by NAME, so a table dropped and recreated under the same name resolves to the same
   * Paimon table. If the recreated table already records an identifier at or above this drop's LSN,
   * the table standing here now was created AFTER the drop was emitted, so the drop is stale and
   * applying it would purge the new table's rows and then have the replayed create rebuild it
   * empty.
   *
   * <p>
   * Today that is reachable only if PostgreSQL reuses the dropped table's OID, because a drop
   * deletes its {@code paimon.table_registry} row in the same transaction as the message, so after
   * any restart the old relid is untracked and {@link DdlMessageHandler} refuses the message before
   * it arrives here (measured: "ignoring drop_table for unregistered relation"). Narrow, but the
   * consequence is a whole table's data, and the gate is two comparisons.
   *
   * <p>
   * A table with no recorded identifier keeps the old behaviour: there is nothing to compare, and
   * refusing on that basis would make a drop unreachable.
   */
  public void dropTable(
      long relid,
      @Nullable String schema,
      @Nullable String table,
      long ddlLsn
  ) {
    Identifier id = identifiers.get(relid);
    if (id == null) {
      return;
    }
    if (ddlLsn != 0) {
      long applied = appliedDdlLsn(relid);
      if (applied != 0 && Long.compareUnsigned(applied, ddlLsn) >= 0) {
        LOG.warning("drop_table for " + id + " at " + SlotConsumer.formatLsn(ddlLsn) + " ("
            + Long.toUnsignedString(ddlLsn)
            + ") is STALE and has NOT been applied: the table standing there records an"
            + " applied-DDL LSN of " + SlotConsumer.formatLsn(applied) + " ("
            + Long.toUnsignedString(applied) + "), at or above the drop, so it was"
            + " created after this drop was emitted. Applying it would purge a table the"
            + " drop was never about. Nothing is dropped and the relation stays tracked");
        return;
      }
    }
    try {
      catalog.dropTable(id, true);
    } catch (Exception e) {
      throw new IllegalStateException("cannot drop Paimon table " + id, e);
    }
    Identifier hist = identifiers.get(HistoryKeys.histKey(relid));
    if (hist != null) {
      // Deliberately left standing: the history table is the retained audit trail of a
      // dropped table (paimon.retained_history records it in PostgreSQL).
      LOG.info("dropped Paimon table " + id + "; its history table " + hist
          + " and its archives are kept as the audit trail and no longer written");
    }
    forget(relid);
  }

  /** The Paimon table a key names: a relation's main table, or its history table. */
  public FileStoreTable table(
      long relid
  ) {
    Identifier id = identifiers.get(relid);
    if (id == null) {
      throw new IllegalStateException(
          (HistoryKeys.isHist(relid) ? "a history table of relation " + HistoryKeys.relidOf(relid)
              : "relation " + relid) + " is not tracked");
    }
    return tables.computeIfAbsent(relid, r -> {
      try {
        return (FileStoreTable) catalog.getTable(id);
      } catch (Exception e) {
        throw new IllegalStateException("cannot load Paimon table " + id, e);
      }
    });
  }

  /**
   * The primary-key column names of a tracked relation, as Paimon holds them (so a renamed key
   * column is followed). This, not pgoutput's Relation flags, defines the key: under REPLICA
   * IDENTITY FULL pgoutput flags every column as a key column.
   */
  public @NotNull List<String> primaryKey(
      long relid
  ) {
    return table(relid).primaryKeys();
  }

  /**
   * Cached per (key, column list); a changed list or a reloaded table rebuilds it. The key is a
   * relid (the main table) or a history key; the builder matches the table's shape.
   */
  public RowMaker rowBuilder(
      long relid,
      @NotNull RelationMeta meta
  ) {
    FileStoreTable t = table(relid);
    CachedBuilder cb = builders.get(relid);
    if (cb != null && cb.table() == t && cb.columns().equals(meta.columnNames())) {
      return cb.builder();
    }
    String name = meta.namespace() + "." + meta.relName();
    RowMaker rb;
    if (HistoryKeys.isHist(relid) || keyless(relid)) {
      rb = new LogRowBuilder(t.fullName(), t.rowType(), meta.columnNames());
    } else {
      rb = new RowBuilder(name, t.rowType(), meta.columnNames(), config.seqField(),
          config.opSeqField(), config.xidField());
    }
    builders.put(relid, new CachedBuilder(List.copyOf(meta.columnNames()), t, rb));
    return rb;
  }

  /**
   * Drops cached state (and tracking) so the next access reloads or re-creates. Forgets the
   * relation's history table too: it is written only while the relation is tracked.
   */
  public void forget(
      long relid
  ) {
    identifiers.remove(relid);
    nameChanges.remove(relid);
    forget0(relid);
    if (!HistoryKeys.isHist(relid)) {
      long hk = HistoryKeys.histKey(relid);
      identifiers.remove(hk);
      forget0(hk);
      keyless.remove(relid);
      HistState st = history.remove(relid);
      if (st != null) {
        for (long lsn : st.archives.keySet()) {
          long ak = HistoryKeys.archiveKey(relid, lsn);
          identifiers.remove(ak);
          forget0(ak);
        }
      }
    }
  }

  @Override
  public boolean tracked(
      long relid
  ) {
    return identifiers.containsKey(relid);
  }

  @Override
  public boolean keyless(
      long relid
  ) {
    return keyless.contains(relid);
  }

  @Override
  public boolean hasHistory(
      long relid
  ) {
    return relid >= 0 && identifiers.containsKey(HistoryKeys.histKey(relid));
  }

  private void forget0(
      long relid
  ) {
    tables.remove(relid);
    builders.remove(relid);
  }

  /**
   * One schema change, plus the DDL message's LSN, in a single alterTable call. Paimon turns a list
   * of SchemaChanges into one atomic schema commit, so the column change and the recorded LSN land
   * together or not at all; recording the LSN in a second call would leave a window in which a
   * crash makes the change replay.
   */
  private void alter(
      long relid,
      List<SchemaChange> change,
      long ddlLsn
  ) {
    Identifier id = identifiers.get(relid);
    if (id == null) {
      throw new IllegalStateException("relation " + relid + " is not tracked");
    }
    List<SchemaChange> changes = new ArrayList<>(change);
    if (ddlLsn != 0) {
      changes.add(SchemaChange.setOption(DDL_LSN_OPTION, Long.toUnsignedString(ddlLsn)));
    }
    if (changes.isEmpty()) {
      return;
    }
    try {
      catalog.alterTable(id, changes, false);
    } catch (Exception e) {
      throw new IllegalStateException("cannot alter Paimon table " + id + ": " + change, e);
    }
    forget0(relid);
    if (!HistoryKeys.isHist(relid)) {
      hook.step("rotate-altered"); // test hook: the main table's alteration is done
    }
  }

  /**
   * Parses a Paimon SQL type string such as {@code "INT NOT NULL"} or {@code "DECIMAL(10, 2)"} with
   * Paimon's own parser. The SQL mapping only ever emits plain TIMESTAMP and TIMESTAMP WITH LOCAL
   * TIME ZONE (precision 6); TextDecoder ignores declared precision, so a precision-qualified
   * timestamp is refused here rather than silently mis-handled.
   */
  static @NotNull DataType parseType(
      @NotNull String sql
  ) {
    DataType t;
    try {
      t = DataTypeJsonParser.parseAtomicTypeSQLString(sql);
    } catch (RuntimeException e) {
      throw new IllegalStateException("unparseable Paimon type '" + sql + "'", e);
    }
    if ((t instanceof TimestampType ts && ts.getPrecision() != TimestampType.DEFAULT_PRECISION)
        || (t instanceof LocalZonedTimestampType lz
            && lz.getPrecision() != LocalZonedTimestampType.DEFAULT_PRECISION)) {
      throw new IllegalStateException(
          "timestamp precision other than 6 is not supported: '" + sql + "'");
    }
    return t;
  }
}
