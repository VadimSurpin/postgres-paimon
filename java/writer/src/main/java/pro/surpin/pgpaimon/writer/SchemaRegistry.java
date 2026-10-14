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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

/**
 * The only class in the writer that talks to the Paimon {@link Catalog}. Everything else asks it
 * for a {@link FileStoreTable} or a {@link RowBuilder}.
 *
 * <p>Tracking is by PostgreSQL relation oid (a {@code long}, see package-info). The mapping
 * relid to Paimon identifier is learned from {@code create} messages, which the writer re-issues
 * for every row of {@code paimon.table_registry} at startup; a relid never created in this
 * process is untracked.
 *
 * <p>Every tracked relation has two Paimon tables in the same database: its main table, keyed
 * by the relid, and its history table ({@code <table>_hist}), keyed by
 * {@link HistoryKeys#histKey}. The main table is a primary-key table, or, for a relation with no
 * primary key, an append-only operation log; the history table is always append-only. Every
 * lookup below takes either key. Spec: docs/superpowers/specs/2026-10-14-operation-history-design.md.
 */
public final class SchemaRegistry implements TableTracker {

    private static final Logger LOG = Logger.getLogger(SchemaRegistry.class.getName());

    private final Catalog catalog;
    private final WriterConfig config;
    private final Map<Long, Identifier> identifiers = new ConcurrentHashMap<>();
    private final Map<Long, FileStoreTable> tables = new ConcurrentHashMap<>();
    private final Map<Long, CachedBuilder> builders = new ConcurrentHashMap<>();
    private final Set<Long> keyless = ConcurrentHashMap.newKeySet();

    private record CachedBuilder(List<String> columns, FileStoreTable table, RowMaker builder) {}

    /**
     * Schema option on a history table: the {@code paimon.table_registry.registration} it was
     * created for. A history table is an audit trail; a different registration -- a table dropped
     * and re-created under the same name, or one registered again after remove_table() -- must
     * not continue it, and a table carrying none was not made by this writer.
     */
    static final String REGISTRATION_OPTION = "pgpaimon.registration";

    /** Schema option on a history table naming the table it records, for people reading it. */
    static final String HISTORY_OF_OPTION = "pgpaimon.history-of";

    public SchemaRegistry(Catalog catalog, WriterConfig config) {
        this.catalog = catalog;
        this.config = config;
    }

    /**
     * Idempotent: an existing Paimon table is left untouched.
     *
     * <p>The {@code id} of each column (the PostgreSQL attnum) is advisory. Paimon assigns field
     * ids itself and {@code Schema.Builder.column} cannot set them. This is a deliberate
     * divergence from main, which writes attnum as the field id
     * (legacy:extension/paimon_schema.cpp:131). Column names are the contract, which is why RowBuilder
     * maps by name.
     *
     * <p>The {@code bucket} option IS set, from the count the create message carries, which
     * {@code paimon.add_table()} resolved (its {@code buckets => N} argument, or
     * {@code paimon.default_buckets()}). That puts the table in fixed-bucket mode, where Paimon
     * hashes the primary key to pick the bucket and keeps no key index. Leaving it unset, as this
     * did for layout parity with main (legacy:extension/paimon_schema.cpp:219-223), costs that index:
     * 1.2 GB of a 1.8 GB warehouse on a measured TPC-C run, for 88 MB of data. Paimon does not
     * redistribute existing data when the count changes, so a wrong guess costs a rewrite rather
     * than being permanent.
     */
    public void createTable(long relid, String schema, String table, List<String> pk,
                            List<DdlMessage.Col> cols) {
        createTable(relid, schema, table, pk, cols, "input");
    }

    /**
     * Records the relid to Paimon identity mapping and NOTHING else: no catalog call, no
     * warehouse write.
     *
     * <p>Startup needs the mapping before it can decide whether to refuse to start -- the
     * decision reads each table's committed offsets -- and it must be able to refuse without
     * having touched the warehouse first, or "the writer refused to start, so nothing happened"
     * is false. So the mapping is established here, the decision is taken, and only then does
     * {@link #bootstrapTable} create and reconcile.
     */
    public void register(long relid, String schema, String table) {
        identifiers.put(relid, identifierOf(schema, table));
    }

    /**
     * The Paimon identity of a PostgreSQL table: its <strong>PostgreSQL schema</strong> as the
     * Paimon database, and its name as the Paimon table. On disk that is
     * {@code <warehouse>/<schema>.db/<table>}.
     *
     * <p>This used to be a single configured database for everything, which meant the PostgreSQL
     * schema was dropped from the identity entirely: {@code public.f} and {@code s2.f} both
     * resolved to one Paimon table and interleaved into it, two tables' rows merging on a shared
     * primary key. Carrying the schema is what makes the identity faithful; it also gives each
     * table its own {@code pgpaimon.applied-ddl-lsn}, where same-named tables previously shared
     * one.
     */
    private static Identifier identifierOf(String schema, String table) {
        return Identifier.create(schema, table);
    }

    /** As above with an explicit changelog-producer (taken from the message's options). */
    public void createTable(long relid, String schema, String table, List<String> pk,
                            List<DdlMessage.Col> cols, String changelogProducer) {
        createTable(relid, schema, table, pk, cols, changelogProducer, 0);
    }

    /** As above, recording {@code ddlLsn} as the table's applied-DDL identifier. */
    public void createTable(long relid, String schema, String table, List<String> pk,
                            List<DdlMessage.Col> cols, String changelogProducer, long ddlLsn) {
        createTable(relid, schema, table, pk, cols, changelogProducer, null, ddlLsn);
    }

    /** As above with the bucket count the create message carried. */
    public void createTable(long relid, String schema, String table, List<String> pk,
                            List<DdlMessage.Col> cols, String changelogProducer,
                            Integer buckets, long ddlLsn) {
        createTable(relid, schema, table, pk, cols, changelogProducer, buckets, ddlLsn,
                    null, null);
    }

    /**
     * As above with the history table's name and the registration id the create message
     * carries ({@code null}: {@code <table>_hist}, and no ownership check).
     */
    public void createTable(long relid, String schema, String table, List<String> pk,
                            List<DdlMessage.Col> cols, String changelogProducer,
                            Integer buckets, long ddlLsn, String histTable,
                            String registration) {
        create(relid, schema, table, pk, cols, changelogProducer, buckets, false, ddlLsn,
               histTable, registration);
    }

    /**
     * The startup bootstrap's replay of one {@code paimon.table_registry} row.
     *
     * <p>{@code reconcileLostDdl} must be true <strong>only when the replication slot is absent</strong>.
     * A DDL message can be lost only along with the slot; while the slot lives, a column the
     * registry has and Paimon lacks is <em>pending</em> in the unconsumed stream, not lost — and
     * at bootstrap the registry is always ahead of the stream by everything unconsumed. Treating
     * that as loss fabricated the column as a new empty one and the still-pending
     * {@code rename_col}/{@code add_col} then collided, wedging the writer on an ordinary
     * {@code RENAME COLUMN} performed while it was stopped.
     */
    public void bootstrapTable(long relid, String schema, String table, List<String> pk,
                               List<DdlMessage.Col> cols, String changelogProducer,
                               boolean reconcileLostDdl) {
        bootstrapTable(relid, schema, table, pk, cols, changelogProducer, null, reconcileLostDdl);
    }

    /** As above with the bucket count {@code paimon.table_registry} recorded. */
    public void bootstrapTable(long relid, String schema, String table, List<String> pk,
                               List<DdlMessage.Col> cols, String changelogProducer,
                               Integer buckets, boolean reconcileLostDdl) {
        bootstrapTable(relid, schema, table, pk, cols, changelogProducer, buckets,
                       reconcileLostDdl, null, null);
    }

    /** As above with the registry row's history table name and registration id. */
    public void bootstrapTable(long relid, String schema, String table, List<String> pk,
                               List<DdlMessage.Col> cols, String changelogProducer,
                               Integer buckets, boolean reconcileLostDdl, String histTable,
                               String registration) {
        // No LSN: the bootstrap replays a registry row, not a message, so it has none to record
        // and must not disturb whatever the table already carries.
        create(relid, schema, table, pk, cols, changelogProducer, buckets, reconcileLostDdl, 0,
               histTable, registration);
    }

    /**
     * The main table of a relation with no primary key: an append-only operation log. The user
     * columns come first, unprefixed and all nullable (a {@code T} row is NULL in every one, and
     * a log never rewrites a column's nullability), then the operation metadata.
     *
     * <p>Bucketing, measured on Paimon 1.4.1 (60 commits of 20 rows each): an unaware-bucket
     * table ({@code bucket = -1}) gets no compaction from the writer and held 60 live files; a
     * fixed-bucket table compacts in the writer and held 16. The bucket key must be a column no
     * DDL can touch: with a user column in {@code bucket-key}, {@code DROP COLUMN} of it fails
     * ("Field names [...] should contains all bucket keys"), which would be a DDL message the
     * writer can never apply. So: the registry's fixed bucket count, keyed on {@code _lsn}, which
     * every row has, differs per change, and is never altered.
     */
    private Schema logSchema(List<DdlMessage.Col> cols, Integer buckets, long ddlLsn) {
        Schema.Builder b = Schema.newBuilder();
        for (DdlMessage.Col c : cols) {
            b.column(c.name(), parseType(c.type()).copy(true));
        }
        appendMeta(b);
        appendOptions(b, buckets, ddlLsn);
        return b.build();
    }

    /**
     * A history table: the operation metadata, then {@code before_<col>} for every column, then
     * {@code after_<col>}. Every user column is prefixed, so none can collide with a metadata
     * column, and all are nullable: {@code before_*} is NULL for an insert, {@code after_*} for a
     * delete. Append-only, bucketed like {@link #logSchema}.
     */
    private Schema histSchema(List<DdlMessage.Col> cols, Integer buckets, long ddlLsn,
                              String schema, String table, String registration) {
        Schema.Builder b = Schema.newBuilder();
        appendMeta(b);
        for (DdlMessage.Col c : cols) {
            b.column(LogRowBuilder.BEFORE + c.name(), parseType(c.type()).copy(true));
        }
        for (DdlMessage.Col c : cols) {
            b.column(LogRowBuilder.AFTER + c.name(), parseType(c.type()).copy(true));
        }
        appendOptions(b, buckets, ddlLsn);
        b.option(HISTORY_OF_OPTION, schema + "." + table);
        if (registration != null) {
            b.option(REGISTRATION_OPTION, registration);
        }
        return b.build();
    }

    private static void appendMeta(Schema.Builder b) {
        b.column(LogRowBuilder.OP, DataTypes.STRING().notNull());
        b.column(LogRowBuilder.COMMIT_LSN, DataTypes.BIGINT().notNull());
        b.column(LogRowBuilder.LSN, DataTypes.BIGINT().notNull());
        b.column(LogRowBuilder.OP_SEQ, DataTypes.INT().notNull());
        b.column(LogRowBuilder.XID, DataTypes.BIGINT().notNull());
        b.column(LogRowBuilder.TS, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull());
    }

    private static void appendOptions(Schema.Builder b, Integer buckets, long ddlLsn) {
        b.option("bucket", String.valueOf(buckets == null ? DEFAULT_BUCKETS : buckets));
        b.option("bucket-key", LogRowBuilder.LSN);
        if (ddlLsn != 0) {
            b.option(DDL_LSN_OPTION, Long.toUnsignedString(ddlLsn));
        }
    }

    private void create(long relid, String schema, String table, List<String> pk,
                        List<DdlMessage.Col> cols, String changelogProducer,
                        Integer buckets, boolean reconcileLostDdl, long ddlLsn,
                        String histTable, String registration) {
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
            b.option("changelog-producer",
                     changelogProducer == null ? "input" : changelogProducer);
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
        Table existing = null;
        try {
            // One Paimon database per PostgreSQL schema, created on demand. Idempotent.
            catalog.createDatabase(schema, true);
            try {
                existing = catalog.getTable(id);
            } catch (Catalog.TableNotExistException e) {
                // normal first-time case
            }
            catalog.createTable(id, wanted, true);
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
        createHistory(relid, schema, table, histTable == null ? table + "_hist" : histTable,
                      cols, buckets, ddlLsn, registration, reconcileLostDdl);
    }

    /**
     * Creates the relation's history table, or adopts the existing one if it is this
     * registration's. Refuses one that belongs to another registration, or to none: appending a
     * different table's changes to an audit trail is the corruption the history exists to
     * prevent, and {@code paimon.add_table()} already refuses every collision it can see
     * (paimon.retained_history). Reaching the refusal here means a history table it could not
     * see -- made by hand, or kept when paimon.forget_history() was run without renaming it.
     * The writer stops on it; renaming that Paimon table and restarting resolves it, so it is
     * an outage with an exact remedy, never silent mixing.
     */
    private void createHistory(long relid, String schema, String table, String histTable,
                               List<DdlMessage.Col> cols, Integer buckets, long ddlLsn,
                               String registration, boolean reconcileLostDdl) {
        long hk = HistoryKeys.histKey(relid);
        Identifier hid = identifierOf(schema, histTable);
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
            if (existing == null) {
                catalog.createTable(hid, histSchema(cols, buckets, ddlLsn, schema, table,
                                                    registration), true);
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot create Paimon history table " + hid, e);
        }
        if (existing != null && registration != null) {
            String owner = existing.options().get(REGISTRATION_OPTION);
            if (!registration.equals(owner)) {
                throw new IllegalStateException("Paimon table " + hid + " exists but is not the"
                    + " history table of this registration of " + schema + "." + table
                    + (owner == null ? " (it carries no " + REGISTRATION_OPTION
                                       + "; this writer did not create it)"
                                     : " (it belongs to registration " + owner
                                       + ", an earlier table of the same name)")
                    + ". A history table is an audit trail and is never continued by a"
                    + " different table. Rename or drop " + hid + " from a compute engine (or"
                    + " move its directory out of the warehouse), then restart the writer: it"
                    + " creates a new, empty history table. Nothing has been written to it.");
            }
        }
        identifiers.put(hk, hid);
        forget0(hk);
        if (existing != null) {
            reconcileHistory(hk, hid, existing, cols, reconcileLostDdl);
        }
    }

    /**
     * The history-table counterpart of {@link #reconcileOrWarn}: a {@code before_}/{@code after_}
     * pair the registry has and Paimon lacks is added only when the slot is absent (its message
     * is then lost, not pending); a type difference is a warning. A pair Paimon has and the
     * registry does not is NOT reported: that is a dropped column's history, kept by design.
     */
    private void reconcileHistory(long hk, Identifier hid, Table existing,
                                  List<DdlMessage.Col> cols, boolean reconcileLostDdl) {
        Map<String, DataType> have = new java.util.LinkedHashMap<>();
        for (DataField f : existing.rowType().getFields()) {
            have.put(f.name(), f.type());
        }
        List<String> diffs = new ArrayList<>();
        List<SchemaChange> add = new ArrayList<>();
        for (DdlMessage.Col c : cols) {
            DataType t = parseType(c.type()).copy(true);
            for (String name : List.of(LogRowBuilder.BEFORE + c.name(),
                                       LogRowBuilder.AFTER + c.name())) {
                DataType got = have.get(name);
                if (got == null) {
                    if (reconcileLostDdl) {
                        add.add(SchemaChange.addColumn(name, t));
                    } else {
                        diffs.add("column '" + name + "' is missing; its DDL message should still"
                            + " be in the slot and is left to it");
                    }
                } else if (!got.equals(t)) {
                    diffs.add("column '" + name + "' is " + got.asSQLString() + " in Paimon but "
                        + t.asSQLString() + " in the registry");
                }
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
            LOG.warning("history table " + hid + " differs from the registry: "
                + String.join("; ", diffs));
        }
    }

    /**
     * A repeated create for an existing table (the restart bootstrap replays every registry row).
     *
     * <p>Columns the create message declares and Paimon lacks are <strong>applied</strong>, one
     * {@code addColumn} each, with a WARNING naming the column — but <strong>only when
     * {@code reconcileLostDdl}</strong>, which the caller sets only if the slot is absent. Warning
     * alone was not enough: a DDL message lost with the slot (the re-bootstrap procedure in the
     * operations guide drops it) leaves the Paimon schema permanently behind, and
     * {@link RowBuilder} then discards that column's value from every row for ever, with nothing
     * but two warnings to show for it. The registry is rebuilt from {@code paimon.table_registry},
     * which is updated in the same transaction as the DDL, so on this one difference the create
     * message is the authority and Paimon is simply behind.
     *
     * <p>With the slot present the same difference means the opposite: the DDL message is still in
     * the stream, waiting. Reconciling then fabricates the column and the pending message collides
     * with it, so it is only reported.
     *
     * <p>Everything else is a WARNING and nothing more, because there is no way to tell which side
     * is right: a column present in both with different types, a different primary key, a column
     * Paimon has that the message does not.
     *
     * <p>Never throws, and a reconciliation that fails degrades to a warning: drift must not block
     * a restart.
     */
    private void reconcileOrWarn(long relid, Identifier id, Table existing, List<String> pk,
                                 List<DdlMessage.Col> cols, boolean reconcileLostDdl) {
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
                diffs.add("column '" + c.name() + "' is " + t.asSQLString()
                    + " in Paimon but " + mainType(relid, c.type()).asSQLString()
                    + " in the create message");
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
                    + " once the writer has caught up, the message was lost: re-register the"
                    + " table");
                continue;
            }
            DataType t = mainType(relid, c.type());
            try {
                catalog.alterTable(id, List.of(SchemaChange.addColumn(c.name(), t)), false);
                forget0(relid);
                LOG.warning("create for existing table " + id + ": column '" + c.name() + "' "
                    + t.asSQLString() + " was missing from its Paimon schema and has been added"
                    + " (the slot is absent, so its DDL message is lost, not pending; rows"
                    + " written before now hold NULL there)");
            } catch (Exception e) {
                diffs.add("column '" + c.name() + "' is missing from Paimon and could not be"
                    + " added: " + e);
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
     * immediately but reaches the server only at the next status tick, and the resume position is
     * the highest Paimon commit identifier, which a DDL-only transaction never moves -- so a DDL
     * that is the last thing in the stream replays on EVERY restart, clean or not. Paimon's own
     * errors for that replay (ColumnAlreadyExistException, ColumnNotExistException) are not
     * retryable, and the slot only advances after a successful commit, so the writer stopped
     * permanently.
     *
     * The fix is the mechanism data already had, applied to the thing that lacked it. Data replay
     * is safe because filterAndCommit compares a commit IDENTIFIER; DDL had no equivalent. Each
     * DDL message carries its own WAL LSN, which is stable across replays, so the LSN of the last
     * applied DDL message is recorded in the Paimon table's own schema options
     * ({@value #DDL_LSN_OPTION}) -- written in the SAME alterTable call as the column change, so
     * the two cannot diverge (measured against Paimon 1.4.1: a list of SchemaChanges becomes one
     * atomic schema commit, and a failing change in the list leaves the option unwritten too).
     * Any DDL message at or below the recorded LSN is skipped.
     *
     * This is what makes replaying a SEQUENCE safe, which per-op state inspection cannot be:
     * "ADD COLUMN num int" then "ALTER COLUMN num TYPE bigint" leaves a replayed add_col looking
     * at a BIGINT column, indistinguishable from a genuine conflict, and "ADD COLUMN x" then
     * "RENAME x TO y" leaves a replayed add_col re-creating a spurious x for the replayed rename
     * to collide with. The invariant the LSN gate gives by construction: replaying any suffix of
     * an already-applied DDL sequence converges to the same schema and never fails.
     *
     * The state-based tolerances below remain as a second line, but ONLY for a table with no
     * recorded LSN -- one whose DDL was applied before this mechanism existed. A table that has
     * an identifier cannot reach them by replay, because the gate filters a replay first, so
     * reaching them there means genuine drift and must still fail loudly. Keying the tolerance
     * on {@code appliedDdlLsn(relid) == 0} is what keeps a tolerance needed for one case from
     * silencing a real fault in the other.
     */

    /**
     * The bucket count for a table whose caller supplied none. It matches
     * {@code paimon.default_buckets()}'s floor, so such a table is bucketed as an empty table
     * registered normally would be -- and never left in dynamic-bucket mode, which the sink's
     * {@code write(row)} cannot use.
     */
    static final int DEFAULT_BUCKETS = 4;

    /**
     * Schema option holding the WAL LSN of the last DDL message applied to this table.
     *
     * <p>Caveat worth knowing: the value carries no cluster or timeline identity. A warehouse
     * pointed at a different server, or at one restored by PITR or re-{@code initdb}'d, can
     * therefore meet DDL messages whose LSNs sit below the recorded value and skip them as
     * replays. Nothing here detects that; the operations guide says so where operators look.
     */
    static final String DDL_LSN_OPTION = "pgpaimon.applied-ddl-lsn";

    /*
     * DDL fan-out to the history table (spec 2026-10-14 section 3, rule 3). Each DDL message is
     * applied to the main table and then to the history table, each gated by ITS OWN recorded DDL
     * LSN, so a crash between the two leaves one applied and the other to be applied on replay,
     * and the gate sorts out which. add_col adds before_x/after_x; drop_col KEEPS them (history
     * is not rewritten; they read NULL for later rows) and only records the LSN; rename_col
     * renames both; alter_col changes both types.
     *
     * A kept pair is why re-adding a dropped column's name needs care: before_x still holds the
     * old column's history. It is renamed aside to before_x_retired_<lsn> in the same atomic
     * schema change that adds the new pair, so history keeps the old values under a name that
     * says what they are, and the new column starts empty. How a kept pair is told apart from a
     * replay: a drop_col records the LSN on the history table, so a history table carrying no
     * recorded LSN has never had a column dropped and its before_x can only be the create's own
     * (built by the startup bootstrap from the registry while the add_col was still in the slot):
     * that case is "already applied".
     */
    public void addColumn(long relid, DdlMessage.Col col, long ddlLsn) {
        addColumnMain(relid, col, ddlLsn);
        if (hasHistory(relid)) {
            addColumnHist(HistoryKeys.histKey(relid), col, ddlLsn);
        }
    }

    private void addColumnHist(long hk, DdlMessage.Col col, long ddlLsn) {
        if (replayed(hk, ddlLsn, "add_col '" + col.name() + "'")) {
            return;
        }
        DataType t = parseType(col.type()).copy(true);
        String b = LogRowBuilder.BEFORE + col.name();
        String a = LogRowBuilder.AFTER + col.name();
        List<SchemaChange> changes = new ArrayList<>();
        if (field(hk, b) != null || field(hk, a) != null) {
            if (appliedDdlLsn(hk) == 0) {
                LOG.info("add_col '" + col.name() + "' on history table " + identifiers.get(hk)
                    + " is already applied (its columns were created with the table): no-op");
                return;
            }
            changes.addAll(retire(hk, col.name(), ddlLsn));
        }
        changes.add(SchemaChange.addColumn(b, t));
        changes.add(SchemaChange.addColumn(a, t));
        alter(hk, changes, ddlLsn);
    }

    /**
     * Renames a kept before_/after_ pair aside, to {@code <prefix><col>_retired_<lsn>}, where the
     * LSN is that of the DDL message reusing the name: the kept values are the history of a
     * column of this name dropped before that point.
     */
    private List<SchemaChange> retire(long hk, String col, long ddlLsn) {
        String suffix = "_retired_" + Long.toUnsignedString(ddlLsn);
        List<SchemaChange> out = new ArrayList<>();
        for (String prefix : List.of(LogRowBuilder.BEFORE, LogRowBuilder.AFTER)) {
            if (field(hk, prefix + col) == null) {
                continue;
            }
            String to = prefix + col + suffix;
            for (int n = 2; field(hk, to) != null; n++) {
                to = prefix + col + suffix + "_" + n;
            }
            out.add(SchemaChange.renameColumn(prefix + col, to));
            LOG.info("history table " + identifiers.get(hk) + ": column '" + prefix + col
                + "' holds the history of a dropped column of that name; renamed to '" + to
                + "' so the column added now starts empty");
        }
        return out;
    }

    private void addColumnMain(long relid, DdlMessage.Col col, long ddlLsn) {
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
        alter(relid, List.of(SchemaChange.addColumn(col.name(), wanted)), ddlLsn);
    }

    public void dropColumn(long relid, String name, long ddlLsn) {
        dropColumnMain(relid, name, ddlLsn);
        if (hasHistory(relid)) {
            long hk = HistoryKeys.histKey(relid);
            if (!replayed(hk, ddlLsn, "drop_col '" + name + "'") && ddlLsn != 0) {
                // The columns stay: history is not rewritten. Only the LSN is recorded, which is
                // what tells a later add_col of the same name that before_x is a kept pair.
                alter(hk, List.of(), ddlLsn);
                LOG.info("drop_col '" + name + "': history table " + identifiers.get(hk)
                    + " keeps " + LogRowBuilder.BEFORE + name + "/" + LogRowBuilder.AFTER + name
                    + " (NULL from now on)");
            }
        }
    }

    private void dropColumnMain(long relid, String name, long ddlLsn) {
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
    public void renameColumn(long relid, String from, String to, long ddlLsn) {
        renameColumnMain(relid, from, to, ddlLsn);
        if (hasHistory(relid)) {
            renameColumnHist(HistoryKeys.histKey(relid), from, to, ddlLsn);
        }
    }

    private void renameColumnHist(long hk, String from, String to, long ddlLsn) {
        String what = "rename_col '" + from + "' -> '" + to + "'";
        if (replayed(hk, ddlLsn, what)) {
            return;
        }
        String bf = LogRowBuilder.BEFORE + from;
        String bt = LogRowBuilder.BEFORE + to;
        if (field(hk, bf) == null) {
            if (field(hk, bt) != null) {
                LOG.info(what + " on history table " + identifiers.get(hk)
                    + " is already applied: no-op");
                return;
            }
            String msg = what + " on history table " + identifiers.get(hk)
                + ": no column '" + bf + "'";
            requireNoRecordedLsn(hk, msg);
            LOG.warning(msg + "; no recorded DDL LSN, so a later message may have renamed or"
                + " dropped it already: skipped");
            return;
        }
        List<SchemaChange> changes = new ArrayList<>();
        if (field(hk, bt) != null || field(hk, LogRowBuilder.AFTER + to) != null) {
            changes.addAll(retire(hk, to, ddlLsn));    // a dropped column's kept pair
        }
        changes.add(SchemaChange.renameColumn(bf, bt));
        changes.add(SchemaChange.renameColumn(LogRowBuilder.AFTER + from,
                                              LogRowBuilder.AFTER + to));
        alter(hk, changes, ddlLsn);
    }

    private void renameColumnMain(long relid, String from, String to, long ddlLsn) {
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

    /** One field of the table's Paimon schema by name, or null if there is none. */
    private DataField field(long key, String name) {
        for (DataField f : table(key).schema().fields()) {
            if (f.name().equals(name)) {
                return f;
            }
        }
        return null;
    }

    /**
     * A no-op when the stored type already equals the new one (varchar vs text are both STRING).
     * A column the message names and Paimon does not have is logged and skipped rather than
     * thrown: in a replayed sequence the column may since have been renamed or dropped.
     */
    public void alterColumn(long relid, String name, String type, long ddlLsn) {
        alterColumnMain(relid, name, type, ddlLsn);
        if (!hasHistory(relid)) {
            return;
        }
        long hk = HistoryKeys.histKey(relid);
        if (replayed(hk, ddlLsn, "alter_col '" + name + "'")) {
            return;
        }
        DataType wanted = parseType(type).copy(true);
        DataField b = field(hk, LogRowBuilder.BEFORE + name);
        DataField a = field(hk, LogRowBuilder.AFTER + name);
        if (b == null || a == null) {
            String what = "alter_col '" + name + "' on history table " + identifiers.get(hk)
                + ": no " + LogRowBuilder.BEFORE + "/" + LogRowBuilder.AFTER + " pair";
            requireNoRecordedLsn(hk, what);
            LOG.warning(what + "; no recorded DDL LSN: skipped");
            return;
        }
        List<SchemaChange> changes = new ArrayList<>();
        for (DataField f : List.of(b, a)) {
            if (!f.type().equals(wanted)) {
                changes.add(SchemaChange.updateColumnType(f.name(), wanted));
            }
        }
        if (!changes.isEmpty()) {
            alter(hk, changes, ddlLsn);
        }
    }

    private void alterColumnMain(long relid, String name, String type, long ddlLsn) {
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
    private DataType mainType(long relid, String sql) {
        DataType t = parseType(sql);
        return keyless(relid) ? t.copy(true) : t;
    }

    /**
     * Whether this DDL message is at or below the last DDL LSN applied to the table, and so has
     * already been applied. LSNs are compared unsigned. An {@code ddlLsn} of 0 means the caller
     * has no LSN (the startup bootstrap, which replays registry rows rather than messages) and
     * disables the gate.
     */
    private boolean replayed(long relid, long ddlLsn, String what) {
        if (ddlLsn == 0) {
            return false;
        }
        long applied = appliedDdlLsn(relid);
        if (applied != 0 && Long.compareUnsigned(ddlLsn, applied) <= 0) {
            // Both forms: the X/XXXXXXXX one to compare with pg_replication_slots and the
            // writer's own messages, the decimal one because that is what the schema option
            // holds and what an operator reading it back will see.
            LOG.info(what + " on " + identifiers.get(relid) + " at "
                + SlotConsumer.formatLsn(ddlLsn) + " (" + Long.toUnsignedString(ddlLsn)
                + ") is at or below the last applied DDL LSN " + SlotConsumer.formatLsn(applied)
                + " (" + Long.toUnsignedString(applied) + "): replay, no-op");
            return true;
        }
        return false;
    }

    /**
     * Throws unless the table has no recorded DDL LSN. The state-based tolerances exist for a
     * table that predates the identifier; a table that HAS one cannot reach them by replay,
     * because {@link #replayed} filters first, so reaching them means the registry and the
     * Paimon schema genuinely disagree and the writer must say so rather than carry on.
     */
    private void requireNoRecordedLsn(long relid, String what) {
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
    long appliedDdlLsn(long relid) {
        String v = table(relid).schema().options().get(DDL_LSN_OPTION);
        if (v == null) {
            return 0;
        }
        try {
            return Long.parseUnsignedLong(v);
        } catch (NumberFormatException e) {
            LOG.warning("ignoring unparseable " + DDL_LSN_OPTION + " '" + v + "' on "
                + identifiers.get(relid));
            return 0;
        }
    }

    /** Purges the warehouse data. Untracked relids are ignored. */
    public void dropTable(long relid, String schema, String table) {
        dropTable(relid, schema, table, 0);
    }

    /**
     * Purges the warehouse data. Untracked relids are ignored.
     *
     * <p>Gated on the applied-DDL identifier like the column ops, for one reason: a Paimon table
     * is identified by NAME, so a table dropped and recreated under the same name resolves to the
     * same Paimon table. If the recreated table already records an identifier at or above this
     * drop's LSN, the table standing here now was created AFTER the drop was emitted, so the drop
     * is stale and applying it would purge the new table's rows and then have the replayed create
     * rebuild it empty.
     *
     * <p>Today that is reachable only if PostgreSQL reuses the dropped table's OID, because a
     * drop deletes its {@code paimon.table_registry} row in the same transaction as the message,
     * so after any restart the old relid is untracked and {@link DdlMessageHandler} refuses the
     * message before it arrives here (measured: "ignoring drop_table for unregistered relation").
     * Narrow, but the consequence is a whole table's data, and the gate is two comparisons.
     *
     * <p>A table with no recorded identifier keeps the old behaviour: there is nothing to
     * compare, and refusing on that basis would make a drop unreachable.
     */
    public void dropTable(long relid, String schema, String table, long ddlLsn) {
        Identifier id = identifiers.get(relid);
        if (id == null) {
            return;
        }
        if (ddlLsn != 0) {
            long applied = appliedDdlLsn(relid);
            if (applied != 0 && Long.compareUnsigned(applied, ddlLsn) >= 0) {
                LOG.warning("drop_table for " + id + " at "
                    + SlotConsumer.formatLsn(ddlLsn) + " (" + Long.toUnsignedString(ddlLsn)
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
                + " is kept as the audit trail and no longer written");
        }
        forget(relid);
    }

    /** The Paimon table a key names: a relation's main table, or its history table. */
    public FileStoreTable table(long relid) {
        Identifier id = identifiers.get(relid);
        if (id == null) {
            throw new IllegalStateException((HistoryKeys.isHist(relid)
                ? "the history table of relation " + HistoryKeys.relidOf(relid)
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
     * The primary-key column names of a tracked relation, as Paimon holds them (so a renamed
     * key column is followed). This, not pgoutput's Relation flags, defines the key: under
     * REPLICA IDENTITY FULL pgoutput flags every column as a key column.
     */
    public List<String> primaryKey(long relid) {
        return table(relid).primaryKeys();
    }

    /**
     * Cached per (key, column list); a changed list or a reloaded table rebuilds it. The key is a
     * relid (the main table) or a history key; the builder matches the table's shape.
     */
    public RowMaker rowBuilder(long relid, RelationMeta meta) {
        FileStoreTable t = table(relid);
        CachedBuilder cb = builders.get(relid);
        if (cb != null && cb.table() == t && cb.columns().equals(meta.columnNames())) {
            return cb.builder();
        }
        String name = meta.namespace() + "." + meta.relName();
        RowMaker rb;
        if (HistoryKeys.isHist(relid)) {
            rb = new LogRowBuilder(name + " (history)", LogRowBuilder.Layout.HISTORY,
                                   t.rowType(), meta.columnNames());
        } else if (keyless(relid)) {
            rb = new LogRowBuilder(name, LogRowBuilder.Layout.LOG, t.rowType(),
                                   meta.columnNames());
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
    public void forget(long relid) {
        identifiers.remove(relid);
        forget0(relid);
        if (!HistoryKeys.isHist(relid)) {
            long hk = HistoryKeys.histKey(relid);
            identifiers.remove(hk);
            forget0(hk);
            keyless.remove(relid);
        }
    }

    @Override
    public boolean tracked(long relid) {
        return identifiers.containsKey(relid);
    }

    @Override
    public boolean keyless(long relid) {
        return keyless.contains(relid);
    }

    @Override
    public boolean hasHistory(long relid) {
        return relid >= 0 && identifiers.containsKey(HistoryKeys.histKey(relid));
    }

    private void forget0(long relid) {
        tables.remove(relid);
        builders.remove(relid);
    }

    /**
     * One schema change, plus the DDL message's LSN, in a single alterTable call. Paimon turns a
     * list of SchemaChanges into one atomic schema commit, so the column change and the recorded
     * LSN land together or not at all; recording the LSN in a second call would leave a window in
     * which a crash makes the change replay.
     */
    private void alter(long relid, List<SchemaChange> change, long ddlLsn) {
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
    }

    /**
     * Parses a Paimon SQL type string such as {@code "INT NOT NULL"} or {@code "DECIMAL(10, 2)"}
     * with Paimon's own parser. The SQL mapping only ever emits plain TIMESTAMP and
     * TIMESTAMP WITH LOCAL TIME ZONE (precision 6); TextDecoder ignores declared precision, so a
     * precision-qualified timestamp is refused here rather than silently mis-handled.
     */
    static DataType parseType(String sql) {
        DataType t;
        try {
            t = DataTypeJsonParser.parseAtomicTypeSQLString(sql);
        } catch (RuntimeException e) {
            throw new IllegalStateException("unparseable Paimon type '" + sql + "'", e);
        }
        if ((t instanceof TimestampType ts && ts.getPrecision() != TimestampType.DEFAULT_PRECISION)
            || (t instanceof LocalZonedTimestampType lz
                && lz.getPrecision() != LocalZonedTimestampType.DEFAULT_PRECISION)) {
            throw new IllegalStateException("timestamp precision other than 6 is not supported: '"
                + sql + "'");
        }
        return t;
    }
}
