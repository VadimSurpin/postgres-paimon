// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
 */
public final class SchemaRegistry {

    private static final Logger LOG = Logger.getLogger(SchemaRegistry.class.getName());

    private final Catalog catalog;
    private final WriterConfig config;
    private final Map<Long, Identifier> identifiers = new ConcurrentHashMap<>();
    private final Map<Long, FileStoreTable> tables = new ConcurrentHashMap<>();
    private final Map<Long, CachedBuilder> builders = new ConcurrentHashMap<>();

    private record CachedBuilder(List<String> columns, FileStoreTable table, RowBuilder builder) {}

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
     * (extension/paimon_schema.cpp:131). Column names are the contract, which is why RowBuilder
     * maps by name.
     *
     * <p>No {@code bucket} option is set, deliberately (extension/paimon_schema.cpp:219-223):
     * a primary-key table is then in dynamic bucket mode and the sink must write(row, 0).
     */
    public void createTable(long relid, String schema, String table, List<String> pk,
                            List<DdlMessage.Col> cols) {
        createTable(relid, schema, table, pk, cols, "input");
    }

    /** As above with an explicit changelog-producer (taken from the message's options). */
    public void createTable(long relid, String schema, String table, List<String> pk,
                            List<DdlMessage.Col> cols, String changelogProducer) {
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

        Identifier id = Identifier.create(config.database(), table);
        Table existing = null;
        try {
            catalog.createDatabase(config.database(), true);
            Schema wanted = b.build();
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
        forget0(relid);
        if (existing != null) {
            reconcileOrWarn(relid, id, existing, pk, cols);
        }
    }

    /**
     * A repeated create for an existing table (the restart bootstrap replays every registry row).
     *
     * <p>Columns the create message declares and Paimon lacks are <strong>applied</strong>, one
     * {@code addColumn} each, with a WARNING naming the column. Warning alone was not enough: a
     * DDL message lost while the writer was down (a dropped slot, the re-bootstrap procedure in
     * the operations guide) leaves the Paimon schema permanently behind, and {@link RowBuilder}
     * then discards that column's value from every row for ever, with nothing but two warnings to
     * show for it. The registry is rebuilt from {@code paimon.table_registry}, which is updated in
     * the same transaction as the DDL, so on this one difference the create message is the
     * authority and Paimon is simply behind.
     *
     * <p>Everything else is a WARNING and nothing more, because there is no way to tell which side
     * is right: a column present in both with different types, a different primary key, a column
     * Paimon has that the message does not.
     *
     * <p>Never throws, and a reconciliation that fails degrades to a warning: drift must not block
     * a restart.
     */
    private void reconcileOrWarn(long relid, Identifier id, Table existing, List<String> pk,
                                 List<DdlMessage.Col> cols) {
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
            } else if (!t.equals(parseType(c.type()))) {
                diffs.add("column '" + c.name() + "' is " + t.asSQLString()
                    + " in Paimon but " + parseType(c.type()).asSQLString()
                    + " in the create message");
            }
        }
        have.remove(config.seqField());
        have.remove(config.opSeqField());
        if (config.xidField() != null) {
            have.remove(config.xidField());
        }
        for (String extra : have.keySet()) {
            diffs.add("column '" + extra + "' exists in Paimon but not in the create message");
        }
        for (DdlMessage.Col c : missing) {
            DataType t = parseType(c.type());
            try {
                catalog.alterTable(id, List.of(SchemaChange.addColumn(c.name(), t)), false);
                forget0(relid);
                LOG.warning("create for existing table " + id + ": column '" + c.name() + "' "
                    + t.asSQLString() + " was missing from its Paimon schema and has been added"
                    + " (a DDL message for it never reached this writer; rows written before now"
                    + " hold NULL there)");
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
     * The state-based tolerances below remain as a second line for a table with no recorded LSN
     * (one whose DDL was applied before this mechanism existed, or reconciled at bootstrap). They
     * never throw: a column that already exists with a different type is logged and left alone,
     * because in the replay case the stored type is the later, correct one.
     */

    /** Schema option holding the WAL LSN of the last DDL message applied to this table. */
    static final String DDL_LSN_OPTION = "pgpaimon.applied-ddl-lsn";

    public void addColumn(long relid, DdlMessage.Col col, long ddlLsn) {
        if (replayed(relid, ddlLsn, "add_col '" + col.name() + "'")) {
            return;
        }
        DataType wanted = parseType(col.type());
        DataField existing = field(relid, col.name());
        if (existing != null) {
            if (existing.type().equals(wanted)) {
                LOG.info("add_col '" + col.name() + "' on " + identifiers.get(relid)
                    + " is already applied as " + wanted.asSQLString() + ": no-op");
            } else {
                // Not a throw, deliberately. A replayed add superseded by a replayed alter_col
                // looks exactly like this, and the stored type is then the later, correct one.
                LOG.warning("add_col '" + col.name() + "' on " + identifiers.get(relid)
                    + ": the column already exists as " + existing.type().asSQLString()
                    + " but the message declares " + wanted.asSQLString()
                    + "; leaving the stored type alone");
            }
            return;
        }
        alter(relid, SchemaChange.addColumn(col.name(), wanted), ddlLsn);
    }

    public void dropColumn(long relid, String name, long ddlLsn) {
        if (replayed(relid, ddlLsn, "drop_col '" + name + "'")) {
            return;
        }
        if (field(relid, name) == null) {
            LOG.info("drop_col '" + name + "' on " + identifiers.get(relid)
                + " is already applied (no such column): no-op");
            return;
        }
        alter(relid, SchemaChange.dropColumn(name), ddlLsn);
    }

    /** A rename is already applied when the source is gone and the target is there. */
    public void renameColumn(long relid, String from, String to, long ddlLsn) {
        if (replayed(relid, ddlLsn, "rename_col '" + from + "' -> '" + to + "'")) {
            return;
        }
        if (field(relid, from) == null && field(relid, to) != null) {
            LOG.info("rename_col '" + from + "' -> '" + to + "' on " + identifiers.get(relid)
                + " is already applied: no-op");
            return;
        }
        alter(relid, SchemaChange.renameColumn(from, to), ddlLsn);
    }

    /** One field of the relation's Paimon schema by name, or null if there is none. */
    private DataField field(long relid, String name) {
        for (DataField f : table(relid).schema().fields()) {
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
        if (replayed(relid, ddlLsn, "alter_col '" + name + "'")) {
            return;
        }
        DataType wanted = parseType(type);
        DataField current = field(relid, name);
        if (current == null) {
            LOG.warning("alter_col '" + name + "' on " + identifiers.get(relid)
                + ": no such column in the Paimon schema; skipped (renamed or dropped since)");
            return;
        }
        if (current.type().equals(wanted)) {
            return;
        }
        alter(relid, SchemaChange.updateColumnType(name, wanted), ddlLsn);
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
            LOG.info(what + " on " + identifiers.get(relid) + " at "
                + SlotConsumer.formatLsn(ddlLsn) + " is at or below the last applied DDL LSN "
                + SlotConsumer.formatLsn(applied) + ": replay, no-op");
            return true;
        }
        return false;
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
        Identifier id = identifiers.get(relid);
        if (id == null) {
            return;
        }
        try {
            catalog.dropTable(id, true);
        } catch (Exception e) {
            throw new IllegalStateException("cannot drop Paimon table " + id, e);
        }
        forget(relid);
    }

    public FileStoreTable table(long relid) {
        Identifier id = identifiers.get(relid);
        if (id == null) {
            throw new IllegalStateException("relation " + relid + " is not tracked");
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

    /** Cached per (relid, column list); a changed list or a reloaded table rebuilds it. */
    public RowBuilder rowBuilder(long relid, RelationMeta meta) {
        FileStoreTable t = table(relid);
        CachedBuilder cb = builders.get(relid);
        if (cb != null && cb.table() == t && cb.columns().equals(meta.columnNames())) {
            return cb.builder();
        }
        RowBuilder rb = new RowBuilder(meta.namespace() + "." + meta.relName(), t.rowType(), meta.columnNames(), config.seqField(),
                                       config.opSeqField(), config.xidField());
        builders.put(relid, new CachedBuilder(List.copyOf(meta.columnNames()), t, rb));
        return rb;
    }

    /** Drops cached state (and tracking) so the next access reloads or re-creates. */
    public void forget(long relid) {
        identifiers.remove(relid);
        forget0(relid);
    }

    public boolean tracked(long relid) {
        return identifiers.containsKey(relid);
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
    private void alter(long relid, SchemaChange change, long ddlLsn) {
        Identifier id = identifiers.get(relid);
        if (id == null) {
            throw new IllegalStateException("relation " + relid + " is not tracked");
        }
        List<SchemaChange> changes = ddlLsn == 0
            ? List.of(change)
            : List.of(change, SchemaChange.setOption(DDL_LSN_OPTION,
                                                     Long.toUnsignedString(ddlLsn)));
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
