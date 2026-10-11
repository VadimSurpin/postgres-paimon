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
        try {
            catalog.createDatabase(config.database(), true);
            Schema wanted = b.build();
            Table existing = null;
            try {
                existing = catalog.getTable(id);
            } catch (Catalog.TableNotExistException e) {
                // normal first-time case
            }
            if (existing != null) {
                warnOnDrift(id, existing, pk, cols);
            }
            catalog.createTable(id, wanted, true);
        } catch (Exception e) {
            throw new IllegalStateException("cannot create Paimon table " + id, e);
        }
        identifiers.put(relid, id);
        forget0(relid);
    }

    /**
     * A repeated create for an existing table stays a no-op (restart bootstrap replays every
     * registry row), but disagreement between the incoming definition and the stored schema is
     * logged as a WARNING naming each difference. Never throws: drift must not block a restart.
     */
    private void warnOnDrift(Identifier id, Table existing, List<String> pk,
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
        for (DdlMessage.Col c : cols) {
            DataType t = have.remove(c.name());
            if (t == null) {
                diffs.add("column '" + c.name() + "' is missing from Paimon");
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
        if (!diffs.isEmpty()) {
            LOG.warning("create for existing table " + id + " differs from its Paimon schema: "
                + String.join("; ", diffs));
        }
    }

    public void addColumn(long relid, DdlMessage.Col col) {
        alter(relid, SchemaChange.addColumn(col.name(), parseType(col.type())));
    }

    public void dropColumn(long relid, String name) {
        alter(relid, SchemaChange.dropColumn(name));
    }

    public void renameColumn(long relid, String from, String to) {
        alter(relid, SchemaChange.renameColumn(from, to));
    }

    /** A no-op when the stored type already equals the new one (varchar vs text are both STRING). */
    public void alterColumn(long relid, String name, String type) {
        DataType wanted = parseType(type);
        DataField current = table(relid).schema().fields().stream()
            .filter(f -> f.name().equals(name)).findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "no column '" + name + "' in " + identifiers.get(relid)));
        if (current.type().equals(wanted)) {
            return;
        }
        alter(relid, SchemaChange.updateColumnType(name, wanted));
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

    private void alter(long relid, SchemaChange change) {
        Identifier id = identifiers.get(relid);
        if (id == null) {
            throw new IllegalStateException("relation " + relid + " is not tracked");
        }
        try {
            catalog.alterTable(id, List.of(change), false);
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
