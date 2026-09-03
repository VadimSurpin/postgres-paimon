// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Paimon schema management using LocalFileIO.
 *
 * These tests run without any external services and verify the schema
 * evolution behaviour that paimon_heap relies on: create, add-field,
 * drop-field, rename-field, and type-change.  They also document the
 * "missing column reads as NULL" behaviour that is the root cause of gap #2
 * (default values are not backfilled into historical Parquet files).
 *
 * Run with:  mvn -B test --no-transfer-progress   (from the java/ directory)
 */
class PaimonSchemaIT {

    @TempDir
    File warehouseDir;

    LocalFileIO fileIO;

    @BeforeEach
    void setUp() {
        fileIO = LocalFileIO.create();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private Path tablePath(String name) {
        return new Path(warehouseDir.getAbsolutePath() + "/" + name);
    }

    private SchemaManager schemaManager(String table) {
        return new SchemaManager(fileIO, tablePath(table));
    }

    /** Minimal schema matching what the extension writes for a PK table. */
    private Schema baseSchema() {
        return Schema.newBuilder()
                .column("id",    DataTypes.BIGINT().notNull())
                .column("name",  DataTypes.STRING())
                .column("value", DataTypes.INT())
                .primaryKey("id")
                .option("merge-engine",       "deduplicate")
                .option("changelog-producer", "input")
                .build();
    }

    // ── creation ──────────────────────────────────────────────────────────────

    @Test
    void createTable_writesSchemaZero() throws Exception {
        SchemaManager sm = schemaManager("t_create");
        sm.createTable(baseSchema());

        assertTrue(sm.latest().isPresent(), "schema-0 should exist after createTable");
        assertEquals(0, sm.latest().get().id());
    }

    @Test
    void createTable_primaryKeysPresent() throws Exception {
        SchemaManager sm = schemaManager("t_pk");
        sm.createTable(baseSchema());

        assertEquals(List.of("id"), sm.latest().orElseThrow().primaryKeys());
    }

    @Test
    void createTable_allColumnsPresent() throws Exception {
        SchemaManager sm = schemaManager("t_all_cols");
        sm.createTable(baseSchema());

        TableSchema schema = sm.latest().orElseThrow();
        List<String> names = schema.fields().stream().map(DataField::name).toList();
        assertTrue(names.contains("id"));
        assertTrue(names.contains("name"));
        assertTrue(names.contains("value"));
    }

    @Test
    void createTable_optionsPreserved() throws Exception {
        SchemaManager sm = schemaManager("t_opts");
        sm.createTable(baseSchema());

        TableSchema schema = sm.latest().orElseThrow();
        assertEquals("deduplicate", schema.options().get("merge-engine"));
        assertEquals("input",       schema.options().get("changelog-producer"));
    }

    @Test
    void createTable_noPrimaryKey_allowed() throws Exception {
        Schema noPk = Schema.newBuilder()
                .column("id",   DataTypes.BIGINT())
                .column("data", DataTypes.STRING())
                .build();
        SchemaManager sm = schemaManager("t_no_pk");
        sm.createTable(noPk);

        assertTrue(sm.latest().orElseThrow().primaryKeys().isEmpty());
    }

    // ── ADD COLUMN ────────────────────────────────────────────────────────────

    @Test
    void addColumn_bumpsSchemaId() throws Exception {
        SchemaManager sm = schemaManager("t_add_col_id");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.addColumn("email", DataTypes.STRING()));

        assertEquals(1, sm.latest().orElseThrow().id(),
                "schema id should be 1 after ADD COLUMN");
    }

    @Test
    void addColumn_fieldPresentInLatestSchema() throws Exception {
        SchemaManager sm = schemaManager("t_add_col_present");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.addColumn("email", DataTypes.STRING()));

        boolean found = sm.latest().orElseThrow().fields().stream()
                .anyMatch(f -> f.name().equals("email"));
        assertTrue(found, "email field should appear after ADD COLUMN");
    }

    @Test
    void addColumn_newFieldIsNullable() throws Exception {
        SchemaManager sm = schemaManager("t_add_col_nullable");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.addColumn("email", DataTypes.STRING()));

        DataField emailField = sm.latest().orElseThrow().fields().stream()
                .filter(f -> f.name().equals("email"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("email field not found"));
        assertTrue(emailField.type().isNullable(),
                "ADD COLUMN without DEFAULT must be nullable — old Parquet files will produce NULL");
    }

    @Test
    void addColumn_existingFieldsPreserved() throws Exception {
        SchemaManager sm = schemaManager("t_add_col_existing");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.addColumn("email", DataTypes.STRING()));

        TableSchema schema = sm.latest().orElseThrow();
        List<String> names = schema.fields().stream().map(DataField::name).toList();
        assertTrue(names.contains("id"));
        assertTrue(names.contains("name"));
        assertTrue(names.contains("value"));
        assertTrue(names.contains("email"));
    }

    @Test
    void addColumn_primaryKeyUnchanged() throws Exception {
        SchemaManager sm = schemaManager("t_add_col_pk");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.addColumn("email", DataTypes.STRING()));

        assertEquals(List.of("id"), sm.latest().orElseThrow().primaryKeys());
    }

    @Test
    void addColumn_optionsUnchanged() throws Exception {
        SchemaManager sm = schemaManager("t_add_col_opts");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.addColumn("extra", DataTypes.INT()));

        TableSchema schema = sm.latest().orElseThrow();
        assertEquals("deduplicate", schema.options().get("merge-engine"));
        assertEquals("input",       schema.options().get("changelog-producer"));
    }

    // ── DROP COLUMN ───────────────────────────────────────────────────────────

    @Test
    void dropColumn_fieldRemovedFromSchema() throws Exception {
        SchemaManager sm = schemaManager("t_drop_col_gone");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.dropColumn("value"));

        boolean gone = sm.latest().orElseThrow().fields().stream()
                .noneMatch(f -> f.name().equals("value"));
        assertTrue(gone, "value field should be absent after DROP COLUMN");
    }

    @Test
    void dropColumn_bumpsSchemaId() throws Exception {
        SchemaManager sm = schemaManager("t_drop_col_id");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.dropColumn("value"));

        assertEquals(1, sm.latest().orElseThrow().id());
    }

    @Test
    void dropColumn_otherFieldsAndPkPreserved() throws Exception {
        SchemaManager sm = schemaManager("t_drop_col_preserve");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.dropColumn("value"));

        TableSchema schema = sm.latest().orElseThrow();
        assertTrue(schema.fields().stream().anyMatch(f -> f.name().equals("id")));
        assertTrue(schema.fields().stream().anyMatch(f -> f.name().equals("name")));
        assertEquals(List.of("id"), schema.primaryKeys());
    }

    // ── RENAME COLUMN ─────────────────────────────────────────────────────────

    @Test
    void renameColumn_newNamePresent_oldNameGone() throws Exception {
        SchemaManager sm = schemaManager("t_rename_col");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.renameColumn("name", "display_name"));

        TableSchema schema = sm.latest().orElseThrow();
        assertTrue(schema.fields().stream().anyMatch(f -> f.name().equals("display_name")),
                "display_name should be present after RENAME");
        assertTrue(schema.fields().stream().noneMatch(f -> f.name().equals("name")),
                "name should be absent after RENAME");
    }

    @Test
    void renameColumn_bumpsSchemaId() throws Exception {
        SchemaManager sm = schemaManager("t_rename_id");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.renameColumn("name", "display_name"));

        assertEquals(1, sm.latest().orElseThrow().id());
    }

    // ── ALTER COLUMN TYPE ─────────────────────────────────────────────────────

    @Test
    void updateColumnType_typeChangedInSchema() throws Exception {
        SchemaManager sm = schemaManager("t_type_change");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.updateColumnType("value", DataTypes.BIGINT()));

        String typeStr = sm.latest().orElseThrow().fields().stream()
                .filter(f -> f.name().equals("value"))
                .findFirst()
                .orElseThrow()
                .type().toString().toUpperCase();
        assertTrue(typeStr.contains("BIGINT"),
                "value column should be BIGINT after ALTER COLUMN TYPE");
    }

    @Test
    void updateColumnType_bumpsSchemaId() throws Exception {
        SchemaManager sm = schemaManager("t_type_id");
        sm.createTable(baseSchema());

        sm.commitChanges(SchemaChange.updateColumnType("value", DataTypes.BIGINT()));

        assertEquals(1, sm.latest().orElseThrow().id());
    }

    // ── multiple evolutions ───────────────────────────────────────────────────

    @Test
    void multipleEvolutions_schemaIdIncrementsEachStep() throws Exception {
        SchemaManager sm = schemaManager("t_multi");
        sm.createTable(baseSchema());
        assertEquals(0, sm.latest().orElseThrow().id());

        sm.commitChanges(SchemaChange.addColumn("a", DataTypes.STRING()));
        assertEquals(1, sm.latest().orElseThrow().id());

        sm.commitChanges(SchemaChange.addColumn("b", DataTypes.INT()));
        assertEquals(2, sm.latest().orElseThrow().id());

        sm.commitChanges(SchemaChange.dropColumn("a"));
        assertEquals(3, sm.latest().orElseThrow().id());

        sm.commitChanges(SchemaChange.renameColumn("b", "b_renamed"));
        assertEquals(4, sm.latest().orElseThrow().id());
    }

    @Test
    void multipleEvolutions_highestFieldIdMonotonicallyIncreases() throws Exception {
        SchemaManager sm = schemaManager("t_highest_id");
        sm.createTable(baseSchema());
        long prev = sm.latest().orElseThrow().highestFieldId();

        sm.commitChanges(SchemaChange.addColumn("x", DataTypes.STRING()));
        long after1 = sm.latest().orElseThrow().highestFieldId();
        assertTrue(after1 > prev, "highestFieldId must increase on ADD COLUMN");

        sm.commitChanges(SchemaChange.addColumn("y", DataTypes.INT()));
        long after2 = sm.latest().orElseThrow().highestFieldId();
        assertTrue(after2 > after1, "highestFieldId must increase again on second ADD COLUMN");
    }

    // ── gap #2 documentation ──────────────────────────────────────────────────

    /**
     * Documents gap #2: after ADD COLUMN, the new field is nullable in the
     * schema (no default), which is exactly what Paimon delivers for rows in
     * old Parquet files that pre-date the column addition.  Paimon fills the
     * missing column with NULL; there is no mechanism to supply a default value
     * from the schema at read time.
     */
    @Test
    void addColumn_noDefaultInSchema_documentingGap2() throws Exception {
        SchemaManager sm = schemaManager("t_gap2");
        sm.createTable(baseSchema());

        // Simulate what paimon_heap bgworker does: ddlAddField writes schema-1
        // with the new column, but NO default expression is stored.
        sm.commitChanges(SchemaChange.addColumn("email", DataTypes.STRING()));

        DataField emailField = sm.latest().orElseThrow().fields().stream()
                .filter(f -> f.name().equals("email"))
                .findFirst()
                .orElseThrow();

        // The field has no default expression in the Paimon schema metadata.
        // Old Parquet files that pre-date the ADD COLUMN will return NULL for
        // this field when read through Spark — regardless of any defaultValue
        // key set in the schema JSON (Paimon 1.4.1 does not apply it).
        assertTrue(emailField.type().isNullable(),
                "email column must be nullable — old rows will produce NULL");
    }
}
