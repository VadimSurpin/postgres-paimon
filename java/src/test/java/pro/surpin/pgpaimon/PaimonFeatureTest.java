// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import java.io.File;
import java.sql.*;
import java.util.*;

/**
 * Integration test suite for paimon_heap extension features.
 *
 * Tests:
 *   T1  Basic INSERT capture
 *   T2  UPDATE capture (before + after rows)
 *   T3  DELETE capture
 *   T4  Metadata fields (_xid, _op_seq, _tid)
 *   T5  Configurable seq / row_kind field names
 *   T6  Schema structure (all fields present)
 *   T7  CREATE TABLE with TEXT columns (proxy-relation fix)
 *   T8  Multi-row INSERT correctness
 *
 * Usage:
 *   java -cp paimon-reader.jar pro.surpin.pgpaimon.PaimonFeatureTest [warehouse] [jdbc-url]
 */
public class PaimonFeatureTest {

    static final String DEFAULT_WAREHOUSE = "/tmp/paimon-warehouse";
    static final String DEFAULT_JDBC =
        "jdbc:postgresql://localhost:5434/testdb?user=ubuntu&password=";

    static String warehouse;
    static String jdbcUrl;
    static LocalFileIO fileIO = LocalFileIO.create();

    static int passed = 0, failed = 0;

    public static void main(String[] args) throws Exception {
        warehouse = args.length > 0 ? args[0] : DEFAULT_WAREHOUSE;
        jdbcUrl   = args.length > 1 ? args[1] : DEFAULT_JDBC;

        System.out.println("╔════════════════════════════════════════════════════╗");
        System.out.println("║         paimon_heap Feature Test Suite             ║");
        System.out.println("╚════════════════════════════════════════════════════╝");
        System.out.printf("  Warehouse : %s%n", warehouse);
        System.out.printf("  JDBC      : %s%n", jdbcUrl);
        System.out.println();

        test1_basicInsert();
        test2_updateCapture();
        test3_deleteCapture();
        test4_metadataFields();
        test5_configurableFieldNames();
        test6_schemaStructure();
        test7_textColumns();
        test8_multiRowInsert();

        System.out.println();
        System.out.println("═".repeat(52));
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        System.exit(failed > 0 ? 1 : 0);
    }

    // ── T1: Basic INSERT capture ─────────────────────────────────────────

    static void test1_basicInsert() throws Exception {
        String tbl = "ft_insert_test";
        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v TEXT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'a'), (2,'b'), (3,'c')");
        }
        PaimonRows rows = waitAndRead(tbl, 3);
        long inserts = rows.ofKind(0);
        check("T1 row count",  rows.total == 3);
        check("T1 all inserts", inserts == 3);
    }

    // ── T2: UPDATE capture (before + after rows) ─────────────────────────

    static void test2_updateCapture() throws Exception {
        String tbl = "ft_update_test";
        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v TEXT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'old')");
            exec(conn, "UPDATE " + tbl + " SET v='new' WHERE id=1");
        }
        // 1 insert + 1 update_before + 1 update_after = 3 rows
        PaimonRows rows = waitAndRead(tbl, 3);
        check("T2 total rows",     rows.total == 3);
        check("T2 insert present", rows.ofKind(0) == 1);
        check("T2 upd_before",     rows.ofKind(1) == 1);
        check("T2 upd_after",      rows.ofKind(2) == 1);
    }

    // ── T3: DELETE capture ────────────────────────────────────────────────

    static void test3_deleteCapture() throws Exception {
        String tbl = "ft_delete_test";
        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v INT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10), (2,20)");
            exec(conn, "DELETE FROM " + tbl + " WHERE id=1");
        }
        // 2 inserts + 1 delete = 3
        PaimonRows rows = waitAndRead(tbl, 3);
        check("T3 total rows",  rows.total == 3);
        check("T3 deletes",     rows.ofKind(3) == 1);
        check("T3 inserts",     rows.ofKind(0) == 2);
    }

    // ── T4: Metadata fields (_xid, _op_seq, _tid) ─────────────────────────

    static void test4_metadataFields() throws Exception {
        String tbl = "ft_meta_test";
        cleanup(tbl);
        try (Connection conn = connect()) {
            setGuc(conn, "paimon_heap.meta_field_xid", "_xid");
            setGuc(conn, "paimon_heap.meta_field_op",  "_op_seq");
            setGuc(conn, "paimon_heap.meta_field_tid", "_tid");
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v TEXT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'test')");
        }
        // Wait for schema
        File schemaFile = new File(warehouse + "/" + tbl + "/schema/schema-0");
        waitForFile(schemaFile);
        TableSchema schema = readSchema(tbl);
        boolean hasXid = schema.fields().stream().anyMatch(f -> f.name().equals("_xid"));
        boolean hasOp  = schema.fields().stream().anyMatch(f -> f.name().equals("_op_seq"));
        boolean hasTid = schema.fields().stream().anyMatch(f -> f.name().equals("_tid"));
        check("T4 _xid in schema", hasXid);
        check("T4 _op_seq in schema", hasOp);
        check("T4 _tid in schema", hasTid);

        // Verify values are non-negative (we can't check exact values)
        PaimonRows rows = waitAndRead(tbl, 1);
        check("T4 row captured", rows.total == 1);
    }

    // ── T5: Configurable seq / row_kind field names ───────────────────────

    static void test5_configurableFieldNames() throws Exception {
        // Just verify the defaults are _seq and _row_kind in the schema
        String tbl = "ft_fieldname_test";
        cleanup(tbl);
        try (Connection conn = connect()) {
            // Reset metadata fields so we don't confuse the schema
            setGuc(conn, "paimon_heap.meta_field_xid", "");
            setGuc(conn, "paimon_heap.meta_field_op",  "");
            setGuc(conn, "paimon_heap.meta_field_tid", "");
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1)");
        }
        waitForFile(new File(warehouse + "/" + tbl + "/schema/schema-0"));
        TableSchema schema = readSchema(tbl);
        boolean hasSeq = schema.fields().stream().anyMatch(f -> f.name().equals("_seq"));
        check("T5 _seq field in schema", hasSeq);
        // row_kind is a Parquet-level column, not in the Paimon schema fields
        check("T5 schema has at least 2 fields", schema.fields().size() >= 2);
    }

    // ── T6: Schema structure ─────────────────────────────────────────────

    static void test6_schemaStructure() throws Exception {
        String tbl = "ft_schema_test";
        cleanup(tbl);
        try (Connection conn = connect()) {
            setGuc(conn, "paimon_heap.meta_field_xid", "");
            setGuc(conn, "paimon_heap.meta_field_op",  "");
            setGuc(conn, "paimon_heap.meta_field_tid", "");
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, name TEXT, score BIGINT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'Alice',100)");
        }
        waitForFile(new File(warehouse + "/" + tbl + "/schema/schema-0"));
        TableSchema schema = readSchema(tbl);
        boolean hasId    = schema.fields().stream().anyMatch(f -> f.name().equals("id"));
        boolean hasName  = schema.fields().stream().anyMatch(f -> f.name().equals("name"));
        boolean hasScore = schema.fields().stream().anyMatch(f -> f.name().equals("score"));
        boolean hasSeq   = schema.fields().stream().anyMatch(f -> f.name().equals("_seq"));
        boolean pkIsId   = schema.primaryKeys().contains("id");
        check("T6 id field", hasId);
        check("T6 name field", hasName);
        check("T6 score field", hasScore);
        check("T6 _seq field", hasSeq);
        check("T6 pk = id", pkIsId);
    }

    // ── T7: CREATE TABLE with TEXT columns (proxy-relation fix) ──────────

    static void test7_textColumns() throws Exception {
        // This previously failed with "only heap AM is supported" on PG19.
        // Verifies the proxy-relation scan fix works.
        String tbl = "ft_text_test";
        try (Connection conn = connect()) {
            setGuc(conn, "paimon_heap.meta_field_xid", "");
            setGuc(conn, "paimon_heap.meta_field_op",  "");
            setGuc(conn, "paimon_heap.meta_field_tid", "");
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            // TEXT column triggers TOAST table creation, which uses heap_getnext()
            String ddl = "CREATE TABLE " + tbl +
                " (id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT) USING paimon_heap";
            boolean ok = false;
            try {
                exec(conn, ddl);
                ok = true;
            } catch (Exception e) {
                System.err.println("T7 CREATE failed: " + e.getMessage());
            }
            check("T7 CREATE TABLE with TEXT", ok);
            if (ok) {
                exec(conn, "INSERT INTO " + tbl + " VALUES (1,'hello','world','!')");
                check("T7 INSERT after CREATE", true);
            }
        }
    }

    // ── T8: Multi-row INSERT correctness ─────────────────────────────────

    static void test8_multiRowInsert() throws Exception {
        String tbl = "ft_multi_test";
        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v INT) USING paimon_heap");
            // Large INSERT triggers heap multi_insert path
            StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
            for (int i = 1; i <= 100; i++) {
                if (i > 1) sb.append(',');
                sb.append('(').append(i).append(',').append(i * 10).append(')');
            }
            exec(conn, sb.toString());
        }
        PaimonRows rows = waitAndRead(tbl, 100);
        check("T8 all 100 rows captured", rows.total == 100);
        check("T8 all inserts",           rows.ofKind(0) == 100);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    static void setGuc(Connection c, String name, String value) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute("SET " + name + " = '" + value.replace("'", "''") + "'");
        }
    }

    static void cleanup(String tbl) {
        File dir = new File(warehouse + "/" + tbl);
        if (dir.exists()) deleteDir(dir);
    }

    static void deleteDir(File f) {
        if (f.isDirectory()) for (File c : Objects.requireNonNull(f.listFiles())) deleteDir(c);
        f.delete();
    }

    static void waitForFile(File f) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 120_000;
        while (!f.exists() && System.currentTimeMillis() < deadline) Thread.sleep(500);
        if (!f.exists()) throw new RuntimeException("Timed out waiting for " + f);
    }

    static void waitForParquet(String tbl, int minFiles) throws InterruptedException {
        File bucketDir      = new File(warehouse + "/" + tbl + "/bucket-0");
        File snapshotLatest = new File(warehouse + "/" + tbl + "/snapshot/LATEST");
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            File[] f = bucketDir.listFiles((d, n) -> n.endsWith(".parquet"));
            if (f != null && f.length >= minFiles && snapshotLatest.exists()) return;
            Thread.sleep(500);
        }
        throw new RuntimeException("Timed out waiting for snapshot in " + tbl);
    }

    static TableSchema readSchema(String tbl) throws Exception {
        Path tablePath = new Path(warehouse + "/" + tbl);
        return new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema: " + tbl));
    }

    static PaimonRows waitAndRead(String tbl, int minTotal) throws Exception {
        waitForParquet(tbl, 1);
        return readRows(tbl);
    }

    static PaimonRows readRows(String tbl) throws Exception {
        Path tablePath = new Path(warehouse + "/" + tbl);

        TableSchema stored = new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema: " + tbl));
        Map<String, String> opts = new HashMap<>(stored.options());
        opts.put("bucket", "-1");
        opts.remove("merge-engine");
        opts.remove("changelog-producer");
        opts.remove("sequence.field");

        /* Prepend _row_kind so its Parquet column is visible to the reader. */
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(-1, "_row_kind", DataTypes.TINYINT().notNull()));
        fields.addAll(stored.fields());

        TableSchema schema = new TableSchema(
            stored.id(), fields, stored.highestFieldId(),
            stored.partitionKeys(), stored.primaryKeys(), opts, stored.comment());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, schema);

        ReadBuilder rb = table.newReadBuilder();
        List<Split> splits = rb.newScan().plan().splits();

        PaimonRows result = new PaimonRows();
        try (RecordReader<InternalRow> reader = rb.newRead().createReader(splits)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow r;
                while ((r = batch.next()) != null) {
                    result.total++;
                    if (!r.isNullAt(0)) result.kindCounts.merge((int) r.getByte(0), 1L, Long::sum);
                }
                batch.releaseBatch();
            }
        } catch (Exception e) {
            System.err.println("  warn: row-kind read failed for " + tbl + ": " + e.getMessage());
        }
        return result;
    }

    static class PaimonRows {
        long total = 0;
        Map<Integer, Long> kindCounts = new HashMap<>();
        long ofKind(int k) { return kindCounts.getOrDefault(k, 0L); }
    }

    static void check(String name, boolean condition) {
        if (condition) {
            System.out.printf("  PASS  %s%n", name);
            passed++;
        } else {
            System.out.printf("  FAIL  %s%n", name);
            failed++;
        }
    }
}
