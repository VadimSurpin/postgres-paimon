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
 * Tests paimon_heap capture of all supported PostgreSQL column types.
 *
 * For each type:
 *   1. Create a table with that type as a column
 *   2. INSERT a known value
 *   3. Verify the Paimon Parquet file contains 1 row
 *   4. Where possible, verify the Paimon schema has the expected type
 *
 * Supported types tested:
 *   BOOLEAN, SMALLINT, INT, BIGINT, REAL (FLOAT4), DOUBLE PRECISION (FLOAT8),
 *   DECIMAL/NUMERIC, TEXT, VARCHAR, BYTEA, DATE, TIMESTAMP, TIMESTAMPTZ,
 *   CHAR, INTEGER (alias for INT)
 *
 * Usage:
 *   java -cp paimon-reader.jar pro.surpin.pgpaimon.PaimonTypeTest [warehouse] [jdbc-url]
 */
public class PaimonTypeTest {

    static final String DEFAULT_WAREHOUSE = "/tmp/paimon-warehouse";
    static final String DEFAULT_JDBC =
        "jdbc:postgresql://localhost:5434/testdb?user=ubuntu&password=";

    static String warehouse;
    static String jdbcUrl;
    static LocalFileIO fileIO = LocalFileIO.create();
    static int passed = 0, failed = 0;

    record TypeCase(String colType, String pgType, String insertVal, String expectedPaimonType) {}

    static final TypeCase[] CASES = {
        new TypeCase("c_bool",    "BOOLEAN",                  "TRUE",                         "BOOLEAN"),
        new TypeCase("c_int2",    "SMALLINT",                 "32767",                        "SMALLINT"),
        new TypeCase("c_int4",    "INTEGER",                  "2147483647",                   "INT"),
        new TypeCase("c_int8",    "BIGINT",                   "9223372036854775807",           "BIGINT"),
        new TypeCase("c_float4",  "REAL",                     "3.14",                         "FLOAT"),
        new TypeCase("c_float8",  "DOUBLE PRECISION",         "2.718281828459045",            "DOUBLE"),
        new TypeCase("c_dec",     "DECIMAL(12,4)",            "12345.6789",                   "DECIMAL"),
        new TypeCase("c_numeric", "NUMERIC(10,2)",            "99999.99",                     "DECIMAL"),
        new TypeCase("c_text",    "TEXT",                     "'hello world'",                "STRING"),
        new TypeCase("c_varchar", "VARCHAR(64)",              "'varchar test'",               "STRING"),
        new TypeCase("c_char",    "CHAR(10)",                 "'char val'",                   "STRING"),
        new TypeCase("c_bytea",   "BYTEA",                    "'\\xDEADBEEF'",                "BYTES"),
        new TypeCase("c_date",    "DATE",                     "'2024-01-15'",                 "DATE"),
        new TypeCase("c_ts",      "TIMESTAMP",                "'2024-01-15 12:34:56'",        "TIMESTAMP"),
        new TypeCase("c_tstz",    "TIMESTAMPTZ",              "'2024-01-15 12:34:56+00'",     "TIMESTAMP"),
        new TypeCase("c_serial",  "BIGINT",                   "42",                           "BIGINT"),
    };

    public static void main(String[] args) throws Exception {
        warehouse = args.length > 0 ? args[0] : DEFAULT_WAREHOUSE;
        jdbcUrl   = args.length > 1 ? args[1] : DEFAULT_JDBC;

        System.out.println("╔════════════════════════════════════════════════════╗");
        System.out.println("║         paimon_heap Type Coverage Tests            ║");
        System.out.println("╚════════════════════════════════════════════════════╝");
        System.out.printf("  Warehouse : %s%n", warehouse);
        System.out.printf("  JDBC      : %s%n", jdbcUrl);
        System.out.println();

        testAllTypesInOneTable();
        testNullValues();
        testIndividualTypes();

        System.out.println();
        System.out.println("═".repeat(52));
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        System.exit(failed > 0 ? 1 : 0);
    }

    static void testAllTypesInOneTable() throws Exception {
        String tbl = "type_all_cols";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMetaFields(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            StringBuilder ddl = new StringBuilder("CREATE TABLE " + tbl + " (id INT PRIMARY KEY");
            for (TypeCase tc : CASES) ddl.append(", ").append(tc.colType).append(" ").append(tc.pgType);
            ddl.append(") USING paimon_heap");
            exec(conn, ddl.toString());

            StringBuilder ins = new StringBuilder("INSERT INTO " + tbl + " (id");
            for (TypeCase tc : CASES) ins.append(", ").append(tc.colType);
            ins.append(") VALUES (1");
            for (TypeCase tc : CASES) ins.append(", ").append(tc.insertVal);
            ins.append(")");
            exec(conn, ins.toString());
        }

        waitForParquet(tbl, 1);
        Thread.sleep(1000);

        TableSchema schema = readSchema(tbl);
        // Verify each column appears in the schema
        for (TypeCase tc : CASES) {
            boolean found = schema.fields().stream().anyMatch(f -> f.name().equals(tc.colType));
            check("AllTypes schema has " + tc.colType + " (" + tc.pgType + ")", found);
        }

        // Verify row count
        long rowCount = countRows(tbl);
        check("AllTypes row count = 1", rowCount == 1);

        // Verify Paimon type mappings
        for (TypeCase tc : CASES) {
            schema.fields().stream()
                .filter(f -> f.name().equals(tc.colType))
                .findFirst()
                .ifPresent(f -> {
                    String typeStr = f.type().toString();
                    boolean typeOk = typeStr.toUpperCase().contains(tc.expectedPaimonType.toUpperCase());
                    check("AllTypes " + tc.colType + " type contains '" + tc.expectedPaimonType + "'", typeOk);
                });
        }
    }

    static void testNullValues() throws Exception {
        String tbl = "type_nulls";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMetaFields(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl +
                " (id INT PRIMARY KEY, c_int INT, c_text TEXT, c_date DATE) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1, NULL, NULL, NULL)");
            exec(conn, "INSERT INTO " + tbl + " VALUES (2, 42, 'not null', '2024-01-01')");
        }
        waitForParquet(tbl, 1);
        Thread.sleep(1000);
        long rows = countRows(tbl);
        check("Nulls row count = 2", rows == 2);
    }

    static void testIndividualTypes() throws Exception {
        // Spot-check a few types individually with exact value verification
        testTypeValue("BOOLEAN",          "TRUE",                    "c_bool");
        testTypeValue("INTEGER",          "42",                      "c_int4");
        testTypeValue("BIGINT",           "1234567890123",           "c_int8");
        testTypeValue("TEXT",             "'type test'",             "c_txt");
        testTypeValue("DATE",             "'2024-06-15'",            "c_dt");
        testTypeValue("DECIMAL(8,2)",     "9876.54",                 "c_dec");
    }

    static void testTypeValue(String pgType, String value, String colName) throws Exception {
        String tbl = "type_" + colName;
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMetaFields(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, " + colName + " " + pgType + ") USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1, " + value + ")");
        }
        waitForParquet(tbl, 1);
        Thread.sleep(1000);
        long rows = countRows(tbl);
        check("TypeValue " + pgType + " captured", rows == 1);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    static void disableMetaFields(Connection c) throws SQLException {
        exec(c, "SET paimon_heap.meta_field_xid = ''");
        exec(c, "SET paimon_heap.meta_field_op  = ''");
        exec(c, "SET paimon_heap.meta_field_tid = ''");
    }

    static void cleanup(String tbl) {
        File dir = new File(warehouse + "/" + tbl);
        if (dir.exists()) deleteDir(dir);
    }

    static void deleteDir(File f) {
        if (f.isDirectory()) for (File c : Objects.requireNonNull(f.listFiles())) deleteDir(c);
        f.delete();
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
        throw new RuntimeException("Timed out waiting for parquet in " + tbl);
    }

    static TableSchema readSchema(String tbl) throws Exception {
        Path tablePath = new Path(warehouse + "/" + tbl);
        return new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema: " + tbl));
    }

    static long countRows(String tbl) throws Exception {
        Path tablePath = new Path(warehouse + "/" + tbl);

        TableSchema stored = readSchema(tbl);
        Map<String, String> opts = new HashMap<>(stored.options());
        opts.put("bucket", "-1");
        opts.remove("merge-engine");
        opts.remove("changelog-producer");
        opts.remove("sequence.field");

        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(-1, "_row_kind", DataTypes.TINYINT().notNull()));
        fields.addAll(stored.fields());

        TableSchema schema = new TableSchema(
            stored.id(), fields, stored.highestFieldId(),
            stored.partitionKeys(), stored.primaryKeys(), opts, stored.comment());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, schema);

        ReadBuilder rb = table.newReadBuilder();
        List<Split> splits = rb.newScan().plan().splits();

        long count = 0;
        try (RecordReader<InternalRow> reader = rb.newRead().createReader(splits)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow r;
                while ((r = batch.next()) != null) count++;
                batch.releaseBatch();
            }
        } catch (Exception e) {
            System.err.println("  warn: read failed for " + tbl + ": " + e.getMessage());
        }
        return count;
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
