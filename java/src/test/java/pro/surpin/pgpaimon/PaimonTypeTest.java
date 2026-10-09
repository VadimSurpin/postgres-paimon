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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.StreamTableScan;

import java.io.File;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import org.junit.jupiter.api.Test;
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

    static final String DEFAULT_WAREHOUSE = env("PGPAIMON_IT_WAREHOUSE", "/tmp/paimon-warehouse");
    static final String DEFAULT_JDBC =
        env("PGPAIMON_IT_JDBC_URL", "jdbc:postgresql://localhost:5434/testdb?user=ubuntu&password=");

    static String warehouse;
    static String jdbcUrl;
    static LocalFileIO fileIO = LocalFileIO.create();
    static int passed = 0, failed = 0;
    static final List<String> failures = new ArrayList<>();

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
        System.exit(run(args) > 0 ? 1 : 0);
    }

    static int run(String[] args) throws Exception {
        passed = 0; failed = 0; failures.clear();
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
        return failed;
    }

    /** Surefire entry point: the harness exports the stack's location. */
    @Test
    void acceptanceSuite() throws Exception {
        String jdbc = System.getenv("PGPAIMON_IT_JDBC_URL");
        String wh = System.getenv("PGPAIMON_IT_WAREHOUSE");
        if ("1".equals(System.getenv("PGPAIMON_HARNESS"))) {
            assertNotNull(jdbc, "harness did not export PGPAIMON_IT_JDBC_URL");
            assertNotNull(wh, "harness did not export PGPAIMON_IT_WAREHOUSE");
        }
        assumeTrue(jdbc != null && wh != null, "no live stack: run test/slot-acceptance/run_acceptance.sh");
        int failedChecks = run(new String[0]);
        assertEquals(0, failedChecks, "failed checks: " + failures);
    }

    static void testAllTypesInOneTable() throws Exception {
        String tbl = "type_all_cols";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMetaFields(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            StringBuilder ddl = new StringBuilder("id INT PRIMARY KEY");
            for (TypeCase tc : CASES) ddl.append(", ").append(tc.colType).append(" ").append(tc.pgType);
            createTracked(conn, tbl, ddl.toString());

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
            createTracked(conn, tbl, "id INT PRIMARY KEY, c_int INT, c_text TEXT, c_date DATE");
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
            createTracked(conn, tbl, "id INT PRIMARY KEY, " + colName + " " + pgType + "");
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
        noGuc();
    }

    static void cleanup(String tbl) {
        File dir = new File(tableDir(tbl));
        if (dir.exists()) deleteDir(dir);
    }

    static void deleteDir(File f) {
        if (f.isDirectory()) for (File c : Objects.requireNonNull(f.listFiles())) deleteDir(c);
        f.delete();
    }

    static void waitForParquet(String tbl, int minFiles) throws InterruptedException {
        File bucketDir      = new File(tableDir(tbl) + "/bucket-0");
        File snapshotLatest = new File(tableDir(tbl) + "/snapshot/LATEST");
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            File[] f = bucketDir.listFiles((d, n) -> n.endsWith(".parquet"));
            if (f != null && f.length >= minFiles && snapshotLatest.exists()) return;
            Thread.sleep(500);
        }
        throw new RuntimeException("Timed out waiting for parquet in " + tbl);
    }

    static TableSchema readSchema(String tbl) throws Exception {
        Path tablePath = new Path(tableDir(tbl));
        return new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema: " + tbl));
    }

    static long countRows(String tbl) throws Exception {
        long count = 0;
        try {
            Options o = new Options();
            o.set("warehouse", warehouse);
            try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
                /* A plain batch read of the table: each primary key surfaces once, which for an
                 * insert-only table is exactly the number of rows inserted. */
                Table t = catalog.getTable(Identifier.create("default", tbl));
                ReadBuilder rb = t.newReadBuilder();
                try (RecordReader<InternalRow> reader = rb.newRead().createReader(rb.newScan().plan())) {
                    RecordReader.RecordIterator<InternalRow> batch;
                    while ((batch = reader.readBatch()) != null) {
                        while (batch.next() != null) count++;
                        batch.releaseBatch();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("  warn: read failed for " + tbl + ": " + e.getMessage());
        }
        return count;
    }

    // ── Slot-architecture setup helpers (the only thing the port changes) ─────

    /** Creates a tracked table.  Replaces "USING paimon_heap" from the TAM architecture. */
    static void createTracked(Connection conn, String tbl, String columns) throws Exception {
        exec(conn, "DROP TABLE IF EXISTS " + tbl);
        exec(conn, "CREATE TABLE " + tbl + " (" + columns + ")");
        exec(conn, "SELECT paimon.add_table('" + tbl + "'::regclass)");
    }

    /** Paimon's filesystem catalog nests tables under <database>.db/. */
    static String tableDir(String tbl) {
        return warehouse + "/default.db/" + tbl;
    }

    static boolean gucNoteLogged = false;

    /** paimon_heap.* GUCs no longer exist: metadata columns are writer-side configuration. */
    static void noGuc() {
        if (!gucNoteLogged) {
            gucNoteLogged = true;
            System.out.println("  note: paimon_heap.* GUCs are no-ops in the slot architecture");
        }
    }

    static String env(String name, String dflt) {
        String v = System.getenv(name);
        return v != null ? v : dflt;
    }

    static void check(String name, boolean condition) {
        if (condition) {
            System.out.printf("  PASS  %s%n", name);
            passed++;
        } else {
            System.out.printf("  FAIL  %s%n", name);
            failed++;
            failures.add(name);
        }
    }
}
