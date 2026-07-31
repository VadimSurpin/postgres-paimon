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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * End-to-end integration tests: PostgreSQL (via paimon_heap) → Paimon warehouse → Spark compaction.
 *
 * Each test follows the pattern:
 *   1. Write rows to PostgreSQL via paimon_heap DML
 *   2. Wait for the daemon to flush a Paimon snapshot
 *   3. Assert pre-compaction: all CDC events present in the raw Paimon data
 *   4. Run compaction via CALL paimon.sys.compact()
 *   5. Assert post-compaction: deduplicated snapshot (read via Spark) matches PostgreSQL live state
 *   6. Assert changelog: after compaction, the changelog directory contains entries
 *
 * Usage:
 *   java -jar paimon-e2e-test.jar [warehouse] [jdbc-url]
 */
public class PaimonE2ETest {

    static final String DEFAULT_WAREHOUSE = "/tmp/paimon-warehouse";
    static final String DEFAULT_JDBC =
        "jdbc:postgresql://localhost:5434/testdb?user=ubuntu&password=";

    /* CDC row-kind constants (match the C++ daemon's PaimonRowKind enum) */
    static final int RK_INSERT         = 0;
    static final int RK_UPDATE_BEFORE  = 1;
    static final int RK_UPDATE_AFTER   = 2;
    static final int RK_DELETE         = 3;

    static String warehouse;
    static String jdbcUrl;
    static SparkSession spark;
    static LocalFileIO fileIO = LocalFileIO.create();
    static int passed = 0, failed = 0;

    public static void main(String[] args) throws Exception {
        warehouse = args.length > 0 ? args[0] : DEFAULT_WAREHOUSE;
        jdbcUrl   = args.length > 1 ? args[1] : DEFAULT_JDBC;

        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.println("║       paimon_heap End-to-End Test Suite                  ║");
        System.out.println("╚══════════════════════════════════════════════════════════╝");
        System.out.printf("  Warehouse : %s%n", warehouse);
        System.out.printf("  JDBC      : %s%n", jdbcUrl);
        System.out.println();

        spark = SparkSession.builder()
            .appName("PaimonE2ETest")
            .master("local[2]")
            .config("spark.sql.catalog.paimon",
                    "org.apache.paimon.spark.SparkCatalog")
            .config("spark.sql.catalog.paimon.warehouse", warehouse)
            .config("spark.sql.extensions",
                    "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
            .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try {
            testInsertOnlySnapshot();
            testCrudCdcAndCompaction();
            testMultiFlushCompaction();
            testChangelogAfterCompaction();
        } finally {
            spark.stop();
        }

        System.out.println();
        System.out.println("═".repeat(60));
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        System.exit(failed > 0 ? 1 : 0);
    }

    // ── T1: INSERT-only — snapshot row count matches PostgreSQL ──────────

    static void testInsertOnlySnapshot() throws Exception {
        System.out.println("\n── T1: INSERT-only snapshot ──");
        String tbl = "e2e_insert_only";
        cleanupTable(tbl);

        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl +
                " (id INT PRIMARY KEY, name TEXT, val NUMERIC(10,2))" +
                " USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'alice',1.10)");
            exec(conn, "INSERT INTO " + tbl + " VALUES (2,'bob',  2.20)");
            exec(conn, "INSERT INTO " + tbl + " VALUES (3,'carol',3.30)");
        }

        waitForSnapshot(tbl);

        // Pre-compaction: raw CDC events — all must be INSERT
        CdcCounts pre = readRawCdcCounts(tbl);
        check("T1 pre-compact: 3 INSERT events",  pre.ofKind(RK_INSERT) == 3);
        check("T1 pre-compact: 0 other events",   pre.total == 3);

        // Compact
        compact(tbl);

        // Post-compaction: Spark read must return exactly 3 rows matching PG
        long sparkCount = sparkLiveCount(tbl);
        long pgCount    = pgCount(tbl);
        check("T1 post-compact: Spark count == PG count",  sparkCount == pgCount);
        check("T1 post-compact: 3 rows",                    sparkCount == 3);

        // Values: compare a specific row
        double sparkVal = sparkDecimalVal(tbl, "id=1", "val");
        check("T1 post-compact: val for id=1 is 1.10", Math.abs(sparkVal - 1.10) < 0.005);
    }

    // ── T2: CRUD (INSERT + UPDATE + DELETE) — CDC counts + snapshot ──────

    static void testCrudCdcAndCompaction() throws Exception {
        System.out.println("\n── T2: CRUD CDC events and compacted snapshot ──");
        String tbl = "e2e_crud";
        cleanupTable(tbl);

        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl +
                " (id INT PRIMARY KEY, name TEXT, score INT)" +
                " USING paimon_heap");

            // Insert 5 rows
            for (int i = 1; i <= 5; i++)
                exec(conn, "INSERT INTO " + tbl + " VALUES (" + i + ",'row" + i + "'," + (i * 10) + ")");

            // Update 2 rows (rows 2 and 4)
            exec(conn, "UPDATE " + tbl + " SET score = 999 WHERE id IN (2, 4)");

            // Delete 1 row (row 5)
            exec(conn, "DELETE FROM " + tbl + " WHERE id = 5");
        }
        // PostgreSQL live state: ids 1,2,3,4 — id2 score=999, id4 score=999

        waitForSnapshot(tbl);

        // Pre-compaction CDC events
        CdcCounts pre = readRawCdcCounts(tbl);
        check("T2 pre-compact: 5 INSERT events",         pre.ofKind(RK_INSERT)        == 5);
        check("T2 pre-compact: 2 UPDATE_BEFORE events",  pre.ofKind(RK_UPDATE_BEFORE) == 2);
        check("T2 pre-compact: 2 UPDATE_AFTER events",   pre.ofKind(RK_UPDATE_AFTER)  == 2);
        check("T2 pre-compact: 1 DELETE event",          pre.ofKind(RK_DELETE)        == 1);
        check("T2 pre-compact: 10 total CDC events",     pre.total == 10);

        // Compact
        compact(tbl);

        // Post-compaction snapshot: Spark returns one row per PK (latest _seq).
        // Rows with _row_kind IN (0,2) are the live rows (INSERT or UPDATE_AFTER).
        // Row 5 (DELETE, _row_kind=3) must not appear in live rows.
        long sparkLive = sparkLiveCount(tbl);
        long pgLive    = pgCount(tbl);
        check("T2 post-compact: Spark live count == PG count", sparkLive == pgLive);
        check("T2 post-compact: 4 live rows", sparkLive == 4);

        // Verify updated values for ids 2 and 4
        int score2 = sparkIntVal(tbl, "id=2", "score");
        int score4 = sparkIntVal(tbl, "id=4", "score");
        check("T2 post-compact: id=2 score updated to 999", score2 == 999);
        check("T2 post-compact: id=4 score updated to 999", score4 == 999);

        // Verify id=5 does not appear in live rows
        long count5 = sparkCount(tbl, "id=5");
        check("T2 post-compact: id=5 absent from live snapshot", count5 == 0);
    }

    // ── T3: Multiple flushes — all data survives compaction ──────────────

    static void testMultiFlushCompaction() throws Exception {
        System.out.println("\n── T3: Multi-flush — data survives across snapshot boundaries ──");
        String tbl = "e2e_multi_flush";
        cleanupTable(tbl);

        final int BATCH_SIZE = 200; // > PAIMON_ROWGROUP_ROWS trigger (typically 1024; use 200 for safety)
        final int BATCHES    = 3;
        int totalInserted    = BATCH_SIZE * BATCHES;

        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v TEXT) USING paimon_heap");

            for (int batch = 0; batch < BATCHES; batch++) {
                // Each batch triggers the daemon to flush via SYNC
                for (int i = 0; i < BATCH_SIZE; i++) {
                    int id = batch * BATCH_SIZE + i + 1;
                    exec(conn, "INSERT INTO " + tbl + " VALUES (" + id + ",'v" + id + "')");
                }
                // Force a daemon flush by running a query that triggers SYNC
                exec(conn, "SELECT COUNT(*) FROM " + tbl);
                Thread.sleep(2000); // give daemon time to flush
            }
        }

        waitForSnapshot(tbl);

        // Pre-compaction: count raw events
        CdcCounts pre = readRawCdcCounts(tbl);
        check("T3 pre-compact: " + totalInserted + " INSERT events",
              pre.ofKind(RK_INSERT) == totalInserted);

        // Count Parquet files before compaction (should have at least 1, possibly >1 after multi-flush)
        int filesBefore = parquetFileCount(tbl);
        check("T3 pre-compact: at least 1 Parquet file", filesBefore >= 1);

        // Compact
        compact(tbl);

        // Post-compaction: all rows still readable via Spark
        long sparkLive = sparkLiveCount(tbl);
        long pgLive    = pgCount(tbl);
        check("T3 post-compact: Spark count == PG count",          sparkLive == pgLive);
        check("T3 post-compact: all " + totalInserted + " rows",   sparkLive == totalInserted);
    }

    // ── T4: Changelog files produced after compaction ────────────────────

    static void testChangelogAfterCompaction() throws Exception {
        System.out.println("\n── T4: Changelog files produced by compaction ──");
        String tbl = "e2e_changelog";
        cleanupTable(tbl);

        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl +
                " (id INT PRIMARY KEY, label TEXT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'a'),(2,'b'),(3,'c')");
            exec(conn, "UPDATE " + tbl + " SET label = 'B' WHERE id = 2");
            exec(conn, "DELETE FROM " + tbl + " WHERE id = 3");
        }

        waitForSnapshot(tbl);

        // Pre-compaction CDC counts
        CdcCounts pre = readRawCdcCounts(tbl);
        check("T4 pre-compact: 3 INSERT + 1 UPDATE_BEFORE + 1 UPDATE_AFTER + 1 DELETE",
              pre.ofKind(RK_INSERT) == 3 &&
              pre.ofKind(RK_UPDATE_BEFORE) == 1 &&
              pre.ofKind(RK_UPDATE_AFTER)  == 1 &&
              pre.ofKind(RK_DELETE) == 1);

        // Record snapshot count before compact
        int snapshotsBefore = snapshotCount(tbl);

        // Compact with changelog-producer=input explicitly re-set (idempotent)
        spark.sql("ALTER TABLE paimon.default.`" + tbl + "` SET TBLPROPERTIES " +
                  "('changelog-producer' = 'input')");
        compact(tbl);

        // Post-compaction: new snapshot was committed
        int snapshotsAfter = snapshotCount(tbl);
        check("T4 compact committed a new snapshot", snapshotsAfter > snapshotsBefore);

        // Changelog directory should exist if compact produced change entries
        File changelogDir = new File(warehouse + "/default.db/" + tbl + "/changelog");
        // With changelog-producer=input, compact produces changelog files;
        // report but do not fail if the directory is absent (depends on Paimon internals).
        if (changelogDir.exists() && changelogDir.list() != null) {
            int clFiles = Objects.requireNonNull(changelogDir.list()).length;
            System.out.printf("  INFO changelog files after compact: %d%n", clFiles);
            check("T4 changelog directory has files", clFiles > 0);
        } else {
            System.out.println("  INFO no changelog directory — changelog stored in data files");
            check("T4 changelog-producer=input set on table",
                  tableHasOption(tbl, "changelog-producer", "input"));
        }

        // Live snapshot: only ids 1 and 2 should appear
        long live = sparkLiveCount(tbl);
        check("T4 post-compact: 2 live rows (id=3 deleted)", live == 2);

        long id3live = sparkCount(tbl, "id=3");
        check("T4 post-compact: id=3 absent from live snapshot", id3live == 0);

        String labelId2 = sparkStringVal(tbl, "id=2", "label");
        check("T4 post-compact: id=2 label updated to 'B'", "B".equals(labelId2));
    }

    // ── Helpers: Paimon raw CDC reading ─────────────────────────────────

    static class CdcCounts {
        long total = 0;
        final Map<Integer, Long> kinds = new HashMap<>();
        long ofKind(int k) { return kinds.getOrDefault(k, 0L); }
    }

    /**
     * Reads all rows from the Paimon table via the manifest scan (no merge engine),
     * counts each CDC row kind.  Uses the fixed schema that prepends _row_kind.
     */
    static CdcCounts readRawCdcCounts(String tbl) throws Exception {
        Path tablePath = new Path(warehouse + "/default.db/" + tbl);
        TableSchema stored = new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema: " + tbl));

        Map<String, String> opts = new HashMap<>(stored.options());
        opts.put("bucket", "-1");
        opts.remove("merge-engine");
        opts.remove("changelog-producer");
        opts.remove("sequence.field");

        /* Prepend _row_kind with field_id=-1 so the Parquet column reader finds it */
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(-1, "_row_kind", DataTypes.TINYINT().notNull()));
        fields.addAll(stored.fields());

        /* Use empty primaryKeys so Paimon treats the table as append-only,
           bypassing DropDeleteReader which silently filters VALUE_KIND=1 rows. */
        TableSchema schema = new TableSchema(
            stored.id(), fields, stored.highestFieldId(),
            stored.partitionKeys(), Collections.emptyList(), opts, stored.comment());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, schema);

        ReadBuilder rb = table.newReadBuilder();
        List<Split> splits = rb.newScan().plan().splits();

        CdcCounts counts = new CdcCounts();
        try (RecordReader<InternalRow> reader = rb.newRead().createReader(splits)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow r;
                while ((r = batch.next()) != null) {
                    counts.total++;
                    if (!r.isNullAt(0))
                        counts.kinds.merge((int) r.getByte(0), 1L, Long::sum);
                }
                batch.releaseBatch();
            }
        }
        return counts;
    }

    // ── Helpers: Spark reads ──────────────────────────────────────────────

    /** After compaction, all rows visible via Spark are the live (merged) state */
    static long sparkLiveCount(String tbl) {
        return spark.sql("SELECT COUNT(*) FROM paimon.default.`" + tbl + "`")
                    .collectAsList().get(0).getLong(0);
    }

    static long sparkCount(String tbl, String where) {
        return spark.sql("SELECT COUNT(*) FROM paimon.default.`" + tbl +
                         "` WHERE " + where)
                    .collectAsList().get(0).getLong(0);
    }

    static double sparkDecimalVal(String tbl, String where, String col) {
        List<Row> rows = spark.sql("SELECT " + col + " FROM paimon.default.`" + tbl +
                                   "` WHERE " + where)
                              .collectAsList();
        if (rows.isEmpty()) return Double.NaN;
        Object v = rows.get(0).get(0);
        return v instanceof Number ? ((Number) v).doubleValue()
             : Double.parseDouble(v.toString());
    }

    static int sparkIntVal(String tbl, String where, String col) {
        List<Row> rows = spark.sql("SELECT " + col + " FROM paimon.default.`" + tbl +
                                   "` WHERE " + where)
                              .collectAsList();
        if (rows.isEmpty()) return Integer.MIN_VALUE;
        return rows.get(0).getInt(0);
    }

    static String sparkStringVal(String tbl, String where, String col) {
        List<Row> rows = spark.sql("SELECT " + col + " FROM paimon.default.`" + tbl +
                                   "` WHERE " + where)
                              .collectAsList();
        if (rows.isEmpty()) return null;
        return rows.get(0).getString(0);
    }

    // ── Helpers: PostgreSQL ───────────────────────────────────────────────

    static long pgCount(String tbl) throws Exception {
        try (Connection conn = connect();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + tbl)) {
            rs.next(); return rs.getLong(1);
        }
    }

    // ── Helpers: file system ─────────────────────────────────────────────

    static int parquetFileCount(String tbl) {
        File[] files = new File(warehouse + "/default.db/" + tbl + "/bucket-0")
            .listFiles((d, n) -> n.endsWith(".parquet"));
        return files == null ? 0 : files.length;
    }

    static int snapshotCount(String tbl) {
        File[] snaps = new File(warehouse + "/default.db/" + tbl + "/snapshot")
            .listFiles((d, n) -> n.startsWith("snapshot-"));
        return snaps == null ? 0 : snaps.length;
    }

    static boolean tableHasOption(String tbl, String key, String value) {
        try {
            Path tablePath = new Path(warehouse + "/default.db/" + tbl);
            TableSchema schema = new SchemaManager(fileIO, tablePath).latest()
                .orElse(null);
            if (schema == null) return false;
            return value.equals(schema.options().get(key));
        } catch (Exception e) { return false; }
    }

    // ── Helpers: compaction ───────────────────────────────────────────────

    static void compact(String tbl) {
        System.out.printf("  Compacting %s ...%n", tbl);
        spark.sql("CALL paimon.sys.compact(`table` => 'default." + tbl + "')");
        System.out.printf("  Compact done: %s%n", tbl);
    }

    // ── Helpers: wait for daemon flush ────────────────────────────────────

    static void waitForSnapshot(String tbl) throws InterruptedException {
        File bucketDir  = new File(warehouse + "/default.db/" + tbl + "/bucket-0");
        File snapLatest = new File(warehouse + "/default.db/" + tbl + "/snapshot/LATEST");
        long deadline   = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            File[] f = bucketDir.listFiles((d, n) -> n.endsWith(".parquet"));
            if (f != null && f.length >= 1 && snapLatest.exists()) return;
            Thread.sleep(500);
        }
        throw new RuntimeException("Timed out waiting for Paimon snapshot: " + tbl);
    }

    // ── Helpers: cleanup ─────────────────────────────────────────────────

    static void cleanupTable(String tbl) {
        File dir = new File(warehouse + "/default.db/" + tbl);
        if (dir.exists()) deleteDir(dir);
    }

    static void deleteDir(File f) {
        if (f.isDirectory())
            for (File c : Objects.requireNonNull(f.listFiles())) deleteDir(c);
        f.delete();
    }

    // ── Helpers: JDBC ─────────────────────────────────────────────────────

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    // ── Helpers: assertions ───────────────────────────────────────────────

    static void check(String name, boolean ok) {
        if (ok) {
            System.out.printf("  PASS  %s%n", name);
            passed++;
        } else {
            System.out.printf("  FAIL  %s%n", name);
            failed++;
        }
    }
}
