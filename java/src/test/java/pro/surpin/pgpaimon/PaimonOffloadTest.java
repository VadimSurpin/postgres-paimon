// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import org.junit.jupiter.api.Test;

/**
 * T18 (S3 via MinIO) and T19 (HDFS via an in-process MiniDFSCluster): the warehouse is a remote
 * filesystem and the rows still round-trip.
 *
 * There is no offload step.  On main a background worker wrote Parquet locally, uploaded it
 * with aws-sdk-cpp and deleted the local copy.  Here the warehouse URI itself is
 * {@code s3://bucket/wh} or {@code hdfs://host:port/wh}: the writer's Paimon FileIO writes
 * straight to it, so there is nothing to schedule, upload or delete.  The assertions about
 * what is readable from S3 and HDFS are unchanged.
 *
 * Both tests need the writer to run against the remote warehouse, so each stops the harness
 * writer, relaunches it through the harness launcher with PGPAIMON_WAREHOUSE_OVERRIDE (and
 * PGPAIMON_S3_* for S3) set, and restores the default writer afterwards.
 *
 * Each test runs three phases and asserts the whole changelog after each:
 *   Phase 1  INSERT 20 rows  -> 20 INSERT
 *   Phase 2  UPDATE 5 rows   -> + 5 UPDATE_BEFORE + 5 UPDATE_AFTER
 *   Phase 3  DELETE 3 rows   -> + 3 DELETE
 * Row-kind ordinals: 0=INSERT, 1=UPDATE_BEFORE, 2=UPDATE_AFTER, 3=DELETE.  Kinds come from the
 * $audit_log system table over a streaming scan; a merged batch read would report INSERT for
 * every surviving row.
 *
 * T18 runs only when E2E_MINIO_BIN names an executable MinIO server; otherwise it prints a
 * SKIPPED line and is counted as skipped, never as passed.  T19 runs with no setup.
 *
 * Usage (live stack): test/slot-acceptance/run_acceptance.sh PaimonOffloadTest
 */
public class PaimonOffloadTest {

    /**
     * The PostgreSQL schema these suites create their tables in, and therefore the
     * Paimon database they land in: the writer maps one to the other, so the warehouse
     * layout is <warehouse>/<schema>.db/<table>.
     */
    static final String PG_SCHEMA = "public";

    static String envOr(String key, String def) {
        String v = System.getenv(key);
        return v != null && !v.isEmpty() ? v : def;
    }

    static final String DEFAULT_JDBC = envOr("PGPAIMON_IT_JDBC_URL",
        envOr("E2E_JDBC_URL", "jdbc:postgresql://localhost:5442/paimon_acc?user=postgres"));

    /* MinIO / S3: a plain binary, no container. */
    static final String MINIO_BIN  = envOr("E2E_MINIO_BIN", "/root/cloud-db/opt/minio3");
    static final int    MINIO_PORT = Integer.parseInt(envOr("E2E_MINIO_PORT", "9100"));
    static final String MINIO_USER = "minioadmin";
    static final String MINIO_PASS = "minioadmin";
    static final String S3_BUCKET  = "paimon-offload-test";

    static final boolean SKIP_HDFS = Boolean.parseBoolean(envOr("E2E_SKIP_HDFS", "false"));

    static String jdbcUrl;
    static Process minioProc;
    static MiniDFSCluster hdfsCluster;
    static int passed = 0, failed = 0, skipped = 0;
    static final List<String> failures = new ArrayList<>();
    static final List<String> skips = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        System.exit(run(args) > 0 ? 1 : 0);
    }

    /** Surefire entry point for S3.  Skipped (a real JUnit skip) when no MinIO binary is named. */
    @Test
    void s3Warehouse() throws Exception {
        requireStack();
        reset(System.getenv("PGPAIMON_IT_JDBC_URL"));
        if (!minioAvailable()) {
            skip("T18", "no MinIO server binary at E2E_MINIO_BIN=" + MINIO_BIN, "E2E_MINIO_BIN=/path/to/minio");
        }
        assumeTrue(minioAvailable(), "T18 SKIPPED: set E2E_MINIO_BIN=/path/to/minio to enable");
        runTest("T18", PaimonOffloadTest::testS3);
        assertEquals(0, failed, "failed checks: " + failures);
    }

    /** Surefire entry point for HDFS: needs nothing but this JVM. */
    @Test
    void hdfsWarehouse() throws Exception {
        requireStack();
        reset(System.getenv("PGPAIMON_IT_JDBC_URL"));
        if (SKIP_HDFS) skip("T19", "E2E_SKIP_HDFS=true", "unset E2E_SKIP_HDFS");
        assumeFalse(SKIP_HDFS, "T19 SKIPPED: E2E_SKIP_HDFS=true");
        runTest("T19", PaimonOffloadTest::testHdfs);
        assertEquals(0, failed, "failed checks: " + failures);
    }

    static void requireStack() {
        String jdbc = System.getenv("PGPAIMON_IT_JDBC_URL");
        if ("1".equals(System.getenv("PGPAIMON_HARNESS"))) {
            assertNotNull(jdbc, "harness did not export PGPAIMON_IT_JDBC_URL");
            assertNotNull(System.getenv("PGPAIMON_WRITER_LAUNCHER"), "harness did not export PGPAIMON_WRITER_LAUNCHER");
        }
        assumeTrue(jdbc != null, "no live stack: run test/slot-acceptance/run_acceptance.sh");
    }

    static boolean minioAvailable() {
        File f = new File(MINIO_BIN);
        return f.isFile() && f.canExecute();
    }

    static void reset(String jdbc) {
        passed = 0; failed = 0; skipped = 0; failures.clear(); skips.clear();
        jdbcUrl = jdbc != null ? jdbc : DEFAULT_JDBC;
    }

    /** Returns the number of failed checks.  args[0], when given, is the JDBC URL. */
    static int run(String[] args) throws Exception {
        reset(args.length > 0 ? args[0] : null);

        runTest("T18", PaimonOffloadTest::testS3);
        if (SKIP_HDFS) skip("T19", "E2E_SKIP_HDFS=true", "unset E2E_SKIP_HDFS");
        else runTest("T19", PaimonOffloadTest::testHdfs);

        System.out.println();
        System.out.println("═".repeat(52));
        System.out.printf("  Results: %d passed, %d failed, %d skipped%n", passed, failed, skipped);
        for (String s : skips) System.out.println("  NOT RUN: " + s);
        return failed;
    }

    @FunctionalInterface interface TestCase { void run() throws Exception; }

    static void runTest(String name, TestCase tc) {
        System.out.printf("%n=== %s ===%n", name);
        try {
            tc.run();
        } catch (Throwable t) {
            t.printStackTrace(System.out);
            check(name + " completed without an exception: " + t, false);
        }
    }

    /** A skip is loud, counted separately and never added to the passes. */
    static void skip(String name, String why, String enable) {
        String line = name + " SKIPPED (" + why + "); to enable: " + enable;
        System.out.println("  " + line);
        skipped++;
        skips.add(line);
    }

    /* ── T18: S3 ─────────────────────────────────────────────────────── */

    static void testS3() throws Exception {
        if (!minioAvailable()) {
            skip("T18", "no MinIO server binary at E2E_MINIO_BIN=" + MINIO_BIN,
                 "E2E_MINIO_BIN=/path/to/minio");
            return;
        }
        String dataDir = "/tmp/minio-offload-test";
        deleteDir(Paths.get(dataDir));
        Files.createDirectories(Paths.get(dataDir, S3_BUCKET));      // a bucket is a directory
        ProcessBuilder pb = new ProcessBuilder(
            MINIO_BIN, "server", dataDir, "--address", "127.0.0.1:" + MINIO_PORT);
        pb.environment().put("MINIO_ROOT_USER", MINIO_USER);
        pb.environment().put("MINIO_ROOT_PASSWORD", MINIO_PASS);
        pb.redirectErrorStream(true);
        pb.redirectOutput(new File("/tmp/minio-offload.log"));
        minioProc = pb.start();
        try {
            waitForPort(MINIO_PORT, 15_000);
            String endpoint = "http://127.0.0.1:" + MINIO_PORT;
            Map<String, String> env = new LinkedHashMap<>();
            env.put("PGPAIMON_S3_ENDPOINT", endpoint);
            env.put("PGPAIMON_S3_ACCESS_KEY", MINIO_USER);
            env.put("PGPAIMON_S3_SECRET_KEY", MINIO_PASS);
            Map<String, String> readerOpts = new LinkedHashMap<>();
            readerOpts.put("s3.endpoint", endpoint);
            readerOpts.put("s3.access-key", MINIO_USER);
            readerOpts.put("s3.secret-key", MINIO_PASS);
            readerOpts.put("s3.path.style.access", "true");
            roundTrip("S3", "s3://" + S3_BUCKET + "/wh", env, readerOpts, "offload_s3_test");
        } finally {
            minioProc.destroyForcibly();
            minioProc = null;
            deleteDir(Paths.get(dataDir));
        }
    }

    /* ── T19: HDFS ───────────────────────────────────────────────────── */

    static void testHdfs() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.safemode.min.datanodes", "0");
        conf.set("dfs.safemode.extension", "0");
        conf.set("dfs.permissions.enabled", "false");
        conf.set("hadoop.security.authentication", "simple");
        File dataDir = new File("/tmp/mini-hdfs-" + System.currentTimeMillis());
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());

        hdfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        try {
            hdfsCluster.waitActive();
            int port = hdfsCluster.getNameNodePort();
            System.out.println("  MiniDFSCluster RPC port: " + port);
            roundTrip("HDFS", "hdfs://localhost:" + port + "/paimon-offload-warehouse",
                      Map.of(), Map.of(), "offload_hdfs_test");
        } finally {
            try { hdfsCluster.shutdown(); } catch (Exception ignored) {}
            hdfsCluster = null;
            deleteDir(dataDir.toPath());
        }
    }

    /* ── The shared three-phase round trip ──────────────────────────── */

    static void roundTrip(String label, String warehouseUri, Map<String, String> writerEnv,
                          Map<String, String> readerOpts, String tbl) throws Exception {
        Map<String, String> launchEnv = new LinkedHashMap<>(writerEnv);
        launchEnv.put("PGPAIMON_WAREHOUSE_OVERRIDE", warehouseUri);
        Map<String, String> catalogOpts = new LinkedHashMap<>(readerOpts);
        catalogOpts.put("warehouse", warehouseUri);

        swapWriter(launchEnv);
        try (Connection c = DriverManager.getConnection(jdbcUrl)) {
            try {
                // Registered while empty: add_table derives backfill_due from emptiness.
                createTracked(c, tbl, "id INT PRIMARY KEY, val TEXT");

                System.out.printf("  [%s] Phase 1: INSERT 20 rows%n", label);
                c.setAutoCommit(false);
                for (int i = 1; i <= 20; i++)
                    exec(c, "INSERT INTO " + tbl + " VALUES (" + i + ", 'row" + i + "')");
                c.commit();
                c.setAutoCommit(true);
                PaimonRows r1 = waitForTotal(catalogOpts, tbl, 20);
                System.out.printf("  [%s] INSERT: total=%d kinds=%s%n", label, r1.total, r1.kindCounts);
                check(label + " INSERT: 20 rows total", r1.total == 20);
                check(label + " INSERT: 20 kind=INSERT(0)", r1.ofKind(0) == 20);
                check(label + " INSERT: 0 UPDATE_BEFORE(1)", r1.ofKind(1) == 0);
                check(label + " INSERT: 0 UPDATE_AFTER(2)", r1.ofKind(2) == 0);
                check(label + " INSERT: 0 DELETE(3)", r1.ofKind(3) == 0);

                System.out.printf("  [%s] Phase 2: UPDATE 5 rows%n", label);
                c.setAutoCommit(false);
                for (int i = 1; i <= 5; i++)
                    exec(c, "UPDATE " + tbl + " SET val='updated" + i + "' WHERE id=" + i);
                c.commit();
                c.setAutoCommit(true);
                PaimonRows r2 = waitForTotal(catalogOpts, tbl, 30);   // cumulative
                System.out.printf("  [%s] UPDATE: total=%d kinds=%s%n", label, r2.total, r2.kindCounts);
                check(label + " UPDATE: 30 rows total", r2.total == 30);
                check(label + " UPDATE: 20 INSERT", r2.ofKind(0) == 20);
                check(label + " UPDATE: 5 UPDATE_BEFORE(1)", r2.ofKind(1) == 5);
                check(label + " UPDATE: 5 UPDATE_AFTER(2)", r2.ofKind(2) == 5);
                check(label + " UPDATE: 0 DELETE(3)", r2.ofKind(3) == 0);

                System.out.printf("  [%s] Phase 3: DELETE 3 rows%n", label);
                c.setAutoCommit(false);
                for (int i = 18; i <= 20; i++)
                    exec(c, "DELETE FROM " + tbl + " WHERE id=" + i);
                c.commit();
                c.setAutoCommit(true);
                PaimonRows r3 = waitForTotal(catalogOpts, tbl, 33);   // cumulative
                System.out.printf("  [%s] DELETE: total=%d kinds=%s%n", label, r3.total, r3.kindCounts);
                check(label + " DELETE: 33 rows total", r3.total == 33);
                check(label + " DELETE: 20 INSERT", r3.ofKind(0) == 20);
                check(label + " DELETE: 5 UPDATE_BEFORE(1)", r3.ofKind(1) == 5);
                check(label + " DELETE: 5 UPDATE_AFTER(2)", r3.ofKind(2) == 5);
                check(label + " DELETE: 3 DELETE(3)", r3.ofKind(3) == 3);
            } finally {
                try { exec(c, "DROP TABLE IF EXISTS " + tbl); } catch (SQLException ignored) {}
                // The default writer must not replay this table's events into the local warehouse.
                try { waitSlotCaughtUp(c); } catch (Exception ignored) {}
            }
        } finally {
            swapWriter(Map.of());       // back to the harness's default writer
        }
    }

    /* ── Writer control, through the harness launcher ───────────────── */

    static String launcher() {
        String l = System.getenv("PGPAIMON_WRITER_LAUNCHER");
        if (l == null) throw new IllegalStateException(
            "no PGPAIMON_WRITER_LAUNCHER: run via test/slot-acceptance/run_acceptance.sh");
        return l;
    }

    /** Stops the running writer and starts the launcher again with {@code env} added. */
    static void swapWriter(Map<String, String> env) throws Exception {
        String pidFile = System.getenv("PGPAIMON_WRITER_PID_FILE");
        String pid = new String(Files.readAllBytes(Paths.get(pidFile))).trim();
        new ProcessBuilder("kill", pid).inheritIO().start().waitFor();
        long deadline = System.currentTimeMillis() + 30_000;
        while (processAlive(pid) && System.currentTimeMillis() < deadline) Thread.sleep(200);
        if (processAlive(pid)) {
            new ProcessBuilder("kill", "-9", pid).inheritIO().start().waitFor();
            Thread.sleep(500);
        }
        deadline = System.currentTimeMillis() + 30_000;
        while (slotActive() && System.currentTimeMillis() < deadline) Thread.sleep(200);
        if (env.isEmpty()) {
            // Back to the default warehouse after the slot was handed to a writer on another one.
            // What that writer consumed is meant never to reach the default warehouse: exactly the
            // loss the clean-stop mark exists to catch (SLOT MOVED WHILE THE WRITER WAS STOPPED,
            // guide section 8). Accepted here as the guide says: delete the mark.
            File mark = new File(System.getenv("PGPAIMON_IT_WAREHOUSE"), "_pgpaimon_stopped_paimon");
            check("the default warehouse's stop mark is there to accept", mark.isFile());
            mark.delete();
        }

        ProcessBuilder pb = new ProcessBuilder("bash", launcher());
        pb.environment().putAll(env);
        pb.redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD);
        check("writer relaunched" + (env.isEmpty() ? " (default warehouse)" : " against "
              + env.get("PGPAIMON_WAREHOUSE_OVERRIDE")), pb.start().waitFor() == 0);
        deadline = System.currentTimeMillis() + 60_000;
        while (!slotActive() && System.currentTimeMillis() < deadline) Thread.sleep(250);
        check("writer reattached to the slot", slotActive());
    }

    static boolean processAlive(String pid) {
        return new File("/proc/" + pid).exists();
    }

    static boolean slotActive() throws SQLException {
        try (Connection c = DriverManager.getConnection(jdbcUrl); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT active FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    static void waitSlotCaughtUp(Connection c) throws Exception {
        String target;
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT pg_current_wal_insert_lsn()::text")) {
            rs.next();
            target = rs.getString(1);
        }
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(
                     "SELECT confirmed_flush_lsn >= '" + target + "'::pg_lsn FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
                if (rs.next() && rs.getBoolean(1)) return;
            }
            Thread.sleep(250);
        }
    }

    /* ── Reading the remote warehouse ───────────────────────────────── */

    /** Polls until the changelog has {@code total} records, then lets stragglers land and rereads. */
    static PaimonRows waitForTotal(Map<String, String> catalogOpts, String tbl, long total) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000;
        PaimonRows rows = readRows(catalogOpts, tbl);
        while (rows.total < total && System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
            rows = readRows(catalogOpts, tbl);
        }
        Thread.sleep(2000);          // an extra record would show up now; the checks are exact
        return readRows(catalogOpts, tbl);
    }

    static PaimonRows readRows(Map<String, String> catalogOpts, String tbl) {
        PaimonRows result = new PaimonRows();
        try {
            Options o = new Options();
            catalogOpts.forEach(o::set);
            try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
                Table al = catalog.getTable(Identifier.create(PG_SCHEMA, tbl + "$audit_log"))
                    .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
                StreamTableScan scan = al.newReadBuilder().newStreamScan();
                scan.plan();                          // the first plan() is always empty: discard
                Long latest = ((FileStoreTable) catalog.getTable(Identifier.create(PG_SCHEMA, tbl)))
                    .snapshotManager().latestSnapshotId();
                List<Split> splits = new ArrayList<>();
                for (long i = 0; i <= (latest == null ? 0 : latest) + 1; i++) {
                    splits.addAll(scan.plan().splits());
                }
                try (RecordReader<InternalRow> reader = al.newReadBuilder().newRead().createReader(splits)) {
                    reader.forEachRemaining(r -> {
                        result.total++;
                        result.kindCounts.merge(kindOf(r.getString(0).toString()), 1L, Long::sum);
                    });
                }
            }
        } catch (Catalog.TableNotExistException e) {
            // not created yet: the caller keeps polling
        } catch (Exception e) {
            System.err.println("  warn: read of " + tbl + " failed: " + e);
        }
        return result;
    }

    /** $audit_log's rowkind string to the ordinal: +I 0, -U 1, +U 2, -D 3. */
    static int kindOf(String k) {
        switch (k) {
            case "+I": return 0;
            case "-U": return 1;
            case "+U": return 2;
            case "-D": return 3;
            default: throw new IllegalStateException("unknown row kind " + k);
        }
    }

    static class PaimonRows {
        long total = 0;
        Map<Integer, Long> kindCounts = new HashMap<>();
        long ofKind(int k) { return kindCounts.getOrDefault(k, 0L); }
    }

    /* ── Helpers ────────────────────────────────────────────────────── */

    /** Creates a tracked table: CREATE TABLE, then register it with paimon.add_table(). */
    static void createTracked(Connection conn, String tbl, String columns) throws Exception {
        exec(conn, "DROP TABLE IF EXISTS " + tbl);
        exec(conn, "CREATE TABLE " + tbl + " (" + columns + ")");
        exec(conn, "SELECT paimon.add_table('" + tbl + "'::regclass)");
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    static void waitForPort(int port, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (java.net.Socket s = new java.net.Socket("127.0.0.1", port)) {
                Thread.sleep(500);
                return;
            } catch (java.net.ConnectException e) {
                Thread.sleep(300);
            }
        }
        throw new RuntimeException("port " + port + " did not open within " + timeoutMs + " ms");
    }

    static void deleteDir(java.nio.file.Path p) {
        if (!Files.exists(p)) return;
        try (Stream<java.nio.file.Path> s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(f -> {
                try { Files.delete(f); } catch (IOException ignored) {}
            });
        } catch (IOException ignored) {}
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
