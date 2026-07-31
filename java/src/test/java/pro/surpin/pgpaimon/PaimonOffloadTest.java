// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.apache.paimon.data.InternalRow;
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

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

/**
 * T18 — S3 offload: starts a local MinIO instance, configures the
 *        paimon_heap S3 GUCs, then verifies INSERT / UPDATE / DELETE
 *        rows round-trip correctly by reading Parquet files from MinIO
 *        with the Paimon library.
 *
 * T19 — HDFS offload: same verification using an in-process
 *        MiniDFSCluster (no external Hadoop required).
 *
 * Each test runs three phases:
 *   Phase 1  INSERT 20 rows  → expect 20 INSERT (kind=0)
 *   Phase 2  UPDATE 5 rows   → expect +5 UPD_BEFORE (1) + 5 UPD_AFTER (2)
 *   Phase 3  DELETE 3 rows   → expect +3 DELETE (3)
 *
 * Row-kind values: 0=INSERT, 1=UPDATE_BEFORE, 2=UPDATE_AFTER, 3=DELETE
 *
 * Prerequisites
 * -------------
 *   • PostgreSQL 5434 running with paimon_heap extension installed.
 *   • /root/cloud-db/opt/minio3 and /root/cloud-db/opt/mc exist.
 */
public class PaimonOffloadTest {

    static String envOr(String key, String def) {
        String v = System.getenv(key);
        return v != null && !v.isEmpty() ? v : def;
    }

    /* ── Connection ─────────────────────────────────────────────────── */
    static final String JDBC_URL = envOr("E2E_JDBC_URL",  "jdbc:postgresql://localhost:5434/testdb");
    static final String DB_USER  = envOr("E2E_DB_USER",   "ubuntu");

    /* ── MinIO / S3 ─────────────────────────────────────────────────── */
    static final String MINIO_BIN  = envOr("E2E_MINIO_BIN", "/root/cloud-db/opt/minio3");
    static final String MC_BIN     = envOr("E2E_MC_BIN",    "/root/cloud-db/opt/mc");
    static final int    MINIO_PORT = Integer.parseInt(envOr("E2E_MINIO_PORT", "9100"));
    static final String HOME_DIR   = envOr("E2E_HOME_DIR",   System.getProperty("user.home"));
    static final String MINIO_DIR  = "/tmp/minio-offload-test";
    static final String MINIO_USER = "minioadmin";
    static final String MINIO_PASS = "minioadmin";
    static final String S3_BUCKET  = "paimon-offload-test";

    /* ── Warehouse ──────────────────────────────────────────────────── */
    static final String WAREHOUSE  = "/tmp/paimon-offload-warehouse";

    /* ── Paimon reader ──────────────────────────────────────────────── */
    static final LocalFileIO fileIO = LocalFileIO.create();

    /* ── Feature flags ──────────────────────────────────────────────── */
    static final boolean SKIP_HDFS = Boolean.parseBoolean(envOr("E2E_SKIP_HDFS", "false"));

    /* ── State ──────────────────────────────────────────────────────── */
    static Process       minioProc;
    static MiniDFSCluster hdfsCluster;
    static int           passed = 0;
    static int           failed = 0;

    /* ── Entry point ────────────────────────────────────────────────── */
    public static void main(String[] args) throws Exception {
        globalSetup();
        try {
            run("T18", PaimonOffloadTest::testS3Offload);
            if (SKIP_HDFS)
                System.out.println("\n=== T19 === SKIPPED (E2E_SKIP_HDFS=true)");
            else
                run("T19", PaimonOffloadTest::testHdfsOffload);
        } finally {
            globalTeardown();
        }
        System.out.printf("%nResults: %d passed, %d failed%n", passed, failed);
        if (failed > 0) System.exit(1);
    }

    @FunctionalInterface interface TestCase { void run() throws Exception; }

    static void run(String name, TestCase tc) {
        System.out.printf("%n=== %s ===%n", name);
        try {
            tc.run();
            System.out.printf("  %s PASSED%n", name);
            passed++;
        } catch (Throwable t) {
            System.out.printf("  %s FAILED: %s%n", name, t.getMessage());
            t.printStackTrace(System.out);
            failed++;
        }
    }

    /* ── Global setup / teardown ────────────────────────────────────── */

    static void globalSetup() throws Exception {
        deleteDir(Paths.get(WAREHOUSE));
        deleteDir(Paths.get(MINIO_DIR));

        try (Connection c = connect()) {
            exec(c, "ALTER SYSTEM SET paimon_heap.warehouse = '" + WAREHOUSE + "'");
            exec(c, "ALTER SYSTEM SET paimon_heap.staging_dir = ''");
            exec(c, "ALTER SYSTEM SET paimon_heap.flush_interval_txns = 1");
            exec(c, "ALTER SYSTEM SET paimon_heap.flush_interval_secs = 3");
            exec(c, "ALTER SYSTEM SET paimon_heap.min_rows_per_file = 0");
            exec(c, "SELECT pg_reload_conf()");
        }
        Thread.sleep(500);
    }

    static void globalTeardown() throws Exception {
        if (minioProc != null && minioProc.isAlive()) minioProc.destroyForcibly();
        if (hdfsCluster != null) { try { hdfsCluster.shutdown(); } catch (Exception ignored) {} }

        try (Connection c = connect()) {
            for (String g : new String[]{
                "paimon_heap.s3_bucket", "paimon_heap.s3_endpoint",
                "paimon_heap.s3_region", "paimon_heap.s3_prefix",
                "paimon_heap.hdfs_namenode", "paimon_heap.hdfs_path",
                "paimon_heap.flush_interval_txns", "paimon_heap.flush_interval_secs",
                "paimon_heap.min_rows_per_file", "paimon_heap.warehouse",
                "paimon_heap.staging_dir"})
                exec(c, "ALTER SYSTEM RESET " + g);
            exec(c, "SELECT pg_reload_conf()");
        }
    }

    /* ── T18: S3 offload via MinIO ──────────────────────────────────── */

    static void testS3Offload() throws Exception {
        final String tbl = "offload_s3_test";

        startMinio();
        writeAwsCreds(HOME_DIR);
        shell(MC_BIN, "alias", "set", "minio-test",
              "http://127.0.0.1:" + MINIO_PORT, MINIO_USER, MINIO_PASS);
        int rc = shellCode(MC_BIN, "mb", "--ignore-existing", "minio-test/" + S3_BUCKET);
        if (rc != 0) throw new RuntimeException("mc mb failed");

        try (Connection c = connect()) {
            exec(c, "ALTER SYSTEM SET paimon_heap.s3_bucket   = '" + S3_BUCKET + "'");
            exec(c, "ALTER SYSTEM SET paimon_heap.s3_endpoint = 'http://127.0.0.1:" + MINIO_PORT + "'");
            exec(c, "ALTER SYSTEM SET paimon_heap.s3_region   = 'us-east-1'");
            exec(c, "SELECT pg_reload_conf()");
            Thread.sleep(1000);

            exec(c, "DROP TABLE IF EXISTS " + tbl);
            exec(c, "CREATE TABLE " + tbl + " (id int, val text) USING paimon_heap");

            /* Phase 1: INSERT 20 rows */
            System.out.println("  [S3] Phase 1: INSERT 20 rows");
            long snap0 = readS3LatestSnapshotId(tbl); // capture before commit
            c.setAutoCommit(false);
            for (int i = 1; i <= 20; i++)
                exec(c, "INSERT INTO " + tbl + " VALUES (" + i + ", 'row" + i + "')");
            c.commit();
            c.setAutoCommit(true);

            waitForS3Flush(tbl, snap0, 30);
            PaimonRows r1 = readS3Rows(tbl);
            System.out.printf("  [S3] INSERT: total=%d kinds=%s%n", r1.total, r1.kindCounts);
            check("S3 INSERT: 20 rows total",       r1.total == 20);
            check("S3 INSERT: 20 kind=INSERT(0)",   r1.ofKind(0) == 20);
            check("S3 INSERT: 0 UPDATE_BEFORE(1)",  r1.ofKind(1) == 0);
            check("S3 INSERT: 0 UPDATE_AFTER(2)",   r1.ofKind(2) == 0);
            check("S3 INSERT: 0 DELETE(3)",         r1.ofKind(3) == 0);

            /* Phase 2: UPDATE 5 rows */
            System.out.println("  [S3] Phase 2: UPDATE 5 rows");
            long snap1 = readS3LatestSnapshotId(tbl); // capture before commit
            c.setAutoCommit(false);
            for (int i = 1; i <= 5; i++)
                exec(c, "UPDATE " + tbl + " SET val='updated" + i + "' WHERE id=" + i);
            c.commit();
            c.setAutoCommit(true);

            waitForS3Flush(tbl, snap1, 30);
            PaimonRows r2 = readS3Rows(tbl);
            // Cumulative: 20 INSERT + 5 UPD_BEFORE + 5 UPD_AFTER = 30
            System.out.printf("  [S3] UPDATE: total=%d kinds=%s%n", r2.total, r2.kindCounts);
            check("S3 UPDATE: 30 rows total",        r2.total == 30);
            check("S3 UPDATE: 20 INSERT",            r2.ofKind(0) == 20);
            check("S3 UPDATE: 5 UPDATE_BEFORE(1)",   r2.ofKind(1) == 5);
            check("S3 UPDATE: 5 UPDATE_AFTER(2)",    r2.ofKind(2) == 5);
            check("S3 UPDATE: 0 DELETE(3)",          r2.ofKind(3) == 0);

            /* Phase 3: DELETE 3 rows */
            System.out.println("  [S3] Phase 3: DELETE 3 rows");
            long snap2 = readS3LatestSnapshotId(tbl); // capture before commit
            c.setAutoCommit(false);
            for (int i = 18; i <= 20; i++)
                exec(c, "DELETE FROM " + tbl + " WHERE id=" + i);
            c.commit();
            c.setAutoCommit(true);

            waitForS3Flush(tbl, snap2, 30);
            PaimonRows r3 = readS3Rows(tbl);
            // Cumulative: 30 + 3 DELETE = 33
            System.out.printf("  [S3] DELETE: total=%d kinds=%s%n", r3.total, r3.kindCounts);
            check("S3 DELETE: 33 rows total",       r3.total == 33);
            check("S3 DELETE: 20 INSERT",           r3.ofKind(0) == 20);
            check("S3 DELETE: 5 UPDATE_BEFORE(1)",  r3.ofKind(1) == 5);
            check("S3 DELETE: 5 UPDATE_AFTER(2)",   r3.ofKind(2) == 5);
            check("S3 DELETE: 3 DELETE(3)",         r3.ofKind(3) == 3);

            exec(c, "DROP TABLE " + tbl);
            exec(c, "ALTER SYSTEM RESET paimon_heap.s3_bucket");
            exec(c, "ALTER SYSTEM RESET paimon_heap.s3_endpoint");
            exec(c, "ALTER SYSTEM RESET paimon_heap.s3_region");
            exec(c, "SELECT pg_reload_conf()");
        }

        stopMinio();
    }

    /* ── T19: HDFS offload via MiniDFSCluster ───────────────────────── */

    static void testHdfsOffload() throws Exception {
        final String tbl       = "offload_hdfs_test";
        final String hdfsBase  = "/paimon-offload-warehouse";

        Configuration conf = new Configuration();
        conf.set("dfs.replication",                "1");
        conf.set("dfs.safemode.min.datanodes",     "0");
        conf.set("dfs.safemode.extension",         "0");
        conf.set("dfs.permissions.enabled",        "false");
        conf.set("hadoop.security.authentication", "simple");

        java.io.File hdfsDataDir =
            new java.io.File("/tmp/mini-hdfs-" + System.currentTimeMillis());
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDataDir.getAbsolutePath());

        hdfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        hdfsCluster.waitActive();
        int namenodePort = hdfsCluster.getNameNodePort();
        System.out.println("  MiniDFSCluster RPC port: " + namenodePort);
        FileSystem fs = hdfsCluster.getFileSystem();

        try (Connection c = connect()) {
            exec(c, "ALTER SYSTEM SET paimon_heap.hdfs_namenode = 'localhost:" + namenodePort + "'");
            exec(c, "ALTER SYSTEM SET paimon_heap.hdfs_path     = '" + hdfsBase + "'");
            exec(c, "SELECT pg_reload_conf()");
            Thread.sleep(1000);

            exec(c, "DROP TABLE IF EXISTS " + tbl);
            exec(c, "CREATE TABLE " + tbl + " (id int, val text) USING paimon_heap");

            /* Phase 1: INSERT 20 rows */
            System.out.println("  [HDFS] Phase 1: INSERT 20 rows");
            c.setAutoCommit(false);
            for (int i = 1; i <= 20; i++)
                exec(c, "INSERT INTO " + tbl + " VALUES (" + i + ", 'row" + i + "')");
            c.commit();
            c.setAutoCommit(true);

            waitForFlushAndCleanup(tbl, 30);
            PaimonRows r1 = readHdfsRows(fs, hdfsBase, tbl);
            System.out.printf("  [HDFS] INSERT: total=%d kinds=%s%n", r1.total, r1.kindCounts);
            check("HDFS INSERT: 20 rows total",       r1.total == 20);
            check("HDFS INSERT: 20 kind=INSERT(0)",   r1.ofKind(0) == 20);
            check("HDFS INSERT: 0 UPDATE_BEFORE(1)",  r1.ofKind(1) == 0);
            check("HDFS INSERT: 0 UPDATE_AFTER(2)",   r1.ofKind(2) == 0);
            check("HDFS INSERT: 0 DELETE(3)",         r1.ofKind(3) == 0);

            /* Phase 2: UPDATE 5 rows */
            System.out.println("  [HDFS] Phase 2: UPDATE 5 rows");
            c.setAutoCommit(false);
            for (int i = 1; i <= 5; i++)
                exec(c, "UPDATE " + tbl + " SET val='updated" + i + "' WHERE id=" + i);
            c.commit();
            c.setAutoCommit(true);

            waitForFlushAndCleanup(tbl, 30);
            PaimonRows r2 = readHdfsRows(fs, hdfsBase, tbl);
            System.out.printf("  [HDFS] UPDATE: total=%d kinds=%s%n", r2.total, r2.kindCounts);
            check("HDFS UPDATE: 30 rows total",       r2.total == 30);
            check("HDFS UPDATE: 20 INSERT",           r2.ofKind(0) == 20);
            check("HDFS UPDATE: 5 UPDATE_BEFORE(1)",  r2.ofKind(1) == 5);
            check("HDFS UPDATE: 5 UPDATE_AFTER(2)",   r2.ofKind(2) == 5);
            check("HDFS UPDATE: 0 DELETE(3)",         r2.ofKind(3) == 0);

            /* Phase 3: DELETE 3 rows */
            System.out.println("  [HDFS] Phase 3: DELETE 3 rows");
            c.setAutoCommit(false);
            for (int i = 18; i <= 20; i++)
                exec(c, "DELETE FROM " + tbl + " WHERE id=" + i);
            c.commit();
            c.setAutoCommit(true);

            waitForFlushAndCleanup(tbl, 30);
            PaimonRows r3 = readHdfsRows(fs, hdfsBase, tbl);
            System.out.printf("  [HDFS] DELETE: total=%d kinds=%s%n", r3.total, r3.kindCounts);
            check("HDFS DELETE: 33 rows total",      r3.total == 33);
            check("HDFS DELETE: 20 INSERT",          r3.ofKind(0) == 20);
            check("HDFS DELETE: 5 UPDATE_BEFORE(1)", r3.ofKind(1) == 5);
            check("HDFS DELETE: 5 UPDATE_AFTER(2)",  r3.ofKind(2) == 5);
            check("HDFS DELETE: 3 DELETE(3)",        r3.ofKind(3) == 3);

            exec(c, "DROP TABLE " + tbl);
            exec(c, "ALTER SYSTEM RESET paimon_heap.hdfs_namenode");
            exec(c, "ALTER SYSTEM RESET paimon_heap.hdfs_path");
            exec(c, "SELECT pg_reload_conf()");
        } finally {
            hdfsCluster.shutdown();
            hdfsCluster = null;
            deleteDir(hdfsDataDir.toPath());
        }
    }

    /* ── Remote Paimon readers ──────────────────────────────────────── */

    /**
     * Download the table from MinIO into a temp local directory and read
     * all Parquet files through the Paimon library.
     */
    static PaimonRows readS3Rows(String tbl) throws Exception {
        String localBase = "/tmp/paimon-s3-dl-" + System.currentTimeMillis();
        String localTbl  = localBase + "/" + tbl;
        Files.createDirectories(Paths.get(localTbl));
        // mc cp --recursive <alias>/<bucket>/<tbl>/ <localTbl>/
        // trailing slash on source = copy contents (not the prefix key itself)
        shell(MC_BIN, "cp", "--recursive",
              "minio-test/" + S3_BUCKET + "/" + tbl + "/",
              localTbl + "/");
        try {
            return readLocalRows(localBase, tbl);
        } finally {
            deleteDir(Paths.get(localBase));
        }
    }

    /**
     * Copy the table from the in-process HDFS cluster to a temp local dir
     * and read all Parquet files through the Paimon library.
     */
    static PaimonRows readHdfsRows(FileSystem fs, String hdfsBase, String tbl) throws Exception {
        String localBase = "/tmp/paimon-hdfs-dl-" + System.currentTimeMillis();
        java.io.File localTbl = new java.io.File(localBase + "/" + tbl);
        localTbl.mkdirs();
        copyHdfsToLocal(fs, new Path(hdfsBase + "/" + tbl), localTbl);
        try {
            return readLocalRows(localBase, tbl);
        } finally {
            deleteDir(Paths.get(localBase));
        }
    }

    /** Recursively copy an HDFS directory tree to a local directory. */
    static void copyHdfsToLocal(FileSystem fs, Path src, java.io.File dst) throws IOException {
        dst.mkdirs();
        FileStatus[] statuses = fs.listStatus(src);
        if (statuses == null) return;
        for (FileStatus st : statuses) {
            java.io.File localFile = new java.io.File(dst, st.getPath().getName());
            if (st.isDirectory()) {
                copyHdfsToLocal(fs, st.getPath(), localFile);
            } else {
                try (InputStream  in  = fs.open(st.getPath());
                     OutputStream out = new java.io.FileOutputStream(localFile)) {
                    byte[] buf = new byte[65536];
                    int n;
                    while ((n = in.read(buf)) >= 0) out.write(buf, 0, n);
                }
            }
        }
    }

    /**
     * Read all Parquet files for {@code tbl} from a local warehouse directory
     * using the Paimon library. Prepends a synthetic {@code _row_kind} field so
     * we can inspect the changelog row type (0=INSERT, 1=UPD_BEFORE,
     * 2=UPD_AFTER, 3=DELETE).
     */
    static PaimonRows readLocalRows(String localWarehouse, String tbl) throws Exception {
        org.apache.paimon.fs.Path tablePath =
            new org.apache.paimon.fs.Path(localWarehouse + "/" + tbl);

        TableSchema stored = new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema found for: " + tbl));
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

        PaimonRows result = new PaimonRows();
        try (RecordReader<InternalRow> reader = rb.newRead().createReader(splits)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow r;
                while ((r = batch.next()) != null) {
                    result.total++;
                    if (!r.isNullAt(0))
                        result.kindCounts.merge((int) r.getByte(0), 1L, Long::sum);
                }
                batch.releaseBatch();
            }
        }
        return result;
    }

    static class PaimonRows {
        long total = 0;
        Map<Integer, Long> kindCounts = new HashMap<>();
        long ofKind(int k) { return kindCounts.getOrDefault(k, 0L); }
    }

    /* ── Wait helpers ───────────────────────────────────────────────── */

    /**
     * Wait until the bgworker has (a) flushed at least one Parquet file,
     * then (b) uploaded and deleted all local Parquet files.
     * Local deletion is the bgworker's signal that the upload succeeded.
     */
    static void waitForFlushAndCleanup(String tbl, int maxSeconds) throws Exception {
        long deadline = System.currentTimeMillis() + (long) maxSeconds * 1000;

        // Step 1: wait for at least one parquet to appear
        while (countLocalParquets(tbl) == 0) {
            if (System.currentTimeMillis() > deadline)
                throw new AssertionError(
                    "No parquet files flushed for " + tbl + " within " + maxSeconds + "s");
            Thread.sleep(200);
        }
        long n = countLocalParquets(tbl);
        System.out.printf("  %d parquet(s) flushed locally, waiting for upload...%n", n);

        // Step 2: wait for upload (bgworker deletes local files after upload)
        while (countLocalParquets(tbl) > 0) {
            if (System.currentTimeMillis() > deadline)
                throw new AssertionError(
                    "Parquets not uploaded for " + tbl + " within " + maxSeconds + "s "
                    + "(" + countLocalParquets(tbl) + " remain)");
            Thread.sleep(200);
        }
        double elapsed = ((long) maxSeconds * 1000 - (deadline - System.currentTimeMillis())) / 1000.0;
        System.out.printf("  Upload complete (%.1fs elapsed)%n", elapsed);
    }

    /**
     * Poll snapshot/LATEST in S3 until the stored snapshot ID is greater than
     * {@code prevSnap}, indicating the bgworker flushed at least one new batch.
     * A 500ms settle sleep after detection allows any trailing manifest/schema
     * uploads (which land after LATEST in the bgworker's upload order) to finish
     * before the caller invokes readS3Rows.
     */
    static void waitForS3Flush(String tbl, long prevSnap, int maxSeconds) throws Exception {
        long deadline = System.currentTimeMillis() + (long) maxSeconds * 1000;
        System.out.printf("  Waiting for S3 flush (prev snapshot: %d)...%n", prevSnap);
        while (true) {
            long cur = readS3LatestSnapshotId(tbl);
            if (cur > prevSnap) {
                System.out.printf("  S3 flush confirmed: snapshot %d -> %d%n", prevSnap, cur);
                Thread.sleep(500); // let any trailing manifest/schema uploads finish
                return;
            }
            if (System.currentTimeMillis() > deadline)
                throw new AssertionError(
                    "No new S3 snapshot for " + tbl + " within " + maxSeconds + "s "
                    + "(prev=" + prevSnap + " cur=" + cur + ")");
            Thread.sleep(500);
        }
    }

    /** Read the numeric content of snapshot/LATEST from S3; returns -1 if absent. */
    static long readS3LatestSnapshotId(String tbl) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                MC_BIN, "cat",
                "minio-test/" + S3_BUCKET + "/" + tbl + "/snapshot/LATEST");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            String out = new String(p.getInputStream().readAllBytes()).trim();
            p.waitFor();
            if (!out.isEmpty()) return Long.parseLong(out);
        } catch (Exception ignored) {}
        return -1;
    }

    static long countLocalParquets(String tbl) throws IOException {
        // Extension stores tables under WAREHOUSE/default.db/<tbl>/
        java.nio.file.Path root = Paths.get(WAREHOUSE, "default.db", tbl);
        if (!Files.exists(root)) return 0;
        try (Stream<java.nio.file.Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".parquet")).count();
        }
    }

    /* ── Assertion helper ───────────────────────────────────────────── */

    static void check(String desc, boolean cond) {
        if (cond) {
            System.out.printf("  PASS  %s%n", desc);
        } else {
            System.out.printf("  FAIL  %s%n", desc);
            throw new AssertionError("FAIL: " + desc);
        }
    }

    /* ── MinIO lifecycle ────────────────────────────────────────────── */

    static void startMinio() throws Exception {
        Files.createDirectories(Paths.get(MINIO_DIR));
        ProcessBuilder pb = new ProcessBuilder(
            MINIO_BIN, "server", MINIO_DIR, "--address", "127.0.0.1:" + MINIO_PORT);
        pb.environment().put("MINIO_ROOT_USER",     MINIO_USER);
        pb.environment().put("MINIO_ROOT_PASSWORD", MINIO_PASS);
        pb.redirectErrorStream(true);
        pb.redirectOutput(new java.io.File("/tmp/minio-offload.log"));
        minioProc = pb.start();

        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                java.net.Socket s = new java.net.Socket("127.0.0.1", MINIO_PORT);
                s.close();
                Thread.sleep(500);
                System.out.println("  MinIO started on port " + MINIO_PORT);
                return;
            } catch (java.net.ConnectException ignored) {
                Thread.sleep(300);
            }
        }
        throw new RuntimeException("MinIO did not start within 10 s");
    }

    static void stopMinio() {
        if (minioProc != null) { minioProc.destroyForcibly(); minioProc = null; }
        deleteDir(Paths.get(MINIO_DIR));
    }

    static void writeAwsCreds(String homeDir) throws IOException {
        java.nio.file.Path awsDir = Paths.get(homeDir, ".aws");
        Files.createDirectories(awsDir);
        String creds =
            "[default]\n" +
            "aws_access_key_id=" + MINIO_USER + "\n" +
            "aws_secret_access_key=" + MINIO_PASS + "\n";
        Files.writeString(awsDir.resolve("credentials"), creds);
        System.out.println("  AWS credentials written to " + awsDir);
    }

    /* ── Connection / SQL helpers ───────────────────────────────────── */

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(JDBC_URL, DB_USER, "");
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    /* ── Shell helpers ──────────────────────────────────────────────── */

    static void shell(String... cmd) throws Exception {
        int rc = shellCode(cmd);
        if (rc != 0) throw new RuntimeException(
            "Command failed (exit " + rc + "): " + Arrays.toString(cmd));
    }

    static int shellCode(String... cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        return pb.start().waitFor();
    }

    /* ── File helpers ───────────────────────────────────────────────── */

    static void collectFiles(FileSystem fs, Path dir, List<String> out)
            throws IOException {
        for (FileStatus st : fs.listStatus(dir)) {
            if (st.isDirectory()) collectFiles(fs, st.getPath(), out);
            else out.add(st.getPath().getName());
        }
    }

    static void deleteDir(java.nio.file.Path p) {
        if (!Files.exists(p)) return;
        try (Stream<java.nio.file.Path> s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(f -> {
                try { Files.delete(f); } catch (IOException ignored) {}
            });
        } catch (IOException ignored) {}
    }
}
