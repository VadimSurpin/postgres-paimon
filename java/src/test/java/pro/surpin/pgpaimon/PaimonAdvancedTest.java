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

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Advanced integration tests for paimon_heap.
 *
 *   T9  – CHECKPOINT: rows survive a PostgreSQL checkpoint
 *   T10 – VACUUM: VACUUM on paimon_heap does not error and leaves warehouse intact
 *   T11 – VACUUM FREEZE: same guarantee for VACUUM FREEZE
 *   T12 – ALTER TABLE ADD COLUMN: rows captured; new column absent from schema (known limitation)
 *   T13 – ALTER TABLE DROP COLUMN: rows still captured after column drop
 *   T14 – ALTER TABLE RENAME COLUMN: data captured correctly; old name stays in schema
 *   T15 – ALTER COLUMN TYPE: no crash; rows captured after type change
 *   T16 – Crash recovery: warehouse data survives immediate (crash) shutdown + restart
 *   T17 – Default TAM: CREATE EXTENSION sets database default; plain CREATE TABLE uses paimon_heap
 *
 * Usage:
 *   java -cp paimon-advanced-test.jar pro.surpin.pgpaimon.PaimonAdvancedTest \
 *        [warehouse] [jdbc-url] [pg_ctl-path] [pgdata-dir]
 */
public class PaimonAdvancedTest {

    static final String DEFAULT_WAREHOUSE = "/tmp/paimon-warehouse";
    static final String DEFAULT_JDBC      = "jdbc:postgresql://localhost:5434/testdb?user=ubuntu&password=";
    static final String DEFAULT_PGCTL     = "/usr/local/pgsql/bin/pg_ctl";
    static final String DEFAULT_PGDATA    = "/root/cloud-db/pgdata3";

    static String warehouse;
    static String jdbcUrl;
    static String pgCtl;
    static String pgData;
    static LocalFileIO fileIO = LocalFileIO.create();
    static int passed = 0, failed = 0;

    public static void main(String[] args) throws Exception {
        warehouse = args.length > 0 ? args[0] : DEFAULT_WAREHOUSE;
        jdbcUrl   = args.length > 1 ? args[1] : DEFAULT_JDBC;
        pgCtl     = args.length > 2 ? args[2] : DEFAULT_PGCTL;
        pgData    = args.length > 3 ? args[3] : DEFAULT_PGDATA;

        System.out.println("╔════════════════════════════════════════════════════╗");
        System.out.println("║       paimon_heap Advanced Test Suite              ║");
        System.out.println("╚════════════════════════════════════════════════════╝");
        System.out.printf("  Warehouse : %s%n", warehouse);
        System.out.printf("  JDBC      : %s%n", jdbcUrl);
        System.out.printf("  pg_ctl    : %s%n", pgCtl);
        System.out.printf("  pgdata    : %s%n", pgData);
        System.out.println();

        test9_checkpoint();
        test10_vacuum();
        test11_vacuumFreeze();
        test12_alterAddColumn();
        test13_alterDropColumn();
        test14_alterRenameColumn();
        test15_alterColumnType();
        test16_crashRecovery();
        test17_defaultTam();

        System.out.println();
        System.out.println("═".repeat(52));
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        System.exit(failed > 0 ? 1 : 0);
    }

    // ── T9: CHECKPOINT ───────────────────────────────────────────────────────
    // CHECKPOINT flushes dirty pages and advances the WAL redo point. Because
    // paimon_heap stores rows in a shared-memory ring buffer (not WAL pages),
    // a checkpoint must not disturb in-flight or already-flushed capture data.
    static void test9_checkpoint() throws Exception {
        System.out.println("\n── T9: CHECKPOINT ──────────────────────────────────");
        String tbl = "adv_checkpoint";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v INT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20),(3,30)");
            exec(conn, "CHECKPOINT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (4,40),(5,50)");
            exec(conn, "CHECKPOINT");  // checkpoint again after second batch
        }
        PaimonRows rows = waitAndRead(tbl, 5);
        check("T9 checkpoint: all 5 rows captured",  rows.total == 5);
        check("T9 checkpoint: all are inserts",       rows.ofKind(0) == 5);
    }

    // ── T10: VACUUM ──────────────────────────────────────────────────────────
    // VACUUM on a paimon_heap table must succeed without error. Since paimon_heap
    // has no on-disk tuple storage, the vacuum TAM callback is a no-op. The already-
    // captured rows in the Paimon warehouse must remain intact after vacuum.
    static void test10_vacuum() throws Exception {
        System.out.println("\n── T10: VACUUM ─────────────────────────────────────");
        String tbl = "adv_vacuum";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v INT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,1),(2,2),(3,3),(4,4),(5,5)");
            exec(conn, "DELETE FROM " + tbl + " WHERE id IN (2,4)");
        }
        waitForParquet(tbl, 1);
        Thread.sleep(1500);

        boolean vacuumOk = true;
        try (Connection conn = connect()) {
            exec(conn, "VACUUM " + tbl);
        } catch (Exception e) {
            vacuumOk = false;
            System.err.println("  VACUUM error: " + e.getMessage());
        }
        check("T10 vacuum: no error",              vacuumOk);

        PaimonRows rows = readRows(tbl);
        // changelog: 5 inserts + 2 deletes
        check("T10 vacuum: warehouse unchanged",   rows.total == 7);
        check("T10 vacuum: 5 inserts present",     rows.ofKind(0) == 5);
        check("T10 vacuum: 2 deletes present",     rows.ofKind(3) == 2);

        // New inserts after vacuum must still be captured
        try (Connection conn = connect()) {
            exec(conn, "INSERT INTO " + tbl + " VALUES (6,6),(7,7)");
        }
        PaimonRows rows2 = waitAndRead(tbl, 9);
        check("T10 vacuum: inserts after vacuum captured",  rows2.total == 9);
    }

    // ── T11: VACUUM FREEZE ───────────────────────────────────────────────────
    // VACUUM FREEZE updates pg_class.relfrozenxid to allow XID wraparound
    // prevention. For paimon_heap, this only touches catalog entries; it must
    // not touch or invalidate the Paimon warehouse files.
    static void test11_vacuumFreeze() throws Exception {
        System.out.println("\n── T11: VACUUM FREEZE ──────────────────────────────");
        String tbl = "adv_vacfreeze";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v TEXT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'a'),(2,'b'),(3,'c')");
        }
        waitForParquet(tbl, 1);
        Thread.sleep(1500);

        boolean freezeOk = true;
        try (Connection conn = connect()) {
            exec(conn, "VACUUM FREEZE " + tbl);
        } catch (Exception e) {
            freezeOk = false;
            System.err.println("  VACUUM FREEZE error: " + e.getMessage());
        }
        check("T11 vacuum freeze: no error",              freezeOk);

        PaimonRows rows = readRows(tbl);
        check("T11 vacuum freeze: 3 rows intact",         rows.total == 3);

        // Inserts after freeze must still work
        try (Connection conn = connect()) {
            exec(conn, "INSERT INTO " + tbl + " VALUES (4,'d'),(5,'e')");
        }
        PaimonRows rows2 = waitAndRead(tbl, 5);
        check("T11 vacuum freeze: inserts after freeze",  rows2.total == 5);
    }

    // ── T12: ALTER TABLE ADD COLUMN ──────────────────────────────────────────
    // paimon_ddl.cpp ProcessUtility_hook intercepts ADD COLUMN post-execute,
    // reads the new column's attnum/type from pg_attribute, and sends
    // PMSG_DDL_ADD_COL to the ring.  The bgworker adds the field to the Paimon
    // schema (schema-1).  DML with the new column must be captured correctly.
    static void test12_alterAddColumn() throws Exception {
        System.out.println("\n── T12: ALTER TABLE ADD COLUMN ──────────────────────");
        String tbl = "adv_alter_add";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v INT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20)");
        }
        waitForParquet(tbl, 1);
        Thread.sleep(1500);
        check("T12 add col: 2 rows captured before ALTER",  readRows(tbl).total == 2);

        boolean alterOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " ADD COLUMN extra TEXT DEFAULT 'x'");
            exec(conn, "INSERT INTO " + tbl + " VALUES (3,30,'hello')");
            exec(conn, "INSERT INTO " + tbl + " VALUES (4,40,'world')");
        } catch (Exception e) {
            alterOk = false;
            System.err.println("  ALTER/INSERT error: " + e.getMessage());
        }
        check("T12 add col: ALTER + INSERT no crash",  alterOk);

        if (alterOk) {
            PaimonRows rows = waitAndRead(tbl, 4);
            check("T12 add col: 4 rows captured after ALTER",  rows.total == 4);

            // DDL hook sends PMSG_DDL_ADD_COL; bgworker adds column to schema
            File schema1 = new File(warehouse + "/" + tbl + "/schema/schema-1");
            long deadline = System.currentTimeMillis() + 30_000;
            while (!schema1.exists() && System.currentTimeMillis() < deadline)
                Thread.sleep(500);

            TableSchema schema = readSchema(tbl);
            boolean extraInSchema = schema.fields().stream().anyMatch(f -> f.name().equals("extra"));
            check("T12 add col: 'extra' present in schema (DDL hook active)",  extraInSchema);
        }
    }

    // ── T13: ALTER TABLE DROP COLUMN ─────────────────────────────────────────
    // Dropping a column changes the fingerprint; re-bootstrap is triggered but
    // only adds meta fields. The dropped column remains in the Paimon schema.
    // New rows no longer carry a value for that column — the writer emits an
    // absent (null) value for it. DML must not crash.
    static void test13_alterDropColumn() throws Exception {
        System.out.println("\n── T13: ALTER TABLE DROP COLUMN ─────────────────────");
        String tbl = "adv_alter_drop";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v INT, extra TEXT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10,'a'),(2,20,'b')");
        }
        waitForParquet(tbl, 1);
        Thread.sleep(1500);
        check("T13 drop col: 2 rows before ALTER",  readRows(tbl).total == 2);

        boolean ok = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " DROP COLUMN extra");
            exec(conn, "INSERT INTO " + tbl + " VALUES (3,30)");
            exec(conn, "INSERT INTO " + tbl + " VALUES (4,40)");
        } catch (Exception e) {
            ok = false;
            System.err.println("  ALTER/INSERT error: " + e.getMessage());
        }
        check("T13 drop col: ALTER + INSERT no crash",  ok);
        if (ok) {
            PaimonRows rows = waitAndRead(tbl, 4);
            check("T13 drop col: 4 rows captured",     rows.total == 4);
        }
    }

    // ── T14: ALTER TABLE RENAME COLUMN ───────────────────────────────────────
    // RENAME COLUMN arrives as a RenameStmt; paimon_ddl.cpp intercepts it
    // pre-execute, looks up the old column's attnum, and sends PMSG_DDL_RENAME
    // with the new name.  The bgworker renames the field in the Paimon schema.
    // The attnum (= Paimon field_id) is unchanged, so existing Parquet data
    // remains readable under the new name.
    static void test14_alterRenameColumn() throws Exception {
        System.out.println("\n── T14: ALTER TABLE RENAME COLUMN ───────────────────");
        String tbl = "adv_alter_rename";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v_old INT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,100),(2,200)");
        }
        waitForParquet(tbl, 1);
        Thread.sleep(1500);
        check("T14 rename: 2 rows before ALTER",  readRows(tbl).total == 2);

        boolean ok = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " RENAME COLUMN v_old TO v_new");
            exec(conn, "INSERT INTO " + tbl + " VALUES (3,300)");
            exec(conn, "INSERT INTO " + tbl + " VALUES (4,400)");
        } catch (Exception e) {
            ok = false;
            System.err.println("  ALTER/INSERT error: " + e.getMessage());
        }
        check("T14 rename: ALTER + INSERT no crash",  ok);
        if (ok) {
            PaimonRows rows = waitAndRead(tbl, 4);
            check("T14 rename: 4 rows captured",  rows.total == 4);

            // DDL hook sends PMSG_DDL_RENAME; bgworker updates field name in schema
            File schema1 = new File(warehouse + "/" + tbl + "/schema/schema-1");
            long deadline = System.currentTimeMillis() + 30_000;
            while (!schema1.exists() && System.currentTimeMillis() < deadline)
                Thread.sleep(500);

            TableSchema schema = readSchema(tbl);
            boolean newName = schema.fields().stream().anyMatch(f -> f.name().equals("v_new"));
            boolean oldGone = schema.fields().stream().noneMatch(f -> f.name().equals("v_old"));
            check("T14 rename: 'v_new' present in schema (DDL hook active)",  newName);
            check("T14 rename: 'v_old' removed from schema",                   oldGone);
        }
    }

    // ── T15: ALTER COLUMN TYPE ────────────────────────────────────────────────
    // Type change alters the fingerprint. Paimon schema is not evolved (no DDL
    // hook). Values after the ALTER are captured using the old Paimon type mapping.
    // For widening casts (SMALLINT → INT) the data fits and is captured correctly.
    // The test verifies no crash and continued row capture.
    static void test15_alterColumnType() throws Exception {
        System.out.println("\n── T15: ALTER COLUMN TYPE ───────────────────────────");
        String tbl = "adv_alter_type";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v SMALLINT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20)");
        }
        waitForParquet(tbl, 1);
        Thread.sleep(1500);
        check("T15 type change: 2 rows before ALTER",  readRows(tbl).total == 2);

        boolean ok = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " ALTER COLUMN v TYPE INT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (3,30000)");
            exec(conn, "INSERT INTO " + tbl + " VALUES (4,40000)");
        } catch (Exception e) {
            ok = false;
            System.err.println("  ALTER/INSERT error: " + e.getMessage());
        }
        check("T15 type change: ALTER + INSERT no crash",  ok);
        if (ok) {
            PaimonRows rows = waitAndRead(tbl, 4);
            check("T15 type change: 4 rows captured",      rows.total == 4);
        }
    }

    // ── T16: CRASH RECOVERY ──────────────────────────────────────────────────
    // Verifies that Paimon warehouse data written to Parquet files before a crash
    // survives PostgreSQL immediate (crash-mode) shutdown + restart.
    //
    // Rows that were in the ring buffer at crash time are intentionally lost —
    // paimon_heap provides best-effort capture with no WAL-based durability.
    // The invariant being tested: any Parquet file fully written before the crash
    // is intact and readable afterwards (Parquet is append-only and immutable).
    //
    // The test waits 10 s after the last INSERT to give the bgworker (default
    // flush_interval_secs = 5 s) enough time to flush the rows before the crash.
    static void test16_crashRecovery() throws Exception {
        System.out.println("\n── T16: CRASH RECOVERY ──────────────────────────────");
        String tbl = "adv_crash_recovery";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v TEXT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'pre-a'),(2,'pre-b'),(3,'pre-c')");
        }

        System.out.println("  Waiting 10 s for bgworker flush before crash...");
        waitForParquet(tbl, 1);
        Thread.sleep(10_000);

        long rowsBeforeCrash = readRows(tbl).total;
        check("T16 recovery: rows flushed to warehouse before crash",  rowsBeforeCrash >= 3);

        // Immediate shutdown simulates a crash (equivalent to kill -SIGQUIT postmaster).
        // No checkpoint is written; shared memory (ring buffer) is lost.
        boolean stopOk = runPgCtl("stop", "-m", "immediate");
        check("T16 recovery: pg_ctl stop -m immediate succeeded",  stopOk);

        System.out.println("  Restarting PostgreSQL...");
        boolean startOk = runPgCtl("start", "-l", pgData + "/pg.log");
        check("T16 recovery: pg_ctl start succeeded",  startOk);

        if (startOk) {
            waitForServer(30_000);

            // Warehouse files live on the local filesystem and survive restart intact.
            long rowsAfterRestart = readRows(tbl).total;
            check("T16 recovery: warehouse row count unchanged",  rowsAfterRestart == rowsBeforeCrash);

            // The crashed table's RocksDB-backed heap may be unusable after immediate
            // shutdown (RocksDB crash recovery may drop uncommitted data). Test that
            // paimon_heap works correctly on a freshly created table after restart.
            String tbl2 = tbl + "_post";
            cleanup(tbl2);
            boolean postOk = true;
            try (Connection conn = connect()) {
                disableMeta(conn);
                exec(conn, "DROP TABLE IF EXISTS " + tbl2);
                exec(conn, "CREATE TABLE " + tbl2 + " (id INT PRIMARY KEY, v TEXT) USING paimon_heap");
                exec(conn, "INSERT INTO " + tbl2 + " VALUES (1,'post-restart-a'),(2,'post-restart-b')");
            } catch (Exception e) {
                postOk = false;
                System.err.println("  post-restart DML error: " + e.getMessage());
            }
            check("T16 recovery: paimon_heap functional after restart",  postOk);
            if (postOk) {
                PaimonRows rows2 = waitAndRead(tbl2, 2);
                check("T16 recovery: new rows captured after restart",  rows2.total == 2);
            }
        }
    }

    // ── T17: DEFAULT TABLE ACCESS METHOD ─────────────────────────────────────
    // CREATE EXTENSION paimon_heap runs:
    //   ALTER DATABASE <current> SET default_table_access_method = 'paimon_heap'
    // so that plain CREATE TABLE (without USING) automatically creates paimon_heap
    // tables. This test verifies:
    //   1. SHOW default_table_access_method returns 'paimon_heap'
    //   2. pg_class.relam for a table created without USING is paimon_heap
    //   3. INSERTs into that table appear in the Paimon warehouse
    static void test17_defaultTam() throws Exception {
        System.out.println("\n── T17: DEFAULT TABLE ACCESS METHOD ─────────────────");
        String tbl = "adv_default_tam";
        cleanup(tbl);

        String showResult = null;
        try (Connection conn = connect();
             Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SHOW default_table_access_method")) {
            if (rs.next()) showResult = rs.getString(1).trim();
        }
        check("T17 default tam: GUC = 'paimon_heap'",  "paimon_heap".equals(showResult));

        // Create table WITHOUT USING clause
        String actualAm = null;
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v TEXT)");
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                     "SELECT am.amname " +
                     "FROM pg_class c JOIN pg_am am ON c.relam = am.oid " +
                     "WHERE c.relname = '" + tbl + "' AND c.relkind = 'r'")) {
                if (rs.next()) actualAm = rs.getString(1);
            }
        }
        check("T17 default tam: pg_class.relam = 'paimon_heap'",  "paimon_heap".equals(actualAm));

        if ("paimon_heap".equals(actualAm)) {
            try (Connection conn = connect()) {
                exec(conn, "INSERT INTO " + tbl + " VALUES (1,'default-am'),(2,'test')");
            }
            PaimonRows rows = waitAndRead(tbl, 2);
            check("T17 default tam: inserts captured in warehouse",  rows.total == 2);
            check("T17 default tam: all are insert rows",            rows.ofKind(0) == 2);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    static void disableMeta(Connection c) throws SQLException {
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
            System.err.println("  warn: row read failed for " + tbl + ": " + e.getMessage());
        }
        return result;
    }

    static boolean runPgCtl(String... extraArgs) {
        try {
            // Build the inner pg_ctl command as a shell string
            List<String> inner = new ArrayList<>();
            inner.add(pgCtl);
            inner.addAll(Arrays.asList(extraArgs));
            inner.add("-D");
            inner.add(pgData);
            String innerCmd = String.join(" ", inner);
            System.out.println("  Running: su - ubuntu -c \"" + innerCmd + "\"");

            // pg_ctl refuses to run as root; delegate to the postgres owner via su
            ProcessBuilder pb = new ProcessBuilder("su", "-", "ubuntu", "-c", innerCmd);
            pb.redirectErrorStream(true);
            Process p = pb.start();
            new Thread(() -> {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                    String line;
                    while ((line = br.readLine()) != null) System.out.println("  [pg_ctl] " + line);
                } catch (IOException ignored) {}
            }).start();
            boolean done = p.waitFor(60, TimeUnit.SECONDS);
            return done && p.exitValue() == 0;
        } catch (Exception e) {
            System.err.println("  pg_ctl error: " + e.getMessage());
            return false;
        }
    }

    // Waits until the server accepts connections AND user-table DDL succeeds.
    // On clouddb, the RocksDB KVServer starts asynchronously after the postmaster:
    // plain connections succeed before RocksDB WAL replay finishes, so a bare
    // connection check is not sufficient.
    static void waitForServer(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;

        // Phase 1: wait for basic TCP connectivity
        while (System.currentTimeMillis() < deadline) {
            try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
                System.out.println("  Server accepting connections.");
                break;
            } catch (SQLException ignored) {
                Thread.sleep(500);
            }
        }

        // Phase 2: wait until user-table DDL works (RocksDB recovery may lag).
        // Use a throwaway paimon_heap table as the canary.
        while (System.currentTimeMillis() < deadline) {
            try (Connection conn = DriverManager.getConnection(jdbcUrl);
                 Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS _kv_ready_probe");
                s.execute("CREATE TABLE _kv_ready_probe (x INT) USING paimon_heap");
                s.execute("DROP TABLE _kv_ready_probe");
                System.out.println("  Storage manager ready.");
                return;
            } catch (Exception e) {
                System.out.println("  Waiting for storage manager recovery...");
                Thread.sleep(2000);
            }
        }
        System.out.println("  Warning: storage may not be fully recovered within timeout.");
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
