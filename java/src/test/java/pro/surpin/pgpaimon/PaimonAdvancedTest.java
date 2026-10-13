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

import java.io.*;
import java.nio.file.Files;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Advanced integration tests for the slot-based writer.
 *
 *   T9  – CHECKPOINT: rows survive a PostgreSQL checkpoint
 *   T10 – VACUUM: emits no logical changes; the record count is unchanged across it
 *   T11 – VACUUM FREEZE: same
 *   T12 – ALTER TABLE ADD COLUMN: rows captured; new column present in schema
 *   T13 – ALTER TABLE DROP COLUMN: rows still captured after column drop
 *   T14 – ALTER TABLE RENAME COLUMN: data captured correctly; old name leaves the schema
 *   T15 – ALTER COLUMN TYPE: no crash; rows captured after type change
 *   T16 – Crash recovery: the WRITER is killed (SIGKILL) mid-load and restarted;
 *         zero loss and zero duplicates
 *   T17 – A table never passed to paimon.add_table() reaches neither paimon_pub nor the warehouse
 *
 * Usage (live stack): test/slot-acceptance/run_acceptance.sh PaimonAdvancedTest
 */
public class PaimonAdvancedTest {

    /**
     * The PostgreSQL schema these suites create their tables in, and therefore the
     * Paimon database they land in: the writer maps one to the other, so the warehouse
     * layout is <warehouse>/<schema>.db/<table>.
     */
    static final String PG_SCHEMA = "public";

    static final String DEFAULT_WAREHOUSE = env("PGPAIMON_IT_WAREHOUSE", "/tmp/paimon-warehouse");
    static final String DEFAULT_JDBC =
        env("PGPAIMON_IT_JDBC_URL", "jdbc:postgresql://localhost:5442/paimon_acc?user=postgres");

    static String warehouse;
    static String jdbcUrl;
    static LocalFileIO fileIO = LocalFileIO.create();
    static int passed = 0, failed = 0;
    static final List<String> failures = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        System.exit(run(args) > 0 ? 1 : 0);
    }

    static int run(String[] args) throws Exception {
        passed = 0; failed = 0; failures.clear();
        warehouse = args.length > 0 ? args[0] : DEFAULT_WAREHOUSE;
        jdbcUrl   = args.length > 1 ? args[1] : DEFAULT_JDBC;

        System.out.println("╔════════════════════════════════════════════════════╗");
        System.out.println("║       paimon slot-writer Advanced Test Suite       ║");
        System.out.println("╚════════════════════════════════════════════════════╝");
        System.out.printf("  Warehouse : %s%n", warehouse);
        System.out.printf("  JDBC      : %s%n", jdbcUrl);
        System.out.println();

        test9_checkpoint();
        test10_vacuum();
        test11_vacuumFreeze();
        test12_alterAddColumn();
        test13_alterDropColumn();
        test14_alterRenameColumn();
        test15_alterColumnType();
        test16_crashRecovery();
        test17_untracked();

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

    // ── T9: CHECKPOINT ───────────────────────────────────────────────────────
    // CHECKPOINT flushes dirty pages and advances the WAL redo point. Because
    // the slot retains WAL independently of checkpoints, a checkpoint must not
    // disturb in-flight or already-flushed capture data.
    static void test9_checkpoint() throws Exception {
        System.out.println("\n── T9: CHECKPOINT ──────────────────────────────────");
        String tbl = "adv_checkpoint";
        cleanup(tbl);
        try (Connection conn = connect()) {
            createTracked(conn, tbl, "id INT PRIMARY KEY, v INT");
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
    // Documented difference: VACUUM emits no logical changes, so nothing may appear
    // in the Paimon changelog.  The record count (and every kind count) is identical
    // before and after, and capture of later inserts continues.
    static void test10_vacuum() throws Exception {
        System.out.println("\n── T10: VACUUM ─────────────────────────────────────");
        String tbl = "adv_vacuum";
        cleanup(tbl);
        try (Connection conn = connect()) {
            createTracked(conn, tbl, "id INT PRIMARY KEY, v INT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,1),(2,2),(3,3),(4,4),(5,5)");
            exec(conn, "DELETE FROM " + tbl + " WHERE id IN (2,4)");
        }
        // changelog: 5 inserts + 2 deletes
        PaimonRows before = waitAndRead(tbl, 7);
        check("T10 vacuum: 7 records before VACUUM", before.total == 7);

        boolean vacuumOk = true;
        try (Connection conn = connect()) {
            exec(conn, "VACUUM " + tbl);
        } catch (Exception e) {
            vacuumOk = false;
            System.err.println("  VACUUM error: " + e.getMessage());
        }
        check("T10 vacuum: no error",              vacuumOk);

        Thread.sleep(3000);   // several flush intervals: anything VACUUM emitted would be committed by now
        PaimonRows rows = readRows(tbl);
        check("T10 vacuum: no record appeared (count unchanged)",  rows.total == before.total);
        check("T10 vacuum: 5 inserts, unchanged",                  rows.ofKind(0) == 5);
        check("T10 vacuum: 2 deletes, unchanged",                  rows.ofKind(3) == 2);

        // New inserts after vacuum must still be captured
        try (Connection conn = connect()) {
            exec(conn, "INSERT INTO " + tbl + " VALUES (6,6),(7,7)");
        }
        PaimonRows rows2 = waitAndRead(tbl, 9);
        check("T10 vacuum: inserts after vacuum captured",  rows2.total == 9);
    }

    // ── T11: VACUUM FREEZE ───────────────────────────────────────────────────
    // Documented difference: like VACUUM, VACUUM FREEZE emits no logical changes
    // (it rewrites no logical row), so the record count must be unchanged across it.
    static void test11_vacuumFreeze() throws Exception {
        System.out.println("\n── T11: VACUUM FREEZE ──────────────────────────────");
        String tbl = "adv_vacfreeze";
        cleanup(tbl);
        try (Connection conn = connect()) {
            createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'a'),(2,'b'),(3,'c')");
        }
        PaimonRows before = waitAndRead(tbl, 3);
        check("T11 vacuum freeze: 3 records before",      before.total == 3);

        boolean freezeOk = true;
        try (Connection conn = connect()) {
            exec(conn, "VACUUM FREEZE " + tbl);
        } catch (Exception e) {
            freezeOk = false;
            System.err.println("  VACUUM FREEZE error: " + e.getMessage());
        }
        check("T11 vacuum freeze: no error",              freezeOk);

        Thread.sleep(3000);
        PaimonRows rows = readRows(tbl);
        check("T11 vacuum freeze: no record appeared (count unchanged)",  rows.total == before.total);
        check("T11 vacuum freeze: all still inserts",     rows.ofKind(0) == 3);

        // Inserts after freeze must still work
        try (Connection conn = connect()) {
            exec(conn, "INSERT INTO " + tbl + " VALUES (4,'d'),(5,'e')");
        }
        PaimonRows rows2 = waitAndRead(tbl, 5);
        check("T11 vacuum freeze: inserts after freeze",  rows2.total == 5);
    }

    // ── T12: ALTER TABLE ADD COLUMN ──────────────────────────────────────────
    // Documented difference: main accepted ADD COLUMN ... DEFAULT but left historical Paimon rows
    // NULL; the extension now rejects the DEFAULT form (spec section 4; pg_regress guardrail tests
    // cover the rejection).  This test uses the supported form, a column without a default.
    // paimon_ddl.cpp ProcessUtility_hook intercepts ADD COLUMN post-execute,
    // reads the new column's attnum/type from pg_attribute, and sends
    // PMSG_DDL_ADD_COL to the ring.  The bgworker adds the field to the Paimon
    // schema (schema-1).  DML with the new column must be captured correctly.
    static void test12_alterAddColumn() throws Exception {
        System.out.println("\n── T12: ALTER TABLE ADD COLUMN ──────────────────────");
        String tbl = "adv_alter_add";
        cleanup(tbl);
        try (Connection conn = connect()) {
            createTracked(conn, tbl, "id INT PRIMARY KEY, v INT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20)");
        }
        check("T12 add col: 2 rows captured before ALTER",  waitAndRead(tbl, 2).total == 2);

        boolean alterOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " ADD COLUMN extra TEXT");
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
            File schema1 = new File(tableDir(tbl) + "/schema/schema-1");
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
            createTracked(conn, tbl, "id INT PRIMARY KEY, v INT, extra TEXT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10,'a'),(2,20,'b')");
        }
        check("T13 drop col: 2 rows before ALTER",  waitAndRead(tbl, 2).total == 2);

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
            createTracked(conn, tbl, "id INT PRIMARY KEY, v_old INT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,100),(2,200)");
        }
        check("T14 rename: 2 rows before ALTER",  waitAndRead(tbl, 2).total == 2);

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
            File schema1 = new File(tableDir(tbl) + "/schema/schema-1");
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
            createTracked(conn, tbl, "id INT PRIMARY KEY, v SMALLINT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20)");
        }
        check("T15 type change: 2 rows before ALTER",  waitAndRead(tbl, 2).total == 2);

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
    // Documented strengthening: main could only promise that files already written
    // survive a crash.  Here the WRITER is killed (SIGKILL) in the middle of a load,
    // more rows are committed in PostgreSQL while it is down (the slot retains the WAL),
    // and it is restarted.  Zero loss and zero duplicates.  This rests on the slot's
    // confirmed position advancing only after a Paimon commit returns, and on
    // filterAndCommit dropping a replayed commitIdentifier.
    //
    // Duplicates are looked for in the raw changelog (every record as stored): a merged
    // read collapses repeats of a key and would hide them.
    static final int T16_BATCHES = 100, T16_PER_BATCH = 20;

    static void test16_crashRecovery() throws Exception {
        System.out.println("\n── T16: CRASH RECOVERY (writer killed mid-load) ─────");
        String launcher = System.getenv("PGPAIMON_WRITER_LAUNCHER");
        String pidFile = System.getenv("PGPAIMON_WRITER_PID_FILE");
        if (launcher == null || pidFile == null) {
            if ("1".equals(System.getenv("PGPAIMON_HARNESS"))) {
                check("T16 harness exports PGPAIMON_WRITER_LAUNCHER and PGPAIMON_WRITER_PID_FILE", false);
            } else {
                System.out.println("  SKIP  T16: no writer launcher (run via test/slot-acceptance/run_acceptance.sh)");
            }
            return;
        }
        String tbl = "adv_crash_recovery";
        // Isolate this test from the DDL the earlier tests ran: after the kill the slot replays
        // everything past its confirmed position, so first let the confirmed position catch up
        // with the WAL written so far.  (Replay across a DDL boundary is a separate matter,
        // reported in the Task 16 report; this test is about a kill in the middle of a load.)
        String target;
        try (Connection conn = connect(); Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT pg_current_wal_insert_lsn()::text")) {
            rs.next();
            target = rs.getString(1);
        }
        long settle = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < settle) {
            try (Connection conn = connect(); Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT confirmed_flush_lsn >= '" + target + "'::pg_lsn FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
                if (rs.next() && rs.getBoolean(1)) break;
            }
            Thread.sleep(250);
        }
        cleanup(tbl);
        try (Connection conn = connect()) {
            createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");   // empty when registered
        }
        final int total = T16_BATCHES * T16_PER_BATCH;

        AtomicReference<Throwable> loadError = new AtomicReference<>();
        Thread loader = new Thread(() -> {
            try (Connection conn = connect()) {
                conn.setAutoCommit(true);
                for (int b = 0; b < T16_BATCHES; b++) {
                    StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
                    for (int k = 0; k < T16_PER_BATCH; k++) {
                        int id = b * T16_PER_BATCH + k + 1;
                        if (k > 0) sb.append(',');
                        sb.append('(').append(id).append(",'r").append(id).append("')");
                    }
                    exec(conn, sb.toString());       // one transaction per batch
                    Thread.sleep(60);
                }
            } catch (Throwable t) {
                loadError.set(t);
            }
        });
        loader.start();

        // Let the writer commit at least one snapshot, then kill it with the load still running.
        File latest = new File(tableDir(tbl) + "/snapshot/LATEST");
        long deadline = System.currentTimeMillis() + 60_000;
        while (!latest.exists() && System.currentTimeMillis() < deadline) Thread.sleep(50);
        check("T16 recovery: writer committed a snapshot before the kill", latest.exists());

        String pid = new String(Files.readAllBytes(new File(pidFile).toPath())).trim();
        boolean loadStillRunning = loader.isAlive();
        int killRc = new ProcessBuilder("kill", "-9", pid).inheritIO().start().waitFor();
        check("T16 recovery: kill -9 of the writer succeeded", killRc == 0);
        check("T16 recovery: load was still running when the writer was killed", loadStillRunning);
        waitProcessGone(pid);
        check("T16 recovery: writer process is gone", !processAlive(pid));

        Thread.sleep(1500);   // rows keep arriving in PostgreSQL while the writer is down
        check("T16 recovery: slot is inactive while the writer is down", !slotActive());

        System.out.println("  Restarting the writer...");
        Process p = new ProcessBuilder("bash", launcher)
            .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        check("T16 recovery: launcher exited cleanly", p.waitFor() == 0);
        deadline = System.currentTimeMillis() + 60_000;
        while (!slotActive() && System.currentTimeMillis() < deadline) Thread.sleep(250);
        check("T16 recovery: restarted writer reattached to the slot", slotActive());

        loader.join();
        check("T16 recovery: load completed without error", loadError.get() == null);
        if (loadError.get() != null) loadError.get().printStackTrace();

        // Wait for the changelog to reach every row.
        List<int[]> recs = readIdsAndKinds(tbl);
        deadline = System.currentTimeMillis() + 90_000;
        while (recs.size() < total && System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
            recs = readIdsAndKinds(tbl);
        }
        Thread.sleep(3000);   // a replayed batch would show up as extra records by now
        recs = readIdsAndKinds(tbl);

        Map<Integer, Integer> perId = new TreeMap<>();
        int nonInsert = 0;
        for (int[] r : recs) {
            perId.merge(r[1], 1, Integer::sum);
            if (r[0] != 0) nonInsert++;
        }
        List<Integer> lost = new ArrayList<>(), dup = new ArrayList<>();
        for (int id = 1; id <= total; id++) {
            int n = perId.getOrDefault(id, 0);
            if (n == 0) lost.add(id);
            if (n > 1) dup.add(id);
        }
        if (!lost.isEmpty()) System.out.println("  lost ids: " + lost);
        if (!dup.isEmpty())  System.out.println("  duplicated ids: " + dup);
        check("T16 recovery: zero loss (all " + total + " ids present)", lost.isEmpty());
        check("T16 recovery: zero duplicates (each id exactly once)", dup.isEmpty());
        check("T16 recovery: exactly " + total + " changelog records", recs.size() == total);
        check("T16 recovery: all records are inserts", nonInsert == 0);
    }

    static boolean processAlive(String pid) throws Exception {
        return new ProcessBuilder("kill", "-0", pid).redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD).start().waitFor() == 0;
    }

    static void waitProcessGone(String pid) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (processAlive(pid) && System.currentTimeMillis() < deadline) Thread.sleep(100);
    }

    static boolean slotActive() throws Exception {
        try (Connection conn = connect();
             Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT bool_or(active) FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    // ── T17: UNTRACKED TABLE ─────────────────────────────────────────────────
    // Documented difference: there is no table access method and no default; a table
    // is captured only when it is passed to paimon.add_table().  A table that never
    // was reaches neither paimon_pub nor the warehouse.  The writer is proven to have
    // read past the untracked table's changes by a tracked sentinel written afterwards.
    static void test17_untracked() throws Exception {
        System.out.println("\n── T17: UNTRACKED TABLE ─────────────────────────────");
        String tbl = "adv_untracked", sentinel = "adv_untracked_sentinel";
        cleanup(tbl);
        cleanup(sentinel);
        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v TEXT)");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'plain'),(2,'heap')");
            exec(conn, "UPDATE " + tbl + " SET v = 'changed' WHERE id = 1");
            exec(conn, "DELETE FROM " + tbl + " WHERE id = 2");
        }
        try (Connection conn = connect()) {
            createTracked(conn, sentinel, "id INT PRIMARY KEY");
            exec(conn, "INSERT INTO " + sentinel + " VALUES (1)");
        }
        check("T17 sentinel (tracked, written after the plain table) reached the warehouse",
              waitAndRead(sentinel, 1).total == 1);

        long inPub = -1, inRegistry = -1;
        String am = null;
        try (Connection conn = connect(); Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                     "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'paimon_pub' AND tablename = '" + tbl + "'")) {
                rs.next(); inPub = rs.getLong(1);
            }
            try (ResultSet rs = s.executeQuery(
                     "SELECT count(*) FROM paimon.table_registry WHERE relid = '" + tbl + "'::regclass")) {
                rs.next(); inRegistry = rs.getLong(1);
            }
            try (ResultSet rs = s.executeQuery(
                     "SELECT am.amname FROM pg_class c JOIN pg_am am ON c.relam = am.oid WHERE c.oid = '" + tbl + "'::regclass")) {
                if (rs.next()) am = rs.getString(1);
            }
        }
        check("T17 untracked: plain CREATE TABLE uses the ordinary heap", "heap".equals(am));
        check("T17 untracked: not in paimon_pub", inPub == 0);
        check("T17 untracked: not in paimon.table_registry", inRegistry == 0);
        check("T17 untracked: no table directory in the warehouse", !new File(tableDir(tbl)).exists());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    /** Creates a tracked table.  Replaces "USING paimon_heap" from the TAM architecture. */
    static void createTracked(Connection conn, String tbl, String columns) throws Exception {
        exec(conn, "DROP TABLE IF EXISTS " + tbl);
        exec(conn, "CREATE TABLE " + tbl + " (" + columns + ")");
        exec(conn, "SELECT paimon.add_table('" + tbl + "'::regclass)");
    }

    /** Paimon's filesystem catalog nests tables under <database>.db/. */
    /** The warehouse path of a table: <warehouse>/<postgres schema>.db/<table>. */
    static String tableDir(String schema, String tbl) {
        return warehouse + "/" + schema + ".db/" + tbl;
    }

    /** These suites create every table in PG_SCHEMA. */
    static String tableDir(String tbl) {
        return tableDir(PG_SCHEMA, tbl);
    }

    static void cleanup(String tbl) {
        File dir = new File(tableDir(tbl));
        if (dir.exists()) deleteDir(dir);
    }

    static void deleteDir(File f) {
        if (f.isDirectory()) for (File c : Objects.requireNonNull(f.listFiles())) deleteDir(c);
        f.delete();
    }

    /**
     * Counts a table's parquet files across EVERY bucket directory. Main's C++ writer put all files
     * in bucket-0; with fixed bucketing Paimon hashes the primary key, so a table's files spread
     * over bucket-0, bucket-1, ... and a helper that looks only in bucket-0 waits forever for a
     * table whose rows hash elsewhere. How the files are found changes with the storage layout;
     * what the suites assert about their content does not.
     */
    static int countParquet(String tbl) {
        File[] buckets = new File(tableDir(tbl)).listFiles((d, n) -> n.startsWith("bucket-"));
        int total = 0;
        if (buckets != null) {
            for (File b : buckets) {
                File[] f = b.listFiles((d, n) -> n.endsWith(".parquet"));
                if (f != null) total += f.length;
            }
        }
        return total;
    }

    static void waitForParquet(String tbl, int minFiles) throws InterruptedException {
        File snapshotLatest = new File(tableDir(tbl) + "/snapshot/LATEST");
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            if (countParquet(tbl) >= minFiles && snapshotLatest.exists()) return;
            Thread.sleep(500);
        }
        throw new RuntimeException("Timed out waiting for snapshot in " + tbl);
    }

    static TableSchema readSchema(String tbl) throws Exception {
        Path tablePath = new Path(tableDir(tbl));
        return new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema: " + tbl));
    }

    static PaimonRows waitAndRead(String tbl, int minTotal) throws Exception {
        waitForParquet(tbl, 1);
        // The writer commits asynchronously: poll until the changelog is complete (the
        // assertions that follow are exact, so this only waits, it never relaxes them).
        long deadline = System.currentTimeMillis() + 60_000;
        PaimonRows rows = readRows(tbl);
        while (rows.total < minTotal && System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
            rows = readRows(tbl);
        }
        return rows;
    }

    static PaimonRows readRows(String tbl) throws Exception {
        PaimonRows result = new PaimonRows();
        for (int[] r : readIdsAndKinds(tbl)) {
            result.total++;
            result.kindCounts.merge(r[0], 1L, Long::sum);
        }
        return result;
    }

    /**
     * Every changelog record as stored, as {kindOrdinal, id}.  Row kinds come from the
     * $audit_log system table over a streaming scan: Paimon stores the kind as the reserved
     * _VALUE_KIND, so a batch read (merged, or with a faked _row_kind column) cannot report it.
     * Column 0 is the kind as a string; the key is the column named "id".
     */
    static List<int[]> readIdsAndKinds(String tbl) {
        List<int[]> out = new ArrayList<>();
        try {
            Options o = new Options();
            o.set("warehouse", warehouse);
            try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
                Table al = catalog.getTable(Identifier.create(PG_SCHEMA, tbl + "$audit_log"))
                    .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
                int idIdx = al.rowType().getFieldNames().indexOf("id");
                StreamTableScan scan = al.newReadBuilder().newStreamScan();
                scan.plan();                          // the first plan() is always empty: discard
                Long latest = ((FileStoreTable) catalog.getTable(Identifier.create(PG_SCHEMA, tbl)))
                    .snapshotManager().latestSnapshotId();
                List<Split> splits = new ArrayList<>();
                for (long i = 0; i <= (latest == null ? 0 : latest) + 1; i++) {
                    splits.addAll(scan.plan().splits());
                }
                try (RecordReader<InternalRow> reader = al.newReadBuilder().newRead().createReader(splits)) {
                    reader.forEachRemaining(r ->
                        out.add(new int[] {kindOf(r.getString(0).toString()), r.getInt(idIdx)}));
                }
            }
        } catch (Exception e) {
            System.err.println("  warn: row-kind read failed for " + tbl + ": " + e.getMessage());
        }
        return out;
    }

    /** $audit_log's rowkind string to the ordinal the assertions use: +I 0, -U 1, +U 2, -D 3. */
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
