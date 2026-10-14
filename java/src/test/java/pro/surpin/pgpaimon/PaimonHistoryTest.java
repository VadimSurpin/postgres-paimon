// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Operation history tables ({@code <table>_hist}) and key-less operation logs, end to end on a
 * real PostgreSQL 18 and the real writer (spec docs/superpowers/specs/2026-10-14-operation-history-design.md).
 *
 *   H6  The initial load: R rows, _ts = PostgreSQL's now() of the load, _dt its UTC date.
 *   H1  BEGIN; INSERT; UPDATE twice; DELETE; COMMIT, five times, in ONE proven mini-batch: six
 *       history rows per transaction (+I, -U, +U, -U, +U, -D), each pair sharing _lsn, one
 *       _commit_lsn, increasing _op_seq, the transaction's xid, _dt = UTC date of the commit; the
 *       same for a key-less table, whose main table is the log (I, U, U, D).
 *  H2a  The writer halts right after the main table's commit, before the history's.
 *  H2b  ... right after the history table's commit, before the main table's. After a restart both
 *       tables hold every change exactly once.
 *   H3  Every DDL archives T_hist as T_hist_<lsn> and starts a new one with the new schema; each
 *       generation holds exactly its rows; DROP TABLE leaves all of them; re-adding is refused.
 *  H3b  A transaction straddling a DDL writes to the archive and the new table; read together by
 *       _dt every change appears once.
 *  H3c  kill -9 (Runtime.halt) after each step of a rotation; the restart converges.
 *   H4  A key-less table: the documented log in T, a row per image in T_hist, TRUNCATE, TOAST.
 *  H4b  A key-less log committed but not confirmed, then the writer dies: the replay adds nothing.
 *   H5  Name collisions: X and X_hist cannot both be tracked, in either order.
 *   H7  A writer JVM and a PostgreSQL session in a zone whose date differs from UTC's: _dt is UTC.
 *   H8  hist_buckets => N reaches the history table and survives a rotation.
 *
 * Row kinds of main tables come from $audit_log over a streaming scan, never from a merged read.
 * History tables and logs are append-only: every stored row is read back as it is.
 * Usage: test/slot-acceptance/run_acceptance.sh PaimonHistoryTest
 */
public class PaimonHistoryTest {

    static final String PG_SCHEMA = "public";
    static final long TIMEOUT_MS = 90_000;

    static String warehouse;
    static String jdbcUrl;
    static String launcher;
    static String pidFile;
    static int passed = 0, failed = 0;
    static final List<String> failures = new ArrayList<>();
    static int sentinelId = 0;

    @Test
    void acceptanceSuite() throws Exception {
        String jdbc = System.getenv("PGPAIMON_IT_JDBC_URL");
        String wh = System.getenv("PGPAIMON_IT_WAREHOUSE");
        if ("1".equals(System.getenv("PGPAIMON_HARNESS"))) {
            assertNotNull(jdbc, "harness did not export PGPAIMON_IT_JDBC_URL");
            assertNotNull(wh, "harness did not export PGPAIMON_IT_WAREHOUSE");
            assertNotNull(System.getenv("PGPAIMON_WRITER_LAUNCHER"), "harness did not export PGPAIMON_WRITER_LAUNCHER");
            assertNotNull(System.getenv("PGPAIMON_WRITER_PID_FILE"), "harness did not export PGPAIMON_WRITER_PID_FILE");
        }
        assumeTrue(jdbc != null && wh != null, "no live stack: run test/slot-acceptance/run_acceptance.sh");
        jdbcUrl = jdbc;
        warehouse = wh;
        launcher = System.getenv("PGPAIMON_WRITER_LAUNCHER");
        pidFile = System.getenv("PGPAIMON_WRITER_PID_FILE");
        passed = 0; failed = 0; failures.clear();

        System.out.println("╔════════════════════════════════════════════════════╗");
        System.out.println("║   operation history tables and key-less logs       ║");
        System.out.println("╚════════════════════════════════════════════════════╝");

        try (Connection c = connect()) {
            exec(c, "CREATE TABLE hist_sentinel (id int PRIMARY KEY, note text)");
            exec(c, "SELECT paimon.add_table('hist_sentinel'::regclass)");
        }
        writerKeepsUp("setup");

        run("H6", PaimonHistoryTest::h6_initialLoad);
        run("H1", PaimonHistoryTest::h1_neverCollapsedInOneMiniBatch);
        run("H2a", () -> h2_crashBetweenTheTwoCommits("h2_main", "public.h2_main"));
        run("H2b", () -> h2_crashBetweenTheTwoCommits("h2_hist", "public.h2_hist_hist"));
        run("H3", PaimonHistoryTest::h3_ddl);
        run("H3b", PaimonHistoryTest::h3b_straddle);
        for (String step : List.of("rotate-renamed", "rotate-created", "rotate-altered")) {
            run("H3c " + step, () -> h3c_rotationCrash(step));
        }
        run("H4", PaimonHistoryTest::h4_keylessLog);
        run("H4b", PaimonHistoryTest::h4b_keylessReplay);
        run("H5", PaimonHistoryTest::h5_collisions);
        run("H7", PaimonHistoryTest::h7_timeZones);
        run("H8", PaimonHistoryTest::h8_histBuckets);

        System.out.println();
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        assertEquals(0, failed, "failed checks: " + failures);
    }

    // ── H6: the initial load ────────────────────────────────────────────────────────────────
    // setup.sql registers harness_smoke with two rows before the writer's first start.
    static void h6_initialLoad() throws Exception {
        System.out.println("\n── H6: initial load writes R rows to the history ───");
        List<Map<String, String>> h = waitRows("harness_smoke_hist",
            rs -> rs.stream().filter(r -> "R".equals(r.get("_op"))).count() >= 2);
        List<Map<String, String>> r = h.stream().filter(m -> "R".equals(m.get("_op"))).toList();
        check("H6 exactly the two loaded rows are R rows (" + r.size() + ")", r.size() == 2);
        Set<String> got = new TreeSet<>();
        for (Map<String, String> m : r) {
            got.add(m.get("id") + "|" + m.get("label") + "|" + m.get("amt"));
        }
        check("H6 R rows carry the loaded values " + got,
              got.equals(new TreeSet<>(List.of("1|one|1.50", "2|two|2.25"))));
        check("H6 R rows: _commit_lsn = _lsn = the load's consistent point, _xid 0",
              r.size() == 2 && r.stream().allMatch(m -> m.get("_commit_lsn").equals(m.get("_lsn"))
                                       && m.get("_xid").equals("0"))
              && r.get(0).get("_lsn").equals(r.get(1).get("_lsn")));
        check("H6 R rows: one _ts (PostgreSQL's now() of the load), _dt its UTC date, in that partition",
              r.size() == 2 && r.get(0).get("_ts").equals(r.get(1).get("_ts"))
              && r.get(0).get("_dt").equals(utcDate(r.get(0).get("_ts")))
              && new File(tableDir(PG_SCHEMA, "harness_smoke_hist") + "/_dt=" + r.get(0).get("_dt")).isDirectory());
        for (String nsp : List.of("public", "billing")) {
            List<Map<String, String>> t = rowsNow(nsp, "twin_hist");
            List<String> who = t.stream().filter(m -> "R".equals(m.get("_op"))).map(m -> m.get("who")).toList();
            check("H6 " + nsp + ".twin_hist holds its own R row (" + who + ")", who.equals(List.of(nsp + "-one")));
        }
        try (Connection c = connect(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT schema_name, table_name, hist_table FROM paimon.table_registry")) {
            boolean all = true;
            while (rs.next()) {
                boolean ok = rs.getString(3).equals(rs.getString(2) + "_hist")
                    && new File(tableDir(rs.getString(1), rs.getString(3))).isDirectory();
                if (!ok) System.out.println("        no history table for " + rs.getString(1) + "." + rs.getString(2));
                all &= ok;
            }
            check("H6 every registry row names <table>_hist and the writer made it", all);
        }
    }

    // ── H1: a row per row image, never collapsed, across one mini-batch ───────────────────
    static void h1_neverCollapsedInOneMiniBatch() throws Exception {
        System.out.println("\n── H1: I, U, U, D in one transaction, five in one batch ─");
        if (launcher == null) { check("H1 harness exports the writer launcher", false); return; }
        // A writer whose limits will not close a batch: everything stays buffered until SIGTERM,
        // whose clean stop commits the buffer -- once.
        restartWriter("H1", Map.of("PGPAIMON_BATCH_MAX_INTERVAL_MS", "600000",
                                   "PGPAIMON_BATCH_MAX_ROWS", "1000000"));
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE h1_pk (id int PRIMARY KEY, v text)");
            exec(c, "CREATE TABLE h1_log (id int, v text)");
            exec(c, "SELECT paimon.add_table('h1_pk'::regclass)");
            exec(c, "SELECT paimon.add_table('h1_log'::regclass)");
            // The first change to a table on the walsender connection brings a Relation message,
            // on which the keepalive guard commits whatever is buffered. Send both now, while
            // nothing is.
            exec(c, "BEGIN; INSERT INTO h1_pk VALUES (0, 'w'); INSERT INTO h1_log VALUES (0, 'w'); COMMIT");
        }
        Map<Integer, Long> xids = new TreeMap<>();
        long t0 = System.currentTimeMillis();
        try (Connection c = connect()) {
            c.setAutoCommit(false);
            for (int i = 1; i <= 5; i++) {
                try (Statement s = c.createStatement();
                     ResultSet rs = s.executeQuery("SELECT pg_current_xact_id()::text::bigint")) {
                    rs.next();
                    xids.put(i, rs.getLong(1));
                }
                for (String t : List.of("h1_pk", "h1_log")) {
                    exec(c, "INSERT INTO " + t + " VALUES (" + i + ", 'a')");
                    exec(c, "UPDATE " + t + " SET v = 'b' WHERE id = " + i);
                    exec(c, "UPDATE " + t + " SET v = NULL WHERE id = " + i);
                    exec(c, "DELETE FROM " + t + " WHERE id = " + i);
                }
                c.commit();
            }
        }
        Thread.sleep(3000);       // longer than the default interval: a timer would have fired
        check("H1 nothing was committed while the six transactions sat in the buffer",
              !new File(tableDir(PG_SCHEMA, "h1_pk_hist") + "/snapshot/LATEST").exists()
              && !new File(tableDir(PG_SCHEMA, "h1_log") + "/snapshot/LATEST").exists());
        restartWriter("H1", Map.of());     // SIGTERM drains the buffer: one commit per table
        long t1 = System.currentTimeMillis();

        for (String table : List.of("h1_pk_hist", "h1_log_hist", "h1_log")) {
            boolean history = table.endsWith("_hist");
            int per = history ? 6 : 4;
            List<Map<String, String>> rows = waitRows(table, rs -> rs.size() >= 1 + 5 * per);
            check("H1 " + table + ": " + (1 + 5 * per) + " rows (1 warm-up + 5 x " + per + "), got " + rows.size(),
                  rows.size() == 1 + 5 * per);
            check("H1 " + table + ": ONE snapshot for the six transactions (" + snapshots(table) + ")",
                  snapshots(table) == 1);
            Map<String, List<Map<String, String>>> byCommit = new LinkedHashMap<>();
            for (Map<String, String> r : rows) byCommit.computeIfAbsent(r.get("_commit_lsn"), k -> new ArrayList<>()).add(r);
            check("H1 " + table + ": six distinct _commit_lsn, one per transaction", byCommit.size() == 6);
            List<List<Map<String, String>>> txns = new ArrayList<>(byCommit.values());
            for (int i = 1; i <= 5 && i < txns.size(); i++) {
                List<Map<String, String>> t = txns.get(i);
                String id = "" + i;
                List<String> ops = t.stream().map(m -> m.get("_op")).toList();
                List<String> wantOps = history ? List.of("+I", "-U", "+U", "-U", "+U", "-D")
                                               : List.of("I", "U", "U", "D");
                check("H1 " + table + " txn " + i + ": " + wantOps + " in order " + ops, ops.equals(wantOps));
                List<String> vals = t.stream().map(m -> m.get("id") + ":" + m.get("v")).toList();
                List<String> wantVals = history
                    ? List.of(id + ":a", id + ":a", id + ":b", id + ":b", id + ":null", id + ":null")
                    : List.of(id + ":a", id + ":b", id + ":null", id + ":null");
                check("H1 " + table + " txn " + i + ": row images " + vals, vals.equals(wantVals));
                if (history) {
                    check("H1 " + table + " txn " + i + ": each -U/+U pair shares _lsn and is consecutive",
                          t.size() == 6 && t.get(1).get("_lsn").equals(t.get(2).get("_lsn"))
                          && t.get(3).get("_lsn").equals(t.get(4).get("_lsn"))
                          && !t.get(2).get("_lsn").equals(t.get(3).get("_lsn")));
                }
                boolean seq = true, lsn = true;
                for (int k = 1; k < t.size(); k++) {
                    seq &= Integer.parseInt(t.get(k).get("_op_seq")) > Integer.parseInt(t.get(k - 1).get("_op_seq"));
                    lsn &= Long.parseLong(t.get(k).get("_lsn")) >= Long.parseLong(t.get(k - 1).get("_lsn"));
                }
                check("H1 " + table + " txn " + i + ": _op_seq increases, _lsn never decreases", seq && lsn);
                long commit = Long.parseLong(t.get(0).get("_commit_lsn"));
                check("H1 " + table + " txn " + i + ": every change LSN is below the commit LSN",
                      t.stream().allMatch(m -> Long.parseLong(m.get("_lsn")) < commit));
                String xid = "" + xids.get(i);
                check("H1 " + table + " txn " + i + ": _xid is the transaction's (" + xid + ")",
                      t.stream().allMatch(m -> m.get("_xid").equals(xid)));
                check("H1 " + table + " txn " + i + ": one commit timestamp, inside the test's window",
                      t.stream().map(m -> m.get("_ts")).distinct().count() == 1
                      && tsWithin(t.get(0).get("_ts"), t0 - 60_000, t1 + 60_000));
                if (history) {
                    check("H1 " + table + " txn " + i + ": _dt is the UTC date of the commit, and its directory exists",
                          t.stream().allMatch(m -> m.get("_dt").equals(utcDate(m.get("_ts"))))
                          && new File(tableDir(PG_SCHEMA, table) + "/_dt=" + t.get(0).get("_dt")).isDirectory());
                }
            }
            List<Long> commits = byCommit.keySet().stream().map(Long::parseLong).toList();
            check("H1 " + table + ": commit LSNs ascend in commit order",
                  commits.equals(commits.stream().sorted().toList()));
        }
        Map<String, Long> kinds = auditKinds("h1_pk");
        check("H1 h1_pk's changelog is unchanged by the history " + kinds,
              kinds.equals(Map.of("+I", 6L, "-U", 10L, "+U", 10L, "-D", 5L)));
        writerKeepsUp("H1");
    }

    // ── H2: kill between the main and the history commit ──────────────────────────────────
    static void h2_crashBetweenTheTwoCommits(String table, String haltAfter) throws Exception {
        String test = table.equals("h2_main") ? "H2a" : "H2b";
        boolean mainFirst = haltAfter.equals(PG_SCHEMA + "." + table);
        System.out.println("\n── " + test + ": crash after the " + (mainFirst ? "main" : "history")
            + " commit, before the other ──");
        if (launcher == null) { check(test + " harness exports the writer launcher", false); return; }
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE " + table + " (id int PRIMARY KEY, v text)");
            exec(c, "SELECT paimon.add_table('" + table + "'::regclass)");
        }
        writerKeepsUp(test + " registered");
        // A batch interval long enough that all ten transactions below are committed together:
        // the halted commit must hold SEVERAL transactions. With one, resuming at the table that
        // is ahead still re-decodes that transaction (its commit record is the resume point), so
        // a writer that left the history out of the resume position passed (review of the history
        // tables, I2, mutation M9: 275/275); with several, the earlier ones are lost under it.
        restartWriter(test, Map.of("PGPAIMON_TEST_HALT_AFTER_COMMIT", haltAfter,
                                   "PGPAIMON_BATCH_MAX_INTERVAL_MS", "8000",
                                   "PGPAIMON_BATCH_MAX_ROWS", "1000000"));
        String pid = writerPid();
        try (Connection c = connect()) {
            for (int k = 1; k <= 10; k++) txnH2(c, table, k);
        }
        check(test + " the writer halted at the hook", waitProcessGone(pid, 60_000));
        check(test + " it says where it halted",
              writerLog().contains("halting right after the commit to " + haltAfter));
        long mainId = latestCommitIdentifier(PG_SCHEMA, table);
        long histId = latestCommitIdentifier(PG_SCHEMA, table + "_hist");
        check(test + " the window was real: " + (mainFirst ? "main" : "history")
              + " is ahead (main " + mainId + ", history " + histId + ")",
              mainFirst ? mainId > histId : histId > mainId);
        int committed = mainFirst ? rowsNow(PG_SCHEMA, table).size()
                                  : rowsNow(PG_SCHEMA, table + "_hist").size() / 4;
        int txns = mainFirst ? committed / 2 : committed;
        check(test + " the halted commit held several transactions (" + txns + ")", txns >= 2);
        try (Connection c = connect()) {
            for (int k = 11; k <= 15; k++) txnH2(c, table, k);    // while the writer is down
        }
        startWriter(test, Map.of());
        writerKeepsUp(test);
        waitRows(table + "_hist", rs -> rs.size() >= 60);
        Thread.sleep(3000);
        List<Map<String, String>> h = rowsNow(PG_SCHEMA, table + "_hist");
        check(test + " history: 60 rows (15 transactions x +I, +I, -U, +U), got " + h.size(), h.size() == 60);
        Set<String> keys = new HashSet<>();
        for (Map<String, String> m : h) keys.add(m.get("_commit_lsn") + "/" + m.get("_op_seq"));
        check(test + " history: no row twice (distinct _commit_lsn/_op_seq)", keys.size() == h.size());
        Map<String, Long> want = new TreeMap<>(), got = new TreeMap<>();
        for (int k = 1; k <= 15; k++) {
            want.merge("+I|" + (2 * k - 1) + "|r", 1L, Long::sum);
            want.merge("+I|" + (2 * k) + "|r", 1L, Long::sum);
            want.merge("-U|" + (2 * k - 1) + "|r", 1L, Long::sum);
            want.merge("+U|" + (2 * k - 1) + "|u" + k, 1L, Long::sum);
        }
        for (Map<String, String> m : h) got.merge(m.get("_op") + "|" + m.get("id") + "|" + m.get("v"), 1L, Long::sum);
        check(test + " history: exactly the row images PostgreSQL made", want.equals(got));
        Map<String, Long> kinds = auditKinds(table);
        check(test + " main changelog exactly once " + kinds,
              kinds.equals(Map.of("+I", 30L, "-U", 15L, "+U", 15L)));
    }

    static void txnH2(Connection c, String table, int k) throws SQLException {
        exec(c, "BEGIN; INSERT INTO " + table + " VALUES (" + (2 * k - 1) + ", 'r'), (" + (2 * k) + ", 'r');"
            + " UPDATE " + table + " SET v = 'u" + k + "' WHERE id = " + (2 * k - 1) + "; COMMIT");
    }

    // ── H3: DDL rotates the history ───────────────────────────────────────────────────────
    static void h3_ddl() throws Exception {
        System.out.println("\n── H3: every DDL archives T_hist and starts a new one ─");
        String t = "h3_t";
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE " + t + " (id int PRIMARY KEY, a int, b text)");
            exec(c, "SELECT paimon.add_table('" + t + "'::regclass)");
            exec(c, "INSERT INTO " + t + " VALUES (1, 10, 'x')");
            exec(c, "ALTER TABLE " + t + " ADD COLUMN c int");
            exec(c, "INSERT INTO " + t + " VALUES (2, 20, 'y', 200)");
            exec(c, "UPDATE " + t + " SET c = 100 WHERE id = 1");
            exec(c, "ALTER TABLE " + t + " RENAME COLUMN a TO a2");
            exec(c, "ALTER TABLE " + t + " ALTER COLUMN c TYPE bigint");
            exec(c, "INSERT INTO " + t + " VALUES (3, 30, 'z', 300)");
            exec(c, "ALTER TABLE " + t + " DROP COLUMN a2");
            exec(c, "INSERT INTO " + t + " VALUES (4, 'w', 400)");
            exec(c, "UPDATE " + t + " SET b = 'y2' WHERE id = 2");
        }
        waitRows(t + "_hist", rs -> rs.size() >= 3);
        List<String> arch = archives(PG_SCHEMA, t);
        check("H3 four DDL messages, four archives, named <t>_hist_<16 hex>, chronological " + arch, arch.size() == 4);
        if (arch.size() != 4) return;
        List<List<String>> schemas = List.of(List.of("id", "a", "b"), List.of("id", "a", "b", "c"),
            List.of("id", "a2", "b", "c"), List.of("id", "a2", "b", "c"), List.of("id", "b", "c"));
        List<String> names = new ArrayList<>(arch);
        names.add(t + "_hist");
        for (int i = 0; i < 5; i++) {
            List<String> f = fields(names.get(i));
            List<String> want = new ArrayList<>(schemas.get(i));
            want.addAll(List.of("_op", "_commit_lsn", "_lsn", "_op_seq", "_xid", "_ts", "_dt"));
            check("H3 " + names.get(i) + " has the schema of its generation " + f, f.equals(want));
        }
        check("H3 the alter_col archive keeps c INT, the next one has BIGINT",
              "INT".equals(type(arch.get(2), "c")) && "BIGINT".equals(type(arch.get(3), "c")));
        List<List<String>> want = List.of(
            List.of("+I|1|10|x|null"),
            List.of("+I|2|20|y|200", "-U|1|10|x|null", "+U|1|10|x|100"),
            List.of(),
            List.of("+I|3|30|z|300"),
            List.of("+I|4|-|w|400", "-U|2|-|y|200", "+U|2|-|y2|200"));
        long total = 0;
        for (int i = 0; i < 5; i++) {
            final String a = i == 0 || i == 1 ? "a" : "a2";
            List<String> got = rowsNow(PG_SCHEMA, names.get(i)).stream().map(m -> m.get("_op") + "|" + m.get("id") + "|"
                + (m.containsKey(a) ? m.get(a) : "-") + "|" + m.get("b") + "|" + m.get("c")).toList();
            if (i == 4) {
                got = waitRows(names.get(i), rs -> rs.size() >= 3).stream().map(m -> m.get("_op") + "|" + m.get("id") + "|-|"
                    + m.get("b") + "|" + m.get("c")).toList();
            }
            check("H3 " + names.get(i) + " holds exactly its generation's rows " + got, got.equals(want.get(i)));
            total += got.size();
        }
        sameKeyed("H3 main table = PostgreSQL", t);
        try (Connection cn = connect()) {
            exec(cn, "DROP TABLE " + t);
        }
        File mainDir = new File(tableDir(PG_SCHEMA, t));
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (mainDir.exists() && System.currentTimeMillis() < deadline) Thread.sleep(250);
        check("H3 DROP TABLE purged the main Paimon table", !mainDir.exists());
        long after = 0;
        for (String n : names) after += rowsNow(PG_SCHEMA, n).size();
        check("H3 DROP TABLE left " + t + "_hist and all four archives, with all " + total + " rows",
              archives(PG_SCHEMA, t).size() == 4 && after == total);
        try (Connection cn = connect()) {
            exec(cn, "CREATE TABLE " + t + " (id int PRIMARY KEY, other text)");
            refused("H3 registering a new table of the same name", cn,
                "SELECT paimon.add_table('" + t + "'::regclass)", "forget_history");
        }
        writerKeepsUp("H3");
    }

    // ── H3b: a transaction that straddles the DDL ─────────────────────────────────────────
    static void h3b_straddle() throws Exception {
        System.out.println("\n── H3b: BEGIN; INSERT; ALTER TABLE; INSERT; COMMIT ─────");
        String t = "h3b_t";
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE " + t + " (id int PRIMARY KEY, v text)");
            exec(c, "SELECT paimon.add_table('" + t + "'::regclass)");
            exec(c, "INSERT INTO " + t + " VALUES (1, 'before')");
            exec(c, "BEGIN; INSERT INTO " + t + " VALUES (2, 'old schema'); ALTER TABLE " + t
                + " ADD COLUMN w int; INSERT INTO " + t + " VALUES (3, 'new schema', 7); COMMIT");
            exec(c, "INSERT INTO " + t + " VALUES (4, 'after', 8)");
        }
        List<Map<String, String>> cur = waitRows(t + "_hist", rs -> rs.size() >= 2);
        List<String> arch = archives(PG_SCHEMA, t);
        check("H3b one archive " + arch, arch.size() == 1);
        if (arch.isEmpty()) return;
        List<Map<String, String>> old = rowsNow(PG_SCHEMA, arch.get(0));
        check("H3b the archive holds the rows decoded under the old schema: ids 1 and 2 "
              + old.stream().map(m -> m.get("id")).toList(),
              old.stream().map(m -> m.get("id")).toList().equals(List.of("1", "2")));
        check("H3b the new history holds the rest: ids 3 and 4, with w "
              + cur.stream().map(m -> m.get("id") + ":" + m.get("w")).toList(),
              cur.stream().map(m -> m.get("id") + ":" + m.get("w")).toList().equals(List.of("3:7", "4:8")));
        check("H3b the straddling transaction is in both, with one commit LSN",
              old.size() == 2 && cur.size() == 2
              && old.get(1).get("_commit_lsn").equals(cur.get(0).get("_commit_lsn")));
        // read both by a _dt range: every change exactly once
        String day = cur.get(0).get("_dt");
        Set<String> keys = new HashSet<>();
        long n = 0;
        for (List<Map<String, String>> x : List.of(old, cur)) {
            for (Map<String, String> m : x) {
                if (m.get("_dt").compareTo(day) <= 0) {
                    keys.add(m.get("_commit_lsn") + "/" + m.get("_op_seq"));
                    n++;
                }
            }
        }
        check("H3b reading the archive and T_hist together by _dt <= " + day + ": 4 changes, each once",
              n == 4 && keys.size() == 4);
    }

    // ── H3c: kill -9 at each step of a rotation ───────────────────────────────────────────
    static void h3c_rotationCrash(String step) throws Exception {
        String test = "H3c[" + step + "]";
        System.out.println("\n── " + test + ": the writer dies after rotation step " + step + " ─");
        if (launcher == null) { check(test + " harness exports the writer launcher", false); return; }
        String t = "h3c_" + step.replace("rotate-", "");
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE " + t + " (id int PRIMARY KEY, v text)");
            exec(c, "SELECT paimon.add_table('" + t + "'::regclass)");
            exec(c, "INSERT INTO " + t + " VALUES (1, 'a'), (2, 'b')");
            exec(c, "UPDATE " + t + " SET v = 'a2' WHERE id = 1");
        }
        waitRows(t + "_hist", rs -> rs.size() >= 4);
        restartWriter(test, Map.of("PGPAIMON_TEST_HALT_AT_STEP", step));
        String pid = writerPid();
        try (Connection c = connect()) {
            exec(c, "INSERT INTO " + t + " VALUES (3, 'c')");
            exec(c, "ALTER TABLE " + t + " ADD COLUMN w int");
            exec(c, "INSERT INTO " + t + " VALUES (4, 'd', 4)");
        }
        check(test + " the writer halted at the step", waitProcessGone(pid, 60_000)
              && writerLog().contains("halting right after rotation step " + step));
        try (Connection c = connect()) {
            exec(c, "UPDATE " + t + " SET w = 2 WHERE id = 2");           // while it is down
        }
        startWriter(test, Map.of());
        writerKeepsUp(test);
        List<Map<String, String>> cur = waitRows(t + "_hist", rs -> rs.size() >= 3);
        Thread.sleep(2000);
        cur = rowsNow(PG_SCHEMA, t + "_hist");
        List<String> arch = archives(PG_SCHEMA, t);
        check(test + " exactly one archive " + arch, arch.size() == 1);
        if (arch.size() != 1) return;
        List<String> old = rowsNow(PG_SCHEMA, arch.get(0)).stream()
            .map(m -> m.get("_op") + "|" + m.get("id") + "|" + m.get("v")).toList();
        check(test + " the archive holds the pre-DDL images exactly once " + old,
              old.equals(List.of("+I|1|a", "+I|2|b", "-U|1|a", "+U|1|a2", "+I|3|c")));
        List<String> now = cur.stream().map(m -> m.get("_op") + "|" + m.get("id") + "|" + m.get("v") + "|" + m.get("w")).toList();
        check(test + " the new history holds the post-DDL images exactly once " + now,
              now.equals(List.of("+I|4|d|4", "-U|2|b|null", "+U|2|b|2")));
        check(test + " the main table has w once and equals PostgreSQL", fields(t).stream().filter("w"::equals).count() == 1);
        sameKeyed(test + " main table = PostgreSQL", t);
        Map<String, Long> kinds = auditKinds(t);
        check(test + " main changelog exactly once " + kinds,
              kinds.equals(Map.of("+I", 4L, "-U", 2L, "+U", 2L)));
    }

    // ── H4: a key-less table is an operation log ──────────────────────────────────────────
    static void h4_keylessLog() throws Exception {
        System.out.println("\n── H4: key-less table ─────────────────────────────");
        String t = "h4_log";
        String big;
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE " + t + " (id int, v text, big text)");
            exec(c, "SELECT paimon.add_table('" + t + "'::regclass)");
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT cardinality(pk_cols), (SELECT relreplident FROM pg_class"
                     + " WHERE oid = '" + t + "'::regclass) FROM paimon.table_registry WHERE relid = '" + t + "'::regclass")) {
                check("H4 add_table registered it with no key and REPLICA IDENTITY FULL",
                      rs.next() && rs.getInt(1) == 0 && "f".equals(rs.getString(2)));
            }
            // An incompressible 40 kB value is stored out of line (TOAST); an UPDATE that does not
            // touch it sends it as "unchanged", and both images must still carry it.
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT string_agg(md5(g::text), '') FROM generate_series(1, 1250) g")) {
                rs.next();
                big = rs.getString(1);
            }
            exec(c, "INSERT INTO " + t + " VALUES (1, 'a', NULL), (2, 'b', NULL), (3, 'c', '" + big + "')");
            exec(c, "UPDATE " + t + " SET v = 'c2' WHERE id = 3");
            exec(c, "UPDATE " + t + " SET v = v || '!' WHERE id <= 2");
            exec(c, "DELETE FROM " + t + " WHERE id = 3");
            exec(c, "INSERT INTO " + t + " VALUES (4, 'dup', NULL), (4, 'dup', NULL)");
            exec(c, "DELETE FROM " + t + " WHERE id = 4");
            exec(c, "TRUNCATE " + t);
            exec(c, "INSERT INTO " + t + " VALUES (5, 'after', NULL)");
        }
        List<String> want = List.of(
            "I|1|a|-", "I|2|b|-", "I|3|c|B", "U|3|c2|B", "U|1|a!|-", "U|2|b!|-", "D|3|c2|B",
            "I|4|dup|-", "I|4|dup|-", "D|4|dup|-", "D|4|dup|-", "T|null|null|-", "I|5|after|-");
        List<Map<String, String>> log = waitRows(t, rs -> rs.size() >= want.size());
        List<String> got = log.stream().map(m -> m.get("_op") + "|" + m.get("id") + "|" + m.get("v") + "|"
            + (m.get("big") == null ? "-" : m.get("big").equals(big) ? "B" : "?")).toList();
        check("H4 the log holds every operation, after values (deleted row for D), not erased by TRUNCATE " + got,
              got.equals(want));
        List<String> schema = fields(t);
        check("H4 the log's columns: no _dt, not partitioned " + schema,
              schema.equals(List.of("id", "v", "big", "_op", "_commit_lsn", "_lsn", "_op_seq", "_xid", "_ts")));
        List<String> hw = List.of(
            "+I|1|a|-", "+I|2|b|-", "+I|3|c|B", "-U|3|c|B", "+U|3|c2|B",
            "-U|1|a|-", "+U|1|a!|-", "-U|2|b|-", "+U|2|b!|-", "-D|3|c2|B", "+I|4|dup|-",
            "+I|4|dup|-", "-D|4|dup|-", "-D|4|dup|-", "T|null|null|-", "+I|5|after|-");
        List<Map<String, String>> h = waitRows(t + "_hist", rs -> rs.size() >= hw.size());
        List<String> hg = h.stream().map(m -> m.get("_op") + "|" + m.get("id") + "|" + m.get("v") + "|"
            + (m.get("big") == null ? "-" : m.get("big").equals(big) ? "B" : "?")).toList();
        check("H4 the history has a row per image, the TOASTed value in both of an update's " + hg, hg.equals(hw));
        Set<String> histKeys = new HashSet<>();
        h.forEach(m -> histKeys.add(m.get("_commit_lsn") + "/" + m.get("_op_seq") + "/" + m.get("_xid") + "/" + m.get("_ts")));
        check("H4 every log row's (_commit_lsn, _op_seq, _xid, _ts) is its change's first history row",
              log.stream().allMatch(m -> histKeys.contains(m.get("_commit_lsn") + "/" + m.get("_op_seq") + "/"
                                                           + m.get("_xid") + "/" + m.get("_ts"))));
        writerKeepsUp("H4");
    }

    // ── H4b: a key-less log replayed after a crash adds nothing ───────────────────────────
    static void h4b_keylessReplay() throws Exception {
        System.out.println("\n── H4b: key-less log, crash before the confirm ─────");
        if (launcher == null) { check("H4b harness exports the writer launcher", false); return; }
        String t = "h4b_log";
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE " + t + " (id int, v text)");
            exec(c, "SELECT paimon.add_table('" + t + "'::regclass)");
        }
        writerKeepsUp("H4b registered");
        // Halts after the log's commit -- and before its history's and before any confirm, so the
        // whole batch is in the log and will be replayed.
        restartWriter("H4b", Map.of("PGPAIMON_TEST_HALT_AFTER_COMMIT", PG_SCHEMA + "." + t));
        String pid = writerPid();
        try (Connection c = connect()) {
            for (int k = 1; k <= 8; k++) {
                exec(c, "BEGIN; INSERT INTO " + t + " VALUES (" + k + ", 'r'), (" + k + ", 'r'); UPDATE " + t
                    + " SET v = 'u' WHERE id = " + k + "; COMMIT");
            }
        }
        check("H4b the writer halted at the hook", waitProcessGone(pid, 60_000));
        long logId = latestCommitIdentifier(PG_SCHEMA, t);
        check("H4b the log's commit (" + logId + ") is not confirmed (" + confirmedFlush() + "): it will replay",
              logId > 0 && confirmedBelow(logId));
        startWriter("H4b", Map.of());
        writerKeepsUp("H4b");
        Thread.sleep(3000);
        List<Map<String, String>> log = rowsNow(PG_SCHEMA, t);
        Map<String, Long> ops = new TreeMap<>();
        for (Map<String, String> m : log) ops.merge(m.get("_op"), 1L, Long::sum);
        check("H4b the replay duplicated nothing in the log " + ops, ops.equals(Map.of("I", 16L, "U", 16L)));
        Map<String, Long> hops = new TreeMap<>();
        for (Map<String, String> m : rowsNow(PG_SCHEMA, t + "_hist")) hops.merge(m.get("_op"), 1L, Long::sum);
        check("H4b ... nor in its history " + hops, hops.equals(Map.of("+I", 16L, "-U", 16L, "+U", 16L)));
    }

    // ── H7: _dt follows the UTC date whatever the time zones ──────────────────────────────
    static void h7_timeZones() throws Exception {
        System.out.println("\n── H7: non-UTC writer JVM and PostgreSQL session ──");
        if (launcher == null) { check("H7 harness exports the writer launcher", false); return; }
        String zone;
        try (Connection c = connect(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT extract(hour FROM now() AT TIME ZONE 'UTC')::int")) {
            rs.next();
            // A zone whose calendar date differs from UTC's right now: Kiritimati (UTC+14) from
            // 10:00 UTC on, Pago Pago (UTC-11) before 11:00 UTC. The switch at 11 is inside both
            // ranges; at 12 it left 11:00-11:59 UTC with Pago Pago on UTC's date (review m6).
            zone = rs.getInt(1) >= 11 ? "Pacific/Kiritimati" : "Pacific/Pago_Pago";
        }
        restartWriter("H7", Map.of("JAVA_TOOL_OPTIONS", "-Duser.timezone=" + zone));
        check("H7 the writer JVM runs in " + zone,
              writerLog().contains("Picked up JAVA_TOOL_OPTIONS: -Duser.timezone=" + zone));
        String t = "h7_t";
        String utcDay, localDay;
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE " + t + " (id int PRIMARY KEY)");
            exec(c, "SELECT paimon.add_table('" + t + "'::regclass)");
            c.setAutoCommit(false);
            exec(c, "SET TimeZone = '" + zone + "'");
            exec(c, "INSERT INTO " + t + " VALUES (1)");
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT (now() AT TIME ZONE 'UTC')::date::text, now()::date::text")) {
                rs.next();
                utcDay = rs.getString(1);
                localDay = rs.getString(2);
            }
            c.commit();
        }
        check("H7 the session's local date (" + localDay + ") differs from the UTC date (" + utcDay + ")",
              !localDay.equals(utcDay));
        List<Map<String, String>> h = waitRows(t + "_hist", rs -> rs.size() >= 1);
        check("H7 _dt is the UTC date of the commit (" + (h.isEmpty() ? "none" : h.get(0).get("_dt")) + ")",
              h.size() == 1 && h.get(0).get("_dt").equals(utcDay)
              && h.get(0).get("_dt").equals(utcDate(h.get(0).get("_ts"))));
        check("H7 the row is in the directory _dt=" + utcDay,
              new File(tableDir(PG_SCHEMA, t + "_hist") + "/_dt=" + utcDay).isDirectory()
              && !new File(tableDir(PG_SCHEMA, t + "_hist") + "/_dt=" + localDay).exists());
        restartWriter("H7", Map.of());
    }

    // ── H8: the history's bucket count ─────────────────────────────────────────────────────
    static void h8_histBuckets() throws Exception {
        System.out.println("\n── H8: hist_buckets reaches the history table ──────");
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE h8_t (id int PRIMARY KEY)");
            exec(c, "SELECT paimon.add_table('h8_t'::regclass, hist_buckets => 3)");
            exec(c, "INSERT INTO h8_t VALUES (1)");
        }
        waitRows("h8_t_hist", rs -> rs.size() >= 1);
        String b = option("h8_t_hist", "bucket");
        check("H8 h8_t_hist has the 3 buckets per partition asked for (" + b + "), h8_t its own ("
              + option("h8_t", "bucket") + ")", "3".equals(b) && "4".equals(option("h8_t", "bucket")));
        try (Connection c = connect()) {
            exec(c, "ALTER TABLE h8_t ADD COLUMN x int");
            exec(c, "INSERT INTO h8_t VALUES (2, 2)");
        }
        waitRows("h8_t_hist", rs -> rs.size() >= 1 && fieldsQuiet("h8_t_hist").contains("x"));
        check("H8 the user's count survives a rotation (" + option("h8_t_hist", "bucket") + ")",
              "3".equals(option("h8_t_hist", "bucket")));
    }

    // ── H5: name collisions ───────────────────────────────────────────────────────────────
    static void h5_collisions() throws Exception {
        System.out.println("\n── H5: X and X_hist cannot both be tracked ─────────");
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE h5_x (id int PRIMARY KEY)");
            exec(c, "CREATE TABLE h5_x_hist (id int PRIMARY KEY)");
            exec(c, "SELECT paimon.add_table('h5_x'::regclass)");
            refused("H5 X tracked, then X_hist", c, "SELECT paimon.add_table('h5_x_hist'::regclass)",
                    "is a history table of replicated table");
            exec(c, "CREATE TABLE h5_y_hist (id int PRIMARY KEY)");
            exec(c, "CREATE TABLE h5_y (id int PRIMARY KEY)");
            exec(c, "SELECT paimon.add_table('h5_y_hist'::regclass)");
            refused("H5 X_hist tracked, then X", c, "SELECT paimon.add_table('h5_y'::regclass)",
                    "would be the replicated table");
            exec(c, "INSERT INTO h5_x VALUES (1)");
            exec(c, "INSERT INTO h5_y_hist VALUES (1)");
        }
        writerKeepsUp("H5");
        check("H5 h5_y_hist is an ordinary replicated table, with its own h5_y_hist_hist",
              rowsNow(PG_SCHEMA, "h5_y_hist_hist").size() == 1 && rowsNow(PG_SCHEMA, "h5_x_hist").size() == 1);
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────

    interface Case { void run() throws Exception; }

    static void run(String name, Case c) {
        try {
            c.run();
        } catch (Exception e) {
            e.printStackTrace();
            check(name + " ran to completion (" + e + ")", false);
        }
    }

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    static void refused(String name, Connection c, String sql, String expected) {
        try {
            exec(c, sql);
            check(name + " is refused by PostgreSQL", false);
        } catch (SQLException e) {
            boolean ok = e.getMessage() != null && e.getMessage().contains(expected);
            check(name + " is refused by PostgreSQL, with the hint", ok);
            if (!ok) System.out.println("        got: " + e.getMessage());
        }
    }

    static String tableDir(String schema, String tbl) {
        return warehouse + "/" + schema + ".db/" + tbl;
    }

    /** A row written now to hist_sentinel reaches Paimon: the writer is up and caught up. */
    static void writerKeepsUp(String test) throws Exception {
        if (pidFile != null) check(test + ": the writer process is alive", processAlive(writerPid()));
        int id = ++sentinelId;
        try (Connection c = connect()) {
            exec(c, "INSERT INTO hist_sentinel VALUES (" + id + ", '" + test.replace("'", "") + "')");
        }
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        boolean seen;
        do {
            seen = rowsNow(PG_SCHEMA, "hist_sentinel_hist").stream().anyMatch(m -> ("" + id).equals(m.get("id")));
            if (!seen) Thread.sleep(250);
        } while (!seen && System.currentTimeMillis() < deadline);
        check(test + ": a row written afterwards reaches Paimon", seen);
    }

    static List<Map<String, String>> waitRows(String table, Predicate<List<Map<String, String>>> done) throws Exception {
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        List<Map<String, String>> rows = rowsNow(PG_SCHEMA, table);
        while (!done.test(rows) && System.currentTimeMillis() < deadline) {
            Thread.sleep(250);
            rows = rowsNow(PG_SCHEMA, table);
        }
        return rows;
    }

    /**
     * Every stored row of an append-only table (a history table or a key-less log), every column
     * as text, in commit order then _op_seq. An append-only table has no merge, so a batch read
     * returns each row as written.
     */
    static List<Map<String, String>> rowsNow(String schema, String table) {
        List<Map<String, String>> out = new ArrayList<>();
        Options o = new Options();
        o.set("warehouse", warehouse);
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
            Table t = catalog.getTable(Identifier.create(schema, table));
            RowType rt = t.rowType();
            ReadBuilder rb = t.newReadBuilder();
            try (RecordReader<InternalRow> r = rb.newRead().createReader(rb.newScan().plan())) {
                r.forEachRemaining(row -> {
                    Map<String, String> m = new LinkedHashMap<>();
                    for (int i = 0; i < rt.getFieldCount(); i++) {
                        DataField f = rt.getFields().get(i);
                        m.put(f.name(), row.isNullAt(i) ? null : render(f, InternalRowUtils.get(row, i, f.type())));
                    }
                    out.add(m);
                });
            }
        } catch (Exception notYet) {
            return out;
        }
        if (!out.isEmpty() && out.get(0).containsKey("_commit_lsn")) {
            out.sort(Comparator.comparingLong((Map<String, String> m) -> Long.parseLong(m.get("_commit_lsn")))
                .thenComparingInt(m -> Integer.parseInt(m.get("_op_seq"))));
        }
        return out;
    }

    /** The UTC calendar date of a {@code _ts} rendered as epoch microseconds. */
    static String utcDate(String micros) {
        return java.time.LocalDate.ofEpochDay(Math.floorDiv(Long.parseLong(micros), 86_400_000_000L)).toString();
    }

    /** The archives of a table's history, {@code <t>_hist_<16 hex>}, sorted (= chronological). */
    static List<String> archives(String schema, String t) throws Exception {
        List<String> out = new ArrayList<>();
        Options o = new Options();
        o.set("warehouse", warehouse);
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
            for (String n : catalog.listTables(schema)) {
                if (n.matches(java.util.regex.Pattern.quote(t + "_hist_") + "[0-9A-F]{16}")) out.add(n);
            }
        }
        Collections.sort(out);
        return out;
    }

    static String option(String table, String key) throws Exception {
        Options o = new Options();
        o.set("warehouse", warehouse);
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
            return catalog.getTable(Identifier.create(PG_SCHEMA, table)).options().get(key);
        }
    }

    static List<String> fieldsQuiet(String table) {
        try {
            return fields(table);
        } catch (Exception e) {
            return List.of();
        }
    }

    static String render(DataField f, Object v) {
        switch (f.type().getTypeRoot()) {
            case DATE: return java.time.LocalDate.ofEpochDay((Integer) v).toString();
            case DECIMAL: return ((org.apache.paimon.data.Decimal) v).toBigDecimal().toPlainString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "" + ((org.apache.paimon.data.Timestamp) v).toMicros();
            default: return v.toString();
        }
    }

    static boolean tsWithin(String micros, long fromMs, long toMs) {
        long ms = Long.parseLong(micros) / 1000;
        return ms >= fromMs && ms <= toMs;
    }

    static List<String> fields(String table) throws Exception {
        Options o = new Options();
        o.set("warehouse", warehouse);
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
            return catalog.getTable(Identifier.create(PG_SCHEMA, table)).rowType().getFieldNames();
        }
    }

    static String type(String table, String field) throws Exception {
        Options o = new Options();
        o.set("warehouse", warehouse);
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
            return catalog.getTable(Identifier.create(PG_SCHEMA, table)).rowType().getField(field)
                .type().copy(true).asSQLString();
        }
    }

    static long snapshots(String table) throws Exception {
        Options o = new Options();
        o.set("warehouse", warehouse);
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
            Long id = ((FileStoreTable) catalog.getTable(Identifier.create(PG_SCHEMA, table)))
                .snapshotManager().latestSnapshotId();
            return id == null ? 0 : id;
        }
    }

    /** Row kinds of a primary-key table's changelog, from $audit_log over a streaming scan. */
    static Map<String, Long> auditKinds(String tbl) throws Exception {
        Map<String, Long> out = new TreeMap<>();
        Options o = new Options();
        o.set("warehouse", warehouse);
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
            Table al = catalog.getTable(Identifier.create(PG_SCHEMA, tbl + "$audit_log"))
                .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
            StreamTableScan scan = al.newReadBuilder().newStreamScan();
            scan.plan();                          // the first plan() is always empty: discard
            Long latest = ((FileStoreTable) catalog.getTable(Identifier.create(PG_SCHEMA, tbl)))
                .snapshotManager().latestSnapshotId();
            List<Split> splits = new ArrayList<>();
            for (long i = 0; i <= (latest == null ? 0 : latest) + 1; i++) splits.addAll(scan.plan().splits());
            try (RecordReader<InternalRow> reader = al.newReadBuilder().newRead().createReader(splits)) {
                reader.forEachRemaining(r -> out.merge(r.getString(0).toString(), 1L, Long::sum));
            }
        }
        return out;
    }

    /** The merged main table equals PostgreSQL, row for row, on the columns PostgreSQL has. */
    static void sameKeyed(String name, String tbl) throws Exception {
        Map<String, Map<String, String>> pg = new TreeMap<>();
        try (Connection c = connect(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM " + tbl)) {
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                Map<String, String> row = new TreeMap<>();
                for (int i = 1; i <= md.getColumnCount(); i++) row.put(md.getColumnName(i), rs.getString(i));
                pg.put(row.get("id"), row);
            }
        }
        Map<String, Map<String, String>> pm = new TreeMap<>();
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        do {
            pm.clear();
            for (Map<String, String> m : rowsNow(PG_SCHEMA, tbl)) {
                Map<String, String> r = new TreeMap<>();
                for (String col : pg.values().iterator().next().keySet()) r.put(col, m.get(col));
                pm.put(r.get("id"), r);
            }
            if (pm.equals(pg)) break;
            Thread.sleep(250);
        } while (System.currentTimeMillis() < deadline);
        check(name, pm.equals(pg));
        if (!pm.equals(pg)) {
            System.out.println("        PostgreSQL: " + pg);
            System.out.println("        Paimon:     " + pm);
        }
    }

    static long latestCommitIdentifier(String schema, String tbl) {
        try {
            File latest = new File(tableDir(schema, tbl) + "/snapshot/LATEST");
            if (!latest.exists()) return -1;
            String id = new String(Files.readAllBytes(latest.toPath())).trim();
            String json = new String(Files.readAllBytes(new File(tableDir(schema, tbl) + "/snapshot/snapshot-" + id).toPath()));
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"commitIdentifier\"\\s*:\\s*(\\d+)").matcher(json);
            return m.find() ? Long.parseLong(m.group(1)) : -1;
        } catch (Exception e) {
            return -1;
        }
    }

    static String confirmedFlush() throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT (confirmed_flush_lsn - '0/0'::pg_lsn)::text FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
            return rs.next() ? rs.getString(1) : "?";
        }
    }

    static boolean confirmedBelow(long lsn) throws Exception {
        try (Connection conn = connect(); Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT (confirmed_flush_lsn - '0/0'::pg_lsn) < " + lsn
                 + " FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    static String writerLog() {
        try {
            return new String(Files.readAllBytes(new File(new File(pidFile).getParentFile(), "writer.log").toPath()));
        } catch (Exception e) {
            return "";
        }
    }

    static String writerPid() throws Exception {
        return new String(Files.readAllBytes(new File(pidFile).toPath())).trim();
    }

    static boolean processAlive(String pid) throws Exception {
        return new ProcessBuilder("kill", "-0", pid).redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD).start().waitFor() == 0;
    }

    static boolean waitProcessGone(String pid, long ms) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        while (processAlive(pid) && System.currentTimeMillis() < deadline) Thread.sleep(200);
        return !processAlive(pid);
    }

    /** SIGTERM (a clean stop commits and confirms the buffer), then start with {@code env}. */
    static void restartWriter(String test, Map<String, String> env) throws Exception {
        String pid = writerPid();
        new ProcessBuilder("kill", pid).start().waitFor();
        check(test + " the writer stopped cleanly on SIGTERM", waitProcessGone(pid, 60_000));
        long deadline = System.currentTimeMillis() + 30_000;
        while (slotActive() && System.currentTimeMillis() < deadline) Thread.sleep(200);
        startWriter(test, env);
    }

    static void startWriter(String test, Map<String, String> env) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("bash", launcher)
            .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.environment().putAll(env);
        check(test + " launcher exited cleanly", pb.start().waitFor() == 0);
        long deadline = System.currentTimeMillis() + 60_000;
        while (!slotActive() && System.currentTimeMillis() < deadline) Thread.sleep(250);
        check(test + " writer attached to the slot", slotActive());
    }

    static boolean slotActive() throws Exception {
        try (Connection conn = connect(); Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT bool_or(active) FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
            return rs.next() && rs.getBoolean(1);
        }
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
