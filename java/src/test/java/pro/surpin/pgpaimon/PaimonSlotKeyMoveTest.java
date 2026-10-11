// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import org.junit.jupiter.api.Test;

/**
 * T20 — intra-transaction key collisions.
 *
 * "UPDATE t SET id = id + 1" rewrites every key in one transaction.  Each row
 * decomposes to -D(old_key) + +I(new_key), and those pairs interleave: the +I for
 * key 3 may arrive before the -D that vacates it.  Correctness rests entirely on
 * per-change LSN ordering surviving, so this is the test that any writer-side
 * collapsing, reordering or per-key deduplication fails.  It is the named acceptance
 * test for spec section 7's intra-transaction key-collision rule.
 *
 * Usage (live stack): test/slot-acceptance/run_acceptance.sh PaimonSlotKeyMoveTest
 */
public class PaimonSlotKeyMoveTest {

    static final int N = 200;

    static final String DEFAULT_WAREHOUSE = env("PGPAIMON_IT_WAREHOUSE", "/tmp/paimon-warehouse");
    static final String DEFAULT_JDBC =
        env("PGPAIMON_IT_JDBC_URL", "jdbc:postgresql://localhost:5442/paimon_acc?user=postgres");

    static String warehouse;
    static String jdbcUrl;
    static final LocalFileIO fileIO = LocalFileIO.create();
    static int passed = 0, failed = 0;
    static final List<String> failures = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        System.exit(run(args) > 0 ? 1 : 0);
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

    static int run(String[] args) throws Exception {
        passed = 0; failed = 0; failures.clear();
        warehouse = args.length > 0 ? args[0] : DEFAULT_WAREHOUSE;
        jdbcUrl   = args.length > 1 ? args[1] : DEFAULT_JDBC;

        System.out.println("── T20: intra-transaction key moves ──────────────────");

        String tbl = "km_t";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            /*
             * Deferrable, so PostgreSQL itself accepts "SET id = id + 1" in any row order: a
             * non-deferrable key checks uniqueness per row and would reject the statement
             * whenever it reaches id=k before id=k+1 has moved.  Deferred checking is
             * what makes the transient collision legal; it is the writer that must cope.
             */
            exec(conn, "CREATE TABLE " + tbl + " (id int PRIMARY KEY DEFERRABLE INITIALLY IMMEDIATE, v text)");
            // Registered while empty: add_table derives backfill_due from emptiness.
            exec(conn, "SELECT paimon.add_table('" + tbl + "'::regclass)");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tbl + " VALUES (?, ?)")) {
                for (int i = 1; i <= N; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, "row" + i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }

        Map<Integer, String> loaded = waitForMerged(tbl, N);
        check("T20 initial load: " + N + " keys", loaded.size() == N);

        /*
         * One statement, one transaction, key ranges overlapping: the server picks the
         * row order, which is the point.
         */
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            conn.setAutoCommit(false);
            exec(conn, "SET CONSTRAINTS ALL DEFERRED");
            exec(conn, "UPDATE " + tbl + " SET id = id + 1");
            conn.commit();
        }

        Map<Integer, String> expected = pgState(tbl);
        check("T20 PostgreSQL holds " + N + " keys after the move", expected.size() == N);
        check("T20 PostgreSQL keys are 2.." + (N + 1),
              expected.keySet().equals(range(2, N + 1)));

        Map<Integer, String> actual = waitForMergedEquals(tbl, expected);
        report("move up", expected, actual);

        // The same collision in the other direction: descending order of transient clashes.
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            conn.setAutoCommit(false);
            exec(conn, "SET CONSTRAINTS ALL DEFERRED");
            exec(conn, "UPDATE " + tbl + " SET id = id - 1");
            conn.commit();
        }
        Map<Integer, String> expected2 = pgState(tbl);
        check("T20 PostgreSQL keys are back to 1.." + N, expected2.keySet().equals(range(1, N)));
        Map<Integer, String> actual2 = waitForMergedEquals(tbl, expected2);
        report("move down", expected2, actual2);

        /*
         * The merged reads above prove the final state is ordered correctly.  The raw changelog
         * proves nothing was dropped or collapsed on the way: every key that was vacated has a
         * delete, and every key that was newly occupied has an insert.
         */
        List<Rec> raw = readChangelog(tbl);
        Map<Integer, Integer> inserts = new TreeMap<>(), deletes = new TreeMap<>();
        for (Rec r : raw) {
            if (r.kind.equals("+I") || r.kind.equals("+U")) inserts.merge(r.id, 1, Integer::sum);
            if (r.kind.equals("-D") || r.kind.equals("-U")) deletes.merge(r.id, 1, Integer::sum);
        }
        // up-move: +I for 2..N+1, -D for 1..N.  down-move: +I for 1..N, -D for 2..N+1.
        boolean insOk = true, delOk = true;
        for (int k = 1; k <= N + 1; k++) {
            // initial load (k<=N) + up-move (k>=2) + down-move (k<=N) inserts
            int wantIns = (k <= N ? 1 : 0) + (k >= 2 ? 1 : 0) + (k <= N ? 1 : 0);
            int wantDel = (k <= N ? 1 : 0) + (k >= 2 ? 1 : 0);
            if (inserts.getOrDefault(k, 0) != wantIns) insOk = false;
            if (deletes.getOrDefault(k, 0) != wantDel) delOk = false;
        }
        check("T20 raw changelog: every new key inserted exactly as often as it was occupied", insOk);
        check("T20 raw changelog: every vacated key deleted exactly as often as it was vacated", delOk);

        System.out.printf("%n  %d passed, %d failed%n", passed, failed);
        return failed;
    }

    static void report(String what, Map<Integer, String> expected, Map<Integer, String> actual) {
        check("T20 " + what + ": key sets match", actual.keySet().equals(expected.keySet()));
        check("T20 " + what + ": values match", actual.equals(expected));
        check("T20 " + what + ": no key duplicated or lost", actual.size() == N);
        if (!actual.equals(expected)) {
            SortedSet<Integer> missing = new TreeSet<>(expected.keySet());
            missing.removeAll(actual.keySet());
            SortedSet<Integer> extra = new TreeSet<>(actual.keySet());
            extra.removeAll(expected.keySet());
            System.out.println("  missing keys: " + missing);
            System.out.println("  extra keys  : " + extra);
        }
    }

    static Set<Integer> range(int lo, int hi) {
        Set<Integer> s = new TreeSet<>();
        for (int i = lo; i <= hi; i++) s.add(i);
        return s;
    }

    static Map<Integer, String> pgState(String tbl) throws Exception {
        Map<Integer, String> m = new TreeMap<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, v FROM " + tbl + " ORDER BY id")) {
            while (rs.next()) m.put(rs.getInt(1), rs.getString(2));
        }
        return m;
    }

    /** Merged read: the table's own options are left intact (merge-engine, sequence.field). */
    static Map<Integer, String> readMerged(String tbl) throws Exception {
        FileStoreTable table = FileStoreTableFactory.create(
            fileIO, new Path(warehouse + "/default.db/" + tbl));
        ReadBuilder rb = table.newReadBuilder();
        Map<Integer, String> out = new TreeMap<>();
        try (RecordReader<InternalRow> reader =
                 rb.newRead().createReader(rb.newScan().plan().splits())) {
            reader.forEachRemaining(r ->
                out.put(r.getInt(0), r.isNullAt(1) ? null : r.getString(1).toString()));
        }
        return out;
    }

    static Map<Integer, String> waitForMerged(String tbl, int want) throws Exception {
        for (int i = 0; i < 120; i++) {
            try {
                Map<Integer, String> m = readMerged(tbl);
                if (m.size() >= want) return m;
            } catch (Exception ignored) {
                // table not created yet
            }
            Thread.sleep(500);
        }
        return readMerged(tbl);
    }

    static Map<Integer, String> waitForMergedEquals(String tbl, Map<Integer, String> want)
            throws Exception {
        Map<Integer, String> last = Map.of();
        for (int i = 0; i < 120; i++) {
            try {
                last = readMerged(tbl);
                if (last.equals(want)) return last;
            } catch (Exception ignored) {
                // snapshot being replaced
            }
            Thread.sleep(500);
        }
        return last;
    }

    static final class Rec {
        final String kind; final int id;
        Rec(String kind, int id) { this.kind = kind; this.id = id; }
    }

    /** Every changelog record as stored, via a streaming scan of $audit_log (first plan() discarded). */
    static List<Rec> readChangelog(String tbl) throws Exception {
        Options o = new Options();
        o.set("warehouse", warehouse);
        List<Rec> out = new ArrayList<>();
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
            Table al = catalog.getTable(Identifier.create("default", tbl + "$audit_log"))
                .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
            int idIdx = al.rowType().getFieldNames().indexOf("id");
            StreamTableScan scan = al.newReadBuilder().newStreamScan();
            scan.plan();
            Long latest = ((FileStoreTable) catalog.getTable(Identifier.create("default", tbl)))
                .snapshotManager().latestSnapshotId();
            List<Split> splits = new ArrayList<>();
            for (long i = 0; i <= (latest == null ? 0 : latest) + 1; i++) {
                splits.addAll(scan.plan().splits());
            }
            try (RecordReader<InternalRow> reader = al.newReadBuilder().newRead().createReader(splits)) {
                reader.forEachRemaining(r -> out.add(new Rec(r.getString(0).toString(), r.getInt(idIdx))));
            }
        }
        return out;
    }

    static void exec(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    static String env(String name, String dflt) {
        String v = System.getenv(name);
        return v != null ? v : dflt;
    }

    static void check(String name, boolean ok) {
        System.out.printf("  %s  %s%n", ok ? "PASS" : "FAIL", name);
        if (ok) {
            passed++;
        } else {
            failed++;
            failures.add(name);
        }
    }
}
