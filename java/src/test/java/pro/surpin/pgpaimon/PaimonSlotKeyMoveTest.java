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
 * T20 — intra-transaction key collisions; the named acceptance test for spec section 7.
 *
 * A key-shifting UPDATE decomposes each row to -D(old_key) + +I(new_key), and the writer keeps
 * every change in LSN order (it never collapses per key).  Whether that converges depends on
 * the order PostgreSQL emits the pairs:
 *
 *  - Descending cascade ("SET id = id - 1"): each key is vacated (-D) before it is filled (+I),
 *    so the higher-LSN event for every key is the +I and the merge ends correct.  This is the
 *    main assertion.
 *  - Ascending cascade ("SET id = id + 1"): the +I for key k+1 would precede the -D that
 *    vacates it, so the delete would win the merge.  It never reaches the writer, because
 *    PostgreSQL checks a non-deferrable key per row and rejects the statement with a unique
 *    violation.  Pinned here so nobody "fixes" T20 by making the writer handle a case that
 *    cannot occur.
 *  - It could commit only under a DEFERRABLE key, where the transaction passes through states
 *    with two rows sharing a key, which a Paimon primary-key table cannot represent.  So
 *    paimon.add_table() refuses a deferrable primary key; asserted here.
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
            exec(conn, "CREATE TABLE " + tbl + " (id int PRIMARY KEY, v text)");
            // Registered while empty: add_table derives backfill_due from emptiness.
            exec(conn, "SELECT paimon.add_table('" + tbl + "'::regclass)");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tbl + " VALUES (?, ?)")) {
                for (int i = 2; i <= N + 1; i++) {      // keys 2..N+1, so the descending move has room
                    ps.setInt(1, i);
                    ps.setString(2, "row" + i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        Map<Integer, String> loaded = waitForMerged(tbl, N);
        check("T20 initial load: " + N + " keys", loaded.size() == N);

        // (2) The ascending cascade cannot commit on an ordinary key: PostgreSQL rejects it.
        boolean rejected = false;
        String msg = "";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            conn.setAutoCommit(false);
            try {
                exec(conn, "UPDATE " + tbl + " SET id = id + 1");
                conn.commit();
            } catch (SQLException e) {
                rejected = true;
                msg = e.getMessage();
                conn.rollback();
            }
        }
        check("T20 ascending cascade is rejected by PostgreSQL", rejected);
        check("T20 ... as a unique violation", msg.toLowerCase().contains("duplicate key"));
        check("T20 ... and nothing changed", pgState(tbl).keySet().equals(range(2, N + 1)));

        // (1) The descending cascade commits and converges: every key is vacated before it is filled.
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            conn.setAutoCommit(false);
            exec(conn, "UPDATE " + tbl + " SET id = id - 1");
            conn.commit();
        }
        Map<Integer, String> expected = pgState(tbl);
        check("T20 PostgreSQL holds keys 1.." + N + " after the descending move",
              expected.keySet().equals(range(1, N)));
        Map<Integer, String> actual = waitForMergedEquals(tbl, expected);
        report("descending move", expected, actual);

        // Nothing dropped or collapsed on the way: every change is in the raw changelog.
        List<Rec> raw = readChangelog(tbl);
        Map<Integer, Integer> inserts = new TreeMap<>(), deletes = new TreeMap<>();
        for (Rec r : raw) {
            if (r.kind.equals("+I") || r.kind.equals("+U")) inserts.merge(r.id, 1, Integer::sum);
            if (r.kind.equals("-D") || r.kind.equals("-U")) deletes.merge(r.id, 1, Integer::sum);
        }
        // initial load: +I for 2..N+1; move: -D for 2..N+1 and +I for 1..N.
        boolean insOk = true, delOk = true;
        for (int k = 1; k <= N + 1; k++) {
            int wantIns = (k >= 2 ? 1 : 0) + (k <= N ? 1 : 0);
            int wantDel = (k >= 2 ? 1 : 0);
            if (inserts.getOrDefault(k, 0) != wantIns) insOk = false;
            if (deletes.getOrDefault(k, 0) != wantDel) delOk = false;
        }
        check("T20 raw changelog: every filled key has its insert", insOk);
        check("T20 raw changelog: every vacated key has its delete", delOk);

        // (3) A DEFERRABLE primary key is refused at registration: it is what would let the
        // ascending cascade commit.
        String dtbl = "km_deferrable";
        boolean refused = false;
        String dmsg = "";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            exec(conn, "DROP TABLE IF EXISTS " + dtbl);
            exec(conn, "CREATE TABLE " + dtbl + " (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED, v text)");
            try {
                exec(conn, "SELECT paimon.add_table('" + dtbl + "'::regclass)");
            } catch (SQLException e) {
                refused = true;
                dmsg = e.getMessage();
            }
        }
        check("T20 add_table refuses a DEFERRABLE primary key", refused);
        check("T20 ... naming paimon_slot and DEFERRABLE",
              dmsg.contains("paimon_slot") && dmsg.toUpperCase().contains("DEFERRABLE"));

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
