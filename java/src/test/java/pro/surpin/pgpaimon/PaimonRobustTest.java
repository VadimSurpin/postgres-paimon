// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * What an administrator or a developer can do to the database under a running replication, end to
 * end (docs/logical-slot-architecture.md, section 8, "Rules for developers and administrators").
 * Each case asserts one of: PostgreSQL refuses it; it replicates and Paimon equals PostgreSQL; or
 * the writer stops with the named diagnosis and the named procedure brings it back, nothing lost.
 *
 *   R1  hostile session settings (ALTER DATABASE / ROLE SET IntervalStyle, DateStyle, TimeZone,
 *       extra_float_digits): every value reads back equal, streamed and (R12) reloaded
 *   R2  the slot is created for failover
 *   R3  the warehouse remembers its cluster: a tampered identity stops the writer; the named
 *       acknowledgement takes it over
 *   R4  a transaction over PGPAIMON_TXN_MAX_BYTES stops the writer, loses nothing, and replicates
 *       whole once the limit allows it
 *   R5  prepared transactions: reported while open, replicated once at COMMIT PREPARED, never
 *       after ROLLBACK PREPARED
 *   R6  TRUNCATE ... CASCADE into a replicated table, RESTART IDENTITY
 *   R7  partitions: rows moving between partitions, TRUNCATE of the parent, DETACH, ATTACH; a
 *       partitioned parent is refused
 *   R8  VACUUM FULL queued behind a lock, then run, CLUSTER, REINDEX: replication of other tables
 *       goes on, and the table's rows are unchanged
 *   R9  SET SCHEMA, RENAME of the schema, OWNER TO, SET UNLOGGED (refused), DML under
 *       session_replication_role = replica
 *   R10 column types carried as text: domains, arrays, ranges, json, jsonb, xml, interval, money,
 *       very long text, control characters, an enum value added
 *   R11 the slot invalidated (max_slot_wal_keep_size): the writer stops with the cause, at start
 *       and while running; scripts/rebootstrap.sh and a restart bring it back
 *   R12 after that re-bootstrap every table is PostgreSQL's again (the reload ran under R1's
 *       hostile settings too, and brings back what R13 lost), and the history kept what it held
 *   R13 the slot advanced by another client while the writer was cleanly stopped: refused at
 *       the next start (runs before R11)
 *
 * Runs LAST, in a Maven invocation of its own (run_acceptance.sh, LAST_SUITES): R11 reloads every
 * table of the database. Usage: test/slot-acceptance/run_acceptance.sh PaimonRobustTest
 */
public class PaimonRobustTest {

    static final long TIMEOUT_MS = 60_000;

    static String warehouse;
    static String jdbcUrl;
    static String launcher;
    static String pidFile;
    static int passed = 0, failed = 0;
    static final List<String> failures = new ArrayList<>();
    static int sentinelId = 0;

    @Test
    void robustnessSuite() throws Exception {
        String jdbc = System.getenv("PGPAIMON_IT_JDBC_URL");
        String wh = System.getenv("PGPAIMON_IT_WAREHOUSE");
        if ("1".equals(System.getenv("PGPAIMON_HARNESS"))) {
            assertNotNull(jdbc, "harness did not export PGPAIMON_IT_JDBC_URL");
            assertNotNull(wh, "harness did not export PGPAIMON_IT_WAREHOUSE");
            assertNotNull(System.getenv("PGPAIMON_WRITER_LAUNCHER"), "no PGPAIMON_WRITER_LAUNCHER");
            assertNotNull(System.getenv("PGPAIMON_WRITER_PID_FILE"), "no PGPAIMON_WRITER_PID_FILE");
        }
        assumeTrue(jdbc != null && wh != null && System.getenv("PGPAIMON_WRITER_LAUNCHER") != null,
                   "no live stack: run test/slot-acceptance/run_acceptance.sh");
        jdbcUrl = jdbc;
        warehouse = wh;
        launcher = System.getenv("PGPAIMON_WRITER_LAUNCHER");
        pidFile = System.getenv("PGPAIMON_WRITER_PID_FILE");
        passed = 0; failed = 0; failures.clear();

        System.out.println("== paimon_slot: what the database's users can do to replication ==");
        try (Connection c = connect()) {
            exec(c, "DROP TABLE IF EXISTS rb_sentinel");
            exec(c, "CREATE TABLE rb_sentinel (id int PRIMARY KEY, note text)");
            exec(c, "SELECT paimon.add_table('rb_sentinel'::regclass)");
        }
        writerKeepsUp("setup");

        try {
            run("R2", PaimonRobustTest::r2_failoverSlot);
            run("R1", PaimonRobustTest::r1_hostileSessionSettings);
            run("R3", PaimonRobustTest::r3_clusterIdentity);
            run("R4", PaimonRobustTest::r4_hugeTransaction);
            run("R5", PaimonRobustTest::r5_preparedTransactions);
            run("R6", PaimonRobustTest::r6_truncateCascade);
            run("R7", PaimonRobustTest::r7_partitions);
            run("R8", PaimonRobustTest::r8_vacuumFull);
            run("R9", PaimonRobustTest::r9_renamesAndRoles);
            run("R10", PaimonRobustTest::r10_typesAsText);
            run("R13", PaimonRobustTest::r13_slotMovedWhileStopped);
            run("R11", PaimonRobustTest::r11_slotInvalidated);
            run("R12", PaimonRobustTest::r12_afterTheReBootstrap);
        } finally {
            cleanUp();
        }

        System.out.println();
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        assertEquals(0, failed, "failed checks: " + failures);
    }

    // ── R1: hostile session settings ────────────────────────────────────────────────────────
    // Before: the replication session took IntervalStyle from ALTER DATABASE/ROLE SET, so an
    // interval column held '1 year 2 days 03:04:05' and, after the change, 'P1Y2DT3H4M5S' --
    // two spellings of one value in one Paimon column, nothing in the log.
    static final String R1_COLS = "id int PRIMARY KEY, d date, ts timestamp, tz timestamptz,"
        + " f4 float4, f8 float8, iv interval, mo money, n numeric(20,6)";

    static void r1_hostileSessionSettings() throws Exception {
        System.out.println("\n-- R1: ALTER DATABASE / ROLE SET of the settings values print with --");
        try (Connection c = connect()) {
            createTracked(c, "rb_settings", R1_COLS);
            exec(c, "INSERT INTO rb_settings VALUES (1, '2026-10-14', '2026-10-14 01:02:03.456789',"
                + " '2026-01-01 10:00:00.5+00', 1.1, 0.1::float8 + 0.2::float8,"
                + " '1 year 2 days 03:04:05.6', 1234.5, 123.456)");
            exec(c, "ALTER DATABASE " + dbName() + " SET IntervalStyle = 'iso_8601'");
            exec(c, "ALTER DATABASE " + dbName() + " SET DateStyle = 'German, DMY'");
            exec(c, "ALTER DATABASE " + dbName() + " SET extra_float_digits = -3");
            exec(c, "ALTER ROLE CURRENT_USER SET TimeZone = 'Asia/Tokyo'");
            exec(c, "ALTER ROLE CURRENT_USER SET IntervalStyle = 'sql_standard'");
        }
        // The settings reach a session when it starts: restart the writer under them.
        stopWriter();
        startWriter("R1 under hostile settings", Map.of(), true);
        try (Connection c = connect()) {
            exec(c, "INSERT INTO rb_settings VALUES (2, '1999-12-31', '2000-02-29 23:59:59',"
                + " '2026-06-01 10:00:00+05:30', 3.4028235e38, 1e-300,"
                + " '-5 mins 3.000001 secs', -0.5, -1.000001)");
            exec(c, "UPDATE rb_settings SET iv = iv + interval '1 day', f8 = f8 * 3 WHERE id = 1");
        }
        writerKeepsUp("R1");
        sameValues("R1 every value replicated under hostile settings equals PostgreSQL's", "rb_settings");
        check("R1 the writer's own sessions pinned them (IntervalStyle as postgres prints it)",
              valuesOf("rb_settings").getOrDefault("1", Map.of()).getOrDefault("iv", "")
                  .equals("1 year 3 days 03:04:05.6"));
    }

    // ── R2: a failover slot ─────────────────────────────────────────────────────────────────
    static void r2_failoverSlot() throws Exception {
        System.out.println("\n-- R2: the slot is created for failover ---------------------------");
        try (Connection c = connect()) {
            check("R2 the writer created its slot with failover = true",
                  "t".equals(scalar(c, "SELECT failover FROM pg_replication_slots WHERE slot_name = 'paimon'")));
        }
    }

    // ── R3: the warehouse belongs to one cluster ────────────────────────────────────────────
    // Before: nothing recorded which cluster a warehouse came from; a writer pointed at another
    // cluster (or a restored one) filtered its transactions as already committed, silently.
    static void r3_clusterIdentity() throws Exception {
        System.out.println("\n-- R3: a warehouse from another cluster ---------------------------");
        String sysid;
        try (Connection c = connect()) {
            sysid = scalar(c, "SELECT system_identifier::text FROM pg_control_system()");
        }
        check("R3 every table carries the cluster's identity",
              sysid.equals(option("rb_sentinel", "pgpaimon.pg-system-identifier"))
              && sysid.equals(option("rb_sentinel_hist", "pgpaimon.pg-system-identifier"))
              && option("rb_sentinel", "pgpaimon.pg-timeline") != null);
        stopWriter();
        setOption("rb_sentinel", "pgpaimon.pg-system-identifier", "1234567890");
        int before = writerLog().length();
        startWriter("R3 against a warehouse of 'another' cluster", Map.of(), false);
        check("R3 the writer refuses to start", waitProcessGone(writerPid(), 60_000));
        String log = writerLog().substring(before);
        check("R3 and says why, with both remedies",
              log.contains("CLUSTER MISMATCH") && log.contains("1234567890")
              && log.contains("backfill_due = true")
              && log.contains("PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER=" + sysid));
        startWriter("R3 with the acknowledgement", Map.of("PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER", sysid), true);
        check("R3 the acknowledged writer re-stamped the table",
              sysid.equals(option("rb_sentinel", "pgpaimon.pg-system-identifier")));
        writerKeepsUp("R3 after the acknowledgement");
        stopWriter();
        startWriter("R3 restart without it", Map.of(), true);
        writerKeepsUp("R3");
    }

    // ── R4: a transaction larger than the writer may hold ───────────────────────────────────
    // Before: buffered whole with no bound; large enough, an OutOfMemoryError on every replay.
    static void r4_hugeTransaction() throws Exception {
        System.out.println("\n-- R4: a transaction over PGPAIMON_TXN_MAX_BYTES ------------------");
        try (Connection c = connect()) {
            createTracked(c, "rb_big", "id int PRIMARY KEY, v text");
        }
        writerKeepsUp("R4 before");
        stopWriter();
        int before = writerLog().length();
        startWriter("R4 with a 200 kB limit", Map.of("PGPAIMON_TXN_MAX_BYTES", "200000"), true);
        try (Connection c = connect()) {
            exec(c, "INSERT INTO rb_big SELECT g, repeat('x', 100) FROM generate_series(1, 5000) g");
        }
        check("R4 the writer stops on the transaction", waitProcessGone(writerPid(), 60_000));
        String log = writerLog().substring(before);
        check("R4 and says what to do", log.contains("TRANSACTION TOO LARGE")
              && log.contains("PGPAIMON_TXN_MAX_BYTES=200000") && log.contains("Nothing is lost"));
        check("R4 nothing of it reached Paimon", valuesOf("rb_big").isEmpty());
        startWriter("R4 with the default limit", Map.of(), true);
        writerKeepsUp("R4 after");
        check("R4 the whole transaction replicated once the limit allowed it",
              valuesOf("rb_big").size() == 5000 && histRows("rb_big_hist").size() == 5000);
    }

    // ── R5: prepared transactions ───────────────────────────────────────────────────────────
    static void r5_preparedTransactions() throws Exception {
        System.out.println("\n-- R5: prepared transactions --------------------------------------");
        try (Connection c = connect()) {
            createTracked(c, "rb_2pc", "id int PRIMARY KEY, v text");
        }
        stopWriter();
        int before = writerLog().length();
        startWriter("R5 with a 1 s health interval", Map.of("PGPAIMON_HEALTH_INTERVAL_MS", "1000"), true);
        try (Connection p = connect(); Connection c = connect()) {
            p.setAutoCommit(false);
            exec(p, "INSERT INTO rb_2pc VALUES (1, 'committed later')");
            exec(p, "PREPARE TRANSACTION 'rb_gid_commit'");
            p.setAutoCommit(true);
            p.setAutoCommit(false);
            exec(p, "INSERT INTO rb_2pc VALUES (2, 'rolled back')");
            exec(p, "PREPARE TRANSACTION 'rb_gid_rollback'");
            p.setAutoCommit(true);
            String found = scalar(c, "SELECT string_agg(problem, ' / ') FROM paimon.slot_health("
                + "prepared_older_than => '0 seconds') WHERE level = 'warning'");
            check("R5 paimon.slot_health() reports the open prepared transactions",
                  found != null && found.contains("rb_gid_commit") && found.contains("rb_gid_rollback")
                  && found.contains("holds back the replication slot"));
            writerKeepsUp("R5 while they are open");
            check("R5 their rows are not in Paimon while they are open", valuesOf("rb_2pc").isEmpty());
            exec(c, "COMMIT PREPARED 'rb_gid_commit'");
            exec(c, "ROLLBACK PREPARED 'rb_gid_rollback'");
        }
        writerKeepsUp("R5 after the commit");
        sameValues("R5 COMMIT PREPARED replicated, ROLLBACK PREPARED did not", "rb_2pc");
        List<Map<String, String>> h = histRows("rb_2pc_hist");
        check("R5 the prepared row is in the history exactly once",
              h.size() == 1 && "1".equals(h.get(0).get("id")) && "+I".equals(h.get(0).get("_op")));
        check("R5 the writer ran on throughout", writerAlive()
              && !writerLog().substring(before).contains("SEVERE"));
        stopWriter();
        startWriter("R5 restart", Map.of(), true);
    }

    // ── R6: TRUNCATE ... CASCADE and RESTART IDENTITY ───────────────────────────────────────
    static void r6_truncateCascade() throws Exception {
        System.out.println("\n-- R6: TRUNCATE CASCADE, RESTART IDENTITY -------------------------");
        try (Connection c = connect()) {
            exec(c, "DROP TABLE IF EXISTS rb_child, rb_parent, rb_ids CASCADE");
            exec(c, "CREATE TABLE rb_parent (id int PRIMARY KEY, v text)");          // not replicated
            createTrackedRaw(c, "rb_child", "id int PRIMARY KEY, pid int REFERENCES rb_parent(id), v text");
            createTrackedRaw(c, "rb_ids", "id serial PRIMARY KEY, v text");
            exec(c, "INSERT INTO rb_parent VALUES (1, 'a'), (2, 'b')");
            exec(c, "INSERT INTO rb_child VALUES (1, 1, 'x'), (2, 2, 'y')");
            exec(c, "INSERT INTO rb_ids (v) VALUES ('one'), ('two')");
        }
        writerKeepsUp("R6 before");
        try (Connection c = connect()) {
            exec(c, "TRUNCATE rb_parent CASCADE");                  // reaches the replicated child
            exec(c, "INSERT INTO rb_parent VALUES (3, 'c')");
            exec(c, "INSERT INTO rb_child VALUES (3, 3, 'after')");
            exec(c, "TRUNCATE rb_ids RESTART IDENTITY");
            exec(c, "INSERT INTO rb_ids (v) VALUES ('again')");     // id 1 again
        }
        writerKeepsUp("R6");
        sameValues("R6 TRUNCATE ... CASCADE emptied the replicated child", "rb_child");
        sameValues("R6 TRUNCATE ... RESTART IDENTITY, then the reused id", "rb_ids");
        check("R6 the history keeps the cascade as a T row",
              histRows("rb_child_hist").stream().anyMatch(r -> "T".equals(r.get("_op"))));
    }

    // ── R7: partitioned tables ──────────────────────────────────────────────────────────────
    static void r7_partitions() throws Exception {
        System.out.println("\n-- R7: partitions -------------------------------------------------");
        try (Connection c = connect()) {
            exec(c, "DROP TABLE IF EXISTS rb_pt, rb_pt3 CASCADE");
            exec(c, "CREATE TABLE rb_pt (id int, k int, v text, PRIMARY KEY (id, k)) PARTITION BY LIST (k)");
            refused("R7 a partitioned parent", c, "SELECT paimon.add_table('rb_pt'::regclass)",
                    "is not an ordinary table");
            exec(c, "CREATE TABLE rb_pt1 PARTITION OF rb_pt FOR VALUES IN (1)");
            exec(c, "CREATE TABLE rb_pt2 PARTITION OF rb_pt FOR VALUES IN (2)");
            exec(c, "SELECT paimon.add_table('rb_pt1'::regclass)");
            exec(c, "SELECT paimon.add_table('rb_pt2'::regclass)");
            exec(c, "INSERT INTO rb_pt VALUES (1, 1, 'one'), (2, 2, 'two'), (3, 1, 'three')");
            exec(c, "UPDATE rb_pt SET k = 2 WHERE id = 1");                  // moves partitions
            exec(c, "TRUNCATE rb_pt1");
            exec(c, "INSERT INTO rb_pt VALUES (4, 1, 'four')");
            exec(c, "ALTER TABLE rb_pt DETACH PARTITION rb_pt2");
            exec(c, "INSERT INTO rb_pt2 VALUES (5, 2, 'detached, still replicated')");
            exec(c, "ALTER TABLE rb_pt ATTACH PARTITION rb_pt2 FOR VALUES IN (2)");
            exec(c, "INSERT INTO rb_pt VALUES (6, 2, 'through the parent again')");
            exec(c, "TRUNCATE rb_pt");                                         // every partition
            exec(c, "INSERT INTO rb_pt VALUES (7, 1, 'seven'), (8, 2, 'eight')");
            exec(c, "UPDATE rb_pt SET k = 1 WHERE id = 8");
            // a new partition nobody registered: its rows are not replicated (documented, SILENT)
            exec(c, "CREATE TABLE rb_pt3 PARTITION OF rb_pt FOR VALUES IN (3)");
            exec(c, "INSERT INTO rb_pt VALUES (9, 3, 'in an unregistered partition')");
        }
        writerKeepsUp("R7");
        sameValues("R7 partition 1 equals PostgreSQL's", "rb_pt1");
        sameValues("R7 partition 2 equals PostgreSQL's", "rb_pt2");
        check("R7 an unregistered new partition is not replicated (by design: register it)",
              valuesOf("rb_pt3").isEmpty() && option("rb_pt3", "bucket") == null);
        List<Map<String, String>> h = histRows("rb_pt2_hist");
        check("R7 a row leaving a partition is a -D in its history",
              h.stream().anyMatch(r -> "-D".equals(r.get("_op")) && "8".equals(r.get("id"))));
    }

    // ── R8: VACUUM FULL, CLUSTER, REINDEX ───────────────────────────────────────────────────
    static void r8_vacuumFull() throws Exception {
        System.out.println("\n-- R8: VACUUM FULL / CLUSTER / REINDEX ----------------------------");
        try (Connection c = connect()) {
            createTracked(c, "rb_vac", "id int PRIMARY KEY, v text");
            exec(c, "INSERT INTO rb_vac SELECT g, md5(g::text) FROM generate_series(1, 20000) g");
            exec(c, "DELETE FROM rb_vac WHERE id % 3 = 0");
        }
        writerKeepsUp("R8 before");
        stopWriter();
        startWriter("R8 with a 1 s health interval", Map.of("PGPAIMON_HEALTH_INTERVAL_MS", "1000"), true);
        try (Connection reader = connect(); Connection vac = connect()) {
            reader.setAutoCommit(false);
            exec(reader, "SELECT count(*) FROM rb_vac");     // ACCESS SHARE, held: VACUUM FULL queues
            Thread vt = new Thread(() -> {
                try { exec(vac, "VACUUM FULL rb_vac"); } catch (SQLException e) { log("R8 vacuum", e.toString()); }
            });
            vt.start();
            Thread.sleep(3000);          // several health checks meet the queued ACCESS EXCLUSIVE
            try (Connection c = connect()) {
                check("R8 VACUUM FULL is waiting for its lock",
                      "1".equals(scalar(c, "SELECT count(*) FROM pg_stat_activity WHERE query = 'VACUUM FULL rb_vac' AND wait_event_type = 'Lock'")));
            }
            long start = System.currentTimeMillis();
            int id = ++sentinelId;
            try (Connection c = connect()) {
                exec(c, "INSERT INTO rb_sentinel VALUES (" + id + ", 'R8 under VACUUM FULL')");
            }
            boolean seen = waitFor(() -> valuesOf("rb_sentinel").containsKey(String.valueOf(id)), 20_000);
            check("R8 another table replicates while VACUUM FULL waits (" + (System.currentTimeMillis() - start) + " ms)", seen);
            check("R8 the writer is still running", writerAlive());
            reader.rollback();
            vt.join(120_000);
            check("R8 VACUUM FULL finished", !vt.isAlive());
        }
        try (Connection c = connect()) {
            exec(c, "CLUSTER rb_vac USING rb_vac_pkey");
            exec(c, "REINDEX TABLE rb_vac");
            exec(c, "UPDATE rb_vac SET v = 'after the rewrites' WHERE id < 10");
            exec(c, "INSERT INTO rb_vac VALUES (30000, 'new')");
        }
        writerKeepsUp("R8 after");
        sameValues("R8 the rewritten table's rows are PostgreSQL's", "rb_vac");
        stopWriter();
        startWriter("R8 restart", Map.of(), true);
    }

    // ── R9: renames, owners, persistence, replica role ──────────────────────────────────────
    static void r9_renamesAndRoles() throws Exception {
        System.out.println("\n-- R9: SET SCHEMA, RENAME, OWNER TO, UNLOGGED, replica role -------");
        try (Connection c = connect()) {
            exec(c, "DROP SCHEMA IF EXISTS rb_s1, rb_s2 CASCADE");
            exec(c, "DROP TABLE IF EXISTS rb_moved");
            exec(c, "DROP ROLE IF EXISTS rb_owner");
            exec(c, "CREATE SCHEMA rb_s1");
            exec(c, "CREATE TABLE rb_s1.rb_moved (id int PRIMARY KEY, v text)");
            exec(c, "SELECT paimon.add_table('rb_s1.rb_moved'::regclass)");
            exec(c, "INSERT INTO rb_s1.rb_moved VALUES (1, 'in rb_s1')");
            exec(c, "ALTER SCHEMA rb_s1 RENAME TO rb_s2");
            exec(c, "INSERT INTO rb_s2.rb_moved VALUES (2, 'schema renamed')");
            exec(c, "ALTER TABLE rb_s2.rb_moved SET SCHEMA public");
            exec(c, "INSERT INTO public.rb_moved VALUES (3, 'moved to public')");
            exec(c, "CREATE ROLE rb_owner");
            exec(c, "ALTER TABLE rb_moved OWNER TO rb_owner");
            exec(c, "INSERT INTO rb_moved VALUES (4, 'new owner')");
            refused("R9 SET UNLOGGED", c, "ALTER TABLE rb_moved SET UNLOGGED", "part of a publication");
            exec(c, "SET session_replication_role = replica");
            exec(c, "INSERT INTO rb_moved VALUES (5, 'under the replica role')");
            exec(c, "UPDATE rb_moved SET v = 'updated under the replica role' WHERE id = 1");
            exec(c, "RESET session_replication_role");
        }
        writerKeepsUp("R9");
        Map<String, Map<String, String>> pm = valuesOf("rb_s1", "rb_moved");
        Map<String, Map<String, String>> pg = pgValues("public.rb_moved");
        check("R9 every change reached the Paimon table of the registered identity (rb_s1.rb_moved)",
              same(pm, pg));
        try (Connection c = connect()) {
            exec(c, "DROP TABLE rb_moved");
            exec(c, "DROP ROLE rb_owner");
            exec(c, "DROP SCHEMA IF EXISTS rb_s2 CASCADE");
        }
    }

    // ── R10: types carried as text ──────────────────────────────────────────────────────────
    static void r10_typesAsText() throws Exception {
        System.out.println("\n-- R10: domains, arrays, ranges, json, xml, money, long text ------");
        try (Connection c = connect()) {
            exec(c, "DROP TABLE IF EXISTS rb_types");
            exec(c, "DROP DOMAIN IF EXISTS rb_posint");
            exec(c, "DROP TYPE IF EXISTS rb_mood");
            exec(c, "CREATE DOMAIN rb_posint AS int CHECK (VALUE > 0)");
            exec(c, "CREATE TYPE rb_mood AS ENUM ('sad', 'ok')");
            createTracked(c, "rb_types", "id int PRIMARY KEY, dm rb_posint, ia int[], ta text[],"
                + " r int4range, tr tstzrange, js json, jb jsonb, x xml, iv interval, mo money,"
                + " lt text, cc text, u uuid, ip inet, b bit(4), t time, ttz timetz, m rb_mood, by bytea");
            exec(c, "INSERT INTO rb_types VALUES (1, 5, '{1,2,NULL}', '{\"a b\",\"c,d\",\"e\\\"f\",NULL}',"
                + " '[1,5)', '[2026-01-01 00:00+00,2026-02-01 00:00+00)', '{\"a\": 1,  \"b\":[1,2]}',"
                + " '{\"a\": 1,  \"b\":[1,2]}', '<a>x &amp; y</a>', '1 year 2 days 03:04:05', 12.34,"
                + " repeat('long text ', 300000), E'tab\\there\\nnewline\\\\back\\x01\\x1bctl\\r',"
                + " 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '10.0.0.1/8', B'1010', '12:34:56.789',"
                + " '12:34:56+05:30', 'sad', '\\x00ff10')");
            exec(c, "ALTER TYPE rb_mood ADD VALUE 'happy'");
            exec(c, "INSERT INTO rb_types (id, m, lt) VALUES (2, 'happy', '')");
            exec(c, "UPDATE rb_types SET cc = 'unchanged long text, TOAST' WHERE id = 1");
        }
        writerKeepsUp("R10");
        sameValues("R10 every value reads back as PostgreSQL prints it", "rb_types");
    }

    // ── R11: the slot invalidated ───────────────────────────────────────────────────────────
    // Before: START_REPLICATION's bare "can no longer access replication slot", no cause, no
    // procedure; while running, the same after a reconnect.
    static void r11_slotInvalidated() throws Exception {
        System.out.println("\n-- R11: the slot invalidated by max_slot_wal_keep_size ------------");
        try (Connection c = connect()) {
            createTracked(c, "rb_lost", "id int PRIMARY KEY, v text");
            exec(c, "INSERT INTO rb_lost VALUES (1, 'before')");
            exec(c, "DROP TABLE IF EXISTS rb_churn");
            exec(c, "CREATE TABLE rb_churn (x text)");                     // not replicated
        }
        writerKeepsUp("R11 before");

        // 1. while running: the writer is frozen, so the slot falls behind and the checkpointer
        //    invalidates it (and kills its walsender); thawed, it must stop with the cause.
        try (Connection c = connect()) {
            exec(c, "ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'");
            exec(c, "SELECT pg_reload_conf()");
        }
        String pid = writerPid();
        int before = writerLog().length();
        new ProcessBuilder("kill", "-STOP", pid).start().waitFor();
        try {
            churnUntilLost();
        } finally {
            new ProcessBuilder("kill", "-CONT", pid).start().waitFor();
        }
        try (Connection c = connect()) {
            check("R11 the slot is lost (wal_removed)",
                  "lost|wal_removed".equals(scalar(c, "SELECT wal_status || '|' || invalidation_reason"
                      + " FROM pg_replication_slots WHERE slot_name = 'paimon'")));
            exec(c, "INSERT INTO rb_lost VALUES (2, 'while the slot was lost')");
        }
        check("R11 the running writer stops", waitProcessGone(pid, 60_000));
        String log = writerLog().substring(before);
        check("R11 and says why and what to do", log.contains("REPLICATION SLOT UNUSABLE while running")
              && log.contains("wal_removed") && log.contains("max_slot_wal_keep_size")
              && log.contains("scripts/rebootstrap.sh"));

        // 2. at startup: refuses, with the same diagnosis.
        before = writerLog().length();
        startWriter("R11 on a lost slot", Map.of(), false);
        check("R11 a writer started on the lost slot refuses", waitProcessGone(writerPid(), 60_000));
        log = writerLog().substring(before);
        check("R11 and says why", log.contains("REPLICATION SLOT UNUSABLE at startup") && log.contains("wal_removed"));
        try (Connection c = connect()) {
            check("R11 paimon.slot_health() reports it as fatal",
                  "fatal".equals(scalar(c, "SELECT level FROM paimon.slot_health() WHERE problem LIKE '%invalidated (wal_removed)%'")));
            exec(c, "ALTER SYSTEM RESET max_slot_wal_keep_size");
            exec(c, "SELECT pg_reload_conf()");
        }

        // 3. the documented procedure.
        String db = dbName();
        ProcessBuilder pb = new ProcessBuilder("bash", repoRoot() + "/scripts/rebootstrap.sh", "-d", db)
            .redirectErrorStream(true);
        pb.environment().put("PGPAIMON_SLOT", "paimon");   // the harness sets paimon_it for suites
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes());
        check("R11 scripts/rebootstrap.sh succeeds (" + out.trim().replace('\n', ' ') + ")",
              p.waitFor() == 0 && out.contains("dropped replication slot 'paimon'"));
        startWriter("R11 after the re-bootstrap", Map.of(), true);
        writerKeepsUp("R11 after the re-bootstrap");
    }

    // ── R12: after the re-bootstrap ─────────────────────────────────────────────────────────
    static void r12_afterTheReBootstrap() throws Exception {
        System.out.println("\n-- R12: after the re-bootstrap ------------------------------------");
        try (Connection c = connect()) {
            check("R12 the new slot is a failover slot too",
                  "t".equals(scalar(c, "SELECT failover FROM pg_replication_slots WHERE slot_name = 'paimon'")));
        }
        sameValues("R12 the row written while the slot was lost is in the reloaded table", "rb_lost");
        sameValues("R12 the table reloaded under R1's hostile settings equals PostgreSQL's", "rb_settings");
        sameValues("R12 the type table reloaded equals PostgreSQL's", "rb_types");
        sameValues("R12 the row R13 lost is back after the reload", "rb_moved_slot");
        List<Map<String, String>> h = histRows("rb_lost_hist");
        check("R12 the history kept its +I and has the reload's R rows, nothing for the lost window",
              h.stream().anyMatch(r -> "+I".equals(r.get("_op")) && "1".equals(r.get("id")))
              && h.stream().filter(r -> "R".equals(r.get("_op"))).count() == 2
              && h.stream().noneMatch(r -> "+I".equals(r.get("_op")) && "2".equals(r.get("id"))));
    }

    // ── R13: the slot moved on while the writer was stopped ─────────────────────────────────
    // Before: undetectable; the changes in between were simply never applied.
    static void r13_slotMovedWhileStopped() throws Exception {
        System.out.println("\n-- R13: the slot advanced by someone else while the writer was stopped --");
        try (Connection c = connect()) {
            createTracked(c, "rb_moved_slot", "id int PRIMARY KEY, v text");
            exec(c, "INSERT INTO rb_moved_slot VALUES (1, 'before')");
        }
        writerKeepsUp("R13 before");
        stopWriter();
        File mark = new File(warehouse, "_pgpaimon_stopped_paimon");
        check("R13 a clean stop recorded where it left the slot", mark.isFile());
        try (Connection c = connect()) {
            exec(c, "INSERT INTO rb_moved_slot VALUES (2, 'consumed by someone else')");
            exec(c, "SELECT pg_replication_slot_advance('paimon', pg_current_wal_lsn())");
        }
        int before = writerLog().length();
        startWriter("R13 after the slot was advanced", Map.of(), false);
        check("R13 the writer refuses to start", waitProcessGone(writerPid(), 60_000));
        String log = writerLog().substring(before);
        check("R13 and says what happened and what to do",
              log.contains("SLOT MOVED WHILE THE WRITER WAS STOPPED")
              && log.contains("pg_replication_slot_advance") && log.contains("scripts/rebootstrap.sh"));
        startWriter("R13 refuses again on the next start", Map.of(), false);
        check("R13 refuses again", waitProcessGone(writerPid(), 60_000));
        // Accepting the loss: delete the mark, as the message says. R11's re-bootstrap then
        // brings the row back (R12).
        check("R13 the mark can be deleted", mark.delete());
        startWriter("R13 with the loss accepted", Map.of(), true);
        writerKeepsUp("R13 after");
        check("R13 the row consumed elsewhere is missing (the loss the refusal warned of)",
              !valuesOf("rb_moved_slot").containsKey("2"));
    }

    static void cleanUp() {
        try (Connection c = connect()) {
            exec(c, "ALTER DATABASE " + dbName() + " RESET ALL");
            exec(c, "ALTER ROLE CURRENT_USER RESET ALL");
            exec(c, "ALTER SYSTEM RESET max_slot_wal_keep_size");
            exec(c, "SELECT pg_reload_conf()");
            for (String gid : List.of("rb_gid_commit", "rb_gid_rollback")) {
                try { exec(c, "ROLLBACK PREPARED '" + gid + "'"); } catch (SQLException ignored) { }
            }
        } catch (Exception e) {
            System.out.println("  cleanup: " + e);
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────────────────

    interface Case { void run() throws Exception; }

    static void run(String name, Case c) {
        try {
            c.run();
        } catch (Exception e) {
            e.printStackTrace(System.out);
            check(name + " ran to completion (" + e + ")", false);
        }
    }

    static void churnUntilLost() throws Exception {
        for (int i = 0; i < 20; i++) {
            try (Connection c = connect()) {
                exec(c, "INSERT INTO rb_churn SELECT repeat('x', 1000) FROM generate_series(1, 4000)");
                exec(c, "SELECT pg_switch_wal()");
                exec(c, "CHECKPOINT");
                if ("lost".equals(scalar(c, "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'paimon'"))) {
                    return;
                }
            }
        }
    }

    static String repoRoot() {
        return new File(launcher).getParentFile().getParentFile().getParentFile().getParent();
    }

    static String dbName() {
        String u = jdbcUrl.substring(jdbcUrl.lastIndexOf('/') + 1);
        int q = u.indexOf('?');
        return q < 0 ? u : u.substring(0, q);
    }

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    static String scalar(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static void createTracked(Connection c, String tbl, String columns) throws Exception {
        exec(c, "DROP TABLE IF EXISTS " + tbl + " CASCADE");
        createTrackedRaw(c, tbl, columns);
    }

    static void createTrackedRaw(Connection c, String tbl, String columns) throws Exception {
        exec(c, "CREATE TABLE " + tbl + " (" + columns + ")");
        exec(c, "SELECT paimon.add_table('" + tbl + "'::regclass)");
    }

    static void refused(String name, Connection c, String sql, String expected) {
        try {
            exec(c, sql);
            check(name + " is refused by PostgreSQL", false);
        } catch (SQLException e) {
            boolean ok = e.getMessage() != null && e.getMessage().contains(expected);
            check(name + " is refused by PostgreSQL", ok);
            if (!ok) System.out.println("        got: " + e.getMessage());
        }
    }

    interface Cond { boolean ok() throws Exception; }

    static boolean waitFor(Cond c, long ms) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        do {
            if (c.ok()) return true;
            Thread.sleep(250);
        } while (System.currentTimeMillis() < deadline);
        return c.ok();
    }

    static void writerKeepsUp(String test) throws Exception {
        check(test + ": the writer process is alive", writerAlive());
        int id = ++sentinelId;
        try (Connection c = connect()) {
            exec(c, "INSERT INTO rb_sentinel VALUES (" + id + ", '" + test.replace("'", "") + "')");
        }
        boolean seen = waitFor(() -> valuesOf("rb_sentinel").containsKey(String.valueOf(id)),
                               writerAlive() ? TIMEOUT_MS : 0);
        check(test + ": a row written afterwards reaches Paimon", seen);
    }

    /**
     * PostgreSQL's rows, each value as text in a session with every setting the writer pins (so
     * the hostile database and role settings do not apply here either), normalised to how the
     * Paimon side renders it. Keyed by id.
     */
    static Map<String, Map<String, String>> pgValues(String tbl) throws SQLException {
        Map<String, Map<String, String>> out = new TreeMap<>();
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("SET TimeZone = 'UTC'");
            s.execute("SET IntervalStyle = 'postgres'");
            s.execute("SET DateStyle = 'ISO, MDY'");
            s.execute("SET extra_float_digits = 3");
            s.execute("SET lc_monetary = 'C'");
            s.execute("SET bytea_output = 'hex'");
            try (ResultSet rs = s.executeQuery("SELECT * FROM " + tbl)) {
                ResultSetMetaData md = rs.getMetaData();
                while (rs.next()) {
                    Map<String, String> row = new TreeMap<>();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        row.put(md.getColumnName(i), canonical(md.getColumnTypeName(i), rs.getString(i)));
                    }
                    out.put(row.get("id"), row);
                }
            }
        }
        return out;
    }

    static String canonical(String pgType, String v) {
        if (v == null) return null;
        switch (pgType) {
            case "numeric": return new java.math.BigDecimal(v).toPlainString();
            case "float8": return String.valueOf(Double.parseDouble(v));
            case "float4": return String.valueOf(Float.parseFloat(v));
            case "timestamp": return java.time.LocalDateTime.parse(v.replace(' ', 'T')).toString();
            case "timestamptz": return java.time.OffsetDateTime.parse(v.replace(' ', 'T') + (v.matches(".*[+-]\\d\\d$") ? ":00" : ""))
                                         .withOffsetSameInstant(java.time.ZoneOffset.UTC).toLocalDateTime().toString();
            case "bytea": return v;
            default: return v;
        }
    }

    static boolean same(Map<String, Map<String, String>> pm, Map<String, Map<String, String>> pg) {
        return restrict(pm, pg).equals(pg);
    }

    static Map<String, Map<String, String>> restrict(Map<String, Map<String, String>> pm,
                                                     Map<String, Map<String, String>> pg) {
        Set<String> cols = pg.values().stream().findFirst().map(Map::keySet).orElse(Set.of());
        Map<String, Map<String, String>> out = new TreeMap<>();
        pm.forEach((k, row) -> {
            Map<String, String> r = new TreeMap<>();
            for (String col : cols) r.put(col, row.get(col));
            out.put(k, r);
        });
        return out;
    }

    static void sameValues(String name, String tbl) throws Exception {
        Map<String, Map<String, String>> pg = pgValues(tbl);
        Map<String, Map<String, String>>[] pm = new Map[1];
        boolean ok = waitFor(() -> same(pm[0] = valuesOf(tbl), pg), writerAlive() ? TIMEOUT_MS : 0);
        check(name, ok);
        if (!ok) {
            Map<String, Map<String, String>> got = restrict(pm[0], pg);
            for (String k : pg.keySet()) {
                if (!Objects.equals(pg.get(k), got.get(k))) {
                    System.out.println("        id " + k + " PostgreSQL: " + abbreviate(pg.get(k)));
                    System.out.println("        id " + k + " Paimon:     " + abbreviate(got.get(k)));
                }
            }
            for (String k : got.keySet()) {
                if (!pg.containsKey(k)) System.out.println("        id " + k + " only in Paimon");
            }
        }
    }

    static String abbreviate(Map<String, String> m) {
        if (m == null) return "(absent)";
        Map<String, String> r = new TreeMap<>();
        m.forEach((k, v) -> r.put(k, v == null || v.length() < 120 ? v : v.substring(0, 60) + "...(" + v.length() + ")"));
        return r.toString();
    }

    static Map<String, Map<String, String>> valuesOf(String tbl) {
        return valuesOf("public", tbl);
    }

    /** The merged Paimon table, keyed by id; empty while it does not exist yet. */
    static Map<String, Map<String, String>> valuesOf(String schema, String tbl) {
        Map<String, Map<String, String>> out = new TreeMap<>();
        for (Map<String, String> m : rows(schema, tbl)) out.put(m.get("id"), m);
        return out;
    }

    static List<Map<String, String>> histRows(String tbl) {
        return rows("public", tbl);
    }

    static List<Map<String, String>> rows(String schema, String tbl) {
        List<Map<String, String>> out = new ArrayList<>();
        try (Catalog catalog = catalog()) {
            Table t = catalog.getTable(Identifier.create(schema, tbl));
            RowType rt = t.rowType();
            ReadBuilder rb = t.newReadBuilder();
            try (RecordReader<InternalRow> r = rb.newRead().createReader(rb.newScan().plan())) {
                r.forEachRemaining(row -> {
                    Map<String, String> m = new TreeMap<>();
                    for (int i = 0; i < rt.getFieldCount(); i++) {
                        DataField f = rt.getFields().get(i);
                        m.put(f.name(), row.isNullAt(i) ? null : render(f, InternalRowUtils.get(row, i, f.type())));
                    }
                    out.add(m);
                });
            }
        } catch (Exception notYet) {
            return new ArrayList<>();
        }
        return out;
    }

    static Catalog catalog() {
        Options o = new Options();
        o.set("warehouse", warehouse);
        return CatalogFactory.createCatalog(CatalogContext.create(o));
    }

    static String render(DataField f, Object v) {
        if (v == null) return null;
        switch (f.type().getTypeRoot()) {
            case DECIMAL: return ((org.apache.paimon.data.Decimal) v).toBigDecimal().toPlainString();
            case DATE: return java.time.LocalDate.ofEpochDay((Integer) v).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((org.apache.paimon.data.Timestamp) v).toLocalDateTime().toString();
            case BINARY:
            case VARBINARY: {
                StringBuilder sb = new StringBuilder("\\x");
                for (byte b : (byte[]) v) sb.append(String.format("%02x", b));
                return sb.toString();
            }
            default: return v.toString();
        }
    }

    static String option(String tbl, String key) {
        try (Catalog catalog = catalog()) {
            return catalog.getTable(Identifier.create("public", tbl)).options().get(key);
        } catch (Exception e) {
            return null;
        }
    }

    static void setOption(String tbl, String key, String value) throws Exception {
        try (Catalog catalog = catalog()) {
            catalog.alterTable(Identifier.create("public", tbl), List.of(SchemaChange.setOption(key, value)), false);
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

    static boolean writerAlive() throws Exception {
        return processAlive(writerPid());
    }

    static boolean waitProcessGone(String pid, long ms) throws Exception {
        long deadline = System.currentTimeMillis() + ms;
        while (processAlive(pid) && System.currentTimeMillis() < deadline) Thread.sleep(200);
        return !processAlive(pid);
    }

    static void stopWriter() throws Exception {
        String pid = writerPid();
        if (!processAlive(pid)) return;
        new ProcessBuilder("kill", pid).start().waitFor();
        check("the writer stops cleanly on SIGTERM", waitProcessGone(pid, 30_000));
    }

    static void startWriter(String test, Map<String, String> extraEnv, boolean attach) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("bash", launcher)
            .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.environment().putAll(extraEnv);
        check(test + " launcher exited cleanly", pb.start().waitFor() == 0);
        if (!attach) return;
        boolean active = waitFor(PaimonRobustTest::slotActive, 90_000);
        check(test + " writer attached to the slot", active);
    }

    static boolean slotActive() throws Exception {
        try (Connection conn = connect(); Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT bool_or(active) FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    static void log(String what, String msg) {
        System.out.println("        " + what + ": " + msg);
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
