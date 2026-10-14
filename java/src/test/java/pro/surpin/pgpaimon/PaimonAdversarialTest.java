// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
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
 * The adversarial DBA: ordinary SQL that used to stop replication for good (a poison record) or
 * lose committed changes without a word. Each case runs the hostile statement and asserts ONE
 * of two outcomes, never "it did not crash":
 *
 *   - PostgreSQL refused it, with the guardrail's own message, AND the writer is still running
 *     AND a row written afterwards reaches Paimon (so the writer read past the refusal); or
 *   - the change replicated, and what Paimon holds reads back equal to what PostgreSQL holds.
 *
 * A writer that dies fails the suite (every case checks the process, then a sentinel row). The
 * last two cases stop the writer on purpose -- paimon.health() finding a disabled event trigger,
 * at startup and while running -- and restart it through the harness launcher.
 *
 * Every hole is in .superpowers/sdd/2026-10-09-logical-slot-writer/poison-report.md with the
 * measurement that found it. Usage: test/slot-acceptance/run_acceptance.sh PaimonAdversarialTest
 */
public class PaimonAdversarialTest {

    static final String PG_SCHEMA = "public";
    static final long TIMEOUT_MS = 60_000;

    static String warehouse;
    static String jdbcUrl;
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
            assertNotNull(System.getenv("PGPAIMON_WRITER_PID_FILE"), "harness did not export PGPAIMON_WRITER_PID_FILE");
        }
        assumeTrue(jdbc != null && wh != null, "no live stack: run test/slot-acceptance/run_acceptance.sh");
        jdbcUrl = jdbc;
        warehouse = wh;
        passed = 0; failed = 0; failures.clear();

        System.out.println("╔════════════════════════════════════════════════════╗");
        System.out.println("║        paimon_slot adversarial DBA suite           ║");
        System.out.println("╚════════════════════════════════════════════════════╝");

        try (Connection c = connect()) {
            exec(c, "DROP TABLE IF EXISTS dba_sentinel");
            exec(c, "CREATE TABLE dba_sentinel (id int PRIMARY KEY, note text)");
            exec(c, "SELECT paimon.add_table('dba_sentinel'::regclass)");
        }
        writerKeepsUp("setup");

        // Each case runs even if an earlier one failed, so a run against an older build reports
        // every hole it still has, not just the first.
        run("A1", PaimonAdversarialTest::a1_numericNaN);
        run("A2", PaimonAdversarialTest::a2_timestampBeyondPaimon);
        run("A3", PaimonAdversarialTest::a3_numericTypmods);
        run("A4", PaimonAdversarialTest::a4_unreplicableColumns);
        run("A5", PaimonAdversarialTest::a5_identifiers);
        run("A6", PaimonAdversarialTest::a6_typeChanges);
        run("A7", PaimonAdversarialTest::a7_inheritanceAndPartitions);
        run("A8", PaimonAdversarialTest::a8_typesWhoseTextChanges);
        run("A9", PaimonAdversarialTest::a9_publication);
        run("A10", PaimonAdversarialTest::a10_replicaRole);
        run("A11", PaimonAdversarialTest::a11_forgedMessages);
        run("A12", PaimonAdversarialTest::a12_truncateThenInsert);
        run("A13", PaimonAdversarialTest::a13_insertThenDropTable);
        run("A14", PaimonAdversarialTest::a14_otherPublications);
        run("A17", PaimonAdversarialTest::a17_renameInsideATransaction);
        run("A16", PaimonAdversarialTest::a16_secretRotation);
        run("A15", PaimonAdversarialTest::a15_healthStopsTheWriter);

        System.out.println();
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        assertEquals(0, failed, "failed checks: " + failures);
    }

    // ── A1: numeric NaN ─────────────────────────────────────────────────────────────────────
    // Before: PostgreSQL committed it and the writer stopped on every restart (POISON RECORD).
    static void a1_numericNaN() throws Exception {
        System.out.println("\n── A1: numeric NaN ─────────────────────────────────");
        String t = "dba_nan";
        try (Connection c = connect()) {
            createTracked(c, t, "id int PRIMARY KEY, amt numeric(10,2)");
            refused("A1 INSERT 'NaN' into numeric(10,2)", c,
                "INSERT INTO " + t + " VALUES (1, 'NaN')", "paimon_guard_2");
            refused("A1 UPDATE to 'NaN'", c,
                "INSERT INTO " + t + " VALUES (2, 1); UPDATE " + t + " SET amt = 'NaN' WHERE id = 2",
                "paimon_guard_2");
            refused("A1 dropping the guard", c,
                "ALTER TABLE " + t + " DROP CONSTRAINT paimon_guard_2", "must stay while the table is replicated");
            exec(c, "INSERT INTO " + t + " VALUES (3, 12345678.91), (4, -0.01)");
        }
        writerKeepsUp("A1");
        sameRows("A1 ordinary values replicate exactly", t, "id");
    }

    // ── A2: timestamps beyond Paimon's largest writable instant ────────────────────────────
    static void a2_timestampBeyondPaimon() throws Exception {
        System.out.println("\n── A2: timestamp range ─────────────────────────────");
        String t = "dba_ts";
        try (Connection c = connect()) {
            createTracked(c, t, "id int PRIMARY KEY, ts timestamp, tz timestamptz, d date");
            refused("A2 timestamp one microsecond above 294247-01-10 04:00:54.775807", c,
                "INSERT INTO " + t + " (id, ts) VALUES (1, '294247-01-10 04:00:54.775808')", "paimon_guard_2");
            refused("A2 timestamptz at PostgreSQL's own maximum", c,
                "INSERT INTO " + t + " (id, tz) VALUES (2, '294276-12-31 23:59:59.999999+00')", "paimon_guard_3");
            exec(c, "INSERT INTO " + t + " VALUES (3, '294247-01-10 04:00:54.775807',"
                + " '294247-01-10 04:00:54.775807+00', '5874897-12-31')");
            exec(c, "INSERT INTO " + t + " VALUES (4, '2000-02-29 23:59:59.999999',"
                + " '1900-01-01 00:00:00+05:41:16', '4714-11-24 BC')");
        }
        writerKeepsUp("A2");
        Map<String, Map<String, String>> rows = paimonRows(t, "id", 2);
        Map<String, String> r3 = rows.getOrDefault("3", Map.of());
        check("A2 the largest writable timestamp arrives exactly",
              "+294247-01-10T04:00:54.775807".equals(r3.get("ts")) && "+294247-01-10T04:00:54.775807".equals(r3.get("tz")));
        check("A2 PostgreSQL's maximum date arrives exactly", "+5874897-12-31".equals(r3.get("d")));
        Map<String, String> r4 = rows.getOrDefault("4", Map.of());
        check("A2 six-digit fraction, a seconds offset and a BC date arrive exactly",
              "2000-02-29T23:59:59.999999".equals(r4.get("ts"))
              && "1899-12-31T18:18:44".equals(r4.get("tz"))
              && "-4713-11-24".equals(r4.get("d")));
    }

    // ── A3: numeric(p,s) with a negative scale, or a scale above the precision ────────────
    // Before: add_table accepted them as DECIMAL(5, 2046) / DECIMAL(3, 5); the writer could
    // never create the Paimon table.
    static void a3_numericTypmods() throws Exception {
        System.out.println("\n── A3: numeric typmods ─────────────────────────────");
        try (Connection c = connect()) {
            exec(c, "DROP TABLE IF EXISTS dba_neg, dba_over");
            exec(c, "CREATE TABLE dba_neg (id int PRIMARY KEY, a numeric(5,-2))");
            exec(c, "CREATE TABLE dba_over (id int PRIMARY KEY, a numeric(3,5))");
            refused("A3 numeric(5,-2)", c, "SELECT paimon.add_table('dba_neg'::regclass)", "negative scale");
            refused("A3 numeric(3,5)", c, "SELECT paimon.add_table('dba_over'::regclass)", "scale larger than the precision");
            createTracked(c, "dba_scale", "id int PRIMARY KEY, a numeric(3,3)");
            exec(c, "INSERT INTO dba_scale VALUES (1, 0.123), (2, -0.001)");
        }
        writerKeepsUp("A3");
        sameRows("A3 numeric(3,3) replicates exactly", "dba_scale", "id");
    }

    // ── A4: columns the stream cannot carry ────────────────────────────────────────────────
    // Before: a generated column made the first start fail ("column ... is a generated
    // column"); a regclass column silently kept old names after a RENAME.
    static void a4_unreplicableColumns() throws Exception {
        System.out.println("\n── A4: generated and reg* columns ──────────────────");
        try (Connection c = connect()) {
            exec(c, "DROP TABLE IF EXISTS dba_gen, dba_reg");
            exec(c, "CREATE TABLE dba_gen (id int PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a * 2) STORED)");
            exec(c, "CREATE TABLE dba_reg (id int PRIMARY KEY, r regclass)");
            refused("A4 a generated column", c, "SELECT paimon.add_table('dba_gen'::regclass)", "generated column");
            refused("A4 a regclass column", c, "SELECT paimon.add_table('dba_reg'::regclass)", "catalog lookup");
            refused("A4 ADD COLUMN regclass on a replicated table", c,
                "ALTER TABLE dba_sentinel ADD COLUMN r regclass", "catalog lookup");
        }
        writerKeepsUp("A4");
    }

    // ── A5: names Paimon or the warehouse cannot take ──────────────────────────────────────
    // Before: '$' made a "system table" the writer could never create; '/' nested one table
    // inside another's directory, and dropping the outer one deleted the inner one's data.
    static void a5_identifiers() throws Exception {
        System.out.println("\n── A5: identifiers ─────────────────────────────────");
        try (Connection c = connect()) {
            exec(c, "DROP TABLE IF EXISTS \"dba$x\", \"dba/x\", \"dba.dot\"");
            exec(c, "CREATE TABLE \"dba$x\" (id int PRIMARY KEY)");
            exec(c, "CREATE TABLE \"dba/x\" (id int PRIMARY KEY)");
            refused("A5 '$' in a table name", c, "SELECT paimon.add_table('\"dba$x\"'::regclass)", "cannot be a Paimon table name");
            refused("A5 '/' in a table name", c, "SELECT paimon.add_table('\"dba/x\"'::regclass)", "cannot be a Paimon table name");
            refused("A5 a column named _seq", c, "ALTER TABLE dba_sentinel ADD COLUMN _seq int", "metadata field");
            refused("A5 a column starting _KEY_", c, "ALTER TABLE dba_sentinel ADD COLUMN \"_KEY_x\" int", "_KEY_");
            createTracked(c, "\"dba.dot\"", "id int PRIMARY KEY, v text");
            exec(c, "INSERT INTO \"dba.dot\" VALUES (1, 'dotted')");
        }
        writerKeepsUp("A5");
        Map<String, Map<String, String>> rows = paimonRows("dba.dot", "id", 1);
        check("A5 a dotted table name replicates", "dotted".equals(rows.getOrDefault("1", Map.of()).get("v")));
    }

    // ── A6: ALTER COLUMN TYPE ──────────────────────────────────────────────────────────────
    // Before: Paimon refused about a third of the pairs after PostgreSQL committed (a wedge),
    // and accepted others that rewrite history wrongly (INT -> SMALLINT read back -1).
    static void a6_typeChanges() throws Exception {
        System.out.println("\n── A6: ALTER COLUMN TYPE ───────────────────────────");
        String t = "dba_type";
        try (Connection c = connect()) {
            createTracked(c, t, "id int PRIMARY KEY, i int, n numeric(10,2), s text, k int");
            exec(c, "INSERT INTO " + t + " VALUES (1, 7, 12345678.91, 'abc', 5)");   // 7 fits smallint: only the guardrail can refuse
            refused("A6 int -> smallint", c, "ALTER TABLE " + t + " ALTER COLUMN i TYPE smallint", "narrows an integer");
            refused("A6 numeric(10,2) -> numeric(10,1)", c, "ALTER TABLE " + t + " ALTER COLUMN n TYPE numeric(10,1)", "removes integer digits");
            refused("A6 text -> char(10)", c, "ALTER TABLE " + t + " ALTER COLUMN s TYPE char(10)", "cannot change the type");
            refused("A6 a USING expression on the same type", c,
                "ALTER TABLE " + t + " ALTER COLUMN k TYPE int USING k * 100", "USING is not supported");
            exec(c, "ALTER TABLE " + t + " ALTER COLUMN i TYPE bigint");
            exec(c, "ALTER TABLE " + t + " ALTER COLUMN n TYPE numeric(14,4)");
            exec(c, "INSERT INTO " + t + " VALUES (2, 9223372036854775807, 1234567890.1234, 'def', 6)");
        }
        writerKeepsUp("A6");
        sameRows("A6 widened columns: old and new rows read back equal", t, "id");
    }

    // ── A7: inheritance and partitions ─────────────────────────────────────────────────────
    // Before: ALTER TABLE parent ADD COLUMN reached a replicated partition, but the writer
    // discarded that column's value from every row; a replicated table's inheritance children
    // were simply invisible.
    static void a7_inheritanceAndPartitions() throws Exception {
        System.out.println("\n── A7: inheritance and partitions ──────────────────");
        try (Connection c = connect()) {
            exec(c, "DROP TABLE IF EXISTS dba_parted, dba_par CASCADE");
            exec(c, "CREATE TABLE dba_parted (id int, k int, PRIMARY KEY (id, k)) PARTITION BY LIST (k)");
            exec(c, "CREATE TABLE dba_part1 PARTITION OF dba_parted FOR VALUES IN (1)");
            exec(c, "SELECT paimon.add_table('dba_part1'::regclass)");
            exec(c, "INSERT INTO dba_parted VALUES (1, 1)");
            exec(c, "ALTER TABLE dba_parted ADD COLUMN extra text");
            exec(c, "INSERT INTO dba_parted VALUES (2, 1, 'reaches Paimon')");
            createTracked(c, "dba_par", "id int PRIMARY KEY, v text");
            refused("A7 an inheritance child of a replicated table", c,
                "CREATE TABLE dba_child () INHERITS (dba_par)", "inheritance child");
        }
        writerKeepsUp("A7");
        sameRows("A7 a column added through the partitioned parent replicates", "dba_part1", "id");
    }

    // ── A8: types whose value text changes with no row change ──────────────────────────────
    static void a8_typesWhoseTextChanges() throws Exception {
        System.out.println("\n── A8: enum and composite types ────────────────────");
        try (Connection c = connect()) {
            exec(c, "DROP TABLE IF EXISTS dba_enum");
            exec(c, "DROP TYPE IF EXISTS dba_mood, dba_pair");
            exec(c, "CREATE TYPE dba_mood AS ENUM ('sad', 'ok')");
            exec(c, "CREATE TYPE dba_pair AS (a int, b text)");
            createTracked(c, "dba_enum", "id int PRIMARY KEY, m dba_mood, p dba_pair");
            exec(c, "INSERT INTO dba_enum VALUES (1, 'sad', ROW(1, 'x'))");
            refused("A8 ALTER TYPE ... RENAME VALUE", c, "ALTER TYPE dba_mood RENAME VALUE 'sad' TO 'blue'", "cannot rename a value");
            refused("A8 ALTER TYPE ... ADD ATTRIBUTE", c, "ALTER TYPE dba_pair ADD ATTRIBUTE z int", "composite type");
            exec(c, "ALTER TYPE dba_mood ADD VALUE 'happy'");
            exec(c, "INSERT INTO dba_enum VALUES (2, 'happy', ROW(2, 'y z'))");
        }
        writerKeepsUp("A8");
        sameRows("A8 enum and composite values read back as PostgreSQL prints them", "dba_enum", "id");
    }

    // ── A9: the publication ────────────────────────────────────────────────────────────────
    // Before: each of these silently stopped changes reaching Paimon (measured: an UPDATE lost
    // under publish = 'insert', a column NULL under a column list, rows absent under a filter).
    static void a9_publication() throws Exception {
        System.out.println("\n── A9: publication tampering ───────────────────────");
        String t = "dba_pub";
        try (Connection c = connect()) {
            createTracked(c, t, "id int PRIMARY KEY, a int, b text");
            exec(c, "INSERT INTO " + t + " VALUES (1, 1, 'one')");
            refused("A9 DROP TABLE from paimon_pub", c, "ALTER PUBLICATION paimon_pub DROP TABLE " + t, "not in publication paimon_pub");
            refused("A9 publish = 'insert'", c, "ALTER PUBLICATION paimon_pub SET (publish = 'insert')", "does not publish every action");
            refused("A9 publish_via_partition_root", c, "ALTER PUBLICATION paimon_pub SET (publish_via_partition_root = true)", "publish_via_partition_root");
            refused("A9 RENAME", c, "ALTER PUBLICATION paimon_pub RENAME TO elsewhere", "does not exist");
            refused("A9 DROP PUBLICATION", c, "DROP PUBLICATION paimon_pub", "does not exist");
            exec(c, "UPDATE " + t + " SET a = 100, b = 'updated' WHERE id = 1");
            exec(c, "INSERT INTO " + t + " VALUES (2, 2, 'two')");
            exec(c, "DELETE FROM " + t + " WHERE id = 2");
        }
        writerKeepsUp("A9");
        sameRows("A9 update and delete still replicate", t, "id");
    }

    // ── A10: session_replication_role = replica ────────────────────────────────────────────
    // Before: the event triggers did not fire, so every guardrail was off for that session.
    static void a10_replicaRole() throws Exception {
        System.out.println("\n── A10: session_replication_role = replica ─────────");
        try (Connection c = connect()) {
            exec(c, "SET session_replication_role = replica");
            refused("A10 ADD COLUMN ... DEFAULT under the replica role", c,
                "ALTER TABLE dba_sentinel ADD COLUMN d int DEFAULT 5", "DEFAULT is not supported");
            refused("A10 REPLICA IDENTITY DEFAULT under the replica role", c,
                "ALTER TABLE dba_sentinel REPLICA IDENTITY DEFAULT", "must stay FULL");
            exec(c, "RESET session_replication_role");
        }
        writerKeepsUp("A10");
    }

    // ── A11: logical messages forged under the 'paimon' prefix ─────────────────────────────
    // Before the credential: one SELECT by a role with no privileges stopped the writer on every
    // restart, and a forged drop_table would have purged a table. Now: text that is no DDL
    // message is ignored; a DDL message without the credential STOPS the writer (review finding
    // B2: a legitimate one emitted under another secret looks the same, and ignoring that lost a
    // column for good), applies nothing, and PGPAIMON_SKIP_LSN at its LSN steps over it.
    static void a11_forgedMessages() throws Exception {
        System.out.println("\n── A11: forged DDL messages ────────────────────────");
        String launcher = System.getenv("PGPAIMON_WRITER_LAUNCHER");
        String pidFile = System.getenv("PGPAIMON_WRITER_PID_FILE");
        String t = "dba_victim";
        int before = writerLog().length();
        try (Connection c = connect()) {
            createTracked(c, t, "id int PRIMARY KEY, v text");
            exec(c, "INSERT INTO " + t + " VALUES (1, 'kept')");
            exec(c, "DROP ROLE IF EXISTS dba_nobody");
            exec(c, "CREATE ROLE dba_nobody");
            exec(c, "SET ROLE dba_nobody");
            exec(c, "SELECT pg_logical_emit_message(true, 'paimon', 'not json')");
            refused("A11 an unprivileged role cannot call paimon.emit", c,
                "SELECT paimon.emit('{}'::jsonb)", "permission denied");
            exec(c, "RESET ROLE");
        }
        writerKeepsUp("A11 after text that is no DDL message");
        check("A11 the writer logged the text as ignored",
              writerLog().substring(before).contains("IGNORED a logical message with prefix 'paimon'"));
        String pid = writerPid(pidFile);
        try (Connection c = connect()) {
            exec(c, "SET ROLE dba_nobody");
            exec(c, "SELECT pg_logical_emit_message(true, 'paimon', '{\"v\":1,\"op\":\"drop_table\",\"relid\":\""
                + oid(c, t) + "\",\"schema\":\"public\",\"table\":\"" + t + "\"}')");
            exec(c, "RESET ROLE");
            exec(c, "INSERT INTO " + t + " VALUES (2, 'after')");
        }
        check("A11 a DDL message without the credential stops the writer", waitProcessGone(pid, 30_000));
        String log = writerLog().substring(before);
        check("A11 and says why, with the LSN to skip",
              log.contains("REJECTED DDL MESSAGE") && log.contains("carries no credential")
              && log.contains("PGPAIMON_SKIP_LSN="));
        java.util.regex.Matcher m = java.util.regex.Pattern
            .compile("set PGPAIMON_SKIP_LSN=([0-9A-F]+/[0-9A-F]+)").matcher(log);
        boolean found = m.find();
        check("A11 the error names the LSN", found);
        if (!found) return;
        startWriter("A11 with PGPAIMON_SKIP_LSN", launcher, Map.of("PGPAIMON_SKIP_LSN", m.group(1)), true);
        writerKeepsUp("A11 after the skip");
        sameRows("A11 the forged drop_table purged nothing, and the row after it replicated", t, "id");
        check("A11 the skip is logged", writerLog().substring(before).contains("SKIPPED DDL MESSAGE"));
        // restart without the variable, so no later case runs with it armed
        stopWriter(pidFile);
        startWriter("A11 restart", launcher, Map.of(), true);
        writerKeepsUp("A11 after the restart");
    }

    // ── A16: paimon.secret rotated ─────────────────────────────────────────────────────────
    // Review finding B2: a DBA rotating the secret (the natural answer to "a role that can read
    // it can forge") made the writer IGNORE the next ADD COLUMN, and the column's values were
    // discarded for good. Now: a rotation while the writer runs is followed (the writer re-reads
    // the secret when a message carries another value); a DDL message emitted under the OLD
    // value and still in the slot stops the writer, and putting the old value back recovers.
    static void a16_secretRotation() throws Exception {
        System.out.println("\n── A16: paimon.secret rotated ──────────────────────");
        String launcher = System.getenv("PGPAIMON_WRITER_LAUNCHER");
        String pidFile = System.getenv("PGPAIMON_WRITER_PID_FILE");
        String t = "dba_rot";
        String original;
        try (Connection c = connect()) {
            createTracked(c, t, "id int PRIMARY KEY, v text");
            exec(c, "INSERT INTO " + t + " VALUES (1, 'x')");
            original = scalar(c, "SELECT token FROM paimon.secret");
            exec(c, "UPDATE paimon.secret SET token = md5(random()::text) || md5(random()::text)");
            exec(c, "ALTER TABLE " + t + " ADD COLUMN c int");
            exec(c, "INSERT INTO " + t + " VALUES (2, 'y', 42)");
        }
        writerKeepsUp("A16 after a rotation while running");
        sameRows("A16 the column added after the rotation replicates with its values", t, "id");

        // An ADD COLUMN emitted under the current secret, then the secret changes while the
        // message is still unconsumed (the writer is stopped).
        stopWriter(pidFile);
        int before = writerLog().length();
        String underOld;
        try (Connection c = connect()) {
            underOld = scalar(c, "SELECT token FROM paimon.secret");
            exec(c, "ALTER TABLE " + t + " ADD COLUMN d int");
            exec(c, "INSERT INTO " + t + " VALUES (3, 'z', 43, 44)");
            exec(c, "UPDATE paimon.secret SET token = md5(random()::text) || md5(random()::text)");
        }
        startWriter("A16 after a rotation with DDL in the slot", launcher, Map.of(), false);
        check("A16 a DDL message under the old secret stops the writer",
              waitProcessGone(writerPid(pidFile), 60_000));
        String log = writerLog().substring(before);
        check("A16 and says to check for a rotation",
              log.contains("REJECTED DDL MESSAGE") && log.contains("was paimon.secret changed"));
        try (Connection c = connect()) {
            exec(c, "UPDATE paimon.secret SET token = '" + underOld + "'");
        }
        startWriter("A16 with the old secret back", launcher, Map.of(), true);
        writerKeepsUp("A16 after the recovery");
        sameRows("A16 the column added before the rotation replicates with its values", t, "id");
        try (Connection c = connect()) {
            exec(c, "UPDATE paimon.secret SET token = '" + original + "'");   // later cases: as set up
        }
        stopWriter(pidFile);
        startWriter("A16 restart", launcher, Map.of(), true);
    }

    static String scalar(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    // ── A12: TRUNCATE and INSERT in one transaction ────────────────────────────────────────
    // Before: "mixes TRUNCATE and row changes", fatal, replayed on every restart.
    static void a12_truncateThenInsert() throws Exception {
        System.out.println("\n── A12: TRUNCATE then INSERT, one transaction ──────");
        String t = "dba_reload";
        try (Connection c = connect()) {
            createTracked(c, t, "id int PRIMARY KEY, v text");
            exec(c, "INSERT INTO " + t + " VALUES (1, 'old'), (2, 'old')");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO " + t + " VALUES (3, 'wiped by the truncate')");
            exec(c, "TRUNCATE " + t);
            exec(c, "INSERT INTO " + t + " VALUES (1, 'reloaded'), (4, 'new')");
            exec(c, "UPDATE " + t + " SET v = 'reloaded+' WHERE id = 1");
            c.commit();
            c.setAutoCommit(true);
        }
        writerKeepsUp("A12");
        sameRows("A12 the table equals PostgreSQL's after a reload transaction", t, "id");
    }

    // ── A13: INSERT then DROP TABLE in one transaction ─────────────────────────────────────
    // Before: "relation ... is not tracked", fatal (it cleared on restart, but stopped the run).
    static void a13_insertThenDropTable() throws Exception {
        System.out.println("\n── A13: INSERT then DROP TABLE, one transaction ────");
        try (Connection c = connect()) {
            createTracked(c, "dba_doomed", "id int PRIMARY KEY");
            createTracked(c, "dba_survivor", "id int PRIMARY KEY");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO dba_doomed VALUES (1)");
            exec(c, "INSERT INTO dba_survivor VALUES (1)");
            exec(c, "DROP TABLE dba_doomed");
            c.commit();
            c.setAutoCommit(true);
        }
        writerKeepsUp("A13");
        sameRows("A13 the rest of the transaction is committed", "dba_survivor", "id");
    }

    // ── A17: a row written, then its column renamed or dropped, in one transaction ─────────
    // Review finding I4. Records are built at the commit against the schema current then, by
    // name: the row inserted before RENAME COLUMN v TO w reached Paimon with w NULL, and with a
    // drop and a re-add of the same name the old value met the new column's type.
    static void a17_renameInsideATransaction() throws Exception {
        System.out.println("\n── A17: INSERT, RENAME/DROP COLUMN, INSERT ─────────");
        String t = "dba_ren";
        try (Connection c = connect()) {
            createTracked(c, t, "id int PRIMARY KEY, v text, x text");
            c.setAutoCommit(false);
            exec(c, "INSERT INTO " + t + " VALUES (20, 'before rename', 'old x')");
            exec(c, "ALTER TABLE " + t + " RENAME COLUMN v TO w");
            exec(c, "ALTER TABLE " + t + " DROP COLUMN x");
            exec(c, "ALTER TABLE " + t + " ADD COLUMN x int");
            exec(c, "INSERT INTO " + t + " VALUES (21, 'after rename', 5)");
            c.commit();
            // the reload shape the review measured: TRUNCATE; INSERT; ALTER; INSERT
            exec(c, "TRUNCATE " + t);
            exec(c, "INSERT INTO " + t + " VALUES (1, 'new', 10)");
            exec(c, "ALTER TABLE " + t + " RENAME COLUMN w TO v2");
            exec(c, "INSERT INTO " + t + " VALUES (2, 'newer', 11)");
            c.commit();
            c.setAutoCommit(true);
        }
        writerKeepsUp("A17");
        sameRows("A17 every value written before the rename or drop is where PostgreSQL has it", t, "id");
    }

    // ── A14: another publication on a replicated table ─────────────────────────────────────
    // Refuted hole: the writer subscribes paimon_pub only, so another publication's row filter
    // or column list cannot affect it. Kept as a guard.
    static void a14_otherPublications() throws Exception {
        System.out.println("\n── A14: other publications ─────────────────────────");
        String t = "dba_shared";
        try (Connection c = connect()) {
            createTracked(c, t, "id int PRIMARY KEY, v text");
            exec(c, "DROP PUBLICATION IF EXISTS dba_other");
            exec(c, "CREATE PUBLICATION dba_other FOR TABLE " + t + " (id) WHERE (id > 100)");
            exec(c, "INSERT INTO " + t + " VALUES (1, 'not filtered for us')");
            exec(c, "DROP PUBLICATION dba_other");
        }
        writerKeepsUp("A14");
        sameRows("A14 another publication's filter and column list do not touch paimon_pub", t, "id");
    }

    // ── A15: paimon.health() stops the writer ──────────────────────────────────────────────
    // What no guardrail can refuse is its own removal. The writer refuses to start on it and
    // stops when it appears while running; both are loud, and the restart afterwards is clean.
    static void a15_healthStopsTheWriter() throws Exception {
        System.out.println("\n── A15: paimon.health() ────────────────────────────");
        String launcher = System.getenv("PGPAIMON_WRITER_LAUNCHER");
        String pidFile = System.getenv("PGPAIMON_WRITER_PID_FILE");
        if (launcher == null || pidFile == null) {
            check("A15 harness exports PGPAIMON_WRITER_LAUNCHER and PGPAIMON_WRITER_PID_FILE", false);
            return;
        }
        // restart with a short interval so the running check fires within the test
        stopWriter(pidFile);
        startWriter("A15", launcher, Map.of("PGPAIMON_HEALTH_INTERVAL_MS", "1000"), true);
        String pid = writerPid(pidFile);
        try (Connection c = connect()) {
            exec(c, "ALTER EVENT TRIGGER paimon_ddl_end ENABLE");     // ORIGIN: off under replica role
        }
        check("A15 the running writer stops on the health problem", waitProcessGone(pid, 30_000));
        check("A15 and says which", writerLog().contains("paimon.health() reports 1 problem(s) while running")
              && writerLog().contains("paimon_ddl_end is not ENABLE ALWAYS"));
        int before = writerLog().length();
        startWriter("A15", launcher, Map.of(), false);
        check("A15 a writer started with the problem refuses to start", waitProcessGone(writerPid(pidFile), 30_000));
        check("A15 and says why", writerLog().substring(before).contains("problem(s) at startup"));
        try (Connection c = connect()) {
            exec(c, "ALTER EVENT TRIGGER paimon_ddl_end ENABLE ALWAYS");
        }
        startWriter("A15", launcher, Map.of(), true);
        writerKeepsUp("A15 after the fix");
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────────────────

    interface Case {
        void run() throws Exception;
    }

    /** An exception inside a case is a failed check, never a skipped remainder. */
    static void run(String name, Case c) {
        try {
            c.run();
        } catch (Exception e) {
            check(name + " ran to completion (" + e + ")", false);
        }
    }

    /** Whether the writer process is up; when it is not, nothing is waited for. */
    static boolean writerAlive() throws Exception {
        String pidFile = System.getenv("PGPAIMON_WRITER_PID_FILE");
        return pidFile == null || processAlive(writerPid(pidFile));
    }

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    static long oid(Connection c, String tbl) throws SQLException {
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT '" + tbl + "'::regclass::oid::bigint")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** CREATE TABLE (dropping any earlier one) and paimon.add_table, while it is empty. */
    static void createTracked(Connection c, String tbl, String columns) throws Exception {
        exec(c, "DROP TABLE IF EXISTS " + tbl);
        exec(c, "CREATE TABLE " + tbl + " (" + columns + ")");
        exec(c, "SELECT paimon.add_table('" + tbl.replace("'", "''") + "'::regclass)");
    }

    /** The statement must fail with {@code expected} in PostgreSQL's message, and change nothing. */
    static void refused(String name, Connection c, String sql, String expected) {
        boolean auto = true;
        try {
            auto = c.getAutoCommit();
            exec(c, sql);
            check(name + " is refused by PostgreSQL", false);
            System.out.println("        (it was accepted)");
        } catch (SQLException e) {
            boolean ok = e.getMessage() != null && e.getMessage().contains(expected);
            check(name + " is refused by PostgreSQL", ok);
            if (!ok) System.out.println("        got: " + e.getMessage());
        } finally {
            try { if (!auto) c.rollback(); } catch (SQLException ignored) { }
        }
    }

    /**
     * The writer is still running AND has consumed everything so far: a fresh sentinel row
     * written now reaches Paimon. A dead or stuck writer fails here.
     */
    static void writerKeepsUp(String test) throws Exception {
        String pidFile = System.getenv("PGPAIMON_WRITER_PID_FILE");
        if (pidFile != null) {
            check(test + ": the writer process is alive", processAlive(writerPid(pidFile)));
        }
        int id = ++sentinelId;
        try (Connection c = connect()) {
            exec(c, "INSERT INTO dba_sentinel VALUES (" + id + ", '" + test.replace("'", "") + "')");
        }
        long deadline = System.currentTimeMillis() + (writerAlive() ? TIMEOUT_MS : 0);
        boolean seen = false;
        do {
            seen = paimonRowsNow("dba_sentinel", "id").containsKey(String.valueOf(id));
            if (!seen) Thread.sleep(250);
        } while (!seen && System.currentTimeMillis() < deadline);
        check(test + ": a row written afterwards reaches Paimon", seen);
    }

    /**
     * Every row of the PostgreSQL table and of the merged Paimon table, compared by key, every
     * column as text (Paimon's metadata columns left out). Waits for Paimon to catch up first.
     */
    static void sameRows(String name, String tbl, String key) throws Exception {
        Map<String, Map<String, String>> pg = pgRows(tbl, key);
        Map<String, Map<String, String>> pm;
        long deadline = System.currentTimeMillis() + (writerAlive() ? TIMEOUT_MS : 0);
        do {
            pm = paimonRowsNow(tbl, key);
            if (normalise(pm, pg).equals(pg)) break;
            Thread.sleep(250);
        } while (System.currentTimeMillis() < deadline);
        boolean ok = normalise(pm, pg).equals(pg);
        check(name, ok);
        if (!ok) {
            System.out.println("        PostgreSQL: " + pg);
            System.out.println("        Paimon:     " + pm);
        }
    }

    /** Paimon's rows restricted to PostgreSQL's columns. */
    static Map<String, Map<String, String>> normalise(Map<String, Map<String, String>> pm,
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

    /** PostgreSQL's rows, every value in the text form the walsender sends (the session is UTC). */
    static Map<String, Map<String, String>> pgRows(String tbl, String key) throws SQLException {
        Map<String, Map<String, String>> out = new TreeMap<>();
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("SET TimeZone = 'UTC'");
            try (ResultSet rs = s.executeQuery("SELECT * FROM " + quoteIdent(tbl))) {
                ResultSetMetaData md = rs.getMetaData();
                while (rs.next()) {
                    Map<String, String> row = new TreeMap<>();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        row.put(md.getColumnName(i), canonical(md.getColumnTypeName(i), rs.getString(i)));
                    }
                    out.put(row.get(key), row);
                }
            }
        }
        return out;
    }

    /** PostgreSQL's numeric text and Paimon's decimal text agree; timestamps compare as Paimon prints. */
    static String canonical(String pgType, String v) {
        if (v == null) return null;
        if (pgType.equals("numeric") && !v.equals("NaN")) return new java.math.BigDecimal(v).toPlainString();
        return v;
    }

    static Map<String, Map<String, String>> paimonRows(String tbl, String key, int min) throws Exception {
        long deadline = System.currentTimeMillis() + (writerAlive() ? TIMEOUT_MS : 0);
        Map<String, Map<String, String>> rows = paimonRowsNow(tbl, key);
        while (rows.size() < min && System.currentTimeMillis() < deadline) {
            Thread.sleep(250);
            rows = paimonRowsNow(tbl, key);
        }
        return rows;
    }

    /** The merged Paimon table, values as text: decimals plain, dates and timestamps ISO. */
    static Map<String, Map<String, String>> paimonRowsNow(String tbl, String key) {
        Map<String, Map<String, String>> out = new TreeMap<>();
        Options o = new Options();
        o.set("warehouse", warehouse);
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
            Table t = catalog.getTable(Identifier.create(PG_SCHEMA, tbl));
            RowType rt = t.rowType();
            ReadBuilder rb = t.newReadBuilder();
            try (RecordReader<InternalRow> r = rb.newRead().createReader(rb.newScan().plan())) {
                r.forEachRemaining(row -> {
                    Map<String, String> m = new TreeMap<>();
                    for (int i = 0; i < rt.getFieldCount(); i++) {
                        DataField f = rt.getFields().get(i);
                        Object v = row.isNullAt(i) ? null : InternalRowUtils.get(row, i, f.type());
                        m.put(f.name(), render(f, v));
                    }
                    out.put(m.get(key), m);
                });
            }
        } catch (Exception notYet) {
            // table or first snapshot not there yet: an empty answer, the caller polls
        }
        return out;
    }

    static String render(DataField f, Object v) {
        if (v == null) return null;
        switch (f.type().getTypeRoot()) {
            case DECIMAL: return ((org.apache.paimon.data.Decimal) v).toBigDecimal().toPlainString();
            case DATE: return java.time.LocalDate.ofEpochDay((Integer) v).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((org.apache.paimon.data.Timestamp) v).toLocalDateTime().toString();
            default: return v.toString();
        }
    }

    static String quoteIdent(String s) {
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }

    static String writerLog() {
        try {
            File log = new File(new File(System.getenv("PGPAIMON_WRITER_PID_FILE")).getParentFile(), "writer.log");
            return new String(Files.readAllBytes(log.toPath()));
        } catch (Exception e) {
            return "";
        }
    }

    static String writerPid(String pidFile) throws Exception {
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

    static void stopWriter(String pidFile) throws Exception {
        String pid = writerPid(pidFile);
        new ProcessBuilder("kill", pid).start().waitFor();
        check("the writer stops cleanly on SIGTERM", waitProcessGone(pid, 30_000));
    }

    /** Starts the writer through the harness launcher; waits for the slot when {@code attach}. */
    static void startWriter(String test, String launcher, Map<String, String> extraEnv, boolean attach)
            throws Exception {
        ProcessBuilder pb = new ProcessBuilder("bash", launcher)
            .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.environment().putAll(extraEnv);
        check(test + " launcher exited cleanly", pb.start().waitFor() == 0);
        if (!attach) return;
        long deadline = System.currentTimeMillis() + 60_000;
        while (!slotActive() && System.currentTimeMillis() < deadline) Thread.sleep(250);
        check(test + " writer attached to the slot", slotActive());
    }

    static boolean slotActive() throws Exception {
        try (Connection conn = connect(); Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT bool_or(active) FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
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
