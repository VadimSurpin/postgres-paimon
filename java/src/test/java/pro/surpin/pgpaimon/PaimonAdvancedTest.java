// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.*;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.junit.jupiter.api.Test;



/**
 * Advanced integration tests for the slot-based writer.
 *
 * <p>
 * T9 – CHECKPOINT: rows survive a PostgreSQL checkpoint T10 – VACUUM: emits no logical changes; the
 * record count is unchanged across it T11 – VACUUM FREEZE: same T12 – ALTER TABLE ADD COLUMN: rows
 * captured; new column present in schema T13 – ALTER TABLE DROP COLUMN: rows still captured after
 * column drop T14 – ALTER TABLE RENAME COLUMN: data captured correctly; old name leaves the schema
 * T15 – ALTER COLUMN TYPE: no crash; rows captured after type change T16 – Crash recovery: the
 * WRITER is killed (SIGKILL) mid-load and restarted; zero loss and zero duplicates T17 – A table
 * never passed to paimon.add_table() reaches neither paimon_pub nor the warehouse
 *
 * <p>
 * Usage (live stack): test/slot-acceptance/run_acceptance.sh PaimonAdvancedTest
 */
public class PaimonAdvancedTest {

  /**
   * The PostgreSQL schema these suites create their tables in, and therefore the Paimon database
   * they land in: the writer maps one to the other, so the warehouse layout is
   * <warehouse>/<schema>.db/
   * <table>
   * .
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

  public static void main(
      String[] args
  ) throws Exception {
    System.exit(run(args) > 0 ? 1 : 0);
  }

  static int run(
      String[] args
  ) throws Exception {
    passed = 0;
    failed = 0;
    failures.clear();
    warehouse = args.length > 0 ? args[0] : DEFAULT_WAREHOUSE;
    jdbcUrl = args.length > 1 ? args[1] : DEFAULT_JDBC;

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
    assumeTrue(jdbc != null && wh != null,
        "no live stack: run test/slot-acceptance/run_acceptance.sh");
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
      exec(conn, "CHECKPOINT"); // checkpoint again after second batch
    }
    PaimonRows rows = waitAndRead(tbl, 5);
    check("T9 checkpoint: all 5 rows captured", rows.total == 5);
    check("T9 checkpoint: all are inserts", rows.ofKind(0) == 5);
  }

  // ── T10: VACUUM ──────────────────────────────────────────────────────────
  // Documented difference: VACUUM emits no logical changes, so nothing may appear
  // in the Paimon changelog. The record count (and every kind count) is identical
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
    check("T10 vacuum: no error", vacuumOk);

    Thread.sleep(3000); // several flush intervals: anything VACUUM emitted would be committed by
                        // now
    PaimonRows rows = readRows(tbl);
    check("T10 vacuum: no record appeared (count unchanged)", rows.total == before.total);
    check("T10 vacuum: 5 inserts, unchanged", rows.ofKind(0) == 5);
    check("T10 vacuum: 2 deletes, unchanged", rows.ofKind(3) == 2);

    // New inserts after vacuum must still be captured
    try (Connection conn = connect()) {
      exec(conn, "INSERT INTO " + tbl + " VALUES (6,6),(7,7)");
    }
    PaimonRows rows2 = waitAndRead(tbl, 9);
    check("T10 vacuum: inserts after vacuum captured", rows2.total == 9);
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
    check("T11 vacuum freeze: 3 records before", before.total == 3);

    boolean freezeOk = true;
    try (Connection conn = connect()) {
      exec(conn, "VACUUM FREEZE " + tbl);
    } catch (Exception e) {
      freezeOk = false;
      System.err.println("  VACUUM FREEZE error: " + e.getMessage());
    }
    check("T11 vacuum freeze: no error", freezeOk);

    Thread.sleep(3000);
    PaimonRows rows = readRows(tbl);
    check("T11 vacuum freeze: no record appeared (count unchanged)", rows.total == before.total);
    check("T11 vacuum freeze: all still inserts", rows.ofKind(0) == 3);

    // Inserts after freeze must still work
    try (Connection conn = connect()) {
      exec(conn, "INSERT INTO " + tbl + " VALUES (4,'d'),(5,'e')");
    }
    PaimonRows rows2 = waitAndRead(tbl, 5);
    check("T11 vacuum freeze: inserts after freeze", rows2.total == 5);
  }

  // ── T12: ALTER TABLE ADD COLUMN ──────────────────────────────────────────
  // Documented difference: main accepted ADD COLUMN ... DEFAULT but left historical Paimon rows
  // NULL; the extension now rejects the DEFAULT form (spec section 4; pg_regress guardrail tests
  // cover the rejection). This test uses the supported form, a column without a default.
  // paimon_ddl.cpp ProcessUtility_hook intercepts ADD COLUMN post-execute,
  // reads the new column's attnum/type from pg_attribute, and sends
  // PMSG_DDL_ADD_COL to the ring. The bgworker adds the field to the Paimon
  // schema (schema-1). DML with the new column must be captured correctly.
  static void test12_alterAddColumn() throws Exception {
    System.out.println("\n── T12: ALTER TABLE ADD COLUMN ──────────────────────");
    String tbl = "adv_alter_add";
    cleanup(tbl);
    try (Connection conn = connect()) {
      createTracked(conn, tbl, "id INT PRIMARY KEY, v INT");
      exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20)");
    }
    check("T12 add col: 2 rows captured before ALTER", waitAndRead(tbl, 2).total == 2);

    boolean alterOk = true;
    try (Connection conn = connect()) {
      exec(conn, "ALTER TABLE " + tbl + " ADD COLUMN extra TEXT");
      exec(conn, "INSERT INTO " + tbl + " VALUES (3,30,'hello')");
      exec(conn, "INSERT INTO " + tbl + " VALUES (4,40,'world')");
    } catch (Exception e) {
      alterOk = false;
      System.err.println("  ALTER/INSERT error: " + e.getMessage());
    }
    check("T12 add col: ALTER + INSERT no crash", alterOk);

    if (alterOk) {
      PaimonRows rows = waitAndRead(tbl, 4);
      check("T12 add col: 4 rows captured after ALTER", rows.total == 4);

      // DDL hook sends PMSG_DDL_ADD_COL; bgworker adds column to schema
      File schema1 = new File(tableDir(tbl) + "/schema/schema-1");
      long deadline = System.currentTimeMillis() + 30_000;
      while (!schema1.exists() && System.currentTimeMillis() < deadline)
        Thread.sleep(500);

      TableSchema schema = readSchema(tbl);
      boolean extraInSchema = schema.fields().stream().anyMatch(f -> f.name().equals("extra"));
      check("T12 add col: 'extra' present in schema (DDL hook active)", extraInSchema);
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
    check("T13 drop col: 2 rows before ALTER", waitAndRead(tbl, 2).total == 2);

    boolean ok = true;
    try (Connection conn = connect()) {
      exec(conn, "ALTER TABLE " + tbl + " DROP COLUMN extra");
      exec(conn, "INSERT INTO " + tbl + " VALUES (3,30)");
      exec(conn, "INSERT INTO " + tbl + " VALUES (4,40)");
    } catch (Exception e) {
      ok = false;
      System.err.println("  ALTER/INSERT error: " + e.getMessage());
    }
    check("T13 drop col: ALTER + INSERT no crash", ok);
    if (ok) {
      PaimonRows rows = waitAndRead(tbl, 4);
      check("T13 drop col: 4 rows captured", rows.total == 4);
    }
  }

  // ── T14: ALTER TABLE RENAME COLUMN ───────────────────────────────────────
  // RENAME COLUMN arrives as a RenameStmt; paimon_ddl.cpp intercepts it
  // pre-execute, looks up the old column's attnum, and sends PMSG_DDL_RENAME
  // with the new name. The bgworker renames the field in the Paimon schema.
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
    check("T14 rename: 2 rows before ALTER", waitAndRead(tbl, 2).total == 2);

    boolean ok = true;
    try (Connection conn = connect()) {
      exec(conn, "ALTER TABLE " + tbl + " RENAME COLUMN v_old TO v_new");
      exec(conn, "INSERT INTO " + tbl + " VALUES (3,300)");
      exec(conn, "INSERT INTO " + tbl + " VALUES (4,400)");
    } catch (Exception e) {
      ok = false;
      System.err.println("  ALTER/INSERT error: " + e.getMessage());
    }
    check("T14 rename: ALTER + INSERT no crash", ok);
    if (ok) {
      PaimonRows rows = waitAndRead(tbl, 4);
      check("T14 rename: 4 rows captured", rows.total == 4);

      // DDL hook sends PMSG_DDL_RENAME; bgworker updates field name in schema
      File schema1 = new File(tableDir(tbl) + "/schema/schema-1");
      long deadline = System.currentTimeMillis() + 30_000;
      while (!schema1.exists() && System.currentTimeMillis() < deadline)
        Thread.sleep(500);

      TableSchema schema = readSchema(tbl);
      boolean newName = schema.fields().stream().anyMatch(f -> f.name().equals("v_new"));
      boolean oldGone = schema.fields().stream().noneMatch(f -> f.name().equals("v_old"));
      check("T14 rename: 'v_new' present in schema (DDL hook active)", newName);
      check("T14 rename: 'v_old' removed from schema", oldGone);
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
    check("T15 type change: 2 rows before ALTER", waitAndRead(tbl, 2).total == 2);

    boolean ok = true;
    try (Connection conn = connect()) {
      exec(conn, "ALTER TABLE " + tbl + " ALTER COLUMN v TYPE INT");
      exec(conn, "INSERT INTO " + tbl + " VALUES (3,30000)");
      exec(conn, "INSERT INTO " + tbl + " VALUES (4,40000)");
    } catch (Exception e) {
      ok = false;
      System.err.println("  ALTER/INSERT error: " + e.getMessage());
    }
    check("T15 type change: ALTER + INSERT no crash", ok);
    if (ok) {
      PaimonRows rows = waitAndRead(tbl, 4);
      check("T15 type change: 4 rows captured", rows.total == 4);
    }
  }

  // ── T16: CRASH RECOVERY ──────────────────────────────────────────────────
  // Documented strengthening: main could only promise that files already written
  // survive a crash. Here the WRITER is killed (SIGKILL) in the middle of a load,
  // more rows are committed in PostgreSQL while it is down (the slot retains the WAL),
  // and it is restarted. Zero loss and zero duplicates. This rests on the slot's
  // confirmed position advancing only after a Paimon commit returns, and on a replayed
  // transaction never being written twice.
  //
  // The writer commits several PostgreSQL transactions as ONE Paimon commit (a mini-batch),
  // and a time-driven batch boundary is not a function of the stream: after a crash the same
  // transactions can be regrouped, so a replay can overlap a batch that already committed.
  // Paimon's own filter would let such an overlap through, and the merged table would hide it
  // (deduplicate collapses identical records) -- only the raw CHANGELOG shows it, because
  // changelog-producer = input writes every input record. So every crash test reads the
  // changelog and asserts it EXACTLY: one +I per inserted row, and one -U and one +U per UPDATE,
  // never a -U without its +U. Each transaction inserts a batch AND updates a row inserted by
  // the previous one, so the -U/+U pairs are part of what is counted.
  //
  // T16a kill -9 mid-load (rows keep arriving while the writer is down)
  // T16b kill -9 immediately after a batch COMMITS, before the server learns of it: the
  // replay overlaps a committed batch. The window is real: the confirm reaches the
  // server only with the next status update (every 10 s), and the test asserts that the
  // server's confirmed position is behind the committed identifier at the kill.
  // T16c kill -9 with transactions sitting in the buffer, uncommitted, across status updates
  // and keepalives: nothing may have been confirmed past committed data
  // T16d SIGTERM with transactions buffered: a clean stop drains them
  // T16e a keepalive inside an OLDER transaction's burst while a newer one is buffered:
  // pgjdbc's auto-advance must not confirm the buffered one (the reviewer's loss).
  // The older table was already used on the connection: the guard rewinds.
  // T16f the same with a table first used inside the burst, whose Relation message carries
  // LSN 0: the guard commits the buffer
  static final int T16_BATCHES = 100, T16_PER_BATCH = 20;

  /** The writer launcher and pid file the harness exports; null when there is no harness. */
  static String[] writerControl(
      String test
  ) {
    String launcher = System.getenv("PGPAIMON_WRITER_LAUNCHER");
    String pidFile = System.getenv("PGPAIMON_WRITER_PID_FILE");
    if (launcher == null || pidFile == null) {
      if ("1".equals(System.getenv("PGPAIMON_HARNESS"))) {
        check(test + " harness exports PGPAIMON_WRITER_LAUNCHER and PGPAIMON_WRITER_PID_FILE",
            false);
      } else {
        System.out.println("  SKIP  " + test
            + ": no writer launcher (run via test/slot-acceptance/run_acceptance.sh)");
      }
      return null;
    }
    return new String[] {launcher, pidFile};
  }

  /**
   * After the kill the slot replays everything past its confirmed position, so first let the
   * confirmed position catch up with the WAL written so far. (Replay across a DDL boundary is a
   * separate matter; these tests are about a kill in the middle of a load.)
   */
  static void settleSlot() throws Exception {
    String target;
    try (Connection conn = connect();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT pg_current_wal_insert_lsn()::text")) {
      rs.next();
      target = rs.getString(1);
    }
    long settle = System.currentTimeMillis() + 60_000;
    while (System.currentTimeMillis() < settle) {
      try (Connection conn = connect();
          Statement st = conn.createStatement();
          ResultSet rs = st.executeQuery("SELECT confirmed_flush_lsn >= '" + target
              + "'::pg_lsn FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
        if (rs.next() && rs.getBoolean(1))
          break;
      }
      Thread.sleep(250);
    }
  }

  /**
   * Batch b of the load, as ONE transaction: 20 inserts, and (from b = 1) one UPDATE of the first
   * row the previous batch inserted.
   */
  static void insertBatch(
      Connection conn,
      String tbl,
      int b
  ) throws SQLException {
    conn.setAutoCommit(false);
    StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
    for (int k = 0; k < T16_PER_BATCH; k++) {
      int id = b * T16_PER_BATCH + k + 1;
      if (k > 0)
        sb.append(',');
      sb.append('(').append(id).append(",'r").append(id).append("')");
    }
    exec(conn, sb.toString());
    if (b > 0) {
      exec(conn,
          "UPDATE " + tbl + " SET v = 'u" + b + "' WHERE id = " + ((b - 1) * T16_PER_BATCH + 1));
    }
    conn.commit();
    conn.setAutoCommit(true);
  }

  static Thread loader(
      String tbl,
      int batches,
      long pauseMs,
      AtomicReference<Throwable> err
  ) {
    Thread t = new Thread(() -> {
      try (Connection conn = connect()) {
        for (int b = 0; b < batches; b++) {
          insertBatch(conn, tbl, b);
          if (pauseMs > 0)
            Thread.sleep(pauseMs);
        }
      } catch (Throwable e) {
        err.set(e);
      }
    });
    t.start();
    return t;
  }

  /**
   * Waits for the changelog to be complete, lets any replayed batch show up, then asserts it is
   * exactly what the load produced: not a record more, not one less.
   */
  static void assertChangelogExactlyOnce(
      String test,
      String tbl,
      int batches
  ) throws Exception {
    final int total = batches * T16_PER_BATCH, updates = batches - 1;
    final int expected = total + 2 * updates;
    List<int[]> recs = readIdsAndKinds(tbl);
    long deadline = System.currentTimeMillis() + 90_000;
    while (recs.size() < expected && System.currentTimeMillis() < deadline) {
      Thread.sleep(500);
      recs = readIdsAndKinds(tbl);
    }
    Thread.sleep(5000); // a replayed batch would show up as extra records by now
    recs = readIdsAndKinds(tbl);

    Map<Integer, Integer> inserts = new TreeMap<>(), before = new TreeMap<>(),
        after = new TreeMap<>();
    int other = 0;
    for (int[] r : recs) {
      switch (r[0]) {
        case 0:
          inserts.merge(r[1], 1, Integer::sum);
          break;
        case 1:
          before.merge(r[1], 1, Integer::sum);
          break;
        case 2:
          after.merge(r[1], 1, Integer::sum);
          break;
        default:
          other++;
      }
    }
    List<Integer> lost = new ArrayList<>(), dup = new ArrayList<>();
    for (int id = 1; id <= total; id++) {
      int n = inserts.getOrDefault(id, 0);
      if (n == 0)
        lost.add(id);
      if (n > 1)
        dup.add(id);
    }
    List<Integer> badUpdate = new ArrayList<>();
    for (int b = 1; b < batches; b++) {
      int id = (b - 1) * T16_PER_BATCH + 1;
      if (before.getOrDefault(id, 0) != 1 || after.getOrDefault(id, 0) != 1)
        badUpdate.add(id);
    }
    if (!lost.isEmpty())
      System.out.println("  lost ids: " + lost);
    if (!dup.isEmpty())
      System.out.println("  duplicated ids: " + dup);
    if (!badUpdate.isEmpty())
      System.out.println("  ids whose -U/+U is not exactly one pair: " + badUpdate);
    check(test + " zero loss (all " + total + " ids present)", lost.isEmpty());
    check(test + " zero duplicates (each id inserted exactly once)", dup.isEmpty());
    check(test + " changelog: +I count equals the row count (" + total + ")",
        inserts.values().stream().mapToInt(Integer::intValue).sum() == total);
    check(test + " changelog: every UPDATE is exactly one -U and one +U", badUpdate.isEmpty());
    check(test + " changelog: no -U without a +U (" + updates + " of each)",
        before.values().stream().mapToInt(Integer::intValue).sum() == updates
            && after.values().stream().mapToInt(Integer::intValue).sum() == updates);
    check(test + " changelog: no other record kind", other == 0);
    check(test + " changelog: exactly " + expected + " records", recs.size() == expected);
  }

  /** Starts the writer through the harness launcher; extraEnv reaches the writer unchanged. */
  static void startWriter(
      String test,
      String launcher,
      Map<String, String> extraEnv
  ) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("bash", launcher).redirectErrorStream(true)
        .redirectOutput(ProcessBuilder.Redirect.DISCARD);
    pb.environment().putAll(extraEnv);
    check(test + " launcher exited cleanly", pb.start().waitFor() == 0);
    long deadline = System.currentTimeMillis() + 60_000;
    while (!slotActive() && System.currentTimeMillis() < deadline)
      Thread.sleep(250);
    check(test + " restarted writer reattached to the slot", slotActive());
  }

  static String writerPid(
      String pidFile
  ) throws Exception {
    return new String(Files.readAllBytes(new File(pidFile).toPath())).trim();
  }

  static void kill9(
      String test,
      String pid
  ) throws Exception {
    int killRc = new ProcessBuilder("kill", "-9", pid).inheritIO().start().waitFor();
    check(test + " kill -9 of the writer succeeded", killRc == 0);
    waitProcessGone(pid);
    check(test + " writer process is gone", !processAlive(pid));
  }

  /** The commitIdentifier (a commit LSN) of a table's latest snapshot; -1 if there is none. */
  static long latestCommitIdentifier(
      String tbl
  ) {
    try {
      File latest = new File(tableDir(tbl) + "/snapshot/LATEST");
      if (!latest.exists())
        return -1;
      String id = new String(Files.readAllBytes(latest.toPath())).trim();
      String json = new String(
          Files.readAllBytes(new File(tableDir(tbl) + "/snapshot/snapshot-" + id).toPath()));
      java.util.regex.Matcher m =
          java.util.regex.Pattern.compile("\"commitIdentifier\"\\s*:\\s*(\\d+)").matcher(json);
      return m.find() ? Long.parseLong(m.group(1)) : -1;
    } catch (Exception e) {
      return -1; // a snapshot being written: ask again
    }
  }

  static String latestSnapshotFile(
      String tbl
  ) throws Exception {
    File latest = new File(tableDir(tbl) + "/snapshot/LATEST");
    return latest.exists() ? new String(Files.readAllBytes(latest.toPath())).trim() : null;
  }

  /**
   * How far the server's confirmed position is behind an LSN given as a number; positive = behind.
   */
  static boolean confirmedBehind(
      long lsn
  ) throws Exception {
    try (Connection conn = connect();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT (confirmed_flush_lsn - '0/0'::pg_lsn) < " + lsn
            + " FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
      return rs.next() && rs.getBoolean(1);
    }
  }

  static String confirmedFlush() throws Exception {
    try (Connection conn = connect();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(
            "SELECT (confirmed_flush_lsn - '0/0'::pg_lsn)::text FROM pg_replication_slots WHERE slot_name = 'paimon'")) {
      return rs.next() ? rs.getString(1) : "?";
    }
  }

  static long walInsertLsn() throws Exception {
    try (Connection conn = connect();
        Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery("SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  static void test16_crashRecovery() throws Exception {
    t16a_killMidLoad();
    t16b_killBetweenCommitAndConfirm();
    t16c_killMidBuffer();
    t16d_cleanStopDrains();
    t16e_keepaliveInAnOlderTransactionsBurst("T16e", true);
    t16e_keepaliveInAnOlderTransactionsBurst("T16f", false);
  }

  static void t16a_killMidLoad() throws Exception {
    System.out.println("\n── T16a: CRASH RECOVERY (writer killed mid-load) ────");
    String[] ctl = writerControl("T16");
    if (ctl == null)
      return;
    String launcher = ctl[0], pidFile = ctl[1];
    String tbl = "adv_crash_recovery";
    settleSlot();
    cleanup(tbl);
    try (Connection conn = connect()) {
      createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT"); // empty when registered
    }

    AtomicReference<Throwable> loadError = new AtomicReference<>();
    Thread loader = loader(tbl, T16_BATCHES, 60, loadError);

    // Let the writer commit at least one snapshot, then kill it with the load still running.
    File latest = new File(tableDir(tbl) + "/snapshot/LATEST");
    long deadline = System.currentTimeMillis() + 60_000;
    while (!latest.exists() && System.currentTimeMillis() < deadline)
      Thread.sleep(50);
    check("T16 recovery: writer committed a snapshot before the kill", latest.exists());

    String pid = writerPid(pidFile);
    boolean loadStillRunning = loader.isAlive();
    kill9("T16 recovery:", pid);
    check("T16 recovery: load was still running when the writer was killed", loadStillRunning);

    Thread.sleep(1500); // rows keep arriving in PostgreSQL while the writer is down
    check("T16 recovery: slot is inactive while the writer is down", !slotActive());

    System.out.println("  Restarting the writer...");
    startWriter("T16 recovery:", launcher, Map.of());

    loader.join();
    check("T16 recovery: load completed without error", loadError.get() == null);
    if (loadError.get() != null)
      loadError.get().printStackTrace();

    assertChangelogExactlyOnce("T16 recovery:", tbl, T16_BATCHES);
  }

  static void t16b_killBetweenCommitAndConfirm() throws Exception {
    System.out.println("\n── T16b: CRASH BETWEEN A BATCH COMMIT AND ITS CONFIRM ─");
    String[] ctl = writerControl("T16b");
    if (ctl == null)
      return;
    String launcher = ctl[0], pidFile = ctl[1];
    String tbl = "adv_crash_commit_confirm";
    settleSlot();
    cleanup(tbl);
    try (Connection conn = connect()) {
      createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");
    }
    AtomicReference<Throwable> loadError = new AtomicReference<>();
    Thread loader = loader(tbl, T16_BATCHES, 60, loadError);

    // Wait for the first batch commit, then for the NEXT one, and kill the instant it lands.
    // The commit is on disk; the server hears of it only with the next status update.
    String first = null;
    long deadline = System.currentTimeMillis() + 60_000;
    while (first == null && System.currentTimeMillis() < deadline) {
      first = latestSnapshotFile(tbl);
      Thread.sleep(10);
    }
    check("T16b writer committed a first batch", first != null);
    String seen = first;
    while (seen != null && seen.equals(latestSnapshotFile(tbl))
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(2);
    }
    String pid = writerPid(pidFile);
    kill9("T16b", pid);
    long identifier = latestCommitIdentifier(tbl);
    check("T16b killed right after a batch commit landed (identifier " + identifier + ")",
        identifier > 0);
    check(
        "T16b the window was real: the server's confirmed position (" + confirmedFlush()
            + ") is behind the committed identifier, so the batch will be replayed",
        confirmedBehind(identifier));

    System.out.println("  Restarting the writer...");
    startWriter("T16b", launcher, Map.of());
    loader.join();
    check("T16b load completed without error", loadError.get() == null);
    assertChangelogExactlyOnce("T16b", tbl, T16_BATCHES);
  }

  static void t16c_killMidBuffer() throws Exception {
    System.out.println("\n── T16c: CRASH WITH TRANSACTIONS BUFFERED, UNCOMMITTED ─");
    String[] ctl = writerControl("T16c");
    if (ctl == null)
      return;
    String launcher = ctl[0], pidFile = ctl[1];
    String tbl = "adv_crash_mid_buffer";
    settleSlot();
    // A writer whose time limit will not fire during the test: what it receives stays buffered.
    new ProcessBuilder("kill", writerPid(pidFile)).inheritIO().start().waitFor();
    waitProcessGone(writerPid(pidFile));
    check("T16c old writer stopped", !slotActiveSoon());
    Map<String, String> hold =
        Map.of("PGPAIMON_BATCH_MAX_INTERVAL_MS", "600000", "PGPAIMON_BATCH_MAX_ROWS", "1000000");
    startWriter("T16c", launcher, hold);
    cleanup(tbl);
    try (Connection conn = connect()) {
      createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");
    }
    Thread.sleep(1500); // let the DDL flow through
    long confirmedBefore = Long.parseLong(confirmedFlush());
    long loadStart = walInsertLsn();
    int batches = 30;
    AtomicReference<Throwable> loadError = new AtomicReference<>();
    loader(tbl, batches, 0, loadError).join();
    check("T16c load completed without error", loadError.get() == null);

    // Idle for longer than the status interval (10 s) so the writer sends status updates and
    // the server sends keepalives while the batch sits in the buffer. pgjdbc advances its
    // flushed position on a keepalive when nothing is owed; it must not do so for data that
    // is buffered but not committed.
    Thread.sleep(14_000);
    boolean noSnapshot = latestSnapshotFile(tbl) == null;
    long confirmedAfter = Long.parseLong(confirmedFlush());
    check("T16c nothing was committed while the batch sat in the buffer", noSnapshot);
    long loadEnd = walInsertLsn();
    check("T16c the load was large enough to mean something (" + (loadEnd - loadStart)
        + " bytes of WAL)", loadEnd - loadStart > 10_000);
    check(
        "T16c the server's confirmed position did not pass the buffered data across status updates"
            + " (before " + confirmedBefore + ", after " + confirmedAfter + ", load began at "
            + loadStart + ")",
        confirmedAfter <= loadStart + 1024);

    kill9("T16c", writerPid(pidFile));
    System.out.println("  Restarting the writer (default limits)...");
    startWriter("T16c", launcher, Map.of());
    assertChangelogExactlyOnce("T16c", tbl, batches);
  }

  static void t16d_cleanStopDrains() throws Exception {
    System.out.println("\n── T16d: CLEAN STOP DRAINS THE BUFFER ───────────────");
    String[] ctl = writerControl("T16d");
    if (ctl == null)
      return;
    String launcher = ctl[0], pidFile = ctl[1];
    String tbl = "adv_clean_stop";
    settleSlot();
    new ProcessBuilder("kill", writerPid(pidFile)).inheritIO().start().waitFor();
    waitProcessGone(writerPid(pidFile));
    Map<String, String> hold =
        Map.of("PGPAIMON_BATCH_MAX_INTERVAL_MS", "600000", "PGPAIMON_BATCH_MAX_ROWS", "1000000");
    startWriter("T16d", launcher, hold);
    cleanup(tbl);
    try (Connection conn = connect()) {
      createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");
    }
    Thread.sleep(1500);
    int batches = 10;
    AtomicReference<Throwable> loadError = new AtomicReference<>();
    loader(tbl, batches, 0, loadError).join();
    long lastLsn = walInsertLsn();
    Thread.sleep(1000);
    check("T16d the batch is buffered, not committed", latestSnapshotFile(tbl) == null);

    String pid = writerPid(pidFile);
    new ProcessBuilder("kill", pid).inheritIO().start().waitFor(); // SIGTERM
    waitProcessGone(pid);
    check("T16d writer exited on SIGTERM", !processAlive(pid));
    check("T16d the buffered transactions were committed on the way out",
        latestSnapshotFile(tbl) != null);
    long pos = Long.parseLong(confirmedFlush());
    check("T16d and confirmed to the server on the way out (confirmed " + pos
        + ", the load ended at " + lastLsn + ")", pos >= lastLsn - 8192);

    startWriter("T16d", launcher, Map.of());
    assertChangelogExactlyOnce("T16d", tbl, batches);
  }

  /**
   * The data-loss sequence found in review, live. pgjdbc 42.7.3 auto-advances its flushed position
   * on a keepalive when the flushed LSN of the last status update it SENT is at or above the WAL
   * start of the last message received. A transaction that began before that status update arrives,
   * in commit order, AFTER later-starting ones, with its records' original (lower) LSNs. So:
   *
   * 1. A: BEGIN; 200,000 rows; left open. Idle keepalives move the slot past A's rows. 2. B: one
   * row, committed. The writer buffers it (interval 600 s), unconfirmed. 3. A commits. Its Begin
   * and rows arrive below the confirmed position; a reply-requested keepalive (every
   * wal_sender_timeout / 2 = 2 s here) lands in the burst; pgjdbc advances to the walsender's send
   * pointer, past B's commit, and sends it at once. 4. kill -9, restart: the slot starts past B, so
   * B's row never reaches Paimon.
   *
   * The fix (SlotConsumer.guardBeforeNextRead): when the last message started at or below the
   * flushed position and something is buffered, rewind pgjdbc's flushed position below it and send
   * that at once, or, for a Relation message (LSN 0), commit the buffer. With
   * {@code tableUsedBefore} the older table has had a change on this connection already, so no
   * Relation message comes in A's burst and the rewind is what protects B (T16e); without it A's
   * first change brings a Relation message and the commit does (T16f). Both failed on 2b22ca9.
   */
  static void t16e_keepaliveInAnOlderTransactionsBurst(
      String t,
      boolean tableUsedBefore
  ) throws Exception {
    System.out
        .println("\n── " + t + ": KEEPALIVE IN AN OLDER TRANSACTION'S BURST, NEWER ONE BUFFERED ("
            + (tableUsedBefore ? "rewind" : "Relation message: commit") + ") ─");
    String[] ctl = writerControl(t);
    if (ctl == null)
      return;
    String launcher = ctl[0], pidFile = ctl[1];
    String big = "adv_ka_older_" + t.toLowerCase(), small = "adv_ka_buffered_" + t.toLowerCase();
    final int rowsA = 200_000;
    settleSlot();
    new ProcessBuilder("kill", writerPid(pidFile)).inheritIO().start().waitFor();
    waitProcessGone(writerPid(pidFile));
    check(t + " old writer stopped", !slotActiveSoon());
    try (Connection conn = connect()) {
      exec(conn, "ALTER SYSTEM SET wal_sender_timeout = '4s'");
      exec(conn, "SELECT pg_reload_conf()");
    }
    try {
      Map<String, String> hold = Map.of("PGPAIMON_BATCH_MAX_INTERVAL_MS", "600000",
          "PGPAIMON_BATCH_MAX_ROWS", "100000000", "PGPAIMON_BATCH_MAX_BYTES", "1099511627776");
      startWriter(t, launcher, hold);
      cleanup(big);
      cleanup(small);
      try (Connection conn = connect()) {
        createTracked(conn, big, "id INT PRIMARY KEY, v TEXT");
        if (tableUsedBefore) {
          // A change to the older table on this connection, so its Relation message
          // is sent now and not in A's burst. The next DDL message (registering the
          // small table) is a batch barrier, so this row is committed and confirmed.
          exec(conn, "INSERT INTO " + big + " VALUES (0, 'before')");
        }
        createTracked(conn, small, "id INT PRIMARY KEY, v TEXT");
      }
      Thread.sleep(1500);
      final int expectBig = rowsA + (tableUsedBefore ? 1 : 0);

      try (Connection a = connect(); Connection b = connect()) {
        // 1. A, open
        a.setAutoCommit(false);
        exec(a, "INSERT INTO " + big + " SELECT g, repeat('x', 20) FROM generate_series(1, " + rowsA
            + ") g");
        long aRowsEnd = walInsertLsn();
        long deadline = System.currentTimeMillis() + 30_000;
        while (Long.parseLong(confirmedFlush()) < aRowsEnd
            && System.currentTimeMillis() < deadline) {
          Thread.sleep(200);
        }
        check(
            t + " idle keepalives moved the confirmed position past A's open rows" + " (confirmed "
                + confirmedFlush() + ", A's rows end at " + aRowsEnd + ")",
            Long.parseLong(confirmedFlush()) >= aRowsEnd);

        // 2. B, committed and buffered
        b.setAutoCommit(false);
        exec(b, "INSERT INTO " + small + " VALUES (1, 'Tb')");
        long bCommit = walInsertLsn(); // B's commit record starts at or after this
        b.commit();
        Thread.sleep(1000);
        check(t + " B reached the writer and sits in its buffer, uncommitted",
            latestSnapshotFile(small) == null);

        // 3. A commits just before the next reply-requested keepalive is due, so that
        // keepalive lands inside A's burst rather than before or after it.
        String r0 = replyTime();
        deadline = System.currentTimeMillis() + 10_000;
        while (Objects.equals(r0, replyTime()) && System.currentTimeMillis() < deadline) {
          Thread.sleep(10);
        }
        Thread.sleep(1600);
        String bigSnapshotBefore = latestSnapshotFile(big);
        String beforeCommit = replyTime();
        long committedAt = System.currentTimeMillis();
        a.commit();
        long aEnd = walInsertLsn();
        boolean statusMidBurst = false;
        deadline = System.currentTimeMillis() + 60_000;
        while (sentLsn() < aEnd && System.currentTimeMillis() < deadline) {
          statusMidBurst |= !Objects.equals(beforeCommit, replyTime());
          Thread.sleep(20);
        }
        System.out.println("  A's burst took about " + (System.currentTimeMillis() - committedAt)
            + " ms to leave the walsender");
        check(t + " a status update was sent while A's burst was in flight"
            + " (the condition the bug needs)", statusMidBurst);
        Thread.sleep(3000); // the writer reads the tail of A and buffers it

        long confirmed = Long.parseLong(confirmedFlush());
        boolean bCommitted = latestSnapshotFile(small) != null;
        System.out
            .println("  at the kill: confirmed " + confirmed + ", B's commit record at or after "
                + bCommit + ", A's commit ends at " + aEnd + ", B in Paimon: " + bCommitted);
        check(t + " A is still buffered, uncommitted, at the kill",
            Objects.equals(bigSnapshotBefore, latestSnapshotFile(big)));
        check(t + " the confirmed position did not pass A's commit record", confirmed < aEnd);
        check(t + " anything confirmed past B's commit record was committed to Paimon first"
            + " (confirmed " + confirmed + ", B at " + bCommit + ", B in Paimon " + bCommitted
            + ")", confirmed <= bCommit || bCommitted);

        // 4. crash and recover
        kill9(t, writerPid(pidFile));
        System.out.println("  Restarting the writer (default limits)...");
        startWriter(t, launcher, Map.of());

        List<int[]> smallRows = readIdsAndKinds(small);
        List<int[]> bigRows = readIdsAndKinds(big);
        deadline = System.currentTimeMillis() + 120_000;
        while ((smallRows.size() < 1 || bigRows.size() < expectBig)
            && System.currentTimeMillis() < deadline) {
          Thread.sleep(1000);
          smallRows = readIdsAndKinds(small);
          bigRows = readIdsAndKinds(big);
        }
        Thread.sleep(3000);
        smallRows = readIdsAndKinds(small);
        bigRows = readIdsAndKinds(big);
        check(t + " B's row is in Paimon after the crash (" + smallRows.size() + " record(s))",
            smallRows.size() == 1 && smallRows.get(0)[0] == 0 && smallRows.get(0)[1] == 1);
        check(
            t + " A's " + rowsA + " rows are in Paimon after the crash, once each ("
                + bigRows.size() + " records, " + expectBig + " expected)",
            bigRows.size() == expectBig
                && bigRows.stream().mapToInt(r -> r[1]).distinct().count() == expectBig);
      }
    } finally {
      try (Connection conn = connect()) {
        exec(conn, "ALTER SYSTEM RESET wal_sender_timeout");
        exec(conn, "SELECT pg_reload_conf()");
      }
    }
  }

  /** pg_stat_replication.reply_time of the writer's walsender: when it last heard from it. */
  static String replyTime() throws Exception {
    try (Connection conn = connect();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT reply_time::text FROM pg_stat_replication"
            + " WHERE pid = (SELECT active_pid FROM pg_replication_slots WHERE slot_name = 'paimon')")) {
      return rs.next() ? rs.getString(1) : null;
    }
  }

  /** How far the writer's walsender has sent, as a number; 0 when there is none. */
  static long sentLsn() throws Exception {
    try (Connection conn = connect();
        Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery("SELECT (sent_lsn - '0/0'::pg_lsn)::bigint FROM pg_stat_replication"
                + " WHERE pid = (SELECT active_pid FROM pg_replication_slots WHERE slot_name = 'paimon')")) {
      return rs.next() ? rs.getLong(1) : 0;
    }
  }

  static boolean slotActiveSoon() throws Exception {
    long end = System.currentTimeMillis() + 10_000;
    while (slotActive() && System.currentTimeMillis() < end)
      Thread.sleep(200);
    return slotActive();
  }

  static boolean processAlive(
      String pid
  ) throws Exception {
    return new ProcessBuilder("kill", "-0", pid).redirectErrorStream(true)
        .redirectOutput(ProcessBuilder.Redirect.DISCARD).start().waitFor() == 0;
  }

  static void waitProcessGone(
      String pid
  ) throws Exception {
    long deadline = System.currentTimeMillis() + 10_000;
    while (processAlive(pid) && System.currentTimeMillis() < deadline)
      Thread.sleep(100);
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
  // is captured only when it is passed to paimon.add_table(). A table that never
  // was reaches neither paimon_pub nor the warehouse. The writer is proven to have
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
          "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'paimon_pub' AND tablename = '"
              + tbl + "'")) {
        rs.next();
        inPub = rs.getLong(1);
      }
      try (ResultSet rs = s.executeQuery(
          "SELECT count(*) FROM paimon.table_registry WHERE relid = '" + tbl + "'::regclass")) {
        rs.next();
        inRegistry = rs.getLong(1);
      }
      try (ResultSet rs = s.executeQuery(
          "SELECT am.amname FROM pg_class c JOIN pg_am am ON c.relam = am.oid WHERE c.oid = '" + tbl
              + "'::regclass")) {
        if (rs.next())
          am = rs.getString(1);
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

  static void exec(
      Connection c,
      String sql
  ) throws SQLException {
    try (Statement s = c.createStatement()) {
      s.execute(sql);
    }
  }

  /** Creates a tracked table: CREATE TABLE, then register it with paimon.add_table(). */
  static void createTracked(
      Connection conn,
      String tbl,
      String columns
  ) throws Exception {
    exec(conn, "DROP TABLE IF EXISTS " + tbl);
    exec(conn, "CREATE TABLE " + tbl + " (" + columns + ")");
    exec(conn, "SELECT paimon.add_table('" + tbl + "'::regclass)");
  }

  /** Paimon's filesystem catalog nests tables under <database>.db/. */
  /**
   * The warehouse path of a table: <warehouse>/<postgres schema>.db/
   * <table>
   * .
   */
  static String tableDir(
      String schema,
      String tbl
  ) {
    return warehouse + "/" + schema + ".db/" + tbl;
  }

  /** These suites create every table in PG_SCHEMA. */
  static String tableDir(
      String tbl
  ) {
    return tableDir(PG_SCHEMA, tbl);
  }

  static void cleanup(
      String tbl
  ) {
    File dir = new File(tableDir(tbl));
    if (dir.exists())
      deleteDir(dir);
  }

  static void deleteDir(
      File f
  ) {
    if (f.isDirectory())
      for (File c : Objects.requireNonNull(f.listFiles()))
        deleteDir(c);
    f.delete();
  }

  /**
   * Counts a table's parquet files across EVERY bucket directory. Main's C++ writer put all files
   * in bucket-0; with fixed bucketing Paimon hashes the primary key, so a table's files spread over
   * bucket-0, bucket-1, ... and a helper that looks only in bucket-0 waits forever for a table
   * whose rows hash elsewhere. How the files are found changes with the storage layout; what the
   * suites assert about their content does not.
   */
  static int countParquet(
      String tbl
  ) {
    File[] buckets = new File(tableDir(tbl)).listFiles((
        d,
        n) -> n.startsWith("bucket-"));
    int total = 0;
    if (buckets != null) {
      for (File b : buckets) {
        File[] f = b.listFiles((
            d,
            n) -> n.endsWith(".parquet"));
        if (f != null)
          total += f.length;
      }
    }
    return total;
  }

  static void waitForParquet(
      String tbl,
      int minFiles
  ) throws InterruptedException {
    File snapshotLatest = new File(tableDir(tbl) + "/snapshot/LATEST");
    long deadline = System.currentTimeMillis() + 120_000;
    while (System.currentTimeMillis() < deadline) {
      if (countParquet(tbl) >= minFiles && snapshotLatest.exists())
        return;
      Thread.sleep(500);
    }
    throw new RuntimeException("Timed out waiting for snapshot in " + tbl);
  }

  static TableSchema readSchema(
      String tbl
  ) throws Exception {
    Path tablePath = new Path(tableDir(tbl));
    return new SchemaManager(fileIO, tablePath).latest()
        .orElseThrow(() -> new RuntimeException("No schema: " + tbl));
  }

  static PaimonRows waitAndRead(
      String tbl,
      int minTotal
  ) throws Exception {
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

  static PaimonRows readRows(
      String tbl
  ) throws Exception {
    PaimonRows result = new PaimonRows();
    for (int[] r : readIdsAndKinds(tbl)) {
      result.total++;
      result.kindCounts.merge(r[0], 1L, Long::sum);
    }
    return result;
  }

  /**
   * Every changelog record as stored, as {kindOrdinal, id}. Row kinds come from the $audit_log
   * system table over a streaming scan: Paimon stores the kind as the reserved _VALUE_KIND, so a
   * batch read (merged, or with a faked _row_kind column) cannot report it. Column 0 is the kind as
   * a string; the key is the column named "id".
   */
  static List<int[]> readIdsAndKinds(
      String tbl
  ) {
    List<int[]> out = new ArrayList<>();
    try {
      Options o = new Options();
      o.set("warehouse", warehouse);
      try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
        Table al = catalog.getTable(Identifier.create(PG_SCHEMA, tbl + "$audit_log"))
            .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
        int idIdx = al.rowType().getFieldNames().indexOf("id");
        StreamTableScan scan = al.newReadBuilder().newStreamScan();
        scan.plan(); // the first plan() is always empty: discard
        Long latest = ((FileStoreTable) catalog.getTable(Identifier.create(PG_SCHEMA, tbl)))
            .snapshotManager().latestSnapshotId();
        List<Split> splits = new ArrayList<>();
        for (long i = 0; i <= (latest == null ? 0 : latest) + 1; i++) {
          splits.addAll(scan.plan().splits());
        }
        try (
            RecordReader<InternalRow> reader = al.newReadBuilder().newRead().createReader(splits)) {
          reader.forEachRemaining(
              r -> out.add(new int[] {kindOf(r.getString(0).toString()), r.getInt(idIdx)}));
        }
      }
    } catch (Exception e) {
      System.err.println("  warn: row-kind read failed for " + tbl + ": " + e.getMessage());
    }
    return out;
  }

  /** $audit_log's rowkind string to the ordinal the assertions use: +I 0, -U 1, +U 2, -D 3. */
  static int kindOf(
      String k
  ) {
    switch (k) {
      case "+I":
        return 0;
      case "-U":
        return 1;
      case "+U":
        return 2;
      case "-D":
        return 3;
      default:
        throw new IllegalStateException("unknown row kind " + k);
    }
  }

  static class PaimonRows {
    long total = 0;
    Map<Integer, Long> kindCounts = new HashMap<>();

    long ofKind(
        int k
    ) {
      return kindCounts.getOrDefault(k, 0L);
    }
  }

  static String env(
      String name,
      String dflt
  ) {
    String v = System.getenv(name);
    return v != null ? v : dflt;
  }

  static void check(
      String name,
      boolean condition
  ) {
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
