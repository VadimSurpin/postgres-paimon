// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.sql.*;
import java.util.*;
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
 * Integration test suite for paimon_heap extension features.
 *
 * <p>
 * Tests: T1 Basic INSERT capture T2 UPDATE capture (before + after rows) T3 DELETE capture T4
 * Metadata fields (_xid, _op_seq, _seq) T5 Configurable seq / row_kind field names T6 Schema
 * structure (all fields present) T7 CREATE TABLE with TEXT columns (proxy-relation fix) T8
 * Multi-row INSERT correctness
 *
 * <p>
 * Usage: java -cp paimon-reader.jar pro.surpin.pgpaimon.PaimonFeatureTest [warehouse] [jdbc-url]
 */
public class PaimonFeatureTest {

  static final String DEFAULT_WAREHOUSE = env("PGPAIMON_IT_WAREHOUSE", "/tmp/paimon-warehouse");
  static final String DEFAULT_JDBC =
      env("PGPAIMON_IT_JDBC_URL", "jdbc:postgresql://localhost:5434/testdb?user=ubuntu&password=");

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
    System.out.println("║         paimon slot-writer Feature Suite               ║");
    System.out.println("╚════════════════════════════════════════════════════╝");
    System.out.printf("  Warehouse : %s%n", warehouse);
    System.out.printf("  JDBC      : %s%n", jdbcUrl);
    System.out.println();

    test1_basicInsert();
    test2_updateCapture();
    test3_deleteCapture();
    test4_metadataFields();
    test5_configurableFieldNames();
    test6_schemaStructure();
    test7_textColumns();
    test8_multiRowInsert();

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

  // ── T1: Basic INSERT capture ─────────────────────────────────────────

  static void test1_basicInsert() throws Exception {
    String tbl = "ft_insert_test";
    try (Connection conn = connect()) {
      exec(conn, "DROP TABLE IF EXISTS " + tbl);
      createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");
      exec(conn, "INSERT INTO " + tbl + " VALUES (1,'a'), (2,'b'), (3,'c')");
    }
    PaimonRows rows = waitAndRead(tbl, 3);
    long inserts = rows.ofKind(0);
    check("T1 row count", rows.total == 3);
    check("T1 all inserts", inserts == 3);
  }

  // ── T2: UPDATE capture (before + after rows) ─────────────────────────

  static void test2_updateCapture() throws Exception {
    String tbl = "ft_update_test";
    try (Connection conn = connect()) {
      exec(conn, "DROP TABLE IF EXISTS " + tbl);
      createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");
      exec(conn, "INSERT INTO " + tbl + " VALUES (1,'old')");
      exec(conn, "UPDATE " + tbl + " SET v='new' WHERE id=1");
    }
    // 1 insert + 1 update_before + 1 update_after = 3 rows
    PaimonRows rows = waitAndRead(tbl, 3);
    check("T2 total rows", rows.total == 3);
    check("T2 insert present", rows.ofKind(0) == 1);
    check("T2 upd_before", rows.ofKind(1) == 1);
    check("T2 upd_after", rows.ofKind(2) == 1);
  }

  // ── T3: DELETE capture ────────────────────────────────────────────────

  static void test3_deleteCapture() throws Exception {
    String tbl = "ft_delete_test";
    try (Connection conn = connect()) {
      exec(conn, "DROP TABLE IF EXISTS " + tbl);
      createTracked(conn, tbl, "id INT PRIMARY KEY, v INT");
      exec(conn, "INSERT INTO " + tbl + " VALUES (1,10), (2,20)");
      exec(conn, "DELETE FROM " + tbl + " WHERE id=1");
    }
    // 2 inserts + 1 delete = 3
    PaimonRows rows = waitAndRead(tbl, 3);
    check("T3 total rows", rows.total == 3);
    check("T3 deletes", rows.ofKind(3) == 1);
    check("T3 inserts", rows.ofKind(0) == 2);
  }

  // ── T4: Metadata fields (_xid, _op_seq, _seq) ─────────────────────────
  // Documented difference: main's _tid (physical tuple identifier) cannot exist here,
  // since no tuple identifier survives the reorder buffer. _seq, the per-change LSN,
  // replaces it: it must be present and strictly increasing across changes.

  static void test4_metadataFields() throws Exception {
    String tbl = "ft_meta_test";
    cleanup(tbl);
    try (Connection conn = connect()) {
      exec(conn, "DROP TABLE IF EXISTS " + tbl);
      createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");
      // Separate autocommit statements: three distinct changes, three distinct LSNs.
      exec(conn, "INSERT INTO " + tbl + " VALUES (1,'test')");
      exec(conn, "INSERT INTO " + tbl + " VALUES (2,'test')");
      exec(conn, "INSERT INTO " + tbl + " VALUES (3,'test')");
    }
    // Wait for schema
    File schemaFile = new File(tableDir(tbl) + "/schema/schema-0");
    waitForFile(schemaFile);
    TableSchema schema = readSchema(tbl);
    boolean hasXid = schema.fields().stream().anyMatch(f -> f.name().equals("_xid"));
    boolean hasOp = schema.fields().stream().anyMatch(f -> f.name().equals("_op_seq"));
    boolean hasSeq = schema.fields().stream().anyMatch(f -> f.name().equals("_seq"));
    check("T4 _xid in schema", hasXid);
    check("T4 _op_seq in schema", hasOp);
    check("T4 _seq in schema", hasSeq);

    PaimonRows rows = waitAndRead(tbl, 3);
    check("T4 rows captured", rows.total == 3);

    // _seq per key, in insertion (= commit = LSN) order
    Map<Integer, Long> seqById = readSeqById(tbl, 3);
    boolean present = seqById.size() == 3 && !seqById.containsValue(null);
    check("T4 _seq present on every row", present);
    boolean increasing = present;
    long prev = Long.MIN_VALUE;
    for (long v : seqById.values()) { // TreeMap: ids 1,2,3
      if (v <= prev)
        increasing = false;
      prev = v;
    }
    check("T4 _seq strictly increasing across changes", increasing);
  }

  /** id -> _seq from the $audit_log changelog (column 0 is the kind; id and _seq by name). */
  static Map<Integer, Long> readSeqById(
      String tbl,
      int want
  ) throws Exception {
    long deadline = System.currentTimeMillis() + 60_000;
    Map<Integer, Long> out = new TreeMap<>();
    while (System.currentTimeMillis() < deadline) {
      out.clear();
      Options o = new Options();
      o.set("warehouse", warehouse);
      try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
        Table t = catalog.getTable(Identifier.create("default", tbl));
        int idIdx = t.rowType().getFieldNames().indexOf("id");
        int seqIdx = t.rowType().getFieldNames().indexOf("_seq");
        try (RecordReader<InternalRow> reader = t.newReadBuilder().newRead()
            .createReader(t.newReadBuilder().newScan().plan().splits())) {
          reader.forEachRemaining(
              r -> out.put(r.getInt(idIdx), r.isNullAt(seqIdx) ? null : r.getLong(seqIdx)));
        }
      } catch (Exception e) {
        System.err.println("  warn: _seq read failed for " + tbl + ": " + e.getMessage());
      }
      if (out.size() >= want)
        break;
      Thread.sleep(500);
    }
    return out;
  }

  // ── T5: Configurable seq / row_kind field names ───────────────────────

  static void test5_configurableFieldNames() throws Exception {
    // Just verify the defaults are _seq and _row_kind in the schema
    String tbl = "ft_fieldname_test";
    cleanup(tbl);
    try (Connection conn = connect()) {
      // Reset metadata fields so we don't confuse the schema
      setGuc(conn, "paimon_heap.meta_field_xid", "");
      setGuc(conn, "paimon_heap.meta_field_op", "");
      setGuc(conn, "paimon_heap.meta_field_tid", "");
      exec(conn, "DROP TABLE IF EXISTS " + tbl);
      createTracked(conn, tbl, "id INT PRIMARY KEY");
      exec(conn, "INSERT INTO " + tbl + " VALUES (1)");
    }
    waitForFile(new File(tableDir(tbl) + "/schema/schema-0"));
    TableSchema schema = readSchema(tbl);
    boolean hasSeq = schema.fields().stream().anyMatch(f -> f.name().equals("_seq"));
    check("T5 _seq field in schema", hasSeq);
    // row_kind is a Parquet-level column, not in the Paimon schema fields
    check("T5 schema has at least 2 fields", schema.fields().size() >= 2);
  }

  // ── T6: Schema structure ─────────────────────────────────────────────

  static void test6_schemaStructure() throws Exception {
    String tbl = "ft_schema_test";
    cleanup(tbl);
    try (Connection conn = connect()) {
      setGuc(conn, "paimon_heap.meta_field_xid", "");
      setGuc(conn, "paimon_heap.meta_field_op", "");
      setGuc(conn, "paimon_heap.meta_field_tid", "");
      exec(conn, "DROP TABLE IF EXISTS " + tbl);
      createTracked(conn, tbl, "id INT PRIMARY KEY, name TEXT, score BIGINT");
      exec(conn, "INSERT INTO " + tbl + " VALUES (1,'Alice',100)");
    }
    waitForFile(new File(tableDir(tbl) + "/schema/schema-0"));
    TableSchema schema = readSchema(tbl);
    boolean hasId = schema.fields().stream().anyMatch(f -> f.name().equals("id"));
    boolean hasName = schema.fields().stream().anyMatch(f -> f.name().equals("name"));
    boolean hasScore = schema.fields().stream().anyMatch(f -> f.name().equals("score"));
    boolean hasSeq = schema.fields().stream().anyMatch(f -> f.name().equals("_seq"));
    boolean pkIsId = schema.primaryKeys().contains("id");
    check("T6 id field", hasId);
    check("T6 name field", hasName);
    check("T6 score field", hasScore);
    check("T6 _seq field", hasSeq);
    check("T6 pk = id", pkIsId);
  }

  // ── T7: CREATE TABLE with TEXT columns (proxy-relation fix) ──────────

  static void test7_textColumns() throws Exception {
    // This previously failed with "only heap AM is supported" on PG19.
    // Verifies the proxy-relation scan fix works.
    String tbl = "ft_text_test";
    try (Connection conn = connect()) {
      setGuc(conn, "paimon_heap.meta_field_xid", "");
      setGuc(conn, "paimon_heap.meta_field_op", "");
      setGuc(conn, "paimon_heap.meta_field_tid", "");
      exec(conn, "DROP TABLE IF EXISTS " + tbl);
      // TEXT column triggers TOAST table creation, which uses heap_getnext()
      String cols = "id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT";
      boolean ok = false;
      try {
        createTracked(conn, tbl, cols);
        ok = true;
      } catch (Exception e) {
        System.err.println("T7 CREATE failed: " + e.getMessage());
      }
      check("T7 CREATE TABLE with TEXT", ok);
      if (ok) {
        exec(conn, "INSERT INTO " + tbl + " VALUES (1,'hello','world','!')");
        check("T7 INSERT after CREATE", true);
      }
    }
  }

  // ── T8: Multi-row INSERT correctness ─────────────────────────────────

  static void test8_multiRowInsert() throws Exception {
    String tbl = "ft_multi_test";
    try (Connection conn = connect()) {
      exec(conn, "DROP TABLE IF EXISTS " + tbl);
      createTracked(conn, tbl, "id INT PRIMARY KEY, v INT");
      // Large INSERT triggers heap multi_insert path
      StringBuilder sb = new StringBuilder("INSERT INTO " + tbl + " VALUES ");
      for (int i = 1; i <= 100; i++) {
        if (i > 1)
          sb.append(',');
        sb.append('(').append(i).append(',').append(i * 10).append(')');
      }
      exec(conn, sb.toString());
    }
    PaimonRows rows = waitAndRead(tbl, 100);
    check("T8 all 100 rows captured", rows.total == 100);
    check("T8 all inserts", rows.ofKind(0) == 100);
  }

  // ── Helpers ──────────────────────────────────────────────────────────

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

  static void setGuc(
      Connection c,
      String name,
      String value
  ) throws SQLException {
    noGuc();
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

  static void waitForFile(
      File f
  ) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 120_000;
    while (!f.exists() && System.currentTimeMillis() < deadline)
      Thread.sleep(500);
    if (!f.exists())
      throw new RuntimeException("Timed out waiting for " + f);
  }

  static void waitForParquet(
      String tbl,
      int minFiles
  ) throws InterruptedException {
    File bucketDir = new File(tableDir(tbl) + "/bucket-0");
    File snapshotLatest = new File(tableDir(tbl) + "/snapshot/LATEST");
    long deadline = System.currentTimeMillis() + 120_000;
    while (System.currentTimeMillis() < deadline) {
      File[] f = bucketDir.listFiles((
          d,
          n) -> n.endsWith(".parquet"));
      if (f != null && f.length >= minFiles && snapshotLatest.exists())
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
    try {
      Options o = new Options();
      o.set("warehouse", warehouse);
      try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
        /*
         * Row kinds come from the $audit_log system table over a streaming scan: Paimon stores the
         * kind as the reserved _VALUE_KIND, so a batch read (merged, or with a faked _row_kind
         * column) cannot report it. Column 0 is the kind as a string.
         */
        Table al = catalog.getTable(Identifier.create("default", tbl + "$audit_log"))
            .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
        StreamTableScan scan = al.newReadBuilder().newStreamScan();
        scan.plan(); // the first plan() is always empty: discard
        Long latest = ((FileStoreTable) catalog.getTable(Identifier.create("default", tbl)))
            .snapshotManager().latestSnapshotId();
        List<Split> splits = new ArrayList<>();
        for (long i = 0; i <= (latest == null ? 0 : latest) + 1; i++) {
          splits.addAll(scan.plan().splits());
        }
        try (
            RecordReader<InternalRow> reader = al.newReadBuilder().newRead().createReader(splits)) {
          reader.forEachRemaining(r -> {
            result.total++;
            result.kindCounts.merge(kindOf(r.getString(0).toString()), 1L, Long::sum);
          });
        }
      }
    } catch (Exception e) {
      System.err.println("  warn: row-kind read failed for " + tbl + ": " + e.getMessage());
    }
    return result;
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

  // ── Slot-architecture setup helpers (the only thing the port changes) ─────

  /** Creates a tracked table. Replaces "USING paimon_heap" from the TAM architecture. */
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
  static String tableDir(
      String tbl
  ) {
    return warehouse + "/default.db/" + tbl;
  }

  static boolean gucNoteLogged = false;

  /** paimon_heap.* GUCs no longer exist: metadata columns are writer-side configuration. */
  static void noGuc() {
    if (!gucNoteLogged) {
      gucNoteLogged = true;
      System.out.println("  note: paimon_heap.* GUCs are no-ops in the slot architecture");
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
