// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.*;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.pgpaimon.writer.dto.*;

/**
 * History tables and key-less operation logs against the real Paimon 1.4.1, through the real
 * pipeline: PipelineHandler, TxnBatcher, MiniBatcher, the confirming flusher, PaimonSink, the DDL
 * handler. The history is read back row by row; row kinds of the main tables come from $audit_log
 * (RawRead), never from a merged read.
 */
class HistoryPaimonTest {

  @TempDir
  static java.nio.file.Path wh;
  static Catalog catalog;
  static SchemaRegistry reg;
  static WriterConfig cfg;
  static DdlMessageHandler ddl;
  static long ddlLsn = 0x1000;
  /** 2026-01-01T00:00:00Z, PostgreSQL epoch. */
  static final long PG_TS = 820_540_800_000_000L;
  static final long DAY = 86_400_000_000L;
  /**
   * Change and commit LSNs of the test transactions start here, above every DDL LSN the tests hand
   * out, as in a real stream: a table's create message precedes its changes.
   */
  static final long BASE = 1_000_000;

  static final MiniBatcher.Limits BIG = new MiniBatcher.Limits(1_000_000, 1L << 40, 3_600_000);

  @BeforeAll
  static void setUp() {
    Options o = new Options();
    o.set("warehouse", wh.toString());
    catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
    Map<String, String> env = new HashMap<>();
    env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db");
    env.put("PGPAIMON_WAREHOUSE", wh.toString());
    cfg = WriterConfig.fromEnv(env);
    reg = new SchemaRegistry(catalog, cfg);
    ddl = new DdlMessageHandler(reg, "paimon", TestAuth.TOKEN);
  }

  static void create(
      long relid,
      String table,
      boolean keyless
  ) {
    ddlLsn += 0x10;
    ddl.handle(new PgEvent.Message(true, ddlLsn, "paimon", TestAuth.authed("""
        {"v":1,"op":"create","relid":%d,"schema":"public","table":"%s",
         "pk":%s,"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                         {"id":2,"name":"v","type":"STRING"}],
         "options":{"changelog-producer":"input","bucket":"2","hist-table":"%s_hist",
                    "registration":"reg-%d","hist-bucket":"2"}}""".formatted(relid, table,
        keyless ? "[]" : "[\"id\"]", table, relid))));
  }

  static RelationMeta meta(
      long relid,
      String table
  ) {
    return new RelationMeta(relid, "public", table, List.of("id", "v"), List.of(false, false));
  }

  /** A writer process: the pipeline WriterBootstrap builds, over the shared catalog. */
  static final class Process implements AutoCloseable {
    final PaimonSink sink;
    final List<Long> confirmed = new ArrayList<>();
    final MiniBatcher mini;
    final WriterBootstrap.PipelineHandler handler;

    Process(
        MiniBatcher.Limits limits
    ) {
      this(limits, k -> true);
    }

    /** {@code commits} decides which Paimon tables this process actually commits to. */
    Process(
        MiniBatcher.Limits limits,
        java.util.function.LongPredicate commits
    ) {
      sink = new PaimonSink(reg, cfg, CrashHook.OFF);
      TxnBatcher.Flusher partial = (
          records,
          commitLsn,
          endLsn) -> sink.flush(records.stream().filter(r -> commits.test(r.relid())).toList(),
              commitLsn, endLsn);
      mini = new MiniBatcher(WriterBootstrap.confirmingFlusher(partial, confirmed::add),
          new OffsetStore(reg, cfg)::highestCommitted, limits);
      TxnBatcher batcher = new TxnBatcher(new ToastResolver(), reg, mini);
      handler = new WriterBootstrap.PipelineHandler(reg::tracked, reg::primaryKey, ddl::handle,
          ddl::isDdl, new WriterBootstrap.PipelineHandler.Target() {
            @Override
            public void setRelation(
                long relid,
                RelationMeta m
            ) {
              sink.setRelation(relid, m);
            }

            @Override
            public void truncate(
                long relid,
                long commitLsn
            ) throws Exception {
              sink.truncate(relid, commitLsn);
            }

            @Override
            public boolean overwrites(
                long relid
            ) {
              return !reg.keyless(relid);
            }
          }, batcher, mini);
    }

    void ev(
        PgEvent e,
        long lsn
    ) throws Exception {
      handler.onEvent(e, lsn);
    }

    @Override
    public void close() throws Exception {
      sink.close();
    }
  }

  static TupleData row(
      String id,
      String v
  ) {
    return new TupleData(
        Arrays.asList(ColumnValue.of(id), v == null ? ColumnValue.ofNull() : ColumnValue.of(v)));
  }

  /** Transaction n (commit LSN 1000n, xid n, commit time PG_TS + n): I, U, U, D of id n. */
  static void iuud(
      Process p,
      long relid,
      int n
  ) throws Exception {
    iuud(p, relid, n, PG_TS + n);
  }

  static void iuud(
      Process p,
      long relid,
      int n,
      long pgCommitTs
  ) throws Exception {
    long c = BASE + 1000L * n;
    String id = "" + n;
    p.ev(new PgEvent.Begin(c, pgCommitTs, n), c - 100);
    p.ev(new PgEvent.Insert(relid, row(id, "a")), c - 90);
    p.ev(new PgEvent.Update(relid, row(id, "a"), false, row(id, "b")), c - 80);
    p.ev(new PgEvent.Update(relid, row(id, "b"), false, row(id, null)), c - 70);
    p.ev(new PgEvent.Delete(relid, row(id, null), false), c - 60);
    p.ev(new PgEvent.Commit(c, c + 8, pgCommitTs), c);
  }

  /** Every stored row of an append-only table, each column as text (null for NULL). */
  static List<Map<String, String>> rows(
      String table
  ) throws Exception {
    FileStoreTable t = (FileStoreTable) catalog.getTable(Identifier.create("public", table));
    List<DataField> fields = t.rowType().getFields();
    var rb = t.newReadBuilder();
    List<Map<String, String>> out = new ArrayList<>();
    try (var reader = rb.newRead().createReader(rb.newScan().plan());
        var it = reader.toCloseableIterator()) {
      while (it.hasNext()) {
        InternalRow r = it.next();
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
          m.put(fields.get(i).name(), r.isNullAt(i) ? null : text(r, i, fields.get(i)));
        }
        out.add(m);
      }
    }
    out.sort(Comparator.comparingLong((
        Map<String, String> m) -> Long.parseLong(m.get("_commit_lsn")))
        .thenComparingInt(m -> Integer.parseInt(m.get("_op_seq"))));
    return out;
  }

  static String text(
      InternalRow r,
      int i,
      DataField f
  ) {
    return switch (f.type().getTypeRoot()) {
      case INTEGER -> "" + r.getInt(i);
      case BIGINT -> "" + r.getLong(i);
      case DATE -> LocalDate.ofEpochDay(r.getInt(i)).toString();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> "" + r.getTimestamp(i, 6).toMicros();
      default -> r.getString(i).toString();
    };
  }

  static long snapshots(
      String table
  ) throws Exception {
    Long id = ((FileStoreTable) catalog.getTable(Identifier.create("public", table)))
        .snapshotManager().latestSnapshotId();
    return id == null ? 0 : id;
  }

  static Set<String> partitionDirs(
      String table
  ) {
    Set<String> out = new TreeSet<>();
    java.io.File[] fs = wh.resolve("public.db").resolve(table).toFile().listFiles();
    for (java.io.File f : fs == null ? new java.io.File[0] : fs) {
      if (f.isDirectory() && f.getName().startsWith("_dt=")) {
        out.add(f.getName());
      }
    }
    return out;
  }

  /** Asserts the six history rows of iuud transaction n. */
  static void assertTxn(
      List<Map<String, String>> rows,
      int n,
      String day
  ) {
    List<Map<String, String>> t =
        rows.stream().filter(m -> m.get("_commit_lsn").equals("" + (BASE + 1000L * n))).toList();
    assertEquals(List.of("+I", "-U", "+U", "-U", "+U", "-D"),
        t.stream().map(m -> m.get("_op")).toList(), "transaction " + n + ": " + t);
    String id = "" + n;
    assertEquals(Arrays.asList("a", "a", "b", "b", null, null),
        t.stream().map(m -> m.get("v")).toList(), "images of transaction " + n);
    assertTrue(t.stream().allMatch(m -> id.equals(m.get("id"))));
    long c = BASE + 1000L * n;
    assertEquals(List.of("" + (c - 90), "" + (c - 80), "" + (c - 80), "" + (c - 70), "" + (c - 70),
        "" + (c - 60)), t.stream().map(m -> m.get("_lsn")).toList(), "each pair shares _lsn");
    for (int i = 0; i < 6; i++) {
      Map<String, String> m = t.get(i);
      assertEquals("" + n, m.get("_xid"));
      assertEquals(day, m.get("_dt"));
      if (i > 0) {
        assertTrue(
            Integer.parseInt(m.get("_op_seq")) > Integer.parseInt(t.get(i - 1).get("_op_seq")));
      }
    }
  }

  @Test
  void historyOfAPrimaryKeyTableIsEveryImageUncollapsedAcrossAMiniBatch() throws Exception {
    long rel = 30001;
    create(rel, "h_pk", false);
    try (Process p = new Process(BIG)) {
      p.ev(new PgEvent.Relation(meta(rel, "h_pk")), 0);
      for (int n = 1; n <= 5; n++) {
        iuud(p, rel, n);
      }
      assertTrue(p.mini.hasBuffered(), "all five transactions wait in one mini-batch");
      p.mini.flushBuffered();
      assertEquals(List.of(BASE + 5008L), p.confirmed, "one commit, confirmed once");
    }
    assertEquals(1, snapshots("h_pk_hist"), "the batcher really batched: one snapshot for 5");
    List<Map<String, String>> h = rows("h_pk_hist");
    assertEquals(30, h.size(), h.toString());
    for (int n = 1; n <= 5; n++) {
      assertTxn(h, n, "2026-01-01");
    }
    assertEquals(Set.of("_dt=2026-01-01"), partitionDirs("h_pk_hist"),
        "rows land in the directory of their commit's UTC date");
    Map<String, Long> kinds = new TreeMap<>();
    for (RawRecord r : RawRead.read(catalog, "public", "h_pk")) {
      kinds.merge(r.rowKind(), 1L, Long::sum);
    }
    assertEquals(Map.of("+I", 5L, "-U", 10L, "+U", 10L, "-D", 5L), kinds,
        "the main table's changelog is unchanged by the history");
  }

  @Test
  void aKeylessTableIsAnUnpartitionedOperationLogAndHasAHistoryToo() throws Exception {
    long rel = 30002;
    create(rel, "h_log", true);
    FileStoreTable main = reg.table(rel);
    assertEquals(List.of(), main.primaryKeys());
    assertEquals(List.of(), main.partitionKeys(), "the key-less log is not partitioned");
    assertEquals(List.of("id", "v", "_op", "_commit_lsn", "_lsn", "_op_seq", "_xid", "_ts"),
        main.rowType().getFieldNames());
    try (Process p = new Process(BIG)) {
      p.ev(new PgEvent.Relation(meta(rel, "h_log")), 0);
      for (int n = 1; n <= 3; n++) {
        iuud(p, rel, n);
      }
      p.mini.flushBuffered();
    }
    List<Map<String, String>> log = rows("h_log");
    assertEquals(12, log.size(), log.toString());
    for (int n = 1; n <= 3; n++) {
      final int k = n;
      List<Map<String, String>> t =
          log.stream().filter(m -> m.get("_commit_lsn").equals("" + (BASE + 1000L * k))).toList();
      assertEquals(List.of("I", "U", "U", "D"), t.stream().map(m -> m.get("_op")).toList());
      assertEquals(Arrays.asList("a", "b", null, null), t.stream().map(m -> m.get("v")).toList(),
          t.toString());
    }
    List<Map<String, String>> h = rows("h_log_hist");
    assertEquals(18, h.size());
    for (int n = 1; n <= 3; n++) {
      assertTxn(h, n, "2026-01-01");
    }
  }

  @Test
  void truncateEmptiesAPrimaryKeyTableAndIsARowInTheHistoryAndTheLog() throws Exception {
    long pk = 30003, lg = 30004;
    create(pk, "tr_pk", false);
    create(lg, "tr_log", true);
    try (Process p = new Process(BIG)) {
      p.ev(new PgEvent.Relation(meta(pk, "tr_pk")), 0);
      p.ev(new PgEvent.Relation(meta(lg, "tr_log")), 0);
      p.ev(new PgEvent.Begin(BASE + 1000, PG_TS, 1), BASE + 900);
      p.ev(new PgEvent.Insert(pk, row("1", "a")), BASE + 910);
      p.ev(new PgEvent.Insert(lg, row("1", "a")), BASE + 920);
      p.ev(new PgEvent.Commit(BASE + 1000, BASE + 1008, PG_TS), BASE + 1000);
      p.ev(new PgEvent.Begin(BASE + 2000, PG_TS, 2), BASE + 1900);
      p.ev(new PgEvent.Insert(pk, row("2", "b")), BASE + 1910);
      p.ev(new PgEvent.Truncate(List.of(pk, lg), false, false), BASE + 1920);
      p.ev(new PgEvent.Insert(pk, row("3", "c")), BASE + 1930);
      p.ev(new PgEvent.Insert(lg, row("3", "c")), BASE + 1940);
      p.ev(new PgEvent.Commit(BASE + 2000, BASE + 2008, PG_TS), BASE + 2000);
      p.mini.flushBuffered();
    }
    assertEquals(List.of("3"), mergedIds("tr_pk"));
    assertEquals(List.of("+I", "+I", "T", "+I"),
        rows("tr_pk_hist").stream().map(m -> m.get("_op")).toList(),
        "history keeps the row the truncate wiped, and the truncate itself");
    assertEquals(List.of("I", "T", "I"), rows("tr_log").stream().map(m -> m.get("_op")).toList(),
        "a log is not erased by TRUNCATE");
    Map<String, String> t = rows("tr_log").get(1);
    assertNull(t.get("id"));
    assertEquals("" + (BASE + 1920), t.get("_lsn"));
    assertEquals("2026-01-01", rows("tr_log_hist").get(1).get("_dt"),
        "a T row takes its transaction's commit date");
  }

  static List<String> mergedIds(
      String table
  ) throws Exception {
    FileStoreTable t = (FileStoreTable) catalog.getTable(Identifier.create("public", table));
    var rb = t.newReadBuilder();
    List<String> out = new ArrayList<>();
    try (var reader = rb.newRead().createReader(rb.newScan().plan());
        var it = reader.toCloseableIterator()) {
      while (it.hasNext()) {
        out.add("" + it.next().getInt(0));
      }
    }
    Collections.sort(out);
    return out;
  }

  @Test
  void aReplayRegroupedLargerPutsNothingInTheHistoryTwice() throws Exception {
    long rel = 30005;
    create(rel, "rp", false);
    try (Process p = new Process(BIG)) {
      p.ev(new PgEvent.Relation(meta(rel, "rp")), 0);
      for (int n = 1; n <= 10; n++) {
        iuud(p, rel, n);
      }
      p.mini.flushBuffered(); // committed under L10, killed before the confirm
    }
    try (Process p = new Process(BIG)) {
      p.ev(new PgEvent.Relation(meta(rel, "rp")), 0);
      for (int n = 1; n <= 12; n++) {
        iuud(p, rel, n); // the timer fired later: T1..T12 under L12
      }
      p.mini.flushBuffered();
    }
    List<Map<String, String>> h = rows("rp_hist");
    assertEquals(72, h.size(), "12 transactions of 6 images, each exactly once");
    for (int n = 1; n <= 12; n++) {
      assertTxn(h, n, "2026-01-01");
    }
  }

  /**
   * Two transactions on either side of UTC midnight in one mini-batch: two partitions, ONE Paimon
   * commit (one snapshot), each row in the partition of its own commit's UTC date; and the batch
   * replayed puts nothing in either partition twice.
   */
  @Test
  void aBatchAcrossUtcMidnightWritesBothPartitionsInOneCommitAndReplaysOnce() throws Exception {
    long rel = 30008;
    create(rel, "mid", false);
    long beforeMidnight = PG_TS + DAY - 1; // 2026-01-01T23:59:59.999999Z
    long atMidnight = PG_TS + DAY; // 2026-01-02T00:00:00Z
    for (int run = 0; run < 2; run++) {
      try (Process p = new Process(BIG)) {
        p.ev(new PgEvent.Relation(meta(rel, "mid")), 0);
        iuud(p, rel, 1, beforeMidnight);
        iuud(p, rel, 2, atMidnight);
        p.mini.flushBuffered();
      }
      assertEquals(1, snapshots("mid_hist"), "one Paimon commit for both partitions");
    }
    assertEquals(Set.of("_dt=2026-01-01", "_dt=2026-01-02"), partitionDirs("mid_hist"));
    List<Map<String, String>> h = rows("mid_hist");
    assertEquals(12, h.size(), "the replay added nothing");
    assertTxn(h, 1, "2026-01-01");
    assertTxn(h, 2, "2026-01-02");
  }

  /** The JVM's zone moves no row: _dt is the UTC date of the commit, in any zone. */
  @Test
  void theJvmTimeZoneDoesNotMoveARowToAnotherPartition() throws Exception {
    TimeZone saved = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Kiritimati")); // UTC+14
      long rel = 30009;
      create(rel, "tz_t", false);
      try (Process p = new Process(BIG)) {
        p.ev(new PgEvent.Relation(meta(rel, "tz_t")), 0);
        iuud(p, rel, 1, PG_TS + 11 * 3_600_000_000L); // 11:00Z = 01:00 next day local
        p.mini.flushBuffered();
      }
      assertEquals(Set.of("_dt=2026-01-01"), partitionDirs("tz_t_hist"));
      TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Pago_Pago")); // UTC-11
      assertTxn(rows("tz_t_hist"), 1, "2026-01-01");
    } finally {
      TimeZone.setDefault(saved);
    }
  }

  /**
   * The main and history commits of one batch are not atomic. A crash after one and before the
   * other leaves one table ahead; the replay must complete the other and repeat nothing. Both
   * orders. The first process commits to one side only, which is exactly what the process killed
   * between the two commits leaves on disk.
   */
  @Test
  void aCrashBetweenTheMainAndTheHistoryCommitIsRepairedByReplayInBothOrders() throws Exception {
    for (boolean mainFirst : new boolean[] {true, false}) {
      long rel = mainFirst ? 30006 : 30007;
      String name = mainFirst ? "cr_main" : "cr_hist";
      create(rel, name, false);
      long hk = HistoryKeys.histKey(rel);
      try (Process p = new Process(BIG, k -> mainFirst ? k == rel : k == hk)) {
        p.ev(new PgEvent.Relation(meta(rel, name)), 0);
        for (int n = 1; n <= 4; n++) {
          iuud(p, rel, n);
        }
        p.mini.flushBuffered();
      }
      OffsetStore offsets = new OffsetStore(reg, cfg);
      assertEquals(mainFirst ? BASE + 4000L : 0L, offsets.highestCommitted(rel));
      assertEquals(mainFirst ? 0L : BASE + 4000L, offsets.highestCommitted(hk));
      // The list WriterBootstrap itself resumes from (review of the history tables, I2: with the
      // history left out of it, every unit test passed while changes were lost).
      List<Long> resumeKeys = WriterBootstrap.resumeKeys(List
          .of(new RegistryRow(rel, "public", name, List.of("id"), List.of(), false, "input", 1)));
      assertEquals(0L, offsets.highestCommittedLsn(resumeKeys),
          "the resume position is the MIN over both tables");
      try (Process p = new Process(BIG)) {
        p.ev(new PgEvent.Relation(meta(rel, name)), 0);
        for (int n = 1; n <= 6; n++) {
          iuud(p, rel, n);
        }
        p.mini.flushBuffered();
      }
      List<Map<String, String>> h = rows(name + "_hist");
      assertEquals(36, h.size(), name + ": " + h.size());
      for (int n = 1; n <= 6; n++) {
        assertTxn(h, n, "2026-01-01");
      }
      Map<String, Long> kinds = new TreeMap<>();
      for (RawRecord r : RawRead.read(catalog, "public", name)) {
        kinds.merge(r.rowKind(), 1L, Long::sum);
      }
      assertEquals(Map.of("+I", 6L, "-U", 12L, "+U", 12L, "-D", 6L), kinds, name);
      assertEquals(BASE + 6000L, offsets.highestCommittedLsn(resumeKeys));
    }
  }

  /**
   * A DDL rotates the history. Transactions before it land in the archive, a transaction that
   * straddles it (BEGIN; INSERT; ALTER TABLE; INSERT; COMMIT) in both, later ones in the new table;
   * the whole stream replayed by a fresh process adds nothing anywhere; and reading the archive and
   * the current table together by a _dt range returns every change exactly once.
   */
  @Test
  void aRotationRoutesEveryChangeToItsSchemasTableAndAReplayAddsNothing() throws Exception {
    long rel = 30010;
    create(rel, "rot", false);
    long addColLsn = BASE + 2500; // inside transaction 3, between its two inserts
    for (int run = 0; run < 2; run++) {
      try (Process p = new Process(BIG)) {
        p.ev(new PgEvent.Relation(meta(rel, "rot")), 0);
        iuud(p, rel, 1);
        iuud(p, rel, 2, PG_TS + DAY + 2); // the next day
        // transaction 3 straddles the DDL
        p.ev(new PgEvent.Begin(BASE + 3000, PG_TS + DAY + 3, 3), BASE + 2400);
        p.ev(new PgEvent.Insert(rel, row("31", "before-ddl")), BASE + 2450);
        p.ev(new PgEvent.Message(true, addColLsn, "paimon", TestAuth.authed("""
            {"v":1,"op":"add_col","relid":%d,"col":{"id":3,"name":"w","type":"INT"}}"""
            .formatted(rel))), addColLsn);
        p.ev(new PgEvent.Relation(new RelationMeta(rel, "public", "rot", List.of("id", "v", "w"),
            List.of(false, false, false))), 0);
        p.ev(
            new PgEvent.Insert(rel, new TupleData(
                List.of(ColumnValue.of("32"), ColumnValue.of("after-ddl"), ColumnValue.of("7")))),
            BASE + 2600);
        p.ev(new PgEvent.Commit(BASE + 3000, BASE + 3008, PG_TS + DAY + 3), BASE + 3000);
        p.mini.flushBuffered();
      }
      reg.forget(rel); // a restart: the registry is rebuilt from scratch
      create(rel, "rot", false); // (a re-issued create is idempotent)
      ddlLsn -= 0x10;
    }
    String archive = "rot_hist_" + String.format("%016X", addColLsn);
    List<String> tables = catalog.listTables("public");
    assertTrue(tables.contains(archive), tables.toString());
    assertEquals(List.of("id", "v", "_op", "_commit_lsn", "_lsn", "_op_seq", "_xid", "_ts", "_dt"),
        ((FileStoreTable) catalog.getTable(Identifier.create("public", archive))).rowType()
            .getFieldNames(),
        "the archive keeps the old schema");
    assertEquals(
        List.of("id", "v", "w", "_op", "_commit_lsn", "_lsn", "_op_seq", "_xid", "_ts", "_dt"),
        ((FileStoreTable) catalog.getTable(Identifier.create("public", "rot_hist"))).rowType()
            .getFieldNames(),
        "the new history has the new schema");
    List<Map<String, String>> old = rows(archive);
    List<Map<String, String>> cur = rows("rot_hist");
    assertEquals(13, old.size(), "T1 and T2 (6 each) and T3's first insert: " + old);
    assertEquals("before-ddl", old.get(12).get("v"));
    assertEquals(1, cur.size(), "T3's second insert only: " + cur);
    assertEquals("after-ddl", cur.get(0).get("v"));
    assertEquals("7", cur.get(0).get("w"));
    assertEquals("" + (BASE + 3000), cur.get(0).get("_commit_lsn"));
    // reading both by a _dt range: every change once
    List<Map<String, String>> day2 = new ArrayList<>();
    for (List<Map<String, String>> t : List.of(old, cur)) {
      t.stream().filter(m -> m.get("_dt").equals("2026-01-02")).forEach(day2::add);
    }
    Set<String> keys = new HashSet<>();
    day2.forEach(m -> keys.add(m.get("_commit_lsn") + "/" + m.get("_op_seq")));
    assertEquals(8, day2.size(), "T2's six images and T3's two inserts on 2026-01-02");
    assertEquals(8, keys.size(), "no change twice across the two tables");
  }
}
