// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.*;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.DdlMessage;
import pro.surpin.pgpaimon.writer.dto.PgEvent;
import pro.surpin.pgpaimon.writer.dto.RelationMeta;
import pro.surpin.pgpaimon.writer.dto.SinkRecord;



@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PaimonSinkTest {

  @TempDir
  static java.nio.file.Path wh;
  static Catalog catalog;
  static SchemaRegistry reg;
  static DdlMessageHandler ddl;
  static PaimonSink sink;
  static WriterConfig cfg;

  static final long RELID = 16500;
  static final RelationMeta META =
      new RelationMeta(RELID, "public", "s", List.of("id", "v"), List.of(true, false));

  @BeforeAll
  static void setUp() throws Exception {
    Options o = new Options();
    o.set("warehouse", wh.toString());
    catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
    Map<String, String> env = new HashMap<>();
    env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db");
    env.put("PGPAIMON_WAREHOUSE", wh.toString());
    cfg = WriterConfigUtils.fromEnv(env);
    reg = new SchemaRegistry(catalog, cfg);
    ddl = new DdlMessageHandler(reg, "paimon", TestAuth.TOKEN);
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16500,"schema":"public","table":"s",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"v","type":"STRING"}],
         "options":{"changelog-producer":"input"}}"""));
    sink = new PaimonSink(reg, cfg);
    sink.setRelation(RELID, META);
  }

  @AfterAll
  static void tearDown() throws Exception {
    sink.close();
  }

  /**
   * A DDL message with a fresh, ascending LSN, as the stream delivers them. The LSN is the DDL
   * commit identifier, so reusing one would make the registry treat the next message as a replay
   * and skip it.
   */
  private static final java.util.concurrent.atomic.AtomicLong NEXT_LSN =
      new java.util.concurrent.atomic.AtomicLong(0x1000);

  private static PgEvent.Message msg(
      String json
  ) {
    return new PgEvent.Message(true, NEXT_LSN.addAndGet(0x10), "paimon", TestAuth.authed(json));
  }

  private static SinkRecord rec(
      RowKind k,
      String id,
      String v,
      long seq,
      int op
  ) {
    return new SinkRecord(RELID, k, Arrays.asList(id, v), seq, op, 4711L);
  }

  private static long latest() {
    Long id = reg.table(RELID).snapshotManager().latestSnapshotId();
    return id == null ? 0 : id;
  }

  private static List<String> mergedValues() throws Exception {
    FileStoreTable t = reg.table(RELID);
    List<String> values = new ArrayList<>();
    ReadBuilder rb = t.newReadBuilder();
    try (RecordReader<InternalRow> r = rb.newRead().createReader(rb.newScan().plan().splits())) {
      r.forEachRemaining(row -> values.add(row.getInt(0) + "=" + row.getString(1)));
    }
    Collections.sort(values);
    return values;
  }

  @Test
  @Order(1)
  void writesEveryRecordIncludingUpdateBefore() throws Exception {
    sink.flush(List.of(rec(RowKind.INSERT, "1", "a", 100L, 0),
        rec(RowKind.UPDATE_BEFORE, "1", "a", 200L, 1),
        rec(RowKind.UPDATE_AFTER, "1", "b", 200L, 2)), 200L, 210L);

    Map<String, Long> kinds = changelogKinds("s");
    assertEquals(1L, kinds.getOrDefault("+I", 0L), "+I");
    assertEquals(1L, kinds.getOrDefault("-U", 0L), "-U must reach the changelog");
    assertEquals(1L, kinds.getOrDefault("+U", 0L), "+U");
  }

  @Test
  @Order(2)
  void theMergedReadKeepsTheUpdateAfterValue() throws Exception {
    assertEquals(List.of("1=b"), mergedValues(),
        "sequence.field = _seq,_op_seq must make +U win over -U");
  }

  @Test
  @Order(3)
  void replayingTheSameCommitIdentifierDoesNotDuplicate() throws Exception {
    long before = latest();
    sink.flush(List.of(rec(RowKind.INSERT, "9", "z", 300L, 0)), 300L, 310L);
    long after = latest();
    assertTrue(after > before);
    Map<String, Long> kindsAfter = changelogKinds("s");

    sink.flush(List.of(rec(RowKind.INSERT, "9", "z", 300L, 0)), 300L, 310L);

    assertEquals(after, latest(), "a replayed commitIdentifier must be a no-op");
    assertFalse(sink.flushApplied(List.of(rec(RowKind.INSERT, "9", "z", 300L, 0)), 300L, 310L),
        "a filtered commit must be reported to the caller");
    assertEquals(kindsAfter, changelogKinds("s"), "no duplicate changelog entries");
    assertEquals(List.of("1=b", "9=z"), mergedValues());
  }

  @Test
  @Order(4)
  void replayAfterARestartIsAlsoFiltered() throws Exception {
    long before = latest();
    // a fresh sink and a config rebuilt from the environment: a new process
    Map<String, String> env = new HashMap<>();
    env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db");
    env.put("PGPAIMON_WAREHOUSE", wh.toString());
    WriterConfig again = WriterConfigUtils.fromEnv(env);
    assertEquals(cfg.getCommitUser(), again.getCommitUser());
    try (PaimonSink restarted = new PaimonSink(reg, again)) {
      restarted.setRelation(RELID, META);
      restarted.flush(List.of(rec(RowKind.INSERT, "9", "z", 300L, 0)), 300L, 310L);
    }
    assertEquals(before, latest());
  }

  @Test
  @Order(5)
  void dataFilesAreHashedAcrossBucketsAndThereIsNoKeyIndex() throws Exception {
    // Fixed-bucket mode is what this buys: Paimon hashes the primary key, so it needs no
    // key index. On a measured TPC-C run that index was 1.2 GB of a 1.8 GB warehouse.
    // Asserted as the ABSENCE of index/, not inferred from a size.
    try (Stream<java.nio.file.Path> s = Files.walk(wh.resolve("public.db/s"))) {
      assertTrue(s.noneMatch(p -> p.getFileName().toString().equals("index")),
          "a fixed-bucket table must have no index/ directory");
    }
    try (Stream<java.nio.file.Path> s = Files.walk(wh.resolve("public.db/s"))) {
      assertTrue(s.anyMatch(p -> p.getFileName().toString().startsWith("bucket-")),
          "data still lands under bucket-N/");
    }
  }

  /**
   * And the hashing really happens: enough keys must reach more than one bucket. Everything landing
   * in bucket-0 would mean the sink was still forcing a bucket, or that the table had ended up with
   * one bucket.
   */
  @Test
  @Order(22)
  void severalKeysReachDifferentBuckets() throws Exception {
    long relid = 16706L;
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16706,"schema":"public","table":"spread",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"v","type":"STRING"}],
         "options":{"changelog-producer":"input","bucket":"4"}}"""));
    assertEquals("4", reg.table(relid).schema().options().get("bucket"));
    try (PaimonSink s = new PaimonSink(reg, cfg)) {
      s.setRelation(relid,
          new RelationMeta(relid, "public", "spread", List.of("id", "v"), List.of(true, false)));
      List<SinkRecord> rows = new ArrayList<>();
      for (int i = 1; i <= 40; i++) {
        rows.add(new SinkRecord(relid, RowKind.INSERT, Arrays.asList(String.valueOf(i), "v" + i),
            1000L, i, 0L));
      }
      s.flush(rows, 1000L, 1010L);
    }
    Set<String> buckets = new TreeSet<>();
    try (Stream<java.nio.file.Path> st = Files.walk(wh.resolve("public.db/spread"))) {
      st.filter(p -> p.getFileName().toString().startsWith("bucket-"))
          .forEach(p -> buckets.add(p.getFileName().toString()));
    }
    assertTrue(buckets.size() > 1, "40 keys over 4 buckets must reach more than one: " + buckets);
    assertTrue(buckets.size() <= 4, "and never more than the bucket count: " + buckets);
    try (Stream<java.nio.file.Path> st = Files.walk(wh.resolve("public.db/spread"))) {
      assertTrue(st.noneMatch(p -> p.getFileName().toString().equals("index")));
    }
  }

  /** The count from the create message is honoured, not the default. */
  @Test
  @Order(23)
  void theBucketCountFromTheMessageIsHonoured() throws Exception {
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16707,"schema":"public","table":"eight",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"}],
         "options":{"changelog-producer":"input","bucket":"8"}}"""));
    assertEquals("8", reg.table(16707L).schema().options().get("bucket"));

    // and a message with no count at all still gets a fixed bucketing, never dynamic
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16708,"schema":"public","table":"nobucket",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"}],
         "options":{"changelog-producer":"input"}}"""));
    assertEquals(String.valueOf(SchemaRegistry.DEFAULT_BUCKETS),
        reg.table(16708L).schema().options().get("bucket"));
  }

  @Test
  @Order(6)
  void offsetStoreReportsTheHighestCommittedIdentifier() throws Exception {
    OffsetStore store = new OffsetStore(reg, cfg);
    assertEquals(300L, store.highestCommittedLsn(List.of(RELID)));
  }

  @Test
  @Order(7)
  void offsetStoreReturnsZeroForAnUncommittedOrUntrackedTable() throws Exception {
    OffsetStore store = new OffsetStore(reg, cfg);
    assertEquals(0L, store.highestCommittedLsn(List.of(999999L)));
    assertEquals(0L, store.highestCommittedLsn(List.of()));
  }

  @Test
  @Order(8)
  void offsetStoreIgnoresSnapshotsFromOtherUsers() throws Exception {
    WriterConfig other =
        WriterConfigUtils.fromEnv(Map.of("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db",
            "PGPAIMON_WAREHOUSE", wh.toString(), "PGPAIMON_COMMIT_USER", "someone-else"));
    assertEquals(0L, new OffsetStore(reg, other).highestCommittedLsn(List.of(RELID)));
  }

  @Test
  @Order(9)
  void anEmptyFlushCommitsNothingAndThrowsNothing() throws Exception {
    long before = latest();
    assertDoesNotThrow(() -> sink.flush(List.of(), 400L, 410L));
    assertEquals(before, latest());
  }

  @Test
  @Order(10)
  void aSchemaChangeInvalidatesTheCachedWriter() throws Exception {
    ddl.handle(msg("""
        {"v":1,"op":"add_col","relid":16500,"schema":"public","table":"s",
         "col":{"id":3,"name":"w","type":"INT"}}"""));
    RelationMeta wide = new RelationMeta(RELID, "public", "s", List.of("id", "v", "w"),
        List.of(true, false, false));
    sink.setRelation(RELID, wide);
    sink.flush(
        List.of(
            new SinkRecord(RELID, RowKind.INSERT, Arrays.asList("5", "q", "7"), 500L, 0, 4711L)),
        500L, 510L);

    FileStoreTable t = reg.table(RELID);
    assertTrue(t.rowType().getFieldNames().contains("w"));
    boolean[] seen = {false};
    ReadBuilder rb = t.newReadBuilder();
    try (RecordReader<InternalRow> r = rb.newRead().createReader(rb.newScan().plan().splits())) {
      r.forEachRemaining(row -> {
        if (row.getInt(t.rowType().getFieldNames().indexOf("id")) == 5) {
          assertEquals(7, row.getInt(t.rowType().getFieldNames().indexOf("w")));
          seen[0] = true;
        }
      });
    }
    assertTrue(seen[0]);
  }

  @Test
  @Order(11)
  void truncateEmptiesTheTableButNotTheOffset() throws Exception {
    sink.truncate(RELID, 550L);
    assertEquals(List.of(), mergedValues());
    assertEquals(550L, new OffsetStore(reg, cfg).highestCommittedLsn(List.of(RELID)),
        "the truncate carries our user and its commit LSN");
    long snaps = latest();
    assertFalse(sink.truncate(RELID, 550L), "a filtered truncate must be reported");
    assertEquals(snaps, latest(), "a replayed truncate is filtered, not a second snapshot");
    assertEquals(cfg.getCommitUser(),
        reg.table(RELID).snapshotManager().latestSnapshot().commitUser());
    sink.flush(List.of(
        new SinkRecord(RELID, RowKind.INSERT, Arrays.asList("6", "after", "1"), 600L, 0, 4711L)),
        600L, 610L);
    assertEquals(List.of("6=after"), mergedValues());
  }

  /**
   * Row kinds as stored in the changelog: a streaming scan over $audit_log. The first plan() is
   * always empty and is discarded; the second carries the changelog.
   */
  private static Map<String, Long> changelogKinds(
      String table
  ) throws Exception {
    Map<String, Long> kinds = new HashMap<>();
    for (RawRecord r : RawRead.read(catalog, "public", table)) {
      kinds.merge(r.rowKind(), 1L, Long::sum);
    }
    return kinds;
  }

  @Test
  @Order(12)
  void aPartlyCommittedTransactionResumesBeforeIt() throws Exception {
    long relB = 16501;
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16501,"schema":"public","table":"s2",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"v","type":"STRING"}],
         "options":{"changelog-producer":"input"}}"""));
    OffsetStore store = new OffsetStore(reg, cfg);
    assertEquals(0L, store.highestCommittedLsn(List.of(RELID, relB)),
        "B has never committed: resume from the start");
    // transaction at 700 reached A only (crash before B)
    sink.flush(
        List.of(
            new SinkRecord(RELID, RowKind.INSERT, Arrays.asList("7", "x", null), 700L, 0, 4711L)),
        700L, 710L);
    long resume = store.highestCommittedLsn(List.of(RELID, relB));
    assertTrue(resume < 700L, "must replay 700, got " + resume);
    // replay: A's commit is filtered, B's proceeds
    sink.setRelation(relB,
        new RelationMeta(relB, "public", "s2", List.of("id", "v"), List.of(true, false)));
    long aSnaps = latest();
    sink.flush(
        List.of(
            new SinkRecord(RELID, RowKind.INSERT, Arrays.asList("7", "x", null), 700L, 0, 4711L),
            new SinkRecord(relB, RowKind.INSERT, Arrays.asList("7", "x"), 700L, 1, 4711L)),
        700L, 710L);
    assertEquals(aSnaps, latest());
    assertEquals(700L, store.highestCommittedLsn(List.of(RELID, relB)));
  }

  @Test
  @Order(13)
  void manyCommitsAfterATruncateSurviveCompaction() throws Exception {
    for (int i = 0; i < 12; i++) {
      long lsn = 800 + i;
      sink.flush(List.of(new SinkRecord(RELID, RowKind.INSERT,
          Arrays.asList(String.valueOf(100 + i), "p", null), lsn, 0, 4711L)), lsn, lsn + 1);
    }
    sink.truncate(RELID, 900L);
    assertDoesNotThrow(() -> {
      for (int i = 0; i < 30; i++) {
        long lsn = 1000 + i;
        sink.flush(List.of(new SinkRecord(RELID, RowKind.INSERT,
            Arrays.asList(String.valueOf(200 + i), "q", null), lsn, 0, 4711L)), lsn, lsn + 1);
      }
    });
    assertEquals(30, mergedValues().size());
    assertEquals(1029L, new OffsetStore(reg, cfg).highestCommittedLsn(List.of(RELID)));
  }

  /** Self-contained: own warehouse, catalog, registry and sink; depends on no other test. */
  @Test
  @Order(14)
  void aFilteredReplayDoesNotPoisonTheWriter(
      @TempDir java.nio.file.Path ownWh
  ) throws Exception {
    Options o = new Options();
    o.set("warehouse", ownWh.toString());
    Catalog cat = CatalogFactory.createCatalog(CatalogContext.create(o));
    WriterConfig c = WriterConfigUtils.fromEnv(Map.of("PGPAIMON_JDBC_URL",
        "jdbc:postgresql://localhost/db", "PGPAIMON_WAREHOUSE", ownWh.toString()));
    SchemaRegistry r = new SchemaRegistry(cat, c);
    new DdlMessageHandler(r, "paimon", TestAuth.TOKEN).handle(msg("""
        {"v":1,"op":"create","relid":16600,"schema":"public","table":"iso",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"v","type":"STRING"}],
         "options":{"changelog-producer":"input"}}"""));
    try (PaimonSink s = new PaimonSink(r, c)) {
      s.setRelation(16600,
          new RelationMeta(16600, "public", "iso", List.of("id", "v"), List.of(true, false)));
      java.util.function.LongFunction<List<SinkRecord>> one = lsn -> List.of(new SinkRecord(16600,
          RowKind.INSERT, Arrays.asList(String.valueOf(lsn), "x"), lsn, 0, 1L));
      for (long lsn = 1; lsn <= 12; lsn++) {
        s.flush(one.apply(lsn), lsn, lsn + 1);
      }
      long snaps = r.table(16600).snapshotManager().latestSnapshotId();
      s.flush(one.apply(12), 12, 13); // filtered replay on the same sink
      assertEquals(snaps, r.table(16600).snapshotManager().latestSnapshotId());
      assertDoesNotThrow(() -> {
        for (long lsn = 13; lsn < 43; lsn++) {
          s.flush(one.apply(lsn), lsn, lsn + 1);
        }
      });
      ReadBuilder rb = r.table(16600).newReadBuilder();
      int[] n = {0};
      try (RecordReader<InternalRow> rr = rb.newRead().createReader(rb.newScan().plan().splits())) {
        rr.forEachRemaining(row -> n[0]++);
      }
      assertEquals(42, n[0]);
      assertEquals(42L, new OffsetStore(r, c).highestCommittedLsn(List.of(16600L)));
    }
  }

  /**
   * "Has this system ever run for this table" must come from Paimon's snapshots, not from
   * paimon.table_registry.backfill_due: a table registered while empty is not due although nothing
   * has ever been committed for it (finding 1 of the whole-branch review).
   */
  @Test
  @Order(15)
  void hasCommittedIsTrueOnlyForTablesThisCommitUserHasWrittenTo() throws Exception {
    OffsetStore ours = new OffsetStore(reg, cfg);
    assertTrue(ours.hasCommitted(RELID), "this test class has committed to it");
    assertFalse(ours.hasCommitted(999999L), "untracked");

    // A tracked table with a Paimon table but no snapshot of ours: never loaded.
    Options o = new Options();
    java.nio.file.Path freshWh = wh.resolveSibling("fresh-wh");
    o.set("warehouse", freshWh.toString());
    Catalog cat = CatalogFactory.createCatalog(CatalogContext.create(o));
    WriterConfig c = WriterConfigUtils.fromEnv(Map.of("PGPAIMON_JDBC_URL",
        "jdbc:postgresql://localhost/db", "PGPAIMON_WAREHOUSE", freshWh.toString()));
    SchemaRegistry r = new SchemaRegistry(cat, c);
    r.createTable(17000, "public", "never_loaded", List.of("id"),
        List.of(new DdlMessage.Col(1, "id", "INT NOT NULL")));
    assertFalse(new OffsetStore(r, c).hasCommitted(17000L),
        "registered (and created in Paimon) but never written: not stranded");

    // Another commitUser's snapshots are not ours either.
    WriterConfig other =
        WriterConfigUtils.fromEnv(Map.of("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db",
            "PGPAIMON_WAREHOUSE", wh.toString(), "PGPAIMON_COMMIT_USER", "someone-else"));
    assertFalse(new OffsetStore(reg, other).hasCommitted(RELID));
  }

  /**
   * Finding 5. INSERT INTO v VALUES (2, 'NaN') into a numeric(10,2) column on an accepted table
   * permanently stops replication for the whole database, and still does after the row is deleted,
   * because the WAL record is in the slot. No guardrail can refuse it -- a numeric column can
   * always hold NaN -- and there is no quarantine mechanism by decision, so the one thing that must
   * be true is that the error says what it is: the table, the column, the value, the LSN, that the
   * record is in the slot, and what the only recourse is.
   */
  @Test
  @Order(16)
  void anUndecodableStreamedValueIsDiagnosedAsAPoisonRecord() throws Exception {
    long poisonRelid = 16700L;
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16700,"schema":"public","table":"poison",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"amt","type":"DECIMAL(10, 2)"}],
         "options":{"changelog-producer":"input"}}"""));
    RelationMeta meta = new RelationMeta(poisonRelid, "public", "poison", List.of("id", "amt"),
        List.of(true, false));
    try (PaimonSink s = new PaimonSink(reg, cfg)) {
      s.setRelation(poisonRelid, meta);
      SinkRecord bad = new SinkRecord(poisonRelid, RowKind.INSERT, Arrays.asList("2", "NaN"),
          0x1_9ABC_DEF0L, 0, 4711L);

      IllegalStateException e = assertThrows(IllegalStateException.class,
          () -> s.flush(List.of(bad), 0x1_9ABC_DEF1L, 0x1_9ABC_DEF2L));
      String m = e.getMessage();
      assertTrue(m.contains("POISON RECORD"), m);
      assertTrue(m.contains("public.poison"), m); // the table
      assertTrue(m.contains("'amt'"), m); // the column
      assertTrue(m.contains("NaN"), m); // the value
      assertTrue(m.contains("1/9ABCDEF0"), m); // the LSN of the change
      assertTrue(m.contains("1/9ABCDEF1"), m); // and of its commit
      assertTrue(m.contains("already in"), m); // the record is in the slot
      assertTrue(m.contains(cfg.getSlotName()), m);
      assertTrue(m.contains("does NOT help"), m); // deleting the row will not do
      assertTrue(m.contains("NOT TRANSIENT"), m);
      // both recoveries, the cheap one first, with the LSN already substituted in
      assertTrue(m.contains("PGPAIMON_SKIP_LSN=1/9ABCDEF0"), m);
      assertTrue(m.contains("pg_drop_replication_slot"), m);
      assertTrue(m.indexOf("PGPAIMON_SKIP_LSN") < m.indexOf("pg_drop_replication_slot"),
          "the skip is the cheaper recovery and must be offered first");
    }
  }

  /**
   * A value that will not parse as its Paimon type at all is NOT a poison record and must not be
   * offered the skip: the cause is systematic (a drifted column type, or a session setting such as
   * bytea_output = 'escape'), so it would mis-handle every row of the column and skipping one would
   * hide it.
   */
  @Test
  @Order(19)
  void aMalformedValueIsNotOfferedTheSkip() throws Exception {
    long relid = 16703L;
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16703,"schema":"public","table":"malformed",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"b","type":"BYTES"}],
         "options":{"changelog-producer":"input"}}"""));
    try (PaimonSink s = new PaimonSink(reg, cfg)) {
      s.setRelation(relid,
          new RelationMeta(relid, "public", "malformed", List.of("id", "b"), List.of(true, false)));
      // what bytea_output = 'escape' sends: octal escapes, not \x hex
      SinkRecord bad =
          new SinkRecord(relid, RowKind.INSERT, Arrays.asList("1", "\\000\\377"), 800L, 0, 0L);
      IllegalStateException e =
          assertThrows(IllegalStateException.class, () -> s.flush(List.of(bad), 800L, 810L));
      String m = e.getMessage();
      assertFalse(m.contains("POISON RECORD"), m);
      assertFalse(m.contains("PGPAIMON_SKIP_LSN"), m);
      assertTrue(m.contains("public.malformed"), m);
      assertTrue(m.contains("systematic mismatch"), m);
      assertTrue(m.contains("bytea_output"), m);
    }
  }

  /** The same value arriving from the initial load is not in the slot, and says so. */
  @Test
  @Order(17)
  void anUndecodableBackfilledValueIsDiagnosedDifferently() throws Exception {
    long relid = 16701L;
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16701,"schema":"public","table":"poison2",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"amt","type":"DECIMAL(10, 2)"}],
         "options":{"changelog-producer":"input"}}"""));
    try (PaimonSink s = new PaimonSink(reg, cfg)) {
      s.setRelation(relid,
          new RelationMeta(relid, "public", "poison2", List.of("id", "amt"), List.of(true, false)));
      SinkRecord bad =
          new SinkRecord(relid, RowKind.INSERT, Arrays.asList("2", "NaN"), 500L, 0, 0L);
      IllegalStateException e =
          assertThrows(IllegalStateException.class, () -> s.flushApplied(List.of(bad), 500L, 500L));
      String m = e.getMessage();
      assertFalse(m.contains("POISON RECORD"), m);
      assertTrue(m.contains("initial backfill"), m);
      assertTrue(m.contains("nothing is stuck in the slot"), m);
      // Both causes, because the remedy differs and only one of them is about the row.
      assertTrue(m.contains("correcting that row and reloading fixes it"), m);
      assertTrue(m.contains("schema drift"), m);
      assertTrue(m.contains("re-register the table"), m);
    }
  }

  /**
   * Not every RuntimeException from RowBuilder is a poison record. An arity mismatch is a
   * structural fault: the record and the Paimon schema disagree about how many columns there are,
   * which is not a value Paimon cannot represent, and calling it a poison record would send the
   * reader to the wrong page.
   */
  @Test
  @Order(18)
  void aStructuralFailureIsNotCalledAPoisonRecord() throws Exception {
    long relid = 16702L;
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16702,"schema":"public","table":"arity",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"v","type":"STRING"}],
         "options":{"changelog-producer":"input"}}"""));
    try (PaimonSink s = new PaimonSink(reg, cfg)) {
      s.setRelation(relid,
          new RelationMeta(relid, "public", "arity", List.of("id", "v"), List.of(true, false)));
      // three values for a two-column relation
      SinkRecord bad =
          new SinkRecord(relid, RowKind.INSERT, Arrays.asList("1", "a", "surplus"), 700L, 0, 0L);
      IllegalStateException e =
          assertThrows(IllegalStateException.class, () -> s.flush(List.of(bad), 700L, 710L));
      String m = e.getMessage();
      assertFalse(m.contains("POISON RECORD"), m);
      assertTrue(m.contains("public.arity"), m);
      assertTrue(m.contains("structural mismatch"), m);
      assertTrue(m.contains("3 values"), m);
    }
  }

  /**
   * The encoded infinity must survive a WRITE to a Paimon file and a read back, not merely a
   * round-trip through the Timestamp object.
   *
   * <p>
   * This is the test whose absence let a broken mapping through: "infinity" appeared in one test
   * file that never wrote a row, so none of the unit tests and none of the 186 acceptance checks
   * ever put a decoded infinity on disk. Parquet's TimestampMicrosWriter calls Timestamp.toMicros()
   * per row, and the previous target was ~30 years inside that method's overflow window -- so every
   * object-level assertion passed while the writer could not store the value, and the failure
   * arrived forty frames deep in prepareCommit where neither poison arm catches it.
   */
  @Test
  @Order(20)
  void theInfinityEncodingSurvivesAWriteAndReadBackThroughParquet() throws Exception {
    long relid = 16704L;
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16704,"schema":"public","table":"inf",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"d","type":"DATE"},
                             {"id":3,"name":"ts","type":"TIMESTAMP"},
                             {"id":4,"name":"tstz",
                              "type":"TIMESTAMP WITH LOCAL TIME ZONE"}],
         "options":{"changelog-producer":"input"}}"""));
    RelationMeta meta = new RelationMeta(relid, "public", "inf", List.of("id", "d", "ts", "tstz"),
        List.of(true, false, false, false));
    try (PaimonSink s = new PaimonSink(reg, cfg)) {
      s.setRelation(relid, meta);
      // exactly what PostgreSQL 18 prints for these values
      s.flush(List.of(
          new SinkRecord(relid, RowKind.INSERT,
              Arrays.asList("1", "infinity", "infinity", "infinity"), 900L, 0, 0L),
          new SinkRecord(relid, RowKind.INSERT,
              Arrays.asList("2", "-infinity", "-infinity", "-infinity"), 900L, 1, 0L),
          new SinkRecord(relid, RowKind.INSERT,
              Arrays.asList("3", "2026-10-09", "2026-10-09 13:46:40", "2026-10-09 13:46:40+00"),
              900L, 2, 0L)),
          900L, 910L);
    }

    // read it back off disk, through the ordinary reader
    FileStoreTable t = reg.table(relid);
    Map<Integer, List<Object>> rows = new LinkedHashMap<>();
    ReadBuilder rb = t.newReadBuilder();
    try (RecordReader<InternalRow> r = rb.newRead().createReader(rb.newScan().plan().splits())) {
      r.forEachRemaining(row -> rows.put(row.getInt(0),
          Arrays.asList(row.getInt(1), row.getTimestamp(2, 6), row.getTimestamp(3, 6))));
    }
    assertEquals(3, rows.size(), "all three rows must be on disk");

    // +infinity
    assertEquals(2145042905, rows.get(1).get(0), "date +infinity = 5874897-12-31");
    assertEquals("+294247-01-10T04:00:54.775807",
        ((org.apache.paimon.data.Timestamp) rows.get(1).get(1)).toLocalDateTime().toString());
    assertEquals("+294247-01-10T04:00:54.775807",
        ((org.apache.paimon.data.Timestamp) rows.get(1).get(2)).toLocalDateTime().toString());
    // -infinity: PostgreSQL's own minimum, 4714-11-24 BC
    assertEquals(-2440588, rows.get(2).get(0));
    assertEquals("-4713-11-24T00:00",
        ((org.apache.paimon.data.Timestamp) rows.get(2).get(1)).toLocalDateTime().toString());
    // and the ordinary row, read back unchanged, sorting between them
    assertTrue((int) rows.get(2).get(0) < (int) rows.get(3).get(0));
    assertTrue((int) rows.get(3).get(0) < (int) rows.get(1).get(0));
  }

  /**
   * And the other half: a timestamp Paimon cannot store is refused as a diagnosed POISON RECORD,
   * not as an ArithmeticException from inside prepareCommit.
   */
  @Test
  @Order(21)
  void aTimestampAbovePaimonsRangeIsADiagnosedPoisonRecord() throws Exception {
    long relid = 16705L;
    ddl.handle(msg("""
        {"v":1,"op":"create","relid":16705,"schema":"public","table":"toobig",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"ts","type":"TIMESTAMP"}],
         "options":{"changelog-producer":"input"}}"""));
    try (PaimonSink s = new PaimonSink(reg, cfg)) {
      s.setRelation(relid,
          new RelationMeta(relid, "public", "toobig", List.of("id", "ts"), List.of(true, false)));
      SinkRecord bad = new SinkRecord(relid, RowKind.INSERT,
          Arrays.asList("1", "294276-12-31 23:59:59.999999"), 0x1_9ABC_DEF0L, 0, 0L);
      IllegalStateException e = assertThrows(IllegalStateException.class,
          () -> s.flush(List.of(bad), 0x1_9ABC_DEF1L, 0x1_9ABC_DEF2L));
      String m = e.getMessage();
      assertTrue(m.contains("POISON RECORD"), m);
      assertTrue(m.contains("public.toobig"), m);
      assertTrue(m.contains("294276-12-31 23:59:59.999999"), m); // the value
      assertTrue(m.contains("PGPAIMON_SKIP_LSN=1/9ABCDEF0"), m); // and it is skippable
    }
  }


  /**
   * BEGIN; INSERT; ALTER TABLE ADD COLUMN; INSERT; COMMIT. Both records reach the sink in one
   * flush, after the ALTER has been applied, but the first was decoded under the two-column
   * relation. Building it against the three-column one used to kill the writer for good.
   */
  @Test
  void anAlterInsideATransactionDoesNotRebuildEarlierRecordsAgainstTheNewColumns(
      @TempDir java.nio.file.Path ownWh
  ) throws Exception {
    Options o = new Options();
    o.set("warehouse", ownWh.toString());
    Catalog cat = CatalogFactory.createCatalog(CatalogContext.create(o));
    WriterConfig c = WriterConfigUtils.fromEnv(Map.of("PGPAIMON_JDBC_URL",
        "jdbc:postgresql://localhost/db", "PGPAIMON_WAREHOUSE", ownWh.toString()));
    SchemaRegistry r = new SchemaRegistry(cat, c);
    DdlMessageHandler h = new DdlMessageHandler(r, "paimon", TestAuth.TOKEN);
    h.handle(msg("""
        {"v":1,"op":"create","relid":16700,"schema":"public","table":"alt",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"v","type":"STRING"}],
         "options":{"changelog-producer":"input"}}"""));
    RelationMeta before =
        new RelationMeta(16700, "public", "alt", List.of("id", "v"), List.of(true, false));
    RelationMeta after = new RelationMeta(16700, "public", "alt", List.of("id", "v", "c"),
        List.of(true, false, false));
    try (PaimonSink s = new PaimonSink(r, c)) {
      s.setRelation(16700, before);
      h.handle(msg("""
          {"v":1,"op":"add_col","relid":"16700",
           "col":{"id":3,"name":"c","type":"INT"}}"""));
      s.setRelation(16700, after);
      s.flush(List.of(
          new SinkRecord(16700, RowKind.INSERT, Arrays.asList("1", "a"), 10L, 0, 1L, before),
          new SinkRecord(16700, RowKind.INSERT, Arrays.asList("2", "b", "7"), 11L, 1, 1L, after)),
          20L, 21L);

      ReadBuilder rb = r.table(16700).newReadBuilder();
      List<String> got = new ArrayList<>();
      int ci = r.table(16700).rowType().getFieldIndex("c");
      InternalRow.FieldGetter cg =
          InternalRow.createFieldGetter(r.table(16700).rowType().getTypeAt(ci), ci);
      try (RecordReader<InternalRow> rr = rb.newRead().createReader(rb.newScan().plan().splits())) {
        rr.forEachRemaining(row -> got.add(
            row.getInt(0) + "|" + row.getString(1) + "|" + String.valueOf(cg.getFieldOrNull(row))));
      }
      Collections.sort(got);
      assertEquals(List.of("1|a|null", "2|b|7"), got);
    }
  }

  /**
   * Review finding I4: BEGIN; INSERT; ALTER TABLE RENAME COLUMN v TO w; INSERT; COMMIT. Both
   * records are built at the commit, against the renamed schema, by name: the first one's value was
   * discarded and Paimon held NULL where PostgreSQL held it. And with a drop and a re-add of the
   * same name, the dropped column's value must NOT land in the new column.
   */
  @Test
  void aRecordDecodedBeforeARenameOrDropInItsTransactionIsBuiltWithTodaysNames(
      @TempDir java.nio.file.Path ownWh
  ) throws Exception {
    Options o = new Options();
    o.set("warehouse", ownWh.toString());
    Catalog cat = CatalogFactory.createCatalog(CatalogContext.create(o));
    WriterConfig c = WriterConfigUtils.fromEnv(Map.of("PGPAIMON_JDBC_URL",
        "jdbc:postgresql://localhost/db", "PGPAIMON_WAREHOUSE", ownWh.toString()));
    SchemaRegistry r = new SchemaRegistry(cat, c);
    DdlMessageHandler h = new DdlMessageHandler(r, "paimon", TestAuth.TOKEN);
    h.handle(msg("""
        {"v":1,"op":"create","relid":16800,"schema":"public","table":"ren",
         "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"v","type":"STRING"},
                             {"id":3,"name":"x","type":"STRING"}],
         "options":{"changelog-producer":"input"}}"""));
    RelationMeta before = new RelationMeta(16800, "public", "ren", List.of("id", "v", "x"),
        List.of(true, false, false));
    RelationMeta after = new RelationMeta(16800, "public", "ren", List.of("id", "w", "x"),
        List.of(true, false, false));
    try (PaimonSink s = new PaimonSink(r, c)) {
      s.setRelation(16800, before);
      long first = NEXT_LSN.get() + 1; // decoded before the DDL below
      h.handle(msg("""
          {"v":1,"op":"rename_col","relid":"16800","id":2,"from":"v","to":"w"}"""));
      h.handle(msg("""
          {"v":1,"op":"drop_col","relid":"16800","name":"x"}"""));
      h.handle(msg("""
          {"v":1,"op":"add_col","relid":"16800","col":{"id":4,"name":"x","type":"INT"}}"""));
      RelationMeta now = new RelationMeta(16800, "public", "ren", List.of("id", "w", "x"),
          List.of(true, false, false));
      long later = NEXT_LSN.get() + 1;
      s.setRelation(16800, now);
      s.flush(List.of(
          new SinkRecord(16800, RowKind.INSERT, Arrays.asList("20", "before rename", "old x"),
              first, 0, 1L, before),
          new SinkRecord(16800, RowKind.INSERT, Arrays.asList("21", "after rename", "5"), later, 1,
              1L, now)),
          later + 1, later + 2);

      FileStoreTable t = r.table(16800);
      ReadBuilder rb = t.newReadBuilder();
      List<String> got = new ArrayList<>();
      int wi = t.rowType().getFieldIndex("w");
      int xi = t.rowType().getFieldIndex("x");
      InternalRow.FieldGetter wg = InternalRow.createFieldGetter(t.rowType().getTypeAt(wi), wi);
      InternalRow.FieldGetter xg = InternalRow.createFieldGetter(t.rowType().getTypeAt(xi), xi);
      try (RecordReader<InternalRow> rr = rb.newRead().createReader(rb.newScan().plan().splits())) {
        rr.forEachRemaining(row -> got
            .add(row.getInt(0) + "|" + wg.getFieldOrNull(row) + "|" + xg.getFieldOrNull(row)));
      }
      Collections.sort(got);
      assertEquals(List.of("20|before rename|null", "21|after rename|5"), got);
    }
    // the translation itself: a rename applies, a drop discards, a later change does not
    assertEquals(List.of("id", "w", SchemaRegistry.DROPPED),
        r.asOfNow(16800, before, 0x1).columnNames());
    assertSame(after, r.asOfNow(16800, after, Long.MAX_VALUE), "nothing after it: unchanged");
    assertEquals(List.of("id", "v", "x"),
        r.asOfNow(HistoryKeys.histKey(16800), before, 0x1).columnNames(),
        "history tables keep the names they were decoded under");
  }
}
