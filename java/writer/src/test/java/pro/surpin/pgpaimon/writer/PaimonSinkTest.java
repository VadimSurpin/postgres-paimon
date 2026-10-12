// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.*;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PaimonSinkTest {

    @TempDir static java.nio.file.Path wh;
    static Catalog catalog;
    static SchemaRegistry reg;
    static DdlMessageHandler ddl;
    static PaimonSink sink;
    static WriterConfig cfg;

    static final long RELID = 16500;
    static final RelationMeta META = new RelationMeta(
        RELID, "public", "s", List.of("id", "v"), List.of(true, false));

    @BeforeAll
    static void setUp() throws Exception {
        Options o = new Options();
        o.set("warehouse", wh.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
        Map<String, String> env = new HashMap<>();
        env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db");
        env.put("PGPAIMON_WAREHOUSE", wh.toString());
        cfg = WriterConfig.fromEnv(env);
        reg = new SchemaRegistry(catalog, cfg);
        ddl = new DdlMessageHandler(reg, "paimon");
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

    private static PgEvent.Message msg(String json) {
        return new PgEvent.Message(true, 1L, "paimon", json.getBytes(StandardCharsets.UTF_8));
    }

    private static SinkRecord rec(RowKind k, String id, String v, long seq, int op) {
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
        try (RecordReader<InternalRow> r =
                 rb.newRead().createReader(rb.newScan().plan().splits())) {
            r.forEachRemaining(row -> values.add(row.getInt(0) + "=" + row.getString(1)));
        }
        Collections.sort(values);
        return values;
    }

    @Test @Order(1)
    void writesEveryRecordIncludingUpdateBefore() throws Exception {
        sink.flush(List.of(
            rec(RowKind.INSERT, "1", "a", 100L, 0),
            rec(RowKind.UPDATE_BEFORE, "1", "a", 200L, 1),
            rec(RowKind.UPDATE_AFTER,  "1", "b", 200L, 2)), 200L, 210L);

        Map<String, Long> kinds = changelogKinds("s");
        assertEquals(1L, kinds.getOrDefault("+I", 0L), "+I");
        assertEquals(1L, kinds.getOrDefault("-U", 0L), "-U must reach the changelog");
        assertEquals(1L, kinds.getOrDefault("+U", 0L), "+U");
    }

    @Test @Order(2)
    void theMergedReadKeepsTheUpdateAfterValue() throws Exception {
        assertEquals(List.of("1=b"), mergedValues(),
                     "sequence.field = _seq,_op_seq must make +U win over -U");
    }

    @Test @Order(3)
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

    @Test @Order(4)
    void replayAfterARestartIsAlsoFiltered() throws Exception {
        long before = latest();
        // a fresh sink and a config rebuilt from the environment: a new process
        Map<String, String> env = new HashMap<>();
        env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db");
        env.put("PGPAIMON_WAREHOUSE", wh.toString());
        WriterConfig again = WriterConfig.fromEnv(env);
        assertEquals(cfg.commitUser(), again.commitUser());
        try (PaimonSink restarted = new PaimonSink(reg, again)) {
            restarted.setRelation(RELID, META);
            restarted.flush(List.of(rec(RowKind.INSERT, "9", "z", 300L, 0)), 300L, 310L);
        }
        assertEquals(before, latest());
    }

    @Test @Order(5)
    void dataFilesLandInBucketZero() throws Exception {
        try (Stream<java.nio.file.Path> s = Files.walk(wh.resolve("default.db/s"))) {
            assertTrue(s.anyMatch(p -> p.getFileName().toString().equals("bucket-0")),
                       "files must sit under bucket-0/, as main's writer lays them out");
        }
    }

    @Test @Order(6)
    void offsetStoreReportsTheHighestCommittedIdentifier() throws Exception {
        OffsetStore store = new OffsetStore(reg, cfg);
        assertEquals(300L, store.highestCommittedLsn(List.of(RELID)));
    }

    @Test @Order(7)
    void offsetStoreReturnsZeroForAnUncommittedOrUntrackedTable() throws Exception {
        OffsetStore store = new OffsetStore(reg, cfg);
        assertEquals(0L, store.highestCommittedLsn(List.of(999999L)));
        assertEquals(0L, store.highestCommittedLsn(List.of()));
    }

    @Test @Order(8)
    void offsetStoreIgnoresSnapshotsFromOtherUsers() throws Exception {
        WriterConfig other = WriterConfig.fromEnv(Map.of(
            "PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db",
            "PGPAIMON_WAREHOUSE", wh.toString(),
            "PGPAIMON_COMMIT_USER", "someone-else"));
        assertEquals(0L, new OffsetStore(reg, other).highestCommittedLsn(List.of(RELID)));
    }

    @Test @Order(9)
    void anEmptyFlushCommitsNothingAndThrowsNothing() throws Exception {
        long before = latest();
        assertDoesNotThrow(() -> sink.flush(List.of(), 400L, 410L));
        assertEquals(before, latest());
    }

    @Test @Order(10)
    void aSchemaChangeInvalidatesTheCachedWriter() throws Exception {
        ddl.handle(msg("""
            {"v":1,"op":"add_col","relid":16500,"schema":"public","table":"s",
             "col":{"id":3,"name":"w","type":"INT"}}"""));
        RelationMeta wide = new RelationMeta(RELID, "public", "s",
            List.of("id", "v", "w"), List.of(true, false, false));
        sink.setRelation(RELID, wide);
        sink.flush(List.of(new SinkRecord(RELID, RowKind.INSERT,
            Arrays.asList("5", "q", "7"), 500L, 0, 4711L)), 500L, 510L);

        FileStoreTable t = reg.table(RELID);
        assertTrue(t.rowType().getFieldNames().contains("w"));
        boolean[] seen = { false };
        ReadBuilder rb = t.newReadBuilder();
        try (RecordReader<InternalRow> r =
                 rb.newRead().createReader(rb.newScan().plan().splits())) {
            r.forEachRemaining(row -> {
                if (row.getInt(t.rowType().getFieldNames().indexOf("id")) == 5) {
                    assertEquals(7, row.getInt(t.rowType().getFieldNames().indexOf("w")));
                    seen[0] = true;
                }
            });
        }
        assertTrue(seen[0]);
    }

    @Test @Order(11)
    void truncateEmptiesTheTableButNotTheOffset() throws Exception {
        sink.truncate(RELID, 550L);
        assertEquals(List.of(), mergedValues());
        assertEquals(550L, new OffsetStore(reg, cfg).highestCommittedLsn(List.of(RELID)),
                     "the truncate carries our user and its commit LSN");
        long snaps = latest();
        assertFalse(sink.truncate(RELID, 550L), "a filtered truncate must be reported");
        assertEquals(snaps, latest(), "a replayed truncate is filtered, not a second snapshot");
        assertEquals(cfg.commitUser(),
            reg.table(RELID).snapshotManager().latestSnapshot().commitUser());
        sink.flush(List.of(new SinkRecord(RELID, RowKind.INSERT,
            Arrays.asList("6", "after", "1"), 600L, 0, 4711L)), 600L, 610L);
        assertEquals(List.of("6=after"), mergedValues());
    }

    /**
     * Row kinds as stored in the changelog: a streaming scan over $audit_log. The first plan()
     * is always empty and is discarded; the second carries the changelog.
     */
    private static Map<String, Long> changelogKinds(String table) throws Exception {
        Map<String, Long> kinds = new HashMap<>();
        for (RawRecord r : RawRead.read(catalog, cfg.database(), table)) {
            kinds.merge(r.rowKind(), 1L, Long::sum);
        }
        return kinds;
    }

    @Test @Order(12)
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
        sink.flush(List.of(new SinkRecord(RELID, RowKind.INSERT, Arrays.asList("7", "x", null), 700L, 0, 4711L)), 700L, 710L);
        long resume = store.highestCommittedLsn(List.of(RELID, relB));
        assertTrue(resume < 700L, "must replay 700, got " + resume);
        // replay: A's commit is filtered, B's proceeds
        sink.setRelation(relB, new RelationMeta(relB, "public", "s2",
            List.of("id", "v"), List.of(true, false)));
        long aSnaps = latest();
        sink.flush(List.of(new SinkRecord(RELID, RowKind.INSERT, Arrays.asList("7", "x", null), 700L, 0, 4711L),
            new SinkRecord(relB, RowKind.INSERT, Arrays.asList("7", "x"), 700L, 1, 4711L)),
            700L, 710L);
        assertEquals(aSnaps, latest());
        assertEquals(700L, store.highestCommittedLsn(List.of(RELID, relB)));
    }

    @Test @Order(13)
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
                    Arrays.asList(String.valueOf(200 + i), "q", null), lsn, 0, 4711L)),
                    lsn, lsn + 1);
            }
        });
        assertEquals(30, mergedValues().size());
        assertEquals(1029L, new OffsetStore(reg, cfg).highestCommittedLsn(List.of(RELID)));
    }

    /** Self-contained: own warehouse, catalog, registry and sink; depends on no other test. */
    @Test @Order(14)
    void aFilteredReplayDoesNotPoisonTheWriter(@TempDir java.nio.file.Path ownWh) throws Exception {
        Options o = new Options();
        o.set("warehouse", ownWh.toString());
        Catalog cat = CatalogFactory.createCatalog(CatalogContext.create(o));
        WriterConfig c = WriterConfig.fromEnv(Map.of(
            "PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db",
            "PGPAIMON_WAREHOUSE", ownWh.toString()));
        SchemaRegistry r = new SchemaRegistry(cat, c);
        new DdlMessageHandler(r, "paimon").handle(msg("""
             {"v":1,"op":"create","relid":16600,"schema":"public","table":"iso",
              "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                  {"id":2,"name":"v","type":"STRING"}],
              "options":{"changelog-producer":"input"}}"""));
        try (PaimonSink s = new PaimonSink(r, c)) {
            s.setRelation(16600, new RelationMeta(16600, "public", "iso",
                List.of("id", "v"), List.of(true, false)));
            java.util.function.LongFunction<List<SinkRecord>> one = lsn -> List.of(
                new SinkRecord(16600, RowKind.INSERT,
                    Arrays.asList(String.valueOf(lsn), "x"), lsn, 0, 1L));
            for (long lsn = 1; lsn <= 12; lsn++) {
                s.flush(one.apply(lsn), lsn, lsn + 1);
            }
            long snaps = r.table(16600).snapshotManager().latestSnapshotId();
            s.flush(one.apply(12), 12, 13);     // filtered replay on the same sink
            assertEquals(snaps, r.table(16600).snapshotManager().latestSnapshotId());
            assertDoesNotThrow(() -> {
                for (long lsn = 13; lsn < 43; lsn++) {
                    s.flush(one.apply(lsn), lsn, lsn + 1);
                }
            });
            ReadBuilder rb = r.table(16600).newReadBuilder();
            int[] n = { 0 };
            try (RecordReader<InternalRow> rr =
                     rb.newRead().createReader(rb.newScan().plan().splits())) {
                rr.forEachRemaining(row -> n[0]++);
            }
            assertEquals(42, n[0]);
            assertEquals(42L, new OffsetStore(r, c).highestCommittedLsn(List.of(16600L)));
        }
    }

    /**
     * "Has this system ever run for this table" must come from Paimon's snapshots, not from
     * paimon.table_registry.backfill_due: a table registered while empty is not due although
     * nothing has ever been committed for it (finding 1 of the whole-branch review).
     */
    @Test @Order(15)
    void hasCommittedIsTrueOnlyForTablesThisCommitUserHasWrittenTo() throws Exception {
        OffsetStore ours = new OffsetStore(reg, cfg);
        assertTrue(ours.hasCommitted(RELID), "this test class has committed to it");
        assertFalse(ours.hasCommitted(999999L), "untracked");

        // A tracked table with a Paimon table but no snapshot of ours: never loaded.
        Options o = new Options();
        java.nio.file.Path freshWh = wh.resolveSibling("fresh-wh");
        o.set("warehouse", freshWh.toString());
        Catalog cat = CatalogFactory.createCatalog(CatalogContext.create(o));
        WriterConfig c = WriterConfig.fromEnv(Map.of(
            "PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db",
            "PGPAIMON_WAREHOUSE", freshWh.toString()));
        SchemaRegistry r = new SchemaRegistry(cat, c);
        r.createTable(17000, "public", "never_loaded", List.of("id"),
                      List.of(new DdlMessage.Col(1, "id", "INT NOT NULL")));
        assertFalse(new OffsetStore(r, c).hasCommitted(17000L),
                    "registered (and created in Paimon) but never written: not stranded");

        // Another commitUser's snapshots are not ours either.
        WriterConfig other = WriterConfig.fromEnv(Map.of(
            "PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db",
            "PGPAIMON_WAREHOUSE", wh.toString(),
            "PGPAIMON_COMMIT_USER", "someone-else"));
        assertFalse(new OffsetStore(reg, other).hasCommitted(RELID));
    }

    /**
     * Finding 5. INSERT INTO v VALUES (2, 'NaN') into a numeric(10,2) column on an accepted table permanently stops
     * replication for the whole database, and still does after the row is deleted, because the
     * WAL record is in the slot. No guardrail can refuse it -- a numeric column can always hold
     * NaN -- and there is no quarantine mechanism by decision, so the one thing that must be
     * true is that the error says what it is: the table, the column, the value, the LSN, that the
     * record is in the slot, and what the only recourse is.
     */
    @Test @Order(16)
    void anUndecodableStreamedValueIsDiagnosedAsAPoisonRecord() throws Exception {
        long poisonRelid = 16700L;
        ddl.handle(msg("""
             {"v":1,"op":"create","relid":16700,"schema":"public","table":"poison",
              "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                  {"id":2,"name":"amt","type":"DECIMAL(10, 2)"}],
              "options":{"changelog-producer":"input"}}"""));
        RelationMeta meta = new RelationMeta(poisonRelid, "public", "poison",
            List.of("id", "amt"), List.of(true, false));
        try (PaimonSink s = new PaimonSink(reg, cfg)) {
            s.setRelation(poisonRelid, meta);
            SinkRecord bad = new SinkRecord(poisonRelid, RowKind.INSERT,
                Arrays.asList("2", "NaN"), 0x1_9ABC_DEF0L, 0, 4711L);

            IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> s.flush(List.of(bad), 0x1_9ABC_DEF1L, 0x1_9ABC_DEF2L));
            String m = e.getMessage();
            assertTrue(m.contains("POISON RECORD"), m);
            assertTrue(m.contains("public.poison"), m);          // the table
            assertTrue(m.contains("'amt'"), m);                   // the column
            assertTrue(m.contains("NaN"), m);                    // the value
            assertTrue(m.contains("1/9ABCDEF0"), m);             // the LSN of the change
            assertTrue(m.contains("1/9ABCDEF1"), m);             // and of its commit
            assertTrue(m.contains("already in"), m);             // the record is in the slot
            assertTrue(m.contains(cfg.slotName()), m);
            assertTrue(m.contains("does NOT help"), m);          // deleting the row will not do
            assertTrue(m.contains("NOT TRANSIENT"), m);
            // both recoveries, the cheap one first, with the LSN already substituted in
            assertTrue(m.contains("PGPAIMON_SKIP_LSN=1/9ABCDEF0"), m);
            assertTrue(m.contains("pg_drop_replication_slot"), m);
            assertTrue(m.indexOf("PGPAIMON_SKIP_LSN") < m.indexOf("pg_drop_replication_slot"),
                       "the skip is the cheaper recovery and must be offered first");
        }
    }

    /**
     * A value that will not parse as its Paimon type at all is NOT a poison record and must not
     * be offered the skip: the cause is systematic (a drifted column type, or a session setting
     * such as bytea_output = 'escape'), so it would mis-handle every row of the column and
     * skipping one would hide it.
     */
    @Test @Order(19)
    void aMalformedValueIsNotOfferedTheSkip() throws Exception {
        long relid = 16703L;
        ddl.handle(msg("""
             {"v":1,"op":"create","relid":16703,"schema":"public","table":"malformed",
              "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                  {"id":2,"name":"b","type":"BYTES"}],
              "options":{"changelog-producer":"input"}}"""));
        try (PaimonSink s = new PaimonSink(reg, cfg)) {
            s.setRelation(relid, new RelationMeta(relid, "public", "malformed",
                List.of("id", "b"), List.of(true, false)));
            // what bytea_output = 'escape' sends: octal escapes, not \x hex
            SinkRecord bad = new SinkRecord(relid, RowKind.INSERT,
                Arrays.asList("1", "\\000\\377"), 800L, 0, 0L);
            IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> s.flush(List.of(bad), 800L, 810L));
            String m = e.getMessage();
            assertFalse(m.contains("POISON RECORD"), m);
            assertFalse(m.contains("PGPAIMON_SKIP_LSN"), m);
            assertTrue(m.contains("public.malformed"), m);
            assertTrue(m.contains("systematic mismatch"), m);
            assertTrue(m.contains("bytea_output"), m);
        }
    }

    /** The same value arriving from the initial load is not in the slot, and says so. */
    @Test @Order(17)
    void anUndecodableBackfilledValueIsDiagnosedDifferently() throws Exception {
        long relid = 16701L;
        ddl.handle(msg("""
             {"v":1,"op":"create","relid":16701,"schema":"public","table":"poison2",
              "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                  {"id":2,"name":"amt","type":"DECIMAL(10, 2)"}],
              "options":{"changelog-producer":"input"}}"""));
        try (PaimonSink s = new PaimonSink(reg, cfg)) {
            s.setRelation(relid, new RelationMeta(relid, "public", "poison2",
                List.of("id", "amt"), List.of(true, false)));
            SinkRecord bad = new SinkRecord(relid, RowKind.INSERT,
                Arrays.asList("2", "NaN"), 500L, 0, 0L);
            IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> s.flushApplied(List.of(bad), 500L, 500L));
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
     * structural fault: the record and the Paimon schema disagree about how many columns there
     * are, which is not a value Paimon cannot represent, and calling it a poison record would
     * send the reader to the wrong page.
     */
    @Test @Order(18)
    void aStructuralFailureIsNotCalledAPoisonRecord() throws Exception {
        long relid = 16702L;
        ddl.handle(msg("""
             {"v":1,"op":"create","relid":16702,"schema":"public","table":"arity",
              "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                  {"id":2,"name":"v","type":"STRING"}],
              "options":{"changelog-producer":"input"}}"""));
        try (PaimonSink s = new PaimonSink(reg, cfg)) {
            s.setRelation(relid, new RelationMeta(relid, "public", "arity",
                List.of("id", "v"), List.of(true, false)));
            // three values for a two-column relation
            SinkRecord bad = new SinkRecord(relid, RowKind.INSERT,
                Arrays.asList("1", "a", "surplus"), 700L, 0, 0L);
            IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> s.flush(List.of(bad), 700L, 710L));
            String m = e.getMessage();
            assertFalse(m.contains("POISON RECORD"), m);
            assertTrue(m.contains("public.arity"), m);
            assertTrue(m.contains("structural mismatch"), m);
            assertTrue(m.contains("3 values"), m);
        }
    }
}
