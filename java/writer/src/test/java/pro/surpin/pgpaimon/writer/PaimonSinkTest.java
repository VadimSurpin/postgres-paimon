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
        sink.truncate(RELID, 550L);
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
        Table al = catalog.getTable(Identifier.create(cfg.database(), table + "$audit_log"))
            .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
        StreamTableScan scan = al.newReadBuilder().newStreamScan();
        scan.plan();
        List<Split> splits = scan.plan().splits();
        Map<String, Long> kinds = new HashMap<>();
        try (RecordReader<InternalRow> r = al.newReadBuilder().newRead().createReader(splits)) {
            r.forEachRemaining(row -> kinds.merge(row.getString(0).toString(), 1L, Long::sum));
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
}
