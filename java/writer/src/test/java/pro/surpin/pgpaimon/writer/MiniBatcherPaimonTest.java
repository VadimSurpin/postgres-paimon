// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.paimon.catalog.*;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Rule 4 against the real Paimon: a replay that regroups the same transactions must not put any
 * of them in the table twice. The changelog is read raw -- {@code changelog-producer = input}
 * writes every input record, so it is exactly where a duplicate would show.
 */
class MiniBatcherPaimonTest {

    @TempDir static java.nio.file.Path wh;
    static Catalog catalog;
    static SchemaRegistry reg;
    static WriterConfig cfg;
    static final long GUARDED = 17001, UNGUARDED = 17002, A = 17003, B = 17004, C = 17005;
    static long ddlLsn = 0x1000;

    static final MiniBatcher.Limits BIG = new MiniBatcher.Limits(1_000_000, 1L << 40, 3_600_000);

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
        DdlMessageHandler ddl = new DdlMessageHandler(reg, "paimon", TestAuth.TOKEN);
        for (long relid : new long[] {GUARDED, UNGUARDED, A, B, C}) {
            ddlLsn += 0x10;
            ddl.handle(new PgEvent.Message(true, ddlLsn, "paimon", ("""
                {"v":1,"op":"create","relid":%d,"schema":"public","table":"t%d",
                 "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                     {"id":2,"name":"v","type":"STRING"}],
                 "options":{"changelog-producer":"input"}}""".formatted(relid, relid))
                .transform(TestAuth::authed)));
        }
    }

    /** A fresh process: a sink and a batcher that know nothing but what Paimon holds. */
    static final class Process implements AutoCloseable {
        final PaimonSink sink = new PaimonSink(reg, cfg);
        final List<Long> confirmed = new ArrayList<>();
        final MiniBatcher batcher;

        Process(MiniBatcher.Limits limits) {
            for (long relid : new long[] {GUARDED, UNGUARDED, A, B, C}) {
                sink.setRelation(relid, new RelationMeta(relid, "public", "t" + relid,
                    List.of("id", "v"), List.of(true, false)));
            }
            OffsetStore store = new OffsetStore(reg, cfg);
            batcher = new MiniBatcher(Main.confirmingFlusher(sink, confirmed::add),
                store::highestCommitted, limits);
        }

        @Override public void close() throws Exception {
            sink.close();
        }
    }

    /**
     * Transaction n (commit LSN 1000n): inserts id n; every even transaction also updates id n-1.
     * Distinct _seq per change, as the stream gives.
     */
    static List<SinkRecord> txn(long relid, int n) {
        List<SinkRecord> rs = new ArrayList<>();
        rs.add(new SinkRecord(relid, RowKind.INSERT, Arrays.asList("" + n, "v" + n), 1000L * n - 9, 0, 1L));
        if (n % 2 == 0) {
            rs.add(new SinkRecord(relid, RowKind.UPDATE_BEFORE,
                Arrays.asList("" + (n - 1), "v" + (n - 1)), 1000L * n - 8, 1, 1L));
            rs.add(new SinkRecord(relid, RowKind.UPDATE_AFTER,
                Arrays.asList("" + (n - 1), "u" + n), 1000L * n - 8, 2, 1L));
        }
        return rs;
    }

    static void feed(MiniBatcher b, long relid, int from, int to) throws Exception {
        for (int n = from; n <= to; n++) {
            b.flush(txn(relid, n), 1000L * n, 1000L * n + 10);
        }
    }

    static Map<String, Long> kinds(long relid) throws Exception {
        Map<String, Long> m = new TreeMap<>();
        for (RawRecord r : RawRead.read(catalog, "public", "t" + relid)) {
            m.merge(r.rowKind(), 1L, Long::sum);
        }
        return m;
    }

    @Test
    void theChangelogHazardIsRealWithoutRuleFour() throws Exception {
        // The control: this is what the replay would do if the second run's batch were handed to
        // the sink as it stands. 12000 > 10000, so Paimon's own filter lets it through.
        try (Process first = new Process(BIG)) {
            feed(first.batcher, UNGUARDED, 1, 10);
            first.batcher.flushBuffered();
        }
        try (Process replay = new Process(BIG)) {
            List<SinkRecord> all = new ArrayList<>();
            for (int n = 1; n <= 12; n++) {
                all.addAll(txn(UNGUARDED, n));
            }
            replay.sink.flush(all, 12000L, 12010L);
        }
        assertEquals(10L + 12L, kinds(UNGUARDED).get("+I"),
            "T1..T10 landed twice in the changelog: " + kinds(UNGUARDED));
    }

    @Test
    void aReplayGroupedLargerLeavesEveryTransactionInTheChangelogExactlyOnce() throws Exception {
        try (Process first = new Process(BIG)) {
            feed(first.batcher, GUARDED, 1, 10);
            first.batcher.flushBuffered();    // committed under L10; killed before the confirm
        }
        try (Process replay = new Process(BIG)) {
            feed(replay.batcher, GUARDED, 1, 12);   // the timer fired later: T1..T12 under L12
            replay.batcher.flushBuffered();
            assertEquals(List.of(12010L), replay.confirmed);
        }
        Map<String, Long> k = kinds(GUARDED);
        assertEquals(12L, k.get("+I"), "one +I per inserted id: " + k);
        assertEquals(6L, k.get("-U"), k.toString());
        assertEquals(6L, k.get("+U"), k.toString());
        assertEquals(new OffsetStore(reg, cfg).highestCommitted(GUARDED), 12000L);
    }

    @Test
    void aReplayGroupedSmallerIsAlsoExactlyOnce() throws Exception {
        long rel = A;
        try (Process first = new Process(BIG)) {
            feed(first.batcher, rel, 1, 10);
            first.batcher.flushBuffered();
        }
        try (Process replay = new Process(new MiniBatcher.Limits(5, 1L << 40, 3_600_000))) {
            feed(replay.batcher, rel, 1, 12);
            replay.batcher.flushBuffered();
        }
        Map<String, Long> k = kinds(rel);
        assertEquals(12L, k.get("+I"), k.toString());
        assertEquals(6L, k.get("-U"), k.toString());
        assertEquals(6L, k.get("+U"), k.toString());
    }

    @Test
    void aBatchPartlyCommittedAcrossTablesAppliesToTheLaggingTableAlone() throws Exception {
        // The crash came between the per-table commits of one batch: B holds T1..T4, C nothing.
        try (Process first = new Process(BIG)) {
            feed(first.batcher, B, 1, 4);
            first.batcher.flushBuffered();
        }
        // Replay: every transaction touches both. B must take only T5 and T6; C all six.
        try (Process replay = new Process(BIG)) {
            for (int n = 1; n <= 6; n++) {
                List<SinkRecord> both = new ArrayList<>(txn(B, n));
                both.addAll(txn(C, n));
                replay.batcher.flush(both, 1000L * n, 1000L * n + 10);
            }
            replay.batcher.flushBuffered();
        }
        assertEquals(6L, kinds(B).get("+I"), "B: T1..T4 not repeated: " + kinds(B));
        assertEquals(6L, kinds(C).get("+I"), "C: all six: " + kinds(C));
        assertEquals(3L, kinds(B).get("-U"), kinds(B).toString());
        assertEquals(3L, kinds(C).get("-U"), kinds(C).toString());
    }
}
