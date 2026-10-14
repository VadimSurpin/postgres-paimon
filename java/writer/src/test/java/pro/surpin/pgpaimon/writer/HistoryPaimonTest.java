// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * History tables and key-less operation logs against the real Paimon 1.4.1, through the real
 * pipeline: PipelineHandler, TxnBatcher, MiniBatcher, the confirming flusher, PaimonSink. The
 * history is read back row by row; row kinds of the main tables come from $audit_log (RawRead),
 * never from a merged read.
 */
class HistoryPaimonTest {

    @TempDir static java.nio.file.Path wh;
    static Catalog catalog;
    static SchemaRegistry reg;
    static WriterConfig cfg;
    static DdlMessageHandler ddl;
    static long ddlLsn = 0x1000;
    static final long PG_TS = 820_540_800_000_000L;     // 2026-01-01T00:00:00Z, PostgreSQL epoch

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

    static void create(long relid, String table, boolean keyless) {
        ddlLsn += 0x10;
        ddl.handle(new PgEvent.Message(true, ddlLsn, "paimon", TestAuth.authed("""
            {"v":1,"op":"create","relid":%d,"schema":"public","table":"%s",
             "pk":%s,"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                             {"id":2,"name":"v","type":"STRING"}],
             "options":{"changelog-producer":"input","bucket":"2","hist-table":"%s_hist",
                        "registration":"reg-%d"}}""".formatted(
                relid, table, keyless ? "[]" : "[\"id\"]", table, relid))));
    }

    static RelationMeta meta(long relid, String table) {
        return new RelationMeta(relid, "public", table, List.of("id", "v"), List.of(false, false));
    }

    /** A writer process: the pipeline Main builds, over the shared catalog. */
    static final class Process implements AutoCloseable {
        final PaimonSink sink;
        final List<Long> confirmed = new ArrayList<>();
        final MiniBatcher mini;
        final Main.PipelineHandler handler;

        Process(MiniBatcher.Limits limits) {
            this(limits, CrashHook.OFF, k -> true);
        }

        /** {@code commits} decides which Paimon tables this process actually commits to. */
        Process(MiniBatcher.Limits limits, CrashHook hook,
                java.util.function.LongPredicate commits) {
            sink = new PaimonSink(reg, cfg, hook);
            TxnBatcher.Flusher partial = (records, commitLsn, endLsn) -> sink.flush(
                records.stream().filter(r -> commits.test(r.relid())).toList(), commitLsn, endLsn);
            mini = new MiniBatcher(Main.confirmingFlusher(partial, confirmed::add),
                new OffsetStore(reg, cfg)::highestCommitted, limits);
            TxnBatcher batcher = new TxnBatcher(new ToastResolver(), reg, mini);
            handler = new Main.PipelineHandler(reg::tracked, reg::primaryKey, m -> { },
                m -> false, new Main.PipelineHandler.Target() {
                    @Override public void setRelation(long relid, RelationMeta m) {
                        sink.setRelation(relid, m);
                    }
                    @Override public void truncate(long relid, long commitLsn) throws Exception {
                        sink.truncate(relid, commitLsn);
                    }
                    @Override public boolean overwrites(long relid) {
                        return !reg.keyless(relid);
                    }
                }, batcher, mini);
        }

        void ev(PgEvent e, long lsn) throws Exception {
            handler.onEvent(e, lsn);
        }

        @Override public void close() throws Exception {
            sink.close();
        }
    }

    static TupleData row(String id, String v) {
        return new TupleData(Arrays.asList(ColumnValue.of(id),
            v == null ? ColumnValue.ofNull() : ColumnValue.of(v)));
    }

    /** Transaction n (commit LSN 1000n, xid n): INSERT id n; UPDATE it twice; DELETE it. */
    static void iuud(Process p, long relid, int n) throws Exception {
        long c = 1000L * n;
        String id = "" + n;
        p.ev(new PgEvent.Begin(c, PG_TS + n, n), c - 100);
        p.ev(new PgEvent.Insert(relid, row(id, "a")), c - 90);
        p.ev(new PgEvent.Update(relid, row(id, "a"), false, row(id, "b")), c - 80);
        p.ev(new PgEvent.Update(relid, row(id, "b"), false, row(id, null)), c - 70);
        p.ev(new PgEvent.Delete(relid, row(id, null), false), c - 60);
        p.ev(new PgEvent.Commit(c, c + 8, PG_TS + n), c);
    }

    /** One stored row of an append-only table, every column as text (null for NULL). */
    static List<Map<String, String>> rows(String table) throws Exception {
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
        out.sort(Comparator.comparingLong((Map<String, String> m) -> Long.parseLong(m.get("_commit_lsn")))
            .thenComparingInt(m -> Integer.parseInt(m.get("_op_seq"))));
        return out;
    }

    static String text(InternalRow r, int i, DataField f) {
        return switch (f.type().getTypeRoot()) {
            case INTEGER -> "" + r.getInt(i);
            case BIGINT -> "" + r.getLong(i);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> "" + r.getTimestamp(i, 6).toMicros();
            default -> r.getString(i).toString();
        };
    }

    static long snapshots(String table) throws Exception {
        Long id = ((FileStoreTable) catalog.getTable(Identifier.create("public", table)))
            .snapshotManager().latestSnapshotId();
        return id == null ? 0 : id;
    }

    /** Asserts the four history rows of transaction n, before/after in the given layout. */
    static void assertTxn(List<Map<String, String>> rows, int n, String b, String a) {
        List<Map<String, String>> t = rows.stream()
            .filter(m -> m.get("_commit_lsn").equals("" + 1000L * n)).toList();
        assertEquals(List.of("I", "U", "U", "D"), t.stream().map(m -> m.get("_op")).toList(),
                     "transaction " + n + ": " + t);
        String id = "" + n;
        List<List<String>> want = Arrays.asList(
            Arrays.asList(null, null, id, "a"),
            Arrays.asList(id, "a", id, "b"),
            Arrays.asList(id, "b", id, null),
            Arrays.asList(id, null, null, null));
        for (int i = 0; i < 4; i++) {
            Map<String, String> m = t.get(i);
            assertEquals(want.get(i), Arrays.asList(m.get(b + "id"), m.get(b + "v"),
                                                    m.get(a + "id"), m.get(a + "v")),
                         "transaction " + n + " row " + i + ": " + m);
            assertEquals("" + n, m.get("_xid"));
            assertEquals("" + (1000L * n - 90 + 10 * i), m.get("_lsn"));
            assertEquals("" + LogRowBuilder.pgToUnixMicros(PG_TS + n), m.get("_ts"));
            if (i > 0) {
                assertTrue(Integer.parseInt(m.get("_op_seq"))
                           > Integer.parseInt(t.get(i - 1).get("_op_seq")));
            }
        }
    }

    @Test
    void historyOfAPrimaryKeyTableIsEveryChangeUncollapsedAcrossAMiniBatch() throws Exception {
        long rel = 30001;
        create(rel, "h_pk", false);
        try (Process p = new Process(BIG)) {
            p.ev(new PgEvent.Relation(meta(rel, "h_pk")), 0);
            for (int n = 1; n <= 5; n++) {
                iuud(p, rel, n);
            }
            assertTrue(p.mini.hasBuffered(), "all five transactions wait in one mini-batch");
            p.mini.flushBuffered();
            assertEquals(List.of(5008L), p.confirmed, "one commit, confirmed once");
        }
        assertEquals(1, snapshots("h_pk_hist"), "the batcher really batched: one snapshot for 5");
        assertEquals(1, snapshots("h_pk"));
        List<Map<String, String>> h = rows("h_pk_hist");
        assertEquals(20, h.size(), h.toString());
        for (int n = 1; n <= 5; n++) {
            assertTxn(h, n, "before_", "after_");
        }
        Map<String, Long> kinds = new TreeMap<>();
        for (RawRecord r : RawRead.read(catalog, "public", "h_pk")) {
            kinds.merge(r.rowKind(), 1L, Long::sum);
        }
        assertEquals(Map.of("+I", 5L, "-U", 10L, "+U", 10L, "-D", 5L), kinds,
                     "the main table's changelog is unchanged by the history");
    }

    @Test
    void aKeylessTableIsAnOperationLogAndHasAHistoryToo() throws Exception {
        long rel = 30002;
        create(rel, "h_log", true);
        FileStoreTable main = reg.table(rel);
        assertEquals(List.of(), main.primaryKeys());
        assertEquals(List.of("id", "v", "_op", "_commit_lsn", "_lsn", "_op_seq", "_xid", "_ts"),
                     main.rowType().getFieldNames());
        assertTrue(main.rowType().getField("id").type().isNullable(),
                   "a T row is NULL everywhere, so the log's user columns are nullable");
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
            List<Map<String, String>> t = log.stream()
                .filter(m -> m.get("_commit_lsn").equals("" + 1000L * k)).toList();
            assertEquals(List.of("I", "U", "U", "D"), t.stream().map(m -> m.get("_op")).toList());
            // after values; the deleted row's values for D
            assertEquals(Arrays.asList("a", "b", null, null),
                         t.stream().map(m -> m.get("v")).toList(), t.toString());
            assertTrue(t.stream().allMatch(m -> m.get("id").equals("" + k)), t.toString());
        }
        List<Map<String, String>> h = rows("h_log_hist");
        assertEquals(12, h.size());
        for (int n = 1; n <= 3; n++) {
            assertTxn(h, n, "before_", "after_");
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
            p.ev(new PgEvent.Begin(1000, PG_TS, 1), 900);
            p.ev(new PgEvent.Insert(pk, row("1", "a")), 910);
            p.ev(new PgEvent.Insert(lg, row("1", "a")), 920);
            p.ev(new PgEvent.Commit(1000, 1008, PG_TS), 1000);
            // BEGIN; INSERT; TRUNCATE both; INSERT; COMMIT
            p.ev(new PgEvent.Begin(2000, PG_TS, 2), 1900);
            p.ev(new PgEvent.Insert(pk, row("2", "b")), 1910);
            p.ev(new PgEvent.Truncate(List.of(pk, lg), false, false), 1920);
            p.ev(new PgEvent.Insert(pk, row("3", "c")), 1930);
            p.ev(new PgEvent.Insert(lg, row("3", "c")), 1940);
            p.ev(new PgEvent.Commit(2000, 2008, PG_TS), 2000);
            p.mini.flushBuffered();
        }
        // main PK table: only id 3 survives (merged read is fine for STATE)
        assertEquals(List.of("3"), mergedIds("tr_pk"));
        assertEquals(List.of("I", "I", "T", "I"),
                     rows("tr_pk_hist").stream().map(m -> m.get("_op")).toList(),
                     "history keeps the row the truncate wiped, and the truncate itself");
        assertEquals(List.of("I", "T", "I"),
                     rows("tr_log").stream().map(m -> m.get("_op")).toList(),
                     "a log is not erased by TRUNCATE");
        Map<String, String> t = rows("tr_log").get(1);
        assertNull(t.get("id"));
        assertNull(t.get("v"));
        assertEquals("1920", t.get("_lsn"));
        assertEquals(List.of("I", "T", "I"),
                     rows("tr_log_hist").stream().map(m -> m.get("_op")).toList());
    }

    static List<String> mergedIds(String table) throws Exception {
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
            p.mini.flushBuffered();          // committed under L10, killed before the confirm
        }
        try (Process p = new Process(BIG)) {
            p.ev(new PgEvent.Relation(meta(rel, "rp")), 0);
            for (int n = 1; n <= 12; n++) {
                iuud(p, rel, n);             // the timer fired later: T1..T12 under L12
            }
            p.mini.flushBuffered();
        }
        List<Map<String, String>> h = rows("rp_hist");
        assertEquals(48, h.size(), "12 transactions of 4 changes, each exactly once");
        for (int n = 1; n <= 12; n++) {
            assertTxn(h, n, "before_", "after_");
        }
    }

    /**
     * The main and history commits of one batch are not atomic. A crash after one and before
     * the other leaves one table ahead; the replay must complete the other and repeat nothing.
     * Both orders. The first process commits to one side only, which is exactly what the
     * process killed between the two commits leaves on disk.
     */
    @Test
    void aCrashBetweenTheMainAndTheHistoryCommitIsRepairedByReplayInBothOrders()
            throws Exception {
        for (boolean mainFirst : new boolean[] {true, false}) {
            long rel = mainFirst ? 30006 : 30007;
            String name = mainFirst ? "cr_main" : "cr_hist";
            create(rel, name, false);
            long hk = HistoryKeys.histKey(rel);
            try (Process p = new Process(BIG, CrashHook.OFF,
                                         k -> mainFirst ? k == rel : k == hk)) {
                p.ev(new PgEvent.Relation(meta(rel, name)), 0);
                for (int n = 1; n <= 4; n++) {
                    iuud(p, rel, n);
                }
                p.mini.flushBuffered();
            }
            OffsetStore offsets = new OffsetStore(reg, cfg);
            assertEquals(mainFirst ? 4000L : 0L, offsets.highestCommitted(rel));
            assertEquals(mainFirst ? 0L : 4000L, offsets.highestCommitted(hk));
            assertEquals(0L, offsets.highestCommittedLsn(List.of(rel, hk)),
                         "the resume position is the MIN over both tables");
            try (Process p = new Process(BIG)) {
                p.ev(new PgEvent.Relation(meta(rel, name)), 0);
                for (int n = 1; n <= 6; n++) {
                    iuud(p, rel, n);
                }
                p.mini.flushBuffered();
            }
            List<Map<String, String>> h = rows(name + "_hist");
            assertEquals(24, h.size(), name + ": " + h.size());
            for (int n = 1; n <= 6; n++) {
                assertTxn(h, n, "before_", "after_");
            }
            Map<String, Long> kinds = new TreeMap<>();
            for (RawRecord r : RawRead.read(catalog, "public", name)) {
                kinds.merge(r.rowKind(), 1L, Long::sum);
            }
            assertEquals(Map.of("+I", 6L, "-U", 12L, "+U", 12L, "-D", 6L), kinds, name);
            assertEquals(6000L, offsets.highestCommittedLsn(List.of(rel, hk)));
        }
    }
}
