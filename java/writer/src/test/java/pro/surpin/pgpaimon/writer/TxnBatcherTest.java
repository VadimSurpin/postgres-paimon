// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class TxnBatcherTest {

    static final RelationMeta META = new RelationMeta(
        16412, "public", "t", List.of("id", "v"), List.of(true, false));

    static final class Recorder implements TxnBatcher.Flusher {
        final List<List<SinkRecord>> batches = new ArrayList<>();
        final List<Long> commitLsns = new ArrayList<>();
        final List<Long> endLsns = new ArrayList<>();
        @Override public void flush(List<SinkRecord> records, long commitLsn, long endLsn) {
            batches.add(List.copyOf(records));
            commitLsns.add(commitLsn);
            endLsns.add(endLsn);
        }
    }

    private static TupleData row(String id, String v) {
        return new TupleData(List.of(ColumnValue.of(id), ColumnValue.of(v)));
    }

    @Test
    void assignsPerChangeLsnAndAMonotoneOpSeq() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);

        b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 410L, META);
        b.onEvent(new PgEvent.Update(16412, row("1", "a"), false, row("1", "b")), 420L, META);
        b.onEvent(new PgEvent.Commit(500L, 510L, 0L), 500L, null);

        assertEquals(1, rec.batches.size());
        List<SinkRecord> batch = rec.batches.get(0);
        assertEquals(3, batch.size());

        assertEquals(RowKind.INSERT, batch.get(0).kind());
        assertEquals(410L, batch.get(0).seq());
        assertEquals(0, batch.get(0).opSeq());

        assertEquals(RowKind.UPDATE_BEFORE, batch.get(1).kind());
        assertEquals(420L, batch.get(1).seq());
        assertEquals(1, batch.get(1).opSeq());

        assertEquals(RowKind.UPDATE_AFTER, batch.get(2).kind());
        assertEquals(420L, batch.get(2).seq());
        assertEquals(2, batch.get(2).opSeq(),
                     "-U must sort before its +U, which is what _op_seq is for");

        assertEquals(500L, rec.commitLsns.get(0), "the commit identifier is the commit LSN");
        assertEquals(510L, rec.endLsns.get(0), "the confirmed position is after the commit");
        assertEquals(4711L, batch.get(0).xid());
    }

    @Test
    void opSeqRestartsEachTransaction() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        for (int i = 0; i < 2; i++) {
            b.onEvent(new PgEvent.Begin(100L * i, 0L, i), 100L * i, null);
            b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 100L * i + 1, META);
            b.onEvent(new PgEvent.Commit(100L * i, 0L, 0L), 100L * i + 2, null);
        }
        assertEquals(0, rec.batches.get(0).get(0).opSeq());
        assertEquals(0, rec.batches.get(1).get(0).opSeq());
    }

    /** Review Focus 3: a DDL-only transaction must still flush, or the LSN never advances. */
    @Test
    void aTransactionWithNoRecordsStillFlushes() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(600L, 0L, 1L), 600L, null);
        b.onEvent(new PgEvent.Message(true, 601L, "paimon",
                  "{\"v\":1,\"op\":\"add_col\"}".getBytes()), 601L, null);
        b.onEvent(new PgEvent.Commit(600L, 610L, 0L), 610L, null);
        assertEquals(1, rec.batches.size(), "the empty transaction must still be flushed");
        assertTrue(rec.batches.get(0).isEmpty());
        assertEquals(600L, rec.commitLsns.get(0));
        assertEquals(610L, rec.endLsns.get(0),
                     "the LSN must still advance or this transaction replays forever");
    }

    @Test
    void keyMovesInterleaveWithoutBeingReordered() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(700L, 0L, 1L), 700L, null);
        // UPDATE t SET id = id + 1 over keys 1 and 2, in server row order
        b.onEvent(new PgEvent.Update(16412, row("1", "a"), false, row("2", "a")), 710L, META);
        b.onEvent(new PgEvent.Update(16412, row("2", "b"), false, row("3", "b")), 720L, META);
        b.onEvent(new PgEvent.Commit(700L, 730L, 0L), 730L, null);

        List<SinkRecord> batch = rec.batches.get(0);
        assertEquals(4, batch.size());
        assertEquals(List.of(RowKind.DELETE, RowKind.INSERT, RowKind.DELETE, RowKind.INSERT),
                     batch.stream().map(SinkRecord::kind).toList());
        assertEquals(List.of(0, 1, 2, 3), batch.stream().map(SinkRecord::opSeq).toList());
    }

    @Test
    void discardThrowsAwayAnOpenTransaction() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(800L, 0L, 1L), 800L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 810L, META);
        assertTrue(b.hasOpenTransaction());
        b.discardOpenTransaction();
        assertFalse(b.hasOpenTransaction());
        assertTrue(rec.batches.isEmpty());
    }

    @Test
    void eventsOutsideATransactionAreRejected() {
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, new Recorder());
        assertThrows(IllegalStateException.class,
            () -> b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 1L, META));
    }

    @Test
    void untrackedRelationIsSkippedButTransactionStillFlushes() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), new SchemaRegistry(null, null), rec);
        b.onEvent(new PgEvent.Begin(900L, 0L, 1L), 900L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 910L, META);
        b.onEvent(new PgEvent.Commit(900L, 920L, 0L), 920L, null);
        assertEquals(1, rec.batches.size());
        assertTrue(rec.batches.get(0).isEmpty());
        assertEquals(920L, rec.endLsns.get(0));
    }

    @Test
    void relationAndTruncateContributeNothingAndKeepOpSeq() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(1000L, 0L, 1L), 1000L, null);
        b.onEvent(new PgEvent.Relation(META), 0L, null);
        b.onEvent(new PgEvent.Truncate(List.of(99999L), false, false), 1005L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 1010L, META);
        b.onEvent(new PgEvent.Commit(1000L, 1020L, 0L), 1020L, null);
        List<SinkRecord> batch = rec.batches.get(0);
        assertEquals(1, batch.size());
        assertEquals(1010L, batch.get(0).seq());
        assertEquals(0, batch.get(0).opSeq());
    }

    @Test
    void commitWithoutBeginAndNestedBeginAreRejected() throws Exception {
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, new Recorder());
        assertThrows(IllegalStateException.class,
            () -> b.onEvent(new PgEvent.Commit(1L, 2L, 0L), 1L, null));
        b.onEvent(new PgEvent.Begin(1L, 0L, 1L), 1L, null);
        assertThrows(IllegalStateException.class,
            () -> b.onEvent(new PgEvent.Begin(2L, 0L, 2L), 2L, null));
    }

    private static PgEvent.Truncate trunc(long... relids) {
        List<Long> l = new ArrayList<>();
        for (long r : relids) l.add(r);
        return new PgEvent.Truncate(l, false, false);
    }

    @Test
    void truncateAloneIsFineAndStillFlushes() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(1100L, 0L, 1L), 1100L, null);
        b.onEvent(trunc(16412), 1105L, null);
        b.onEvent(new PgEvent.Commit(1100L, 1110L, 0L), 1110L, null);
        assertEquals(1, rec.batches.size());
        assertTrue(rec.batches.get(0).isEmpty());
        assertEquals(1110L, rec.endLsns.get(0));
    }

    @Test
    void recordAfterTruncateOfSameTableThrowsNamingTheTable() throws Exception {
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, new Recorder());
        b.onEvent(new PgEvent.Begin(1200L, 0L, 1L), 1200L, null);
        b.onEvent(trunc(16412), 1205L, null);
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 1210L, META));
        assertTrue(e.getMessage().contains("16412"), e.getMessage());
    }

    @Test
    void truncateAfterRecordOfSameTableThrowsNamingTheTable() throws Exception {
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, new Recorder());
        b.onEvent(new PgEvent.Begin(1300L, 0L, 1L), 1300L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 1310L, META);
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> b.onEvent(trunc(16412), 1320L, null));
        assertTrue(e.getMessage().contains("16412"), e.getMessage());
    }

    @Test
    void truncateOfOneTableWithRecordForAnotherIsFine() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(1400L, 0L, 1L), 1400L, null);
        b.onEvent(trunc(77777), 1405L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 1410L, META);
        b.onEvent(new PgEvent.Commit(1400L, 1420L, 0L), 1420L, null);
        assertEquals(1, rec.batches.get(0).size());
        assertEquals(16412L, rec.batches.get(0).get(0).relid());
    }

    @Test
    void discardClearsTruncateState() throws Exception {
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, new Recorder());
        b.onEvent(new PgEvent.Begin(1500L, 0L, 1L), 1500L, null);
        b.onEvent(trunc(16412), 1505L, null);
        b.discardOpenTransaction();
        b.onEvent(new PgEvent.Begin(1600L, 0L, 2L), 1600L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 1610L, META);
    }
}
