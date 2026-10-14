// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;

/**
 * The operation records TxnBatcher emits for history tables and key-less operation logs
 * (spec 2026-10-14): one per row-level change, both images, never collapsed, with the
 * transaction's metadata. Pure: a fake tracker stands in for the registry.
 */
class TxnBatcherHistoryTest {

    static final long PK = 16412, LOG = 16413;
    static final RelationMeta PK_META = new RelationMeta(
        PK, "public", "t", List.of("id", "v"), List.of(true, false));
    static final RelationMeta LOG_META = new RelationMeta(
        LOG, "public", "l", List.of("id", "v"), List.of(false, false));
    /** 2026-01-01T00:00:00Z as PostgreSQL sends it: microseconds after 2000-01-01. */
    static final long PG_TS = 820_540_800_000_000L;

    static final TableTracker TRACKER = new TableTracker() {
        @Override public boolean tracked(long key) {
            long r = HistoryKeys.relidOf(key);
            return r == PK || r == LOG;
        }
        @Override public boolean keyless(long relid) { return relid == LOG; }
        @Override public boolean hasHistory(long relid) { return relid == PK || relid == LOG; }
    };

    static final class Recorder implements TxnBatcher.Flusher {
        final List<List<SinkRecord>> batches = new ArrayList<>();
        @Override public void flush(List<SinkRecord> records, long commitLsn, long endLsn) {
            batches.add(List.copyOf(records));
        }
    }

    static TupleData row(String id, String v) {
        return new TupleData(Arrays.asList(ColumnValue.of(id),
            v == null ? ColumnValue.ofNull() : ColumnValue.of(v)));
    }

    static List<SinkRecord> ops(List<SinkRecord> batch, long key) {
        return batch.stream().filter(r -> r.relid() == key && r.op() != null).toList();
    }

    /** BEGIN; INSERT; UPDATE; UPDATE; DELETE; COMMIT on one row. */
    static List<SinkRecord> insertUpdateUpdateDelete(long relid, RelationMeta meta)
            throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), TRACKER, rec);
        b.onEvent(new PgEvent.Begin(900L, PG_TS, 77L), 100L, null);
        b.onEvent(new PgEvent.Insert(relid, row("1", "a")), 110L, meta);
        b.onEvent(new PgEvent.Update(relid, row("1", "a"), false, row("1", "b")), 120L, meta);
        b.onEvent(new PgEvent.Update(relid, row("1", "b"), false, row("1", "c")), 130L, meta);
        b.onEvent(new PgEvent.Delete(relid, row("1", "c"), false), 140L, meta);
        b.onEvent(new PgEvent.Commit(900L, 910L, PG_TS), 900L, null);
        assertEquals(1, rec.batches.size());
        return rec.batches.get(0);
    }

    static void assertFourOps(List<SinkRecord> h) {
        assertEquals(List.of('I', 'U', 'U', 'D'), h.stream().map(r -> r.op().code()).toList(),
                     "two updates of one row in one transaction are two rows, never collapsed");
        assertNull(h.get(0).op().before());
        assertEquals(List.of("1", "a"), h.get(0).op().after());
        assertEquals(List.of("1", "a"), h.get(1).op().before());
        assertEquals(List.of("1", "b"), h.get(1).op().after());
        assertEquals(List.of("1", "b"), h.get(2).op().before());
        assertEquals(List.of("1", "c"), h.get(2).op().after());
        assertEquals(List.of("1", "c"), h.get(3).op().before());
        assertNull(h.get(3).op().after());
        assertEquals(List.of(110L, 120L, 130L, 140L), h.stream().map(SinkRecord::seq).toList());
        for (SinkRecord r : h) {
            assertEquals(900L, r.op().commitLsn(), "stamped with the commit LSN at the Commit");
            assertEquals(77L, r.xid());
            assertEquals(1_767_225_600_000_000L, r.op().commitMicros(),
                         "PostgreSQL's commit time, moved to the Unix epoch");
            assertEquals(RowKind.INSERT, r.kind(), "an append-only table takes +I only");
        }
        for (int i = 1; i < h.size(); i++) {
            assertTrue(h.get(i).opSeq() > h.get(i - 1).opSeq(), "_op_seq increases");
        }
    }

    @Test
    void aPrimaryKeyTableGetsItsChangelogAndOneHistoryRecordPerChange() throws Exception {
        List<SinkRecord> batch = insertUpdateUpdateDelete(PK, PK_META);
        List<SinkRecord> main = batch.stream().filter(r -> r.relid() == PK).toList();
        assertEquals(List.of(RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER,
                             RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE),
                     main.stream().map(SinkRecord::kind).toList(), "the changelog as before");
        assertTrue(main.stream().allMatch(r -> r.op() == null));
        List<SinkRecord> h = ops(batch, HistoryKeys.histKey(PK));
        assertFourOps(h);
        // the history record of a change takes the _op_seq of the change's first record
        assertEquals(List.of(0, 1, 3, 5), h.stream().map(SinkRecord::opSeq).toList());
    }

    @Test
    void aKeylessTableIsAnOperationLogWithNoChangelogRecords() throws Exception {
        List<SinkRecord> batch = insertUpdateUpdateDelete(LOG, LOG_META);
        assertTrue(batch.stream().allMatch(r -> r.op() != null),
                   "a key-less table has no -U/+U/-D semantics: " + batch);
        assertFourOps(ops(batch, LOG));
        assertFourOps(ops(batch, HistoryKeys.histKey(LOG)));
        assertEquals(List.of(0, 1, 2, 3),
                     ops(batch, LOG).stream().map(SinkRecord::opSeq).toList());
    }

    @Test
    void aKeyMoveIsOneUpdateInTheHistory() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), TRACKER, rec);
        b.onEvent(new PgEvent.Begin(900L, PG_TS, 1L), 100L, null);
        b.onEvent(new PgEvent.Update(PK, row("1", "a"), false, row("2", "a")), 110L, PK_META);
        b.onEvent(new PgEvent.Commit(900L, 910L, PG_TS), 900L, null);
        List<SinkRecord> batch = rec.batches.get(0);
        assertEquals(List.of(RowKind.DELETE, RowKind.INSERT),
            batch.stream().filter(r -> r.relid() == PK).map(SinkRecord::kind).toList());
        List<SinkRecord> h = ops(batch, HistoryKeys.histKey(PK));
        assertEquals(1, h.size());
        assertEquals('U', h.get(0).op().code());
        assertEquals(List.of("1", "a"), h.get(0).op().before());
        assertEquals(List.of("2", "a"), h.get(0).op().after());
    }

    @Test
    void aTruncateIsATRecordAndRemovesNothingFromTheHistoryOrTheLog() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), TRACKER, rec);
        b.onEvent(new PgEvent.Begin(900L, PG_TS, 5L), 100L, null);
        b.onEvent(new PgEvent.Insert(PK, row("1", "a")), 110L, PK_META);
        b.onEvent(new PgEvent.Insert(LOG, row("1", "a")), 115L, LOG_META);
        b.onEvent(new PgEvent.Truncate(List.of(PK, LOG), false, false), 120L, null);
        b.onEvent(new PgEvent.Insert(PK, row("2", "b")), 130L, PK_META);
        b.onEvent(new PgEvent.Insert(LOG, row("2", "b")), 135L, LOG_META);
        b.onEvent(new PgEvent.Commit(900L, 910L, PG_TS), 900L, null);
        List<SinkRecord> batch = rec.batches.get(0);
        // the PK table's changelog loses the row the truncate wiped (the overwrite comes first)
        assertEquals(List.of(List.of("2", "b")), batch.stream()
            .filter(r -> r.relid() == PK && r.op() == null).map(SinkRecord::values).toList());
        for (long key : new long[] {HistoryKeys.histKey(PK), LOG, HistoryKeys.histKey(LOG)}) {
            List<SinkRecord> o = ops(batch, key);
            assertEquals(List.of('I', 'T', 'I'), o.stream().map(r -> r.op().code()).toList(),
                         "key " + key);
            assertNull(o.get(1).op().before());
            assertNull(o.get(1).op().after());
            assertEquals(120L, o.get(1).seq());
            assertTrue(o.get(0).opSeq() < o.get(1).opSeq() && o.get(1).opSeq() < o.get(2).opSeq());
        }
    }

    @Test
    void severalTransactionsKeepTheirOwnCommitLsnsAndOpSeqs() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), TRACKER, rec);
        for (int t = 1; t <= 3; t++) {
            b.onEvent(new PgEvent.Begin(1000L * t, PG_TS + t, t), 1000L * t - 50, null);
            b.onEvent(new PgEvent.Insert(PK, row("" + t, "a")), 1000L * t - 40, PK_META);
            b.onEvent(new PgEvent.Update(PK, row("" + t, "a"), false, row("" + t, "b")),
                      1000L * t - 30, PK_META);
            b.onEvent(new PgEvent.Commit(1000L * t, 1000L * t + 8, PG_TS + t), 1000L * t, null);
        }
        for (int t = 1; t <= 3; t++) {
            List<SinkRecord> h = ops(rec.batches.get(t - 1), HistoryKeys.histKey(PK));
            assertEquals(2, h.size());
            assertEquals(1000L * t, h.get(0).op().commitLsn());
            assertEquals(List.of(0, 1), h.stream().map(SinkRecord::opSeq).toList());
        }
    }

    @Test
    void anUntrackedRelationYieldsNoOperationRecords() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), TRACKER, rec);
        b.onEvent(new PgEvent.Begin(900L, PG_TS, 5L), 100L, null);
        b.onEvent(new PgEvent.Insert(99999L, row("1", "a")), 110L,
            new RelationMeta(99999L, "public", "x", List.of("id", "v"), List.of(false, false)));
        b.onEvent(new PgEvent.Truncate(List.of(99999L), false, false), 120L, null);
        b.onEvent(new PgEvent.Commit(900L, 910L, PG_TS), 900L, null);
        assertTrue(rec.batches.get(0).isEmpty());
    }
}
