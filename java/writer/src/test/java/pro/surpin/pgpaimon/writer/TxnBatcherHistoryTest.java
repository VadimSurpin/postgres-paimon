// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;

/**
 * The operation records TxnBatcher emits for history tables and key-less operation logs
 * (spec 2026-10-14): a history row per row image ({@code +I}; {@code -U} then {@code +U};
 * {@code -D}), a log row per change, never collapsed, with the transaction's metadata, each history
 * row routed to the history table of the schema it was decoded under. Pure: a fake tracker stands
 * in for the registry.
 */
class TxnBatcherHistoryTest {

    static final long PK = 16412, LOG = 16413;
    static final RelationMeta PK_META = new RelationMeta(
        PK, "public", "t", List.of("id", "v"), List.of(true, false));
    static final RelationMeta LOG_META = new RelationMeta(
        LOG, "public", "l", List.of("id", "v"), List.of(false, false));
    /** 2026-01-01T00:00:00Z as PostgreSQL sends it: microseconds after 2000-01-01. */
    static final long PG_TS = 820_540_800_000_000L;

    /** Tracks PK and LOG; a change of PK below {@code rotatedAt} belongs to an archive. */
    static TableTracker tracker(long rotatedAt) {
        return new TableTracker() {
            @Override public boolean tracked(long key) {
                long r = HistoryKeys.relidOf(key);
                return r == PK || r == LOG;
            }
            @Override public boolean keyless(long relid) { return relid == LOG; }
            @Override public boolean hasHistory(long relid) { return relid == PK || relid == LOG; }
            @Override public long historyKey(long relid, long changeLsn) {
                if (relid == PK && changeLsn < rotatedAt) {
                    return HistoryKeys.archiveKey(PK, rotatedAt);
                }
                return HistoryKeys.histKey(relid);
            }
        };
    }

    static final TableTracker TRACKER = tracker(0);

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

    static List<String> codes(List<SinkRecord> rs) {
        return rs.stream().map(r -> r.op().code()).toList();
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

    /** The six history rows of insertUpdateUpdateDelete, whatever the table's kind. */
    static void assertHistory(List<SinkRecord> h) {
        assertEquals(List.of("+I", "-U", "+U", "-U", "+U", "-D"), codes(h),
                     "one row per row image, Paimon's markers, never collapsed");
        assertEquals(List.of(List.of("1", "a"), List.of("1", "a"), List.of("1", "b"),
                             List.of("1", "b"), List.of("1", "c"), List.of("1", "c")),
                     h.stream().map(r -> r.op().image()).toList());
        assertEquals(List.of(110L, 120L, 120L, 130L, 130L, 140L),
                     h.stream().map(SinkRecord::seq).toList(), "an update's pair shares _lsn");
        assertEquals(List.of(0, 1, 2, 3, 4, 5), h.stream().map(SinkRecord::opSeq).toList(),
                     "_op_seq increases, -U before its +U");
        for (SinkRecord r : h) {
            assertEquals(900L, r.op().commitLsn(), "stamped with the commit LSN at the Commit");
            assertEquals(77L, r.xid());
            assertEquals(1_767_225_600_000_000L, r.op().commitMicros(),
                         "PostgreSQL's commit time, moved to the Unix epoch");
            assertEquals(RowKind.INSERT, r.kind(), "an append-only table takes +I only");
        }
    }

    @Test
    void aPrimaryKeyTableGetsItsChangelogAndOneHistoryRowPerImage() throws Exception {
        List<SinkRecord> batch = insertUpdateUpdateDelete(PK, PK_META);
        List<SinkRecord> main = batch.stream().filter(r -> r.relid() == PK).toList();
        assertEquals(List.of(RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER,
                             RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE),
                     main.stream().map(SinkRecord::kind).toList(), "the changelog as before");
        List<SinkRecord> h = ops(batch, HistoryKeys.histKey(PK));
        assertHistory(h);
        assertEquals(main.stream().map(SinkRecord::opSeq).toList(),
                     h.stream().map(SinkRecord::opSeq).toList(),
                     "each history row takes the _op_seq of the changelog record of its image");
    }

    @Test
    void aKeylessTableIsAnOperationLogAndItsHistoryHasEveryImage() throws Exception {
        List<SinkRecord> batch = insertUpdateUpdateDelete(LOG, LOG_META);
        assertTrue(batch.stream().allMatch(r -> r.op() != null),
                   "a key-less table has no -U/+U/-D changelog: " + batch);
        List<SinkRecord> log = ops(batch, LOG);
        assertEquals(List.of("I", "U", "U", "D"), codes(log));
        assertEquals(List.of(List.of("1", "a"), List.of("1", "b"), List.of("1", "c"),
                             List.of("1", "c")), log.stream().map(r -> r.op().image()).toList(),
                     "the log holds after values, the deleted row for D");
        assertEquals(List.of(0, 1, 3, 5), log.stream().map(SinkRecord::opSeq).toList(),
                     "a log row takes its change's first _op_seq");
        assertHistory(ops(batch, HistoryKeys.histKey(LOG)));
    }

    @Test
    void aKeyMoveIsAMinusUPlusUPairInTheHistory() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), TRACKER, rec);
        b.onEvent(new PgEvent.Begin(900L, PG_TS, 1L), 100L, null);
        b.onEvent(new PgEvent.Update(PK, row("1", "a"), false, row("2", "a")), 110L, PK_META);
        b.onEvent(new PgEvent.Commit(900L, 910L, PG_TS), 900L, null);
        List<SinkRecord> batch = rec.batches.get(0);
        assertEquals(List.of(RowKind.DELETE, RowKind.INSERT),
            batch.stream().filter(r -> r.relid() == PK).map(SinkRecord::kind).toList());
        List<SinkRecord> h = ops(batch, HistoryKeys.histKey(PK));
        assertEquals(List.of("-U", "+U"), codes(h), "an UPDATE is -U/+U in the history");
        assertEquals(List.of("1", "a"), h.get(0).op().image());
        assertEquals(List.of("2", "a"), h.get(1).op().image());
    }

    @Test
    void anUpdateThatChangesNothingStillHasItsPair() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), TRACKER, rec);
        b.onEvent(new PgEvent.Begin(900L, PG_TS, 1L), 100L, null);
        b.onEvent(new PgEvent.Update(PK, row("1", "a"), false, row("1", "a")), 110L, PK_META);
        b.onEvent(new PgEvent.Commit(900L, 910L, PG_TS), 900L, null);
        assertEquals(List.of("-U", "+U"), codes(ops(rec.batches.get(0), HistoryKeys.histKey(PK))));
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
        assertEquals(List.of(List.of("2", "b")), batch.stream()
            .filter(r -> r.relid() == PK && r.op() == null).map(SinkRecord::values).toList(),
            "the PK table's changelog loses the row the truncate wiped (the overwrite is first)");
        assertEquals(List.of("I", "T", "I"), codes(ops(batch, LOG)));
        for (long key : new long[] {HistoryKeys.histKey(PK), HistoryKeys.histKey(LOG)}) {
            List<SinkRecord> o = ops(batch, key);
            assertEquals(List.of("+I", "T", "+I"), codes(o), "key " + key);
            assertNull(o.get(1).op().image());
            assertEquals(120L, o.get(1).seq());
            assertTrue(o.get(0).opSeq() < o.get(1).opSeq() && o.get(1).opSeq() < o.get(2).opSeq());
        }
    }

    /**
     * BEGIN; INSERT; ALTER TABLE (a DDL message at 115); INSERT; COMMIT: the first insert was
     * decoded under the old schema and belongs to the archive, the second to the new history.
     */
    @Test
    void aTransactionStraddlingADdlWritesToTheArchiveAndToTheNewHistory() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), tracker(115L), rec);
        b.onEvent(new PgEvent.Begin(900L, PG_TS, 5L), 100L, null);
        b.onEvent(new PgEvent.Insert(PK, row("1", "a")), 110L, PK_META);
        b.onEvent(new PgEvent.Insert(PK, row("2", "b")), 120L, PK_META);
        b.onEvent(new PgEvent.Commit(900L, 910L, PG_TS), 900L, null);
        List<SinkRecord> batch = rec.batches.get(0);
        List<SinkRecord> archived = ops(batch, HistoryKeys.archiveKey(PK, 115L));
        List<SinkRecord> current = ops(batch, HistoryKeys.histKey(PK));
        assertEquals(List.of(List.of("1", "a")), archived.stream().map(r -> r.op().image()).toList());
        assertEquals(List.of(List.of("2", "b")), current.stream().map(r -> r.op().image()).toList());
        assertEquals(900L, archived.get(0).op().commitLsn());
        assertEquals(900L, current.get(0).op().commitLsn());
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
            assertEquals(List.of("+I", "-U", "+U"), codes(h));
            assertTrue(h.stream().allMatch(r -> r.op().commitLsn() == 1000L * (h.get(0).xid())));
            assertEquals(List.of(0, 1, 2), h.stream().map(SinkRecord::opSeq).toList());
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

    /**
     * Review of the history tables, m8: PGPAIMON_SKIP_LSN at an UPDATE whose NEW image is
     * unrepresentable used to drop only the +U, leaving the history a -U with no +U. Both go.
     */
    @Test
    void theSkipDropsBothImagesOfAnUpdateOrNeither() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher.RowCheck refusesNaN = (record, meta) -> {
            if (record.values().contains("NaN")) {
                throw new TextDecoder.UnrepresentableValueException("column 'v': NaN");
            }
        };
        TxnBatcher b = new TxnBatcher(new ToastResolver(), TRACKER, rec, 120L, refusesNaN);
        b.onEvent(new PgEvent.Begin(900L, PG_TS, 77L), 100L, null);
        b.onEvent(new PgEvent.Insert(PK, row("1", "a")), 110L, PK_META);
        b.onEvent(new PgEvent.Update(PK, row("1", "a"), false, row("1", "NaN")), 120L, PK_META);
        b.onEvent(new PgEvent.Update(PK, row("1", "NaN"), false, row("1", "c")), 130L, PK_META);
        b.onEvent(new PgEvent.Commit(900L, 910L, PG_TS), 900L, null);
        List<SinkRecord> hist = ops(rec.batches.get(0), HistoryKeys.histKey(PK));
        // the change at 120 is gone whole; the one at 130 is not at the skip LSN and stays whole
        assertEquals(List.of("+I", "-U", "+U"), codes(hist));
        assertEquals(List.of(110L, 130L, 130L), hist.stream().map(SinkRecord::seq).toList());
    }
}
