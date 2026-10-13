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

    /**
     * BEGIN; INSERT INTO t ...; INSERT INTO u ...; DROP TABLE t; COMMIT. The drop_table message
     * untracks t between its INSERT and the Commit. Measured on the writer before this: the
     * commit then asked for t's Paimon table ("relation ... is not tracked") and the writer
     * stopped. t's rows are meant to be gone with it; u's must still be committed.
     */
    @Test
    void recordsOfATableDroppedLaterInTheSameTransactionAreDroppedAtCommit() throws Exception {
        Recorder rec = new Recorder();
        SchemaRegistry reg = new SchemaRegistry(null, null);
        reg.register(16412, "public", "t");
        reg.register(16413, "public", "u");
        TxnBatcher b = new TxnBatcher(new ToastResolver(), reg, rec);
        RelationMeta u = new RelationMeta(16413, "public", "u", List.of("id", "v"),
                                          List.of(true, false));
        b.onEvent(new PgEvent.Begin(900L, 0L, 1L), 900L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 910L, META);
        b.onEvent(new PgEvent.Insert(16413, row("2", "b")), 912L, u);
        reg.forget(16412);          // what drop_table does, before the Commit arrives
        b.onEvent(new PgEvent.Commit(900L, 920L, 0L), 920L, null);
        assertEquals(1, rec.batches.size());
        assertEquals(1, rec.batches.get(0).size(), "only u's record");
        assertEquals(16413L, rec.batches.get(0).get(0).relid());
        assertEquals(List.of("2", "b"), rec.batches.get(0).get(0).values());
        assertEquals(920L, rec.endLsns.get(0), "the transaction is still confirmed");
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
        assertDoesNotThrow(
            () -> b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 1610L, META));
    }

    @Test
    void aThrowMidTransactionLeavesNothingOpenAndCommitFlushesNothing() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(1700L, 0L, 1L), 1700L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 1710L, META);
        // key-only old tuple: ToastResolver.resolve throws
        assertThrows(IllegalStateException.class, () -> b.onEvent(
            new PgEvent.Delete(16412, row("1", "a"), true), 1720L, META));
        assertFalse(b.hasOpenTransaction());
        assertThrows(IllegalStateException.class,
            () -> b.onEvent(new PgEvent.Commit(1700L, 1730L, 0L), 1730L, null));
        assertTrue(rec.batches.isEmpty(), "a partial batch must never be flushed");
    }

    @Test
    void aTruncateConflictAlsoLeavesNothingOpen() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(1800L, 0L, 1L), 1800L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 1810L, META);
        assertThrows(IllegalStateException.class, () -> b.onEvent(trunc(16412), 1820L, null));
        assertFalse(b.hasOpenTransaction());
        assertThrows(IllegalStateException.class,
            () -> b.onEvent(new PgEvent.Commit(1800L, 1830L, 0L), 1830L, null));
        assertTrue(rec.batches.isEmpty());
    }

    @Test
    void twoUpdatesToTheSameKeySurviveInOrder() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(1900L, 0L, 1L), 1900L, null);
        b.onEvent(new PgEvent.Update(16412, row("1", "a"), false, row("1", "b")), 1910L, META);
        b.onEvent(new PgEvent.Update(16412, row("1", "b"), false, row("1", "c")), 1920L, META);
        b.onEvent(new PgEvent.Commit(1900L, 1930L, 0L), 1930L, null);
        List<SinkRecord> batch = rec.batches.get(0);
        assertEquals(List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER,
                             RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER),
                     batch.stream().map(SinkRecord::kind).toList());
        assertEquals(List.of(0, 1, 2, 3), batch.stream().map(SinkRecord::opSeq).toList());
        assertEquals(List.of(1910L, 1910L, 1920L, 1920L),
                     batch.stream().map(SinkRecord::seq).toList());
        assertEquals(List.of("a", "b", "b", "c"),
                     batch.stream().map(r -> r.values().get(1)).toList());
    }

    @Test
    void typeMsgAndOriginContributeNothing() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(2000L, 0L, 1L), 2000L, null);
        b.onEvent(new PgEvent.TypeMsg(23L, "pg_catalog", "int4"), 0L, null);
        b.onEvent(new PgEvent.Origin(2000L, "o"), 0L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 2010L, META);
        b.onEvent(new PgEvent.Commit(2000L, 2020L, 0L), 2020L, null);
        assertEquals(1, rec.batches.get(0).size());
        assertEquals(0, rec.batches.get(0).get(0).opSeq());
        assertEquals(2010L, rec.batches.get(0).get(0).seq());
    }

    @Test
    void nestedBeginDiscardsTheOpenBatchAndCommitFlushesNothing() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(2100L, 0L, 1L), 2100L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 2110L, META);
        assertThrows(IllegalStateException.class,
            () -> b.onEvent(new PgEvent.Begin(2200L, 0L, 2L), 2200L, null));
        assertFalse(b.hasOpenTransaction());
        assertThrows(IllegalStateException.class,
            () -> b.onEvent(new PgEvent.Commit(2200L, 2210L, 0L), 2210L, null));
        assertTrue(rec.batches.isEmpty(), "records of the first txn must not be flushed");
    }
    /*
     * The poison-record escape, PGPAIMON_SKIP_LSN. The point of these tests is not that it works
     * but that it is NARROW: one LSN, one kind of failure, everything else still fatal. An
     * "ignore errors" switch would pass the first test and fail the rest.
     */

    /** A RowCheck that refuses one value, as TextDecoder does for a numeric it cannot hold. */
    private static TxnBatcher.RowCheck refuses(String badValue) {
        return (record, meta) -> {
            if (record.values().contains(badValue)) {
                throw new TextDecoder.UnrepresentableValueException(
                    "column 'v': cannot parse DECIMAL(10, 2) from \"" + badValue
                    + "\": numeric NaN and Infinity have no Paimon representation");
            }
        };
    }

    private static List<java.util.logging.LogRecord> warningsFrom(Runnable r) {
        List<java.util.logging.LogRecord> out = new ArrayList<>();
        java.util.logging.Logger log =
            java.util.logging.Logger.getLogger(TxnBatcher.class.getName());
        java.util.logging.Handler h = new java.util.logging.Handler() {
            public void publish(java.util.logging.LogRecord x) { out.add(x); }
            public void flush() { }
            public void close() { }
        };
        log.addHandler(h);
        try { r.run(); } finally { log.removeHandler(h); }
        return out;
    }

    @Test
    void aMatchingSkipLsnDropsOneRecordAndTheTransactionStillCommits() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec, 420L, refuses("NaN"));

        List<java.util.logging.LogRecord> logs = warningsFrom(() -> {
            try {
                b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
                b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 410L, META);
                b.onEvent(new PgEvent.Insert(16412, row("2", "NaN")), 420L, META);   // poison
                b.onEvent(new PgEvent.Insert(16412, row("3", "c")), 430L, META);
                b.onEvent(new PgEvent.Commit(500L, 510L, 0L), 500L, null);
            } catch (Exception e) {
                throw new AssertionError("the skip must not throw", e);
            }
        });

        assertEquals(1, rec.batches.size(), "the transaction must still commit");
        List<SinkRecord> batch = rec.batches.get(0);
        assertEquals(2, batch.size(), "exactly the poison record is gone");
        assertEquals(List.of("1", "a"), batch.get(0).values());
        assertEquals(List.of("3", "c"), batch.get(1).values(),
                     "records after the skipped one are still there");
        assertEquals(500L, rec.commitLsns.get(0));
        assertFalse(b.hasOpenTransaction());

        // the audit trail: table, column, both LSNs, and the complete raw values
        assertEquals(1, logs.size(), logs.toString());
        String m = logs.get(0).getMessage();
        assertEquals(java.util.logging.Level.WARNING, logs.get(0).getLevel());
        assertTrue(m.contains("SKIPPED POISON RECORD"), m);
        assertTrue(m.contains("public.t"), m);                  // the table
        assertTrue(m.contains("column 'v'"), m);                // the column
        assertTrue(m.contains("0/000001A4"), m);                // the change LSN, 420
        assertTrue(m.contains("0/000001F4"), m);                // the commit LSN, 500
        assertTrue(m.contains("[\"2\", \"NaN\"]"), m);           // every raw value
        assertTrue(m.contains("[id, v]"), m);                   // in which column order
        assertTrue(m.contains("reinsert it by hand"), m);
    }

    /**
     * With commit batching the flusher only buffers the transaction. "Unset PGPAIMON_SKIP_LSN"
     * must not be said until the batch holding it has been committed: a writer killed inside the
     * interval would replay the poison record, and with the variable gone it would fail again.
     */
    @Test
    void theAuditLineWaitsForTheBatchCommitNotForTheBuffer() throws Exception {
        List<String> commits = new ArrayList<>();
        MiniBatcher mini = new MiniBatcher((r, c, e) -> commits.add("commit@" + c), x -> 0L,
            new MiniBatcher.Limits(1_000_000, 1L << 40, 3_600_000));
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, mini, 420L, refuses("NaN"));
        Runnable poisonTxn = () -> {
            try {
                b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
                b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 410L, META);
                b.onEvent(new PgEvent.Insert(16412, row("2", "NaN")), 420L, META);   // poison
                b.onEvent(new PgEvent.Commit(500L, 510L, 0L), 500L, null);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };

        List<java.util.logging.LogRecord> buffered = warningsFrom(poisonTxn);
        assertTrue(mini.hasBuffered());
        assertTrue(buffered.isEmpty(), "only buffered: nothing is committed, nothing skipped yet");

        // killed or disconnected now: the transaction replays, so it has skipped nothing
        mini.discard();
        assertTrue(commits.isEmpty());

        List<java.util.logging.LogRecord> replayed = warningsFrom(() -> {
            poisonTxn.run();
            try {
                mini.flushBuffered();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        assertEquals(List.of("commit@500"), commits);
        assertEquals(1, replayed.size(), replayed.toString());
        assertTrue(replayed.get(0).getMessage().contains("WAS committed to Paimon"));
        assertTrue(replayed.get(0).getMessage().contains("0/000001F4"), "the commit LSN, 500");
    }

    @Test
    void theAuditLineIsNotLoggedWhenTheBatchCommitFails() throws Exception {
        MiniBatcher mini = new MiniBatcher(
            (r, c, e) -> { throw new java.io.IOException("commit failed"); }, x -> 0L,
            new MiniBatcher.Limits(1, 1L << 40, 0));
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, mini, 420L, refuses("NaN"));
        List<java.util.logging.LogRecord> logs = warningsFrom(() -> {
            try {
                b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
                b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 410L, META);
                b.onEvent(new PgEvent.Insert(16412, row("2", "NaN")), 420L, META);
                assertThrows(java.io.IOException.class,
                    () -> b.onEvent(new PgEvent.Commit(500L, 510L, 0L), 500L, null));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        assertTrue(logs.isEmpty(), "the commit failed: the record replays, nothing was skipped");
    }

    /** A transaction whose only record was the poison one commits at once: logged at once. */
    @Test
    void anAuditForATransactionLeftEmptyIsLoggedWhenItIsConfirmed() throws Exception {
        List<String> commits = new ArrayList<>();
        MiniBatcher mini = new MiniBatcher((r, c, e) -> commits.add("commit@" + c), x -> 0L,
            new MiniBatcher.Limits(1_000_000, 1L << 40, 3_600_000));
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, mini, 420L, refuses("NaN"));
        List<java.util.logging.LogRecord> logs = warningsFrom(() -> {
            try {
                b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
                b.onEvent(new PgEvent.Insert(16412, row("2", "NaN")), 420L, META);
                b.onEvent(new PgEvent.Commit(500L, 510L, 0L), 500L, null);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        assertEquals(List.of("commit@500"), commits, "empty with nothing buffered: confirmed now");
        assertEquals(1, logs.size());
    }

    @Test
    void theAuditLineIsOnlyLoggedIfTheTransactionActuallyCommits() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec, 420L, refuses("NaN"));
        List<java.util.logging.LogRecord> logs = warningsFrom(() -> {
            try {
                b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
                b.onEvent(new PgEvent.Insert(16412, row("2", "NaN")), 420L, META);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            b.discardOpenTransaction();
        });
        assertTrue(logs.isEmpty(), "a transaction that never committed skipped nothing");
    }

    @Test
    void aNonMatchingSkipLsnLeavesTheFailureToTheSink() throws Exception {
        Recorder rec = new Recorder();
        // armed at 999, the poison record is at 420
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec, 999L, refuses("NaN"));
        b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
        b.onEvent(new PgEvent.Insert(16412, row("2", "NaN")), 420L, META);
        b.onEvent(new PgEvent.Commit(500L, 510L, 0L), 500L, null);

        assertEquals(1, rec.batches.size());
        assertEquals(1, rec.batches.get(0).size(),
                     "the record stays in the batch, so the sink fails on it as always");
        assertEquals(List.of("2", "NaN"), rec.batches.get(0).get(0).values());
    }

    /**
     * The test that proves the escape is not a blanket switch: a DIFFERENT failure at the very
     * same LSN must not be skipped.
     */
    @Test
    void anotherFailureAtTheMatchingLsnIsNotSkipped() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher.RowCheck somethingElse = (record, meta) -> {
            throw new IllegalStateException("record has 2 values but the relation has 3 columns");
        };
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec, 420L, somethingElse);

        List<java.util.logging.LogRecord> logs = warningsFrom(() -> {
            try {
                b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
                b.onEvent(new PgEvent.Insert(16412, row("2", "x")), 420L, META);
                b.onEvent(new PgEvent.Commit(500L, 510L, 0L), 500L, null);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        assertEquals(1, rec.batches.get(0).size(),
                     "an arity mismatch is not an unrepresentable value: the record stays");
        assertTrue(logs.isEmpty(), "and nothing is reported as skipped");

        // a plain IllegalArgumentException (a parse failure, not an unrepresentable value) too
        Recorder rec2 = new Recorder();
        TxnBatcher.RowCheck malformed = (record, meta) -> {
            throw new IllegalArgumentException("not hex-format bytea (expected \\x prefix)");
        };
        TxnBatcher b2 = new TxnBatcher(new ToastResolver(), null, rec2, 420L, malformed);
        b2.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
        b2.onEvent(new PgEvent.Insert(16412, row("2", "x")), 420L, META);
        b2.onEvent(new PgEvent.Commit(500L, 510L, 0L), 500L, null);
        assertEquals(1, rec2.batches.get(0).size(),
                     "a parse failure is systematic, not one bad value: never skipped");
    }

    @Test
    void theNoTransactionOpenInvariantSurvivesTheSkipPath() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec, 420L, refuses("NaN"));
        b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
        b.onEvent(new PgEvent.Insert(16412, row("2", "NaN")), 420L, META);
        assertTrue(b.hasOpenTransaction(), "the skip deliberately leaves the transaction open");

        // but any other throw still closes it, exactly as before
        assertThrows(IllegalStateException.class,
            () -> b.onEvent(new PgEvent.Begin(600L, 0L, 4712L), 600L, null));
        assertFalse(b.hasOpenTransaction());
    }

    @Test
    void withNoSkipLsnConfiguredTheCheckIsNeverCalled() throws Exception {
        Recorder rec = new Recorder();
        boolean[] called = { false };
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec, 0L,
            (record, meta) -> called[0] = true);
        b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 410L, META);
        b.onEvent(new PgEvent.Commit(500L, 510L, 0L), 500L, null);
        assertFalse(called[0], "unarmed, the escape must cost nothing on the hot path");
        assertEquals(1, rec.batches.get(0).size());
    }

}
