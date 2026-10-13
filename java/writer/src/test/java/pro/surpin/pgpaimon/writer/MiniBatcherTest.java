// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;

/** The mini-batch layer on its own: triggers, identifiers, confirm order and replay safety. */
class MiniBatcherTest {

    /** A stand-in for Paimon that applies {@code filterAndCommit}'s measured rule. */
    static final class FakeStore {
        final Map<Long, List<SinkRecord>> rows = new TreeMap<>();    // relid -> committed records
        final Map<Long, Long> latest = new HashMap<>();              // relid -> highest identifier
        final List<String> log = new ArrayList<>();                  // commit / confirm, in order
        final List<Long> confirmed = new ArrayList<>();
        int commits;
        boolean failNext;

        /** What Paimon 1.4.1 does: a committable at or below the user's latest is dropped. */
        void commit(List<SinkRecord> records, long id) {
            if (failNext) {
                failNext = false;
                throw new IllegalStateException("commit failed");
            }
            Map<Long, List<SinkRecord>> by = new LinkedHashMap<>();
            for (SinkRecord r : records) {
                by.computeIfAbsent(r.relid(), k -> new ArrayList<>()).add(r);
            }
            for (var e : by.entrySet()) {
                if (id <= latest.getOrDefault(e.getKey(), 0L)) {
                    log.add("filtered@" + id + " rel" + e.getKey());
                    continue;
                }
                rows.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).addAll(e.getValue());
                latest.put(e.getKey(), id);
                commits++;
                log.add("commit@" + id + " rel" + e.getKey() + " n=" + e.getValue().size());
            }
        }

        MiniBatcher batcher(MiniBatcher.Limits limits, long[] clock) {
            TxnBatcher.Flusher inner = (records, id, end) -> commit(records, id);
            return new MiniBatcher(Main.confirmingFlusher(inner, lsn -> {
                confirmed.add(lsn);
                log.add("confirm@" + lsn);
            }), relid -> latest.getOrDefault(relid, 0L), limits, () -> clock[0]);
        }

        long count(long relid) {
            return rows.getOrDefault(relid, List.of()).size();
        }
    }

    static final MiniBatcher.Limits BIG = new MiniBatcher.Limits(1_000_000, 1L << 40, 3_600_000);

    static SinkRecord rec(long relid, long seq, String v) {
        return new SinkRecord(relid, RowKind.INSERT, Arrays.asList(Long.toString(seq), v), seq, 0, 1L);
    }

    /** Transaction n: commit LSN 1000*n, end LSN 1000*n + 10, one record for each relid given. */
    static void tx(MiniBatcher b, int n, long... relids) throws Exception {
        List<SinkRecord> rs = new ArrayList<>();
        for (long r : relids) {
            rs.add(rec(r, 1000L * n - 5, "t" + n));
        }
        b.flush(rs, 1000L * n, 1000L * n + 10);
    }

    static final long NS_PER_MS = 1_000_000L;

    // ---- the three triggers ---------------------------------------------------------------

    @Test
    void theRowLimitFiresAndCommitsEverythingBuffered() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(3, 1L << 40, 3_600_000), new long[1]);
        tx(b, 1, 1);
        tx(b, 2, 1);
        assertEquals(0, s.commits, "two rows: below the limit, nothing committed");
        assertEquals(List.of(), s.confirmed, "nothing buffered may be confirmed");
        tx(b, 3, 1);
        assertEquals(1, s.commits);
        assertEquals(3, s.count(1));
        assertEquals(List.of(3010L), s.confirmed);
    }

    @Test
    void theByteLimitFires() throws Exception {
        FakeStore s = new FakeStore();
        long perRecord = MiniBatcher.estimate(List.of(rec(1, 1, "t1")));
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(1_000_000, perRecord * 2, 3_600_000),
                                  new long[1]);
        tx(b, 1, 1);
        assertEquals(0, s.commits);
        tx(b, 2, 1);
        assertEquals(1, s.commits, "two records reach the byte estimate");
        assertEquals(2, s.count(1));
    }

    @Test
    void theByteEstimateCountsTextPayloadAndAFixedChargePerRecord() {
        SinkRecord r = new SinkRecord(1, RowKind.INSERT, Arrays.asList("abc", null, "de"), 1, 0, 1);
        assertEquals(MiniBatcher.RECORD_OVERHEAD_BYTES + 5, MiniBatcher.estimate(List.of(r)));
    }

    @Test
    void theIntervalIsMeasuredFromTheFirstTransactionNotTheLast() throws Exception {
        FakeStore s = new FakeStore();
        long[] clock = {0};
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(1_000_000, 1L << 40, 1000), clock);
        tx(b, 1, 1);                       // t = 0
        clock[0] = 900 * NS_PER_MS;
        tx(b, 2, 1);                       // later traffic must not push the deadline back
        clock[0] = 999 * NS_PER_MS;
        b.tick();
        assertEquals(0, s.commits, "999 ms after the first transaction: not yet");
        clock[0] = 1000 * NS_PER_MS;
        b.tick();                          // what the idle callback does
        assertEquals(1, s.commits);
        assertEquals(2, s.count(1), "both transactions, in one commit");
        assertEquals(List.of(2010L), s.confirmed);
    }

    @Test
    void theIntervalRestartsWithTheNextBufferNotTheLastFlush() throws Exception {
        FakeStore s = new FakeStore();
        long[] clock = {0};
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(1_000_000, 1L << 40, 1000), clock);
        tx(b, 1, 1);
        clock[0] = 1000 * NS_PER_MS;
        b.tick();
        clock[0] = 5000 * NS_PER_MS;
        tx(b, 2, 1);                       // first transaction of a new buffer, at t = 5000
        clock[0] = 5999 * NS_PER_MS;
        b.tick();
        assertEquals(1, s.commits);
        clock[0] = 6000 * NS_PER_MS;
        b.tick();
        assertEquals(2, s.commits);
    }

    @Test
    void anAgedBufferIsFlushedWhenTheNextTransactionArrivesToo() throws Exception {
        FakeStore s = new FakeStore();
        long[] clock = {0};
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(1_000_000, 1L << 40, 1000), clock);
        tx(b, 1, 1);
        clock[0] = 2000 * NS_PER_MS;
        tx(b, 2, 1);
        assertEquals(1, s.commits);
        assertEquals(2, s.count(1));
    }

    // ---- transactions, identifiers and confirm -------------------------------------------

    @Test
    void aTransactionIsNeverSplitEvenWhenItAloneExceedsTheLimit() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(3, 1L << 40, 3_600_000), new long[1]);
        tx(b, 1, 1, 1, 1, 1, 1);           // five records, limit three
        assertEquals(1, s.commits);
        assertEquals(5, s.count(1));
        assertEquals(List.of("commit@1000 rel1 n=5", "confirm@1010"), s.log);
    }

    @Test
    void theIdentifierAndTheConfirmedPositionAreTheLastTransactions() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(3, 1L << 40, 3_600_000), new long[1]);
        tx(b, 1, 1);
        tx(b, 2, 1);
        tx(b, 3, 1);
        assertEquals(3000L, s.latest.get(1L), "commit LSN of the last transaction");
        assertEquals(List.of(3010L), s.confirmed, "end LSN of the last transaction");
    }

    @Test
    void theConfirmComesStrictlyAfterTheCommit() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(2, 1L << 40, 3_600_000), new long[1]);
        tx(b, 1, 1);
        tx(b, 2, 1);
        assertEquals(List.of("commit@2000 rel1 n=2", "confirm@2010"), s.log);
    }

    @Test
    void aFailedCommitIsFatalAndNothingIsConfirmed() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(2, 1L << 40, 3_600_000), new long[1]);
        tx(b, 1, 1);
        s.failNext = true;
        assertThrows(IllegalStateException.class, () -> tx(b, 2, 1));
        assertEquals(List.of(), s.confirmed, "no half-committed state may be confirmed");
        assertFalse(b.hasBuffered(), "the failed batch is not retried by the next call");
    }

    @Test
    void anEmptyTransactionAdvancesTheConfirmedPositionWithoutASnapshot() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(BIG, new long[1]);
        b.flush(List.of(), 1000L, 1010L);  // DDL-only, nothing buffered
        assertEquals(List.of(1010L), s.confirmed);
        assertEquals(0, s.commits, "no records, no snapshot");
        assertFalse(b.hasBuffered());
    }

    @Test
    void anEmptyTransactionBehindBufferedDataQueuesAndIsNotConfirmedEarly() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(BIG, new long[1]);
        tx(b, 1, 1);
        b.flush(List.of(), 2000L, 2010L);
        assertEquals(List.of(), s.confirmed,
            "confirming the empty transaction would confirm the buffered data before it commits");
        b.flushBuffered();
        assertEquals(List.of(2010L), s.confirmed);
        assertEquals(2000L, s.latest.get(1L));
    }

    @Test
    void rowsEqualOneReproducesPerTransactionCommits() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(1, 1L << 40, 3_600_000), new long[1]);
        for (int n = 1; n <= 4; n++) {
            tx(b, n, 1);
        }
        assertEquals(4, s.commits);
        assertEquals(List.of(1010L, 2010L, 3010L, 4010L), s.confirmed);
        assertEquals(List.of("commit@1000 rel1 n=1", "confirm@1010",
                             "commit@2000 rel1 n=1", "confirm@2010",
                             "commit@3000 rel1 n=1", "confirm@3010",
                             "commit@4000 rel1 n=1", "confirm@4010"), s.log);
    }

    // ---- barrier, stop, disconnect -------------------------------------------------------

    @Test
    void flushBufferedIsABarrierThatCommitsThenConfirms() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(BIG, new long[1]);
        tx(b, 1, 1);
        tx(b, 2, 1);
        b.flushBuffered();
        assertEquals(List.of("commit@2000 rel1 n=2", "confirm@2010"), s.log);
        b.flushBuffered();
        assertEquals(2, s.log.size(), "an empty buffer is a no-op");
    }

    @Test
    void aCleanStopDrainsAndADisconnectDiscards() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(BIG, new long[1]);
        tx(b, 1, 1);
        b.discard();
        assertFalse(b.hasBuffered());
        b.drain();
        assertEquals(0, s.commits, "a discarded transaction is replayed, never committed");
        assertEquals(List.of(), s.confirmed);

        tx(b, 2, 1);
        b.drain();
        assertEquals(1, s.commits);
        assertEquals(List.of(2010L), s.confirmed);
    }

    @Test
    void memoryIsBoundedByTheLimitsPlusOneTransaction() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(new MiniBatcher.Limits(10, 1L << 40, 3_600_000), new long[1]);
        long peak = 0;
        for (int n = 1; n <= 100; n++) {
            tx(b, n, 1, 1, 1);             // three-record transactions
            peak = Math.max(peak, b.bufferedRows());
        }
        assertTrue(peak < 10, "after every transaction the buffer is below the limit: " + peak);
    }

    // ---- replay: rule 4 ------------------------------------------------------------------

    /** Every transaction's record exactly once per table, nothing else. */
    private static void assertExactlyOnce(FakeStore s, long relid, int txCount) {
        Map<Long, Integer> perSeq = new TreeMap<>();
        for (SinkRecord r : s.rows.getOrDefault(relid, List.of())) {
            perSeq.merge(r.seq(), 1, Integer::sum);
        }
        for (int n = 1; n <= txCount; n++) {
            assertEquals(1, perSeq.getOrDefault(1000L * n - 5, 0),
                "table " + relid + " transaction " + n + " must be present exactly once: " + perSeq);
        }
        assertEquals(txCount, perSeq.size(), "no stray records: " + perSeq);
    }

    /**
     * Reconnect in the same process. The new stream starts at the server's confirmed_flush_lsn,
     * which can be behind what this process has already committed (the confirm travels with the
     * next status update), so committed transactions are replayed into the SAME MiniBatcher,
     * whose cached identifier was read before that commit. Only the per-table update after the
     * commit lets rule 4 recognise them; the lookup is never consulted again.
     */
    @Test
    void afterAnInProcessReconnectTheTransactionsThisProcessCommittedAreDropped() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(BIG, new long[1]);
        tx(b, 1, 1);
        tx(b, 2, 1);
        tx(b, 3, 1);
        b.flushBuffered();                         // T1..T3 committed under L3
        assertEquals(List.of("commit@3000 rel1 n=3", "confirm@3010"), s.log);

        b.discard();                               // the connection drops
        tx(b, 1, 1);                               // the server replays from before T1
        tx(b, 2, 1);
        tx(b, 3, 1);
        tx(b, 4, 1);
        b.flushBuffered();                         // regrouped as T1..T4 under L4 > L3

        assertEquals("commit@4000 rel1 n=1", s.log.get(2),
            "T1..T3 dropped at assembly; only T4 is committed");
        assertExactlyOnce(s, 1, 4);
    }

    @Test
    void replayGroupedLargerDoesNotDuplicateTheOverlap() throws Exception {
        FakeStore s = new FakeStore();
        // First run: T1..T10 committed under L10; the process dies before the confirm reaches
        // the server (the confirm happened in memory only, which a restart forgets).
        MiniBatcher first = s.batcher(BIG, new long[1]);
        for (int n = 1; n <= 10; n++) {
            tx(first, n, 1);
        }
        first.flushBuffered();
        assertEquals(10000L, s.latest.get(1L));

        // Replay: the timer fires later, so T1..T12 are grouped under L12 > L10. Paimon's own
        // filter would COMMIT this (12 > 10); only rule 4 stops T1..T10 being written twice.
        MiniBatcher replay = s.batcher(BIG, new long[1]);     // a fresh process: empty cache
        for (int n = 1; n <= 12; n++) {
            tx(replay, n, 1);
        }
        replay.flushBuffered();
        assertEquals(12000L, s.latest.get(1L));
        assertExactlyOnce(s, 1, 12);
        assertFalse(s.log.stream().anyMatch(l -> l.startsWith("filtered")),
            "rule 4 acted before Paimon's filter was needed: " + s.log);
    }

    @Test
    void replayGroupedSmallerIsAlsoExactlyOnce() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher first = s.batcher(BIG, new long[1]);
        for (int n = 1; n <= 10; n++) {
            tx(first, n, 1);
        }
        first.flushBuffered();

        MiniBatcher replay = s.batcher(new MiniBatcher.Limits(7, 1L << 40, 3_600_000), new long[1]);
        for (int n = 1; n <= 12; n++) {
            tx(replay, n, 1);              // T1..T7 under L7, T8..T12 buffered
        }
        replay.flushBuffered();
        assertExactlyOnce(s, 1, 12);
    }

    @Test
    void aPartlyCommittedTransactionAppliesToTheUncommittedTableAlone() throws Exception {
        FakeStore s = new FakeStore();
        // Crash between the per-table commits of one batch: table 1 got T1..T4 under L4, table 2
        // got nothing.
        s.rows.put(1L, new ArrayList<>(List.of(
            rec(1, 995, "t1"), rec(1, 1995, "t2"), rec(1, 2995, "t3"), rec(1, 3995, "t4"))));
        s.latest.put(1L, 4000L);

        MiniBatcher replay = s.batcher(BIG, new long[1]);
        for (int n = 1; n <= 5; n++) {
            tx(replay, n, 1, 2);           // every transaction touches both tables
        }
        replay.flushBuffered();

        assertExactlyOnce(s, 1, 5);        // table 1: T1..T4 not repeated, T5 added
        assertExactlyOnce(s, 2, 5);        // table 2: all five
        assertEquals(5000L, s.latest.get(1L));
        assertEquals(5000L, s.latest.get(2L));
    }

    @Test
    void aTableAlreadyAheadOfTheWholeBatchIsLeftAlone() throws Exception {
        FakeStore s = new FakeStore();
        s.rows.put(1L, new ArrayList<>(List.of(rec(1, 995, "t1"), rec(1, 1995, "t2"))));
        s.latest.put(1L, 2000L);
        MiniBatcher replay = s.batcher(BIG, new long[1]);
        tx(replay, 1, 1);
        tx(replay, 2, 1);
        replay.flushBuffered();
        assertEquals(2, s.count(1));
        assertEquals(List.of("confirm@2010"), s.log, "nothing committed, position still advances");
    }

    @Test
    void thePerTableMemoryFollowsOurOwnCommitsWithoutRereadingPaimon() throws Exception {
        FakeStore s = new FakeStore();
        int[] lookups = {0};
        TxnBatcher.Flusher inner = (records, id, end) -> s.commit(records, id);
        MiniBatcher b = new MiniBatcher(Main.confirmingFlusher(inner, s.confirmed::add),
            relid -> { lookups[0]++; return s.latest.getOrDefault(relid, 0L); },
            new MiniBatcher.Limits(1, 1L << 40, 0), () -> 0L);
        for (int n = 1; n <= 5; n++) {
            tx(b, n, 1);
        }
        assertEquals(1, lookups[0], "read once, then kept current in memory");
        assertEquals(5, s.count(1));
    }

    @Test
    void forgettingTheCacheRereadsPaimonAfterADdlMessage() throws Exception {
        FakeStore s = new FakeStore();
        int[] lookups = {0};
        TxnBatcher.Flusher inner = (records, id, end) -> s.commit(records, id);
        MiniBatcher b = new MiniBatcher(Main.confirmingFlusher(inner, s.confirmed::add),
            relid -> { lookups[0]++; return s.latest.getOrDefault(relid, 0L); },
            new MiniBatcher.Limits(1, 1L << 40, 0), () -> 0L);
        tx(b, 1, 1);
        b.forgetCommitted();
        tx(b, 2, 1);
        assertEquals(2, lookups[0]);
    }

    @Test
    void aTruncateIdentifierCountsAsCommitted() throws Exception {
        FakeStore s = new FakeStore();
        MiniBatcher b = s.batcher(BIG, new long[1]);
        b.noteCommitted(1, 3000L);         // an overwrite committed under T3's LSN
        tx(b, 2, 1);                       // a replayed earlier transaction: dropped
        tx(b, 4, 1);
        b.flushBuffered();
        assertEquals(1, s.count(1));
        assertEquals(3995L, s.rows.get(1L).get(0).seq(), "only T4 survives");
    }

    @Test
    void lessThanOneIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new MiniBatcher.Limits(0, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new MiniBatcher.Limits(1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new MiniBatcher.Limits(1, 1, -1));
    }
}
