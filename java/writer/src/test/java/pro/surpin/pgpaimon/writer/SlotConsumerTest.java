// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class SlotConsumerTest {

    @Test
    void lsnTextFormRoundTrips() {
        assertEquals(0x16B374D8L, SlotConsumer.parseLsn("0/16B374D8"));
        assertEquals(0x100000000L, SlotConsumer.parseLsn("1/0"));
        assertEquals("0/16B374D8", SlotConsumer.formatLsn(0x16B374D8L));
        assertEquals("1/00000000", SlotConsumer.formatLsn(0x100000000L));
    }

    @Test
    void zeroLsnMeansStartFromTheSlotsOwnPosition() {
        assertEquals("0/00000000", SlotConsumer.formatLsn(0L));
    }

    /** Review Focus 4: a disconnect mid-transaction discards, never flushes. */
    @Test
    void disconnectDiscardsAnOpenTransaction() throws Exception {
        List<List<SinkRecord>> flushed = new ArrayList<>();
        TxnBatcher batcher = new TxnBatcher(new ToastResolver(), null,
            (records, commitLsn, endLsn) -> flushed.add(List.copyOf(records)));

        RelationMeta meta = new RelationMeta(1, "public", "t",
            List.of("id"), List.of(true));
        batcher.onEvent(new PgEvent.Begin(10L, 0L, 1L), 10L, null);
        batcher.onEvent(new PgEvent.Insert(1,
            new TupleData(List.of(ColumnValue.of("1")))), 11L, meta);

        assertTrue(batcher.hasOpenTransaction());
        batcher.discardOpenTransaction();          // what onDisconnect() does
        assertFalse(batcher.hasOpenTransaction());
        assertTrue(flushed.isEmpty(),
                   "a transaction PostgreSQL may still abort must never be committed");
    }

    // ---- the real handler wiring (Main.PipelineHandler) -----------------------------------

    static final RelationMeta META = new RelationMeta(1, "public", "t",
        List.of("id"), List.of(true));

    /** Records every call the handler makes, in order. */
    static final class Fixture implements Main.PipelineHandler.Target {
        final List<String> calls = new ArrayList<>();
        final List<List<SinkRecord>> flushed = new ArrayList<>();
        final List<Long> confirmed = new ArrayList<>();
        final Set<Long> tracked = new HashSet<>(Set.of(1L, 2L));
        final TxnBatcher batcher;
        final MiniBatcher mini;
        final Main.PipelineHandler handler;
        long now;    // fake clock, nanoseconds

        /** One transaction per commit: the pre-batching behaviour, which these tests assume. */
        Fixture() {
            this(new MiniBatcher.Limits(1, Long.MAX_VALUE, 0));
        }

        Fixture(MiniBatcher.Limits limits) {
            TxnBatcher.Flusher inner = (records, commitLsn, endLsn) -> {
                calls.add("flush@" + commitLsn);
                flushed.add(List.copyOf(records));
            };
            mini = new MiniBatcher(Main.confirmingFlusher(inner, lsn -> {
                calls.add("confirm@" + lsn);
                confirmed.add(lsn);
            }), relid -> 0L, limits, () -> now);
            batcher = new TxnBatcher(new ToastResolver(), null, mini);
            handler = new Main.PipelineHandler(tracked::contains, r -> List.of("id"),
                m -> calls.add("ddl"), m -> true,
                this, batcher, mini);
        }

        RelationMeta lastMeta;
        @Override public void setRelation(long relid, RelationMeta meta) {
            lastMeta = meta;
            calls.add("relation" + relid);
        }
        @Override public void truncate(long relid, long commitLsn) {
            calls.add("truncate" + relid + "@" + commitLsn);
        }
    }

    static PgEvent.Insert insert(long relid, String id) {
        return new PgEvent.Insert(relid, new TupleData(List.of(ColumnValue.of(id))));
    }

    @Test
    void aTruncateIsBufferedAndAppliedAtCommitJustBelowTheCommitLsn() throws Exception {
        Fixture f = new Fixture();
        f.handler.onEvent(new PgEvent.Begin(50L, 0L, 7L), 40L);
        f.handler.onEvent(new PgEvent.Truncate(List.of(1L, 99L), false, false), 41L);
        assertEquals(List.of(), f.calls, "nothing may be applied before the Commit is known");
        f.handler.onEvent(new PgEvent.Commit(50L, 55L, 0L), 50L);
        // the untracked relation 99 is dropped; the truncate precedes the confirm
        assertEquals(List.of("truncate1@49", "flush@50", "confirm@55"), f.calls);
        assertEquals(List.of(55L), f.confirmed);
    }

    /** Review Focus 4, asserting the discard itself: buffered truncates and rows both vanish. */
    @Test
    void reconnectMidTransactionDiscardsRowsAndTruncatesThenReplaysWhole() throws Exception {
        Fixture f = new Fixture();
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        f.handler.onEvent(new PgEvent.Begin(50L, 0L, 7L), 40L);
        f.handler.onEvent(new PgEvent.Truncate(List.of(2L), false, false), 41L);
        f.handler.onEvent(insert(1, "1"), 42L);
        assertTrue(f.batcher.hasOpenTransaction());

        f.handler.onDisconnect();

        assertFalse(f.batcher.hasOpenTransaction(), "the open batch was discarded");
        assertEquals(List.of("relation1"), f.calls, "no truncate, no flush");
        assertTrue(f.flushed.isEmpty());
        assertTrue(f.confirmed.isEmpty(), "nothing partial may be confirmed");

        // The server replays the transaction whole from the last confirmed LSN. Had the
        // truncate buffer survived, relation 2 would be truncated twice (or relation 2 would
        // appear here without having been replayed); the commit must see exactly one.
        f.handler.onEvent(new PgEvent.Begin(50L, 0L, 7L), 40L);
        f.handler.onEvent(insert(1, "1"), 42L);
        f.handler.onEvent(new PgEvent.Commit(50L, 55L, 0L), 50L);
        assertEquals(List.of("relation1", "flush@50", "confirm@55"), f.calls,
            "the stale truncate of relation 2 must not survive the disconnect");
        assertEquals(1, f.flushed.size());
        assertEquals(1, f.flushed.get(0).size());
        assertEquals(List.of(55L), f.confirmed);
    }

    @Test
    void aFailedFlushIsNotConfirmedAndPropagates() {
        List<Long> confirmed = new ArrayList<>();
        TxnBatcher.Flusher f = Main.confirmingFlusher(
            (r, c, e) -> { throw new java.io.IOException("commit failed"); }, confirmed::add);
        assertThrows(java.io.IOException.class, () -> f.flush(List.of(), 1L, 2L));
        assertTrue(confirmed.isEmpty(), "confirm only after the Paimon commit returned");
    }

    @Test
    void aBatcherFailurePropagatesRatherThanBeingSwallowed() throws Exception {
        Fixture f = new Fixture();
        f.handler.onEvent(new PgEvent.Begin(50L, 0L, 7L), 40L);
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        // a DELETE with no old tuple (replica identity not FULL): the batcher refuses; the
        // handler must not hide it
        assertThrows(IllegalStateException.class, () -> f.handler.onEvent(
            new PgEvent.Delete(1, new TupleData(List.of(ColumnValue.of("1"))), true), 42L));
        assertFalse(f.batcher.hasOpenTransaction());
    }

    @Test
    void relationAndDdlMessagesReachTheirConsumers() throws Exception {
        Fixture f = new Fixture();
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        f.handler.onEvent(new PgEvent.Message(true, 5L, "paimon", new byte[0]), 5L);
        assertEquals(List.of("relation1", "ddl"), f.calls);
    }

    // ---- mini-batch commits through the real handler --------------------------------------

    static final MiniBatcher.Limits HOLD = new MiniBatcher.Limits(1_000_000, 1L << 40, 3_600_000);

    /** One complete transaction with one insert into {@code relid}. */
    static void txn(Fixture f, long commitLsn, long endLsn, long relid) throws Exception {
        f.handler.onEvent(new PgEvent.Begin(endLsn, 0L, commitLsn), commitLsn - 5);
        f.handler.onEvent(insert(relid, "1"), commitLsn - 3);
        f.handler.onEvent(new PgEvent.Commit(commitLsn, endLsn, 0L), commitLsn);
    }

    @Test
    void aDdlMessageFlushesTheBufferBeforeItIsApplied() throws Exception {
        Fixture f = new Fixture(HOLD);
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        txn(f, 100, 110, 1);
        txn(f, 200, 210, 1);
        assertEquals(List.of("relation1"), f.calls, "buffered, not committed");
        f.handler.onEvent(new PgEvent.Message(true, 250L, "paimon", new byte[0]), 250L);
        assertEquals(List.of("relation1", "flush@200", "confirm@210", "ddl"), f.calls,
            "the batch (identifier = the last transaction's commit LSN) precedes the schema change");
        assertEquals(2, f.flushed.get(0).size());
    }

    @Test
    void aMessageThatIsNotDdlDoesNotBreakTheBatch() throws Exception {
        Fixture f = new Fixture(HOLD);
        // a second handler whose predicate rejects the message
        List<String> calls = new ArrayList<>();
        MiniBatcher mini = new MiniBatcher((r, c, e) -> calls.add("flush@" + c), x -> 0L, HOLD);
        TxnBatcher batcher = new TxnBatcher(new ToastResolver(), null, mini);
        Main.PipelineHandler h = new Main.PipelineHandler(x -> true, r -> List.of("id"),
            m -> calls.add("other-message"), m -> false, f, batcher, mini);
        h.onEvent(new PgEvent.Relation(META), 0L);
        h.onEvent(new PgEvent.Begin(110, 0L, 1L), 95L);
        h.onEvent(insert(1, "1"), 97L);
        h.onEvent(new PgEvent.Commit(100, 110, 0L), 100L);
        h.onEvent(new PgEvent.Message(true, 120L, "someone-else", new byte[0]), 120L);
        assertEquals(List.of("other-message"), calls);
        assertTrue(mini.hasBuffered());
    }

    @Test
    void aTruncateFlushesTheBufferFirstAndTheRestOfItsTransactionStartsANewOne() throws Exception {
        Fixture f = new Fixture(HOLD);
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        f.handler.onEvent(new PgEvent.Relation(new RelationMeta(2, "public", "u",
            List.of("id"), List.of(true))), 0L);
        txn(f, 100, 110, 1);                              // an append to table 1, buffered
        f.calls.clear();

        f.handler.onEvent(new PgEvent.Begin(310, 0L, 7L), 290L);
        f.handler.onEvent(new PgEvent.Truncate(List.of(1L), false, false), 291L);
        f.handler.onEvent(insert(2, "5"), 292L);          // DML on ANOTHER table, same transaction
        f.handler.onEvent(new PgEvent.Commit(300, 310, 0L), 300L);

        assertEquals(List.of("flush@100", "confirm@110", "truncate1@299"), f.calls,
            "earlier append, then the overwrite under this transaction's commit LSN; the"
            + " remainder waits in a new buffer");
        assertTrue(f.mini.hasBuffered());
        assertEquals(List.of(110L), f.confirmed, "the truncating transaction is not confirmed yet");

        f.handler.onShutdown();
        assertEquals(List.of("flush@100", "confirm@110", "truncate1@299", "flush@300", "confirm@310"),
            f.calls);
        assertEquals(1, f.flushed.get(1).size());
        assertEquals(2L, f.flushed.get(1).get(0).relid());
    }

    /**
     * BEGIN; TRUNCATE t; INSERT INTO t ...; COMMIT. The overwrite goes in one below the commit
     * LSN, so the per-table "already committed" rule does not then drop the rows inserted after
     * it: under the commit LSN itself they were filtered as a replay and silently lost.
     */
    @Test
    void rowsInsertedAfterATruncateInTheSameTransactionAreCommittedAfterTheOverwrite()
            throws Exception {
        Fixture f = new Fixture();
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        f.handler.onEvent(new PgEvent.Begin(50L, 0L, 7L), 40L);
        f.handler.onEvent(insert(1, "old"), 41L);
        f.handler.onEvent(new PgEvent.Truncate(List.of(1L), false, false), 42L);
        f.handler.onEvent(insert(1, "new"), 43L);
        f.handler.onEvent(new PgEvent.Commit(50L, 55L, 0L), 50L);
        assertEquals(List.of("relation1", "truncate1@49", "flush@50", "confirm@55"), f.calls);
        assertEquals(1, f.flushed.get(0).size(), "the row inserted after the truncate");
        assertEquals(List.of("new"), f.flushed.get(0).get(0).values());
    }

    @Test
    void aTruncateOnlyTransactionConfirmsAfterTheOverwrite() throws Exception {
        Fixture f = new Fixture(HOLD);
        f.handler.onEvent(new PgEvent.Begin(310, 0L, 7L), 290L);
        f.handler.onEvent(new PgEvent.Truncate(List.of(1L), false, false), 291L);
        f.handler.onEvent(new PgEvent.Commit(300, 310, 0L), 300L);
        assertEquals(List.of("truncate1@299", "flush@300", "confirm@310"), f.calls);
    }

    @Test
    void theIdleCallbackFiresTheTimeLimitWhenNoEventsArrive() throws Exception {
        Fixture f = new Fixture(new MiniBatcher.Limits(1_000_000, 1L << 40, 1000));
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        txn(f, 100, 110, 1);
        f.now = 999_000_000L;
        f.handler.onIdle();
        assertEquals(List.of(), f.confirmed);
        f.now = 1_000_000_000L;
        f.handler.onIdle();
        assertEquals(List.of(110L), f.confirmed);
        assertEquals(List.of("relation1", "flush@100", "confirm@110"), f.calls);
    }

    @Test
    void theTimeLimitAlsoFiresWhileEventsAreFlowing() throws Exception {
        Fixture f = new Fixture(new MiniBatcher.Limits(1_000_000, 1L << 40, 1000));
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        txn(f, 100, 110, 1);
        f.now = 5_000_000_000L;
        // a long transaction is streaming in; its first event reaches the aged buffer
        f.handler.onEvent(new PgEvent.Begin(210, 0L, 8L), 190L);
        assertEquals(List.of(110L), f.confirmed, "only the completed transaction, never the open one");
    }

    @Test
    void aCleanStopDrainsAndAConnectionLossDiscards() throws Exception {
        Fixture f = new Fixture(HOLD);
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        txn(f, 100, 110, 1);
        f.handler.onDisconnect();
        f.handler.onShutdown();
        assertEquals(List.of(), f.confirmed, "discarded: complete but unconfirmed, replays");
        assertTrue(f.flushed.isEmpty());

        txn(f, 100, 110, 1);                  // the replay
        f.handler.onShutdown();
        assertEquals(List.of(110L), f.confirmed);
    }

    @Test
    void aChangedRelationFlushesTheBufferButAnIdenticalOneDoesNot() throws Exception {
        Fixture f = new Fixture(HOLD);
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        txn(f, 100, 110, 1);
        f.handler.onEvent(new PgEvent.Relation(META), 0L);        // pgoutput resends it
        assertTrue(f.flushed.isEmpty(), "same description: keep batching");
        f.handler.onEvent(new PgEvent.Relation(new RelationMeta(1, "public", "t",
            List.of("id", "extra"), List.of(true, false))), 0L);
        assertEquals(1, f.flushed.size(), "records were shaped by the old description");
        assertEquals(List.of(110L), f.confirmed);
    }

    @Test
    void anExceptionFromTheCommitPropagatesOutOfTheHandlerAndConfirmsNothing() throws Exception {
        List<Long> confirmed = new ArrayList<>();
        MiniBatcher mini = new MiniBatcher(Main.confirmingFlusher(
            (r, c, e) -> { throw new java.io.IOException("commit failed"); }, confirmed::add),
            x -> 0L, new MiniBatcher.Limits(1, 1L << 40, 0));
        TxnBatcher batcher = new TxnBatcher(new ToastResolver(), null, mini);
        Main.PipelineHandler h = new Main.PipelineHandler(x -> true, r -> List.of("id"),
            m -> { }, m -> true, new Fixture(), batcher, mini);
        h.onEvent(new PgEvent.Relation(META), 0L);
        h.onEvent(new PgEvent.Begin(110, 0L, 1L), 95L);
        h.onEvent(insert(1, "1"), 97L);
        assertThrows(java.io.IOException.class,
            () -> h.onEvent(new PgEvent.Commit(100, 110, 0L), 100L));
        assertTrue(confirmed.isEmpty());
        assertFalse(batcher.hasOpenTransaction());
    }

    @Test
    void relidOfIsMinusOneForNonRowEvents() {
        assertEquals(5L, Main.relidOf(insert(5, "1")));
        assertEquals(-1L, Main.relidOf(new PgEvent.Begin(1L, 0L, 1L)));
    }

    @Test
    void registryColsJsonParses() {
        List<DdlMessage.Col> cols = DdlMessage.parseCols(
            "[{\"id\":1,\"name\":\"id\",\"type\":\"INT NOT NULL\"},"
            + "{\"id\":2,\"name\":\"v\",\"type\":\"STRING\"}]");
        assertEquals(List.of(new DdlMessage.Col(1, "id", "INT NOT NULL"),
                             new DdlMessage.Col(2, "v", "STRING")), cols);
    }

    @Test
    void onlyAPlainConnectionSqlExceptionIsRetryable() {
        assertTrue(SlotConsumer.isConnectionLoss(new java.sql.SQLException("lost", "08006")));
        assertTrue(SlotConsumer.isConnectionLoss(new java.sql.SQLException("no state")));
        assertTrue(SlotConsumer.isConnectionLoss(
            new java.sql.SQLException("slot is active for PID 5", "55006")));
        assertFalse(SlotConsumer.isConnectionLoss(new IllegalStateException("bad setting")));
        assertFalse(SlotConsumer.isConnectionLoss(new RuntimeException("decoder")));
    }

    @Test
    void aMissingOrInvalidatedSlotIsFatal() {
        assertFalse(SlotConsumer.isConnectionLoss(new java.sql.SQLException(
            "replication slot \"paimon\" does not exist", "42704")));
        assertFalse(SlotConsumer.isConnectionLoss(new java.sql.SQLException(
            "can no longer get changes from replication slot", "55000")));
    }

    @Test
    void aHandlerFailureWrappingASqlExceptionIsNotRetryable() {
        Throwable wrapped = new SlotConsumer.HandlerFailure(
            new java.sql.SQLException("paimon side", "08006"));
        assertFalse(SlotConsumer.isConnectionLoss(wrapped));
        assertInstanceOf(java.sql.SQLException.class, wrapped.getCause());
    }

    // ---- key flags come from the registry, not from pgoutput ------------------------------

    /** What pgoutput really sends under REPLICA IDENTITY FULL: every column flagged as a key. */
    static final RelationMeta ALL_KEY = new RelationMeta(1, "public", "t",
        List.of("id", "n", "body"), List.of(true, true, true));

    static PgEvent.Update update(String[] oldV, String[] newV) {
        List<ColumnValue> o = new ArrayList<>(), n = new ArrayList<>();
        for (String v : oldV) o.add(ColumnValue.of(v));
        for (String v : newV) n.add(ColumnValue.of(v));
        return new PgEvent.Update(1, new TupleData(o), false, new TupleData(n));
    }

    private List<RowKindAt> kindsOf(String[] oldV, String[] newV) throws Exception {
        List<List<SinkRecord>> out = new ArrayList<>();
        Fixture f = new Fixture();
        f.handler.onEvent(new PgEvent.Relation(ALL_KEY), 0L);
        f.handler.onEvent(new PgEvent.Begin(50L, 0L, 7L), 40L);
        f.handler.onEvent(update(oldV, newV), 41L);
        f.handler.onEvent(new PgEvent.Commit(50L, 55L, 0L), 50L);
        List<RowKindAt> kinds = new ArrayList<>();
        for (SinkRecord r : f.flushed.get(0)) kinds.add(new RowKindAt(r.kind().shortString()));
        return kinds;
    }

    record RowKindAt(String kind) {}

    /** Guards the flag rebuild itself, independently of what the resolver does with it. */
    @Test
    void theRegistrysPrimaryKeyReplacesPgoutputsAllTrueFlags() throws Exception {
        Fixture f = new Fixture();
        f.handler.onEvent(new PgEvent.Relation(ALL_KEY), 0L);
        assertEquals(List.of(true, false, false), f.lastMeta.isKey());
    }

    /** Guards the key-change rule: only the primary key column decides -U/+U versus -D/+I. */
    @Test
    void updateOfANonKeyColumnIsUpdateBeforeAfterNotDeleteInsert() throws Exception {
        assertEquals(List.of(new RowKindAt("-U"), new RowKindAt("+U")),
            kindsOf(new String[]{"1", "5", "a"}, new String[]{"1", "6", "a"}));
    }

    @Test
    void updateOfThePrimaryKeyIsStillDeleteInsert() throws Exception {
        assertEquals(List.of(new RowKindAt("-D"), new RowKindAt("+I")),
            kindsOf(new String[]{"1", "5", "a"}, new String[]{"2", "5", "a"}));
    }

    @Test
    void anUntrackedRelationIsPassedThroughUnchanged() throws Exception {
        Fixture f = new Fixture();
        f.tracked.clear();
        f.handler.onEvent(new PgEvent.Relation(ALL_KEY), 0L);
        assertSame(ALL_KEY, f.lastMeta);
    }
}
