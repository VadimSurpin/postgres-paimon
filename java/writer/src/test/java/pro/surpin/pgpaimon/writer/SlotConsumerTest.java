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
        final Main.PipelineHandler handler;

        Fixture() {
            TxnBatcher.Flusher inner = (records, commitLsn, endLsn) -> {
                calls.add("flush@" + commitLsn);
                flushed.add(List.copyOf(records));
            };
            batcher = new TxnBatcher(new ToastResolver(), null,
                Main.confirmingFlusher(inner, confirmed::add));
            handler = new Main.PipelineHandler(tracked::contains, r -> List.of("id"),
                m -> calls.add("ddl"),
                this, batcher);
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
    void aTruncateIsBufferedAndAppliedAtCommitUnderTheCommitLsn() throws Exception {
        Fixture f = new Fixture();
        f.handler.onEvent(new PgEvent.Begin(50L, 0L, 7L), 40L);
        f.handler.onEvent(new PgEvent.Truncate(List.of(1L, 99L), false, false), 41L);
        assertEquals(List.of(), f.calls, "nothing may be applied before the Commit is known");
        f.handler.onEvent(new PgEvent.Commit(50L, 55L, 0L), 50L);
        // the untracked relation 99 is dropped; the truncate precedes the confirm
        assertEquals(List.of("truncate1@50", "flush@50"), f.calls);
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
        assertEquals(List.of("relation1", "flush@50"), f.calls,
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
        f.handler.onEvent(new PgEvent.Truncate(List.of(1L), false, false), 41L);
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        // truncate then DML on the same table: the batcher refuses; the handler must not hide it
        assertThrows(IllegalStateException.class, () -> f.handler.onEvent(insert(1, "1"), 42L));
        assertFalse(f.batcher.hasOpenTransaction());
    }

    @Test
    void relationAndDdlMessagesReachTheirConsumers() throws Exception {
        Fixture f = new Fixture();
        f.handler.onEvent(new PgEvent.Relation(META), 0L);
        f.handler.onEvent(new PgEvent.Message(true, 5L, "paimon", new byte[0]), 5L);
        assertEquals(List.of("relation1", "ddl"), f.calls);
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
