// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import pro.surpin.pgpaimon.writer.dto.*;

/**
 * The keepalive guard ({@link SlotConsumer#autoAdvanceGateMayOpen}) against a model of pgjdbc
 * 42.7.3's {@code V3PGReplicationStream}, driving the real {@link SlotConsumer#consume} loop, the
 * real {@link WriterBootstrap.PipelineHandler}, {@link TxnBatcher} and {@link MiniBatcher}.
 *
 * <p>
 * The property checked at <em>every</em> status update the model sends: each transaction whose
 * commit record starts below the flushed LSN being sent is already committed to Paimon. PostgreSQL
 * skips, on the next start, exactly the transactions whose commit record starts below
 * {@code confirmed_flush_lsn}, so a violation is a transaction lost for good.
 */
class KeepaliveGuardTest {

  // ---- a model of pgjdbc 42.7.3 V3PGReplicationStream, from javap -c -p ----------------

  sealed interface Item permits Data, Keepalive, StatusDue, Pause {
  }

  /** An XLogData message: its WAL start and its pgoutput body. */
  record Data(
      long start,
      ByteBuffer body
  ) implements Item {
  }

  /** A primary keepalive: the walsender's send pointer, and whether it asks for a reply. */
  record Keepalive(
      long server,
      boolean reply
  ) implements Item {
  }

  /** The status interval elapses (isTimeUpdate() becomes true). Takes no read. */
  record StatusDue() implements Item {
  }

  /** Nothing pending: readPending() returns null, so the consumer calls onIdle. */
  record Pause(
      Runnable action
  ) implements Item {
  }

  /**
   * Mirrors readInternal(false), processKeepAliveMessage, processXLogData and updateStatusInternal
   * field for field. Every field starts at INVALID_LSN (0).
   */
  static final class PgJdbcModel implements PGReplicationStream {
    final Deque<Item> script;
    final Runnable onExhausted;
    final java.util.function.LongConsumer onSend;
    long lastReceive, lastFlushed, lastApplied, startOfLast, explicitlyFlushed;
    boolean timeDue;
    final List<Long> sent = new ArrayList<>();
    int autoAdvances;

    PgJdbcModel(
        Deque<Item> script,
        Runnable onExhausted,
        java.util.function.LongConsumer onSend
    ) {
      this.script = script;
      this.onExhausted = onExhausted;
      this.onSend = onSend;
    }

    /** receiveNextData(false): the next message, or null when nothing is pending. */
    private Item receive() {
      while (true) {
        Item it = script.poll();
        if (it == null) {
          onExhausted.run();
          return null;
        }
        if (it instanceof StatusDue) {
          timeDue = true;
          continue;
        }
        if (it instanceof Pause p) {
          p.action().run();
          return null;
        }
        return it;
      }
    }

    @Override
    public ByteBuffer readPending() {
      boolean needReply = false;
      while (true) {
        Item it = receive();
        if (needReply || timeDue) {
          send(); // timeUpdateStatus()
        }
        if (it == null) {
          return null;
        }
        if (it instanceof Keepalive k) {
          needReply = keepalive(k);
          continue;
        }
        Data d = (Data) it; // processXLogData, LOGICAL
        startOfLast = d.start();
        lastReceive = d.start();
        return d.body().duplicate();
      }
    }

    private boolean keepalive(
        Keepalive k
    ) {
      if (k.server() > lastReceive) {
        lastReceive = k.server();
      }
      if (explicitlyFlushed >= startOfLast && k.server() > explicitlyFlushed
          && k.server() > lastFlushed) {
        lastFlushed = k.server();
        autoAdvances++;
      }
      return k.reply();
    }

    private void send() {
      onSend.accept(lastFlushed);
      sent.add(lastFlushed);
      explicitlyFlushed = lastFlushed;
      timeDue = false;
    }

    @Override
    public ByteBuffer read() {
      throw new UnsupportedOperationException();
    }

    @Override
    public LogSequenceNumber getLastReceiveLSN() {
      return LogSequenceNumber.valueOf(lastReceive);
    }

    @Override
    public LogSequenceNumber getLastFlushedLSN() {
      return LogSequenceNumber.valueOf(lastFlushed);
    }

    @Override
    public LogSequenceNumber getLastAppliedLSN() {
      return LogSequenceNumber.valueOf(lastApplied);
    }

    @Override
    public void setFlushedLSN(
        LogSequenceNumber l
    ) {
      lastFlushed = l.asLong();
    }

    @Override
    public void setAppliedLSN(
        LogSequenceNumber l
    ) {
      lastApplied = l.asLong();
    }

    @Override
    public void forceUpdateStatus() {
      send();
    }

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public void close() {}
  }

  // ---- the script: pgoutput messages with the LSNs a walsender gives them ---------------

  static final class Script {
    final Deque<Item> items = new ArrayDeque<>();
    /** xid -> commit LSN (the start of the commit record) of every transaction sent. */
    final Map<Long, Long> commits = new LinkedHashMap<>();
    private long xid;

    Script begin(
        long start,
        long commitLsn,
        long xid
    ) {
      this.xid = xid;
      return data(start,
          new PgOutputDecoderTest.Msg().b('B').i64(commitLsn).i64(0).i32((int) xid).buf());
    }

    /** A Relation message: pgoutput sends it with LSN 0. */
    Script relation(
        long relid
    ) {
      return data(0, new PgOutputDecoderTest.Msg().b('R').i32((int) relid).cstr("public")
          .cstr("t" + relid).b('d').i16(1).b(1).cstr("id").i32(23).i32(-1).buf());
    }

    Script insert(
        long start,
        long relid,
        int id
    ) {
      return data(start, new PgOutputDecoderTest.Msg().b('I').i32((int) relid).b('N').i16(1)
          .text(Integer.toString(id)).buf());
    }

    /** A Commit message's WAL start is the transaction's end LSN. */
    Script commit(
        long commitLsn,
        long endLsn
    ) {
      commits.put(xid, commitLsn);
      return data(endLsn,
          new PgOutputDecoderTest.Msg().b('C').b(0).i64(commitLsn).i64(endLsn).i64(0).buf());
    }

    /** One whole transaction: Begin at start, one insert, commit record at commitLsn. */
    Script txn(
        long xid,
        long start,
        long relid,
        long commitLsn,
        long endLsn
    ) {
      return begin(start, commitLsn, xid).insert(start, relid, (int) xid).commit(commitLsn, endLsn);
    }

    Script keepalive(
        long server,
        boolean reply
    ) {
      items.add(new Keepalive(server, reply));
      return this;
    }

    Script statusDue() {
      items.add(new StatusDue());
      return this;
    }

    Script pause(
        Runnable action
    ) {
      items.add(new Pause(action));
      return this;
    }

    private Script data(
        long start,
        ByteBuffer body
    ) {
      items.add(new Data(start, body));
      return this;
    }
  }

  /** The real pipeline wired to the model: confirm goes through SlotConsumer.confirm. */
  static final class Rig {
    final Script script;
    final Set<Long> committedXids = new HashSet<>();
    final List<Long> confirmed = new ArrayList<>();
    final List<String> violations = new ArrayList<>();
    long now;
    final MiniBatcher mini;
    final SlotConsumer consumer;
    final PgJdbcModel model;

    Rig(
        MiniBatcher.Limits limits,
        Script script
    ) {
      this.script = script;
      SlotConsumer[] ref = new SlotConsumer[1];
      TxnBatcher.Flusher paimon = (
          records,
          commitLsn,
          endLsn) -> {
        for (SinkRecord r : records) {
          committedXids.add(r.xid());
        }
      };
      mini = new MiniBatcher(WriterBootstrap.confirmingFlusher(paimon, lsn -> {
        confirmed.add(lsn);
        ref[0].confirm(lsn);
      }), relid -> 0L, limits, () -> now);
      TxnBatcher batcher = new TxnBatcher(new ToastResolver(), null, mini);
      WriterBootstrap.PipelineHandler handler =
          new WriterBootstrap.PipelineHandler(r -> true, r -> List.of("id"), m -> {
          }, m -> false, new WriterBootstrap.PipelineHandler.Target() {
            @Override
            public void setRelation(
                long relid,
                RelationMeta meta
            ) {}

            @Override
            public void truncate(
                long relid,
                long commitLsn
            ) {}
          }, batcher, mini);
      consumer = new SlotConsumer(null, PgOutputDecoderTest.decoder(), handler);
      ref[0] = consumer;
      model = new PgJdbcModel(script.items, consumer::stop, this::check);
    }

    /** The property: nothing below the flushed LSN being sent may be uncommitted. */
    void check(
        long flushed
    ) {
      for (var e : script.commits.entrySet()) {
        if (Long.compareUnsigned(e.getValue(), flushed) < 0
            && !committedXids.contains(e.getKey())) {
          violations.add("a status update confirmed " + SlotConsumer.formatLsn(flushed)
              + ", past the commit record of transaction " + e.getKey() + " at "
              + SlotConsumer.formatLsn(e.getValue()) + ", which was never committed to"
              + " Paimon: a crash now loses it");
        }
      }
    }

    Rig run() throws Exception {
      consumer.consume(model);
      assertEquals(List.of(), violations);
      return this;
    }
  }

  static final MiniBatcher.Limits HOLD = new MiniBatcher.Limits(1_000_000, 1L << 40, 3_600_000);
  static final MiniBatcher.Limits UNBATCHED = new MiniBatcher.Limits(1, 1L << 40, 0);

  /**
   * Idle keepalives move the confirmed position past the records long transaction A has written so
   * far, and a status update carries it. Exactly what the live reproduction did.
   */
  static Script idleAdvancePast(
      long lsn
  ) {
    return new Script().keepalive(lsn, false).statusDue().pause(() -> {
    });
  }

  // ---- the decision itself ------------------------------------------------------------

  @Test
  void theGuardDoesNothingWithNothingBufferedAndRewindsOrCommitsOtherwise() throws Exception {
    Rig r = new Rig(HOLD, new Script());
    PgJdbcModel m = r.model;
    m.lastFlushed = 0x2000;
    assertEquals(SlotConsumer.Guard.NOTHING, r.consumer.guardBeforeNextRead(m, 0x1000),
        "nothing buffered: an auto-advance passes nothing complete");
    r.mini.flush(
        List.of(
            new SinkRecord(1, org.apache.paimon.types.RowKind.INSERT, List.of("1"), 0x2100, 0, 2)),
        0x2180, 0x21A0);
    assertEquals(SlotConsumer.Guard.NOTHING, r.consumer.guardBeforeNextRead(m, 0x2001),
        "a message above the flushed position: the gate is shut");
    assertEquals(SlotConsumer.Guard.REWOUND, r.consumer.guardBeforeNextRead(m, 0x1000));
    assertEquals(0x0FFFL, m.lastFlushed);
    assertEquals(0x0FFFL, m.explicitlyFlushed, "sent at once");
    assertTrue(r.mini.hasBuffered(), "and nothing committed");
    assertEquals(SlotConsumer.Guard.COMMITTED, r.consumer.guardBeforeNextRead(m, 0));
    assertFalse(r.mini.hasBuffered());
    assertEquals(List.of(0x21A0L), r.confirmed);
  }

  @Test
  void theGateMayOpenWhenTheLastMessageStartsAtOrBelowTheFlushedPosition() {
    assertTrue(SlotConsumer.autoAdvanceGateMayOpen(0x100, 0x200), "an older transaction");
    assertTrue(SlotConsumer.autoAdvanceGateMayOpen(0x200, 0x200), "equal: >= in pgjdbc");
    assertFalse(SlotConsumer.autoAdvanceGateMayOpen(0x201, 0x200), "a newer message");
    assertTrue(SlotConsumer.autoAdvanceGateMayOpen(0, 0x200), "a Relation message carries 0");
    assertTrue(SlotConsumer.autoAdvanceGateMayOpen(0, 0), "a fresh stream: all fields 0");
    assertFalse(SlotConsumer.autoAdvanceGateMayOpen(0x8000_0000_0000_0000L, 0x7FFF_FFFF_FFFF_FFFFL),
        "LSNs compare unsigned");
  }

  // ---- the reviewer's sequence and its neighbours ---------------------------------------

  /**
   * A (xid 1) began at 0x1100 and is still open; idle keepalives confirm 0x2000, past its rows. B
   * (xid 2) commits and is buffered. A commits: its Begin and rows arrive with their original LSNs,
   * below the confirmed position, and a reply-requested keepalive lands mid-burst. Without the
   * guard pgjdbc auto-advances to 0x2200 and sends it in the same read: past B's commit record at
   * 0x2180, with B never committed. With it, pgjdbc's flushed position is rewound below A's Begin
   * and sent, which shuts the gate, and B stays in the batch.
   */
  @Test
  void anOlderTransactionArrivingBehindABufferedOneRewindsWithoutCommitting() throws Exception {
    Rig[] rig = new Rig[1];
    Script s = idleAdvancePast(0x2000).relation(1).txn(2, 0x2100, 1, 0x2180, 0x21A0) // B: buffered
        .begin(0x1100, 0x2200, 1)
        .pause(() -> assertEquals(0x10FFL, rig[0].model.explicitlyFlushed,
            "rewound below A's Begin and sent before the next read"))
        .insert(0x1100, 1, 1).insert(0x1200, 1, 2).keepalive(0x2200, true) // mid-burst, reply
                                                                           // requested
        .statusDue().insert(0x1300, 1, 3).keepalive(0x2200, false).commit(0x2200, 0x2220) // A:
                                                                                          // buffered
        .keepalive(0x2220, true);
    rig[0] = new Rig(HOLD, s);
    Rig r = rig[0].run();
    assertEquals(1, r.model.autoAdvances, "only the idle advance before B");
    assertEquals(List.of(0x2220L), r.confirmed, "B and A committed together at the clean stop");
  }

  /**
   * The gate also opens without an older transaction: a Relation message carries LSN 0, so a
   * keepalive between it and the next change auto-advances whatever the flushed position is. No
   * position is below 0, so the guard commits the buffer instead of rewinding.
   */
  @Test
  void aRelationMessageAtLsnZeroCommitsTheBufferFirst() throws Exception {
    Script s = idleAdvancePast(0x2000).relation(1).txn(2, 0x2100, 1, 0x2180, 0x21A0) // B: buffered
        .begin(0x2300, 0x2400, 3) // C: NEWER than everything
        .relation(2) // first use of table 2: LSN 0
        .keepalive(0x2400, true).insert(0x2300, 2, 1).commit(0x2400, 0x2420);
    Rig r = new Rig(HOLD, s).run();
    assertTrue(r.model.sent.contains(0x2400L), "the gate opened on the Relation message");
    assertEquals(0x21A0L, r.confirmed.get(0), "B was committed before that read");
  }

  @Test
  void severalBufferedTransactionsAreCommittedTogetherWhenARelationMessageArrives()
      throws Exception {
    Script s = idleAdvancePast(0x2000).relation(1).txn(2, 0x2100, 1, 0x2180, 0x21A0)
        .txn(3, 0x21B0, 1, 0x2200, 0x2220).txn(4, 0x2230, 1, 0x2280, 0x22A0)
        .begin(0x1100, 0x2300, 1) // rewind
        .relation(2) // then LSN 0: commit
        .keepalive(0x2300, true).insert(0x1100, 2, 1).insert(0x1200, 2, 2).commit(0x2300, 0x2320);
    Rig r = new Rig(HOLD, s).run();
    assertEquals(0x22A0L, r.confirmed.get(0), "one commit of B1..B3, confirmed at B3's end");
    assertTrue(r.committedXids.containsAll(List.of(2L, 3L, 4L)));
  }

  /** Each still older transaction rewinds further; none of them commits anything. */
  @Test
  void successivelyOlderTransactionsRewindAgain() throws Exception {
    Script s = idleAdvancePast(0x3000).relation(1).txn(2, 0x3100, 1, 0x3180, 0x31A0) // B: buffered
        .begin(0x1500, 0x3200, 5).insert(0x1500, 1, 1) // rewind to 0x14FF
        .keepalive(0x3200, true).commit(0x3200, 0x3220).begin(0x1100, 0x3300, 6)
        .insert(0x1100, 1, 2) // older still: 0x10FF
        .keepalive(0x3300, true).statusDue().insert(0x1200, 1, 3).keepalive(0x3300, true)
        .commit(0x3300, 0x3320).keepalive(0x3320, true);
    Rig r = new Rig(HOLD, s).run();
    assertTrue(r.model.sent.containsAll(List.of(0x14FFL, 0x10FFL)));
    assertEquals(List.of(0x3320L), r.confirmed, "one batch, at the clean stop");
  }

  /**
   * The interval fires in the middle of an older transaction's burst, after a rewind: the commit
   * raises the flushed position over the burst again, with nothing left buffered, and the
   * auto-advance that follows stops at the open transaction's commit record.
   */
  @Test
  void anIntervalCommitInsideTheBurstAfterARewindIsSafe() throws Exception {
    Rig[] rig = new Rig[1];
    Script s = idleAdvancePast(0x2000).relation(1).txn(2, 0x2100, 1, 0x2180, 0x21A0)
        .begin(0x1100, 0x2200, 1).insert(0x1100, 1, 1).pause(() -> rig[0].now += 2_000_000_000L) // the
                                                                                                 // interval
                                                                                                 // elapses
        .insert(0x1200, 1, 2) // tick: B is committed
        .statusDue() // sends B's end: gate open
        .keepalive(0x2200, true).insert(0x1300, 1, 3).commit(0x2200, 0x2220);
    rig[0] = new Rig(new MiniBatcher.Limits(1_000_000, 1L << 40, 1000), s);
    Rig r = rig[0].run();
    assertEquals(List.of(0x21A0L, 0x2220L), r.confirmed);
    assertTrue(r.model.sent.contains(0x2200L), "auto-advanced to A's commit record, not past it");
  }

  /**
   * Periodic status updates and idle keepalives, reply-requested or not, with a transaction
   * buffered and no older one arriving: the gate stays shut on its own, so the guard does not
   * commit early and batching is kept.
   */
  @Test
  void statusUpdatesAndIdleKeepalivesNeitherPassNorFlushABufferedTransaction() throws Exception {
    Rig[] rig = new Rig[1];
    Script s = idleAdvancePast(0x2000).relation(1).txn(2, 0x2100, 1, 0x2180, 0x21A0).statusDue()
        .keepalive(0x2500, false).keepalive(0x2500, true)
        .pause(() -> assertTrue(rig[0].mini.hasBuffered(), "still batching")).statusDue()
        .keepalive(0x2600, true)
        .pause(() -> assertTrue(rig[0].mini.hasBuffered(), "still batching"));
    rig[0] = new Rig(HOLD, s);
    rig[0].run();
    assertEquals(1, rig[0].model.autoAdvances, "only the idle advance before B");
    assertEquals(List.of(0x21A0L), rig[0].confirmed, "committed only at the clean stop");
  }

  /**
   * A transaction whose Begin is low and whose commit record is high is itself never passed: the
   * send pointer during its burst is at most the start of its commit record, and the guard keeps it
   * from passing anything completed.
   */
  @Test
  void aLowBeginHighCommitTransactionIsNotConfirmedBeforeItIsCommitted() throws Exception {
    Script s = idleAdvancePast(0x2000).relation(1).begin(0x1100, 0x3000, 1).insert(0x1100, 1, 1)
        .keepalive(0x3000, true).insert(0x1200, 1, 2).commit(0x3000, 0x3020) // buffered
        .keepalive(0x3020, true).statusDue().keepalive(0x3100, false).statusDue().pause(() -> {
        });
    Rig r = new Rig(HOLD, s).run();
    assertEquals(List.of(0x3020L), r.confirmed);
    assertFalse(
        r.model.sent.subList(0, r.model.sent.size() - 1).stream().anyMatch(f -> f > 0x3000L),
        "nothing past A's commit record is sent until A is committed");
  }

  /**
   * One commit per transaction: nothing complete is ever held back. The auto-advance in the open
   * transaction's burst stops at its commit record, which PostgreSQL replays.
   */
  @Test
  void unbatchedTheOpenTransactionIsNotPassed() throws Exception {
    Script s = idleAdvancePast(0x2000).relation(1).txn(2, 0x2100, 1, 0x2180, 0x21A0)
        .begin(0x1100, 0x2200, 1).insert(0x1100, 1, 1).keepalive(0x2200, true).insert(0x1200, 1, 2)
        .commit(0x2200, 0x2220);
    Rig r = new Rig(UNBATCHED, s).run();
    assertEquals(List.of(0x21A0L, 0x2220L), r.confirmed);
  }

  // ---- the idle and shutdown callbacks are really called by the loop ----------------------

  /** A single transaction followed by silence reaches Paimon within the interval. */
  @Test
  void theLoopCallsOnIdleSoTheIntervalFiresWithNoFurtherEvents() throws Exception {
    Rig[] rig = new Rig[1];
    Script s = new Script().relation(1).txn(2, 0x2100, 1, 0x2180, 0x21A0)
        .pause(() -> rig[0].now += 999_000_000L)
        .pause(() -> assertEquals(List.of(), rig[0].confirmed, "not yet: 999 ms"))
        .pause(() -> rig[0].now += 1_000_000L)
        .pause(() -> assertEquals(List.of(0x21A0L), rig[0].confirmed,
            "the interval fired from the idle callback, before any further event"));
    rig[0] = new Rig(new MiniBatcher.Limits(1_000_000, 1L << 40, 1000), s);
    rig[0].run();
  }

  @Test
  void aCleanStopCommitsTheBufferAndSendsItsPositionAtOnce() throws Exception {
    Script s = new Script().relation(1).txn(2, 0x2100, 1, 0x2180, 0x21A0);
    Rig r = new Rig(HOLD, s).run();
    assertTrue(r.committedXids.contains(2L), "onShutdown drained the buffer");
    assertEquals(0x21A0L, r.model.sent.get(r.model.sent.size() - 1),
        "and the final, forced status update carries its end");
  }

  // ---- confirm never lowers what pgjdbc may already have sent ---------------------------

  @Test
  void confirmNeverLowersTheFlushedPosition() throws Exception {
    Rig[] rig = new Rig[1];
    Script s = new Script().keepalive(0x5000, false).pause(() -> {
      rig[0].consumer.confirm(0x4000);
      assertEquals(0x5000L, rig[0].model.lastFlushed, "a lower confirm is ignored");
      rig[0].consumer.confirm(0x6000);
      assertEquals(0x6000L, rig[0].model.lastFlushed);
      assertEquals(0x6000L, rig[0].model.lastApplied);
    });
    rig[0] = new Rig(HOLD, s);
    rig[0].run();
  }
}
