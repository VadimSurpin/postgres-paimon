// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.springframework.stereotype.Service;
import pro.surpin.pgpaimon.writer.decoder.PgOutputDecoder;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.PgEvent;



/**
 * Owns the replication stream: connect, read, confirm, reconnect.
 *
 * <p>
 * Two kinds of failure are kept apart. A lost <em>connection</em> ({@link SQLException} from
 * connecting, opening or reading the stream) calls {@link Handler#onDisconnect()} and reconnects
 * with backoff. Anything else, including every exception from {@link Handler#onEvent} (so from the
 * batcher and from the Paimon flush) and from the decoder, is fatal and propagates out of
 * {@link #run}: continuing would advance the slot past data that was never written.
 */
@Slf4j
@Service
public final class SlotConsumer {

  public interface Handler {
    void onEvent(
        @NotNull PgEvent event,
        long changeLsn
    ) throws Exception;

    void onDisconnect();

    /**
     * Called from the loop when no message is pending, about every 10 ms. The only way a time-based
     * flush can fire when no events arrive; the loop is single-threaded and there is deliberately
     * no timer thread. An exception is fatal, like one from onEvent.
     */
    default void onIdle() throws Exception {}

    /**
     * Called once on a clean stop, after the last event and before the final status update, so
     * anything buffered can be committed and confirmed first. An exception is fatal.
     */
    default void onShutdown() throws Exception {}

    /**
     * Whether a complete transaction is being held back unconfirmed (buffered for a later commit).
     * Deliberately not a default method, nor is the next one: a handler that holds completed
     * transactions back and does not report them loses them on a crash.
     */
    boolean hasUnconfirmed();

    /**
     * Commit and confirm every complete transaction that is not yet confirmed, so that nothing
     * complete-but-unconfirmed remains. Called before the next read when pgjdbc's keepalive
     * auto-advance could otherwise move the flushed position past such a transaction and the
     * consumer cannot shut that off itself: see {@link SlotConsumer#guardBeforeNextRead}. An
     * exception is fatal.
     */
    void commitUnconfirmed() throws Exception;
  }


  private final @NotNull WriterConfig config;
  private final @NotNull PgOutputDecoder decoder;
  private final @NotNull Handler handler;
  // private volatile @Nullable PGReplicationStream stream;
  private volatile boolean running = true;
  private volatile @Nullable PGReplicationStream stream;
  private volatile long flushedAtCleanStop;

  public SlotConsumer(
      @NotNull WriterConfig config,
      @NotNull PgOutputDecoder decoder,
      @NotNull Handler handler
  ) {
    this.config = config;
    this.decoder = decoder;
    this.handler = handler;
  }

  /**
   * The flushed position last sent to the server by a clean stop; 0 when the stream did not end
   * with one (see {@link StopMark}).
   */
  public long flushedAtCleanStop() {
    return flushedAtCleanStop;
  }

  public void stop() {
    running = false;
  }

  /**
   * Sets the flushed and applied LSN. Call only after the Paimon commit has returned.
   *
   * <p>
   * Never lowers the flushed position. pgjdbc may have advanced it itself on a keepalive (see
   * {@link #autoAdvanceGateMayOpen}); setting it lower here would break the property the keepalive
   * guard relies on: that {@code getLastFlushedLSN()} is never below the last value pgjdbc sent.
   * The guard's own rewind lowers it, and sends the lowered value at once for exactly that reason.
   */
  public void confirm(
      long lsn
  ) {
    PGReplicationStream s = stream;
    if (s != null && Long.compareUnsigned(lsn, s.getLastFlushedLSN().asLong()) > 0) {
      LogSequenceNumber n = LogSequenceNumber.valueOf(lsn);
      s.setFlushedLSN(n);
      s.setAppliedLSN(n);
    }
  }

  /**
   * Whether pgjdbc's keepalive auto-advance can fire before the next message arrives, given the
   * start LSN of the message just handled and the stream's current flushed LSN.
   *
   * <p>
   * <b>What pgjdbc 42.7.3 does</b> ({@code V3PGReplicationStream.processKeepAliveMessage}, read
   * from the bytecode and reproduced with a standalone client): on every keepalive it sets
   * {@code lastFlushedLSN = serverLSN} when {@code explicitlyFlushedLSN >=
   * startOfLastMessageLSN}, {@code serverLSN > explicitlyFlushedLSN} and {@code serverLSN >
   * lastFlushedLSN}. {@code explicitlyFlushedLSN} is the flushed LSN carried by the last status
   * update actually <em>sent</em>; {@code startOfLastMessageLSN} is the WAL start of the last
   * XLogData message, which for a logical stream is also what {@code getLastReceiveLSN()} returns
   * for that message. If the keepalive asks for a reply, the status update carrying the advanced
   * value is sent inside the same {@code readPending()} call, so the application never sees it
   * before the server does.
   *
   * <p>
   * <b>Why that is unsafe for buffered transactions.</b> {@code serverLSN} is the walsender's send
   * pointer: the end of the last WAL record it has decoded. Everything committed below it has been
   * delivered, but delivered is not committed to Paimon. Logical decoding sends transactions in
   * commit order with their records' original LSNs, so a transaction that began before the last
   * status update arrives with a Begin and changes below {@code explicitlyFlushedLSN}, and Relation
   * messages carry LSN 0 (measured on PostgreSQL 18). Either opens the gate, and the next keepalive
   * confirms past every transaction still buffered.
   *
   * <p>
   * <b>The test.</b> {@code explicitlyFlushedLSN} is not readable, but it is always a value
   * {@code lastFlushedLSN} held when a status update went out, and {@code lastFlushedLSN} only
   * decreases together with an immediate status update carrying the lower value ({@link #confirm}
   * never lowers it; pgjdbc only raises it; the guard's rewind sends at once). So
   * {@code lastFlushedLSN >= explicitlyFlushedLSN} at every read, and when this method returns
   * false the gate is shut until the next message: opening it would need a status update carrying a
   * value at or above the message's start, so {@code lastFlushedLSN} would have to reach it, which
   * only a commit-and-confirm does. The test is also necessary, not merely sufficient: a periodic
   * status update can go out inside any read and make {@code explicitlyFlushedLSN} equal to
   * {@code lastFlushedLSN}. What the caller does when it returns true is
   * {@link #guardBeforeNextRead}.
   *
   * <p>
   * What an auto-advance can then still pass is the transaction being received, never a complete
   * one, and that is safe: the send pointer during its burst is at most the start of its commit
   * record, and PostgreSQL replays a transaction whose commit record starts at or above
   * {@code confirmed_flush_lsn} (it skips only those strictly below).
   */
  static boolean autoAdvanceGateMayOpen(
      long lastMessageStart,
      long lastFlushed
  ) {
    return Long.compareUnsigned(lastMessageStart, lastFlushed) <= 0;
  }

  /** What {@link #guardBeforeNextRead} did; for tests. */
  enum Guard {
    NOTHING,
    REWOUND,
    COMMITTED
  }

  /**
   * Called after a message has been handled and before the next read. When the gate may open (see
   * {@link #autoAdvanceGateMayOpen}) and the handler holds complete, unconfirmed transactions, it
   * shuts the gate in one of two ways:
   * <ul>
   * <li><b>Rewind</b>, when the message started above 0: set pgjdbc's flushed position just below
   * the message's start and send a status update now. {@code explicitlyFlushedLSN} is then below
   * {@code startOfLastMessageLSN}, so no keepalive can advance anything until the next message,
   * which is checked in turn. Nothing is committed, so batching survives: committing instead ended
   * a batch at nearly every transaction under concurrent load (8 pgbench clients, a standalone
   * pgjdbc client applying each rule: 1.3 transactions per batch, against 175 with the rewind). The
   * server ignores a flushed position below its own (measured on PostgreSQL 18), and one that did
   * not would only replay more, which the commit-identifier filter and rule 4 absorb. So the rewind
   * never confirms anything.</li>
   * <li><b>Commit</b>, when the message started at 0 (a Relation message): no position is below 0,
   * so the buffer is committed and confirmed instead.</li>
   * </ul>
   * A {@link SQLException} from the status update propagates as connection loss, which discards the
   * buffer; it replays.
   */
  Guard guardBeforeNextRead(
      PGReplicationStream s,
      long lastMessageStart
  ) throws SQLException {
    if (!autoAdvanceGateMayOpen(lastMessageStart, s.getLastFlushedLSN().asLong())
        || !handler.hasUnconfirmed()) {
      return Guard.NOTHING;
    }
    if (lastMessageStart != 0) {
      LogSequenceNumber below = LogSequenceNumber.valueOf(lastMessageStart - 1);
      s.setFlushedLSN(below);
      s.setAppliedLSN(below);
      s.forceUpdateStatus();
      return Guard.REWOUND;
    }
    call(handler::commitUnconfirmed);
    return Guard.COMMITTED;
  }

  /**
   * @param startLsn 0 imposes no additional constraint: the slot resumes from its own
   *        {@code confirmed_flush_lsn}. It does not mean "replay all WAL".
   */
  public void run(
      long startLsn
  ) throws Exception {
    long backoffMs = 500;
    while (running)
      backoffMs = consumeOnce(startLsn, backoffMs);
  }

  private long consumeOnce(
      long startLsn,
      long backoffMs
  ) throws Exception {
    try (Connection conn = connect()) {
      PGReplicationStream s = open(conn, startLsn);
      backoffMs = 500;
      consume(s);
    } catch (SQLException e) {
      if (!running)
        return backoffMs;
      if (!isConnectionLoss(e))
        throw e;
      log.warn("replication stream lost; discarding any open transaction and reconnecting in {}ms",
          backoffMs, e);
      stream = null;
      handler.onDisconnect();
      Thread.sleep(backoffMs);
      backoffMs = Math.min(backoffMs * 2, 30_000);
    } finally {
      stream = null;
    }
    return backoffMs;
  }

  /**
   * Reads one open stream until {@link #stop()}, then drains the handler and sends the final status
   * update. Separate from {@link #run} so it can be driven by a model of pgjdbc's stream. A
   * {@link SQLException} from the stream propagates (the caller decides whether it is connection
   * loss); everything the handler throws is fatal.
   */
  void consume(
      PGReplicationStream s
  ) throws Exception {
    stream = s;
    while (running) {
      // The buffer is big-endian (the ByteBuffer default), which PgOutputDecoder relies
      // on. It is passed on untouched.
      ByteBuffer buf = s.readPending();
      if (buf == null) {
        call(handler::onIdle);
        Thread.sleep(10);
        continue;
      }
      // For a logical stream this is the message's WAL start, the very value pgjdbc keeps
      // as startOfLastMessageLSN: readPending returns as soon as an XLogData arrives, so no
      // keepalive is processed between the two.
      long changeLsn = s.getLastReceiveLSN().asLong();
      // Wrapped so that nothing from here, a SQLException included, can be taken for
      // connection loss: failures here are fatal. Decoder errors are already unchecked and
      // likewise never match the caller's SQLException handler.
      PgEvent event = decoder.decode(buf);
      call(() -> handler.onEvent(event, changeLsn));
      // Before reading again: the next keepalive must not be able to auto-advance the
      // flushed position past a complete transaction that is still unconfirmed.
      guardBeforeNextRead(s, changeLsn);
    }
    // Clean stop: commit and confirm what the handler still buffers, then report the
    // confirmed position now rather than at the next interval.
    call(handler::onShutdown);
    try {
      s.forceUpdateStatus();
      flushedAtCleanStop = s.getLastFlushedLSN().asLong();
    } catch (SQLException e) {
      log.warn("final status update failed: " + e);
    }
  }

  private interface HandlerCall {
    void call() throws Exception;
  }

  private static void call(
      HandlerCall c
  ) {
    try {
      c.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new HandlerFailure(e);
    }
  }

  /** Wraps a checked exception from {@link Handler#onEvent}; never retried. */
  static final class HandlerFailure extends RuntimeException {
    HandlerFailure(
        @NotNull Throwable cause
    ) {
      super("event handler failed: " + cause, cause);
    }
  }

  /**
   * Whether a failure should reconnect with backoff. Only a {@link SQLException} from the
   * connection itself qualifies, and not every one: a missing slot (42704) is fatal because nothing
   * creates it but the Backfiller, and an invalidated slot (55000, "can no longer get changes") is
   * unrecoverable without a re-bootstrap. Retrying either is a loop, not resilience. The cause
   * chain is deliberately not searched, so a failure wrapped by the handler boundary is never
   * retryable.
   */
  static boolean isConnectionLoss(
      @NotNull Throwable t
  ) {
    if (!(t instanceof SQLException e)) {
      return false;
    }
    String state = e.getSQLState();
    return !("42704".equals(state) || "55000".equals(state));
  }

  private @NotNull Connection connect() throws SQLException {
    Properties p = new Properties();
    p.setProperty("user", config.getUser());
    p.setProperty("password", config.getPassword());
    p.setProperty("replication", "database");
    p.setProperty("preferQueryMode", "simple");
    p.setProperty("assumeMinServerVersion", "18");
    Connection conn = DriverManager.getConnection(config.getJdbcUrl(), p);
    try {
      // Pinned, not defaulted, and read back: see SessionSettings for each setting and
      // why. A mismatch is a misconfiguration, not connection loss.
      SessionSettings.pin(conn);
    } catch (SQLException | RuntimeException e) {
      try {
        conn.close();
      } catch (SQLException ignored) {
        // the original failure is the one that matters
      }
      throw e;
    }
    return conn;
  }

  private PGReplicationStream open(
      Connection conn,
      long startLsn
  ) throws SQLException {
    var b = conn.unwrap(PGConnection.class).getReplicationAPI().replicationStream().logical()
        .withSlotName(config.getSlotName()).withSlotOption("proto_version", "2")
        .withSlotOption("publication_names", config.getPublicationName())
        .withSlotOption("messages", "true").withSlotOption("binary", "false")
        .withSlotOption("streaming", "off").withStatusInterval(10_000, TimeUnit.MILLISECONDS);
    if (startLsn > 0) {
      b = b.withStartPosition(LogSequenceNumber.valueOf(startLsn));
    }
    return b.start();
  }

  public static long parseLsn(
      @NotNull String text
  ) {
    int slash = text.indexOf('/');
    long hi = Long.parseUnsignedLong(text.substring(0, slash), 16);
    long lo = Long.parseUnsignedLong(text.substring(slash + 1), 16);
    return (hi << 32) | lo;
  }

  public static @NotNull String formatLsn(
      long lsn
  ) {
    return String.format("%X/%08X", lsn >>> 32, lsn & 0xFFFFFFFFL);
  }
}
