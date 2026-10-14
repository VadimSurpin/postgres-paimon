// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.logging.Logger;

/**
 * Accumulates several completed PostgreSQL transactions and commits them to Paimon together.
 *
 * <p>It sits between {@link TxnBatcher}, which guarantees whole transactions, and the confirming
 * sink: {@code TxnBatcher} flushes one transaction at a time into {@link #flush}, and this class
 * decides when the accumulated transactions become one Paimon commit. It flushes when ANY limit
 * is reached: change records buffered, estimated bytes buffered, or time since the buffer's
 * FIRST transaction arrived (not reset by later traffic, so latency is bounded). A limit is
 * checked only between whole transactions, so a transaction is never split and a limit means "at
 * least this much", never "exactly".
 *
 * <p>Everything here runs on the consumer's single thread. There is no timer thread, deliberately:
 * the time limit is checked on every event and from {@link SlotConsumer.Handler#onIdle()}, which
 * is what keeps ordering and the "after an exception, nothing is half-committed" invariant
 * tractable. Not thread-safe, by design.
 *
 * <h2>The invariants</h2>
 * <ol>
 *   <li><b>Whole transactions.</b> Only complete transactions are ever added.</li>
 *   <li><b>A batch never spans a schema change or an overwrite.</b> Records are built against the
 *       table schema current at flush time. The caller invokes {@link #flushBuffered()} before it
 *       applies a DDL message, before it applies a truncate, and before it accepts a changed
 *       relation description.</li>
 *   <li><b>The commit identifier is the commit LSN of the LAST transaction</b> in the batch, and
 *       the confirmed position is that transaction's end LSN. The downstream flusher commits and
 *       only then confirms (see {@code Main.confirmingFlusher}). Transactions still buffered are
 *       never confirmed by this class; that pgjdbc cannot confirm them either, on a keepalive,
 *       is the job of the keepalive guard ({@link SlotConsumer#autoAdvanceGateMayOpen}), which
 *       rewinds pgjdbc's position below the message just received or, after a Relation
 *       message, calls {@link #flushBuffered()}, before the next read whenever it could.</li>
 *   <li><b>Already-committed transactions are dropped at assembly, per table.</b> See below.</li>
 *   <li><b>An exception is fatal.</b> The buffer is emptied before the commit is attempted and
 *       the exception propagates; nothing after it is confirmed.</li>
 *   <li><b>Memory is bounded</b> by the row and byte limits, plus the one transaction that
 *       crossed them.</li>
 * </ol>
 *
 * <h2>Why a timeout makes the grouping non-deterministic, and rule 4</h2>
 * A size limit is a function of the stream, so a replay regroups identically. A time limit is not:
 * after a crash the same transactions can be grouped differently. Say T1..T10 committed under
 * identifier L10 and the process died before confirming. On replay the timer may fire later and
 * produce T1..T12 under L12. Paimon's {@code filterAndCommit} drops a committable whose
 * identifier is at or BELOW the latest one committed by this commit user (measured on 1.4.1:
 * with 10 committed, 7, 10 and 11-after-12 are dropped, 12 after 10 is committed), so a regrouping
 * that ends lower is already filtered. But L12 &gt; L10, so it commits, and T1..T10 are in Paimon
 * twice. The merged table survives (deduplicate on {@code (_seq, _op_seq)}, both per-change), but
 * {@code changelog-producer = input} writes every input record, so the changelog would carry the
 * duplicates and effectively-once would silently become at-least-once there.
 *
 * <p>Rule 4 closes it: at assembly, a transaction's records for a table are dropped when the
 * transaction's commit LSN is at or below that table's highest committed identifier. It is per
 * table because the sink commits table by table: a transaction touching A and B where A is
 * committed past it and B is not applies to B alone. The identifier is read lazily from Paimon
 * (via the supplied lookup) and kept current in memory after each commit, truncate and DDL.
 */
public final class MiniBatcher implements TxnBatcher.Flusher {

    private static final Logger LOG = Logger.getLogger(MiniBatcher.class.getName());

    /** Bytes charged per record on top of its text values: object and list headers. */
    static final int RECORD_OVERHEAD_BYTES = 48;

    /** The three triggers. Any one reached flushes. {@code 1, 1, 0} is per-transaction commits. */
    public record Limits(long maxRows, long maxBytes, long maxIntervalMs) {
        public Limits {
            if (maxRows < 1 || maxBytes < 1 || maxIntervalMs < 0) {
                throw new IllegalArgumentException("limits must be rows >= 1, bytes >= 1,"
                    + " interval >= 0: " + maxRows + ", " + maxBytes + ", " + maxIntervalMs);
            }
        }
    }

    /** The highest identifier our commit user committed to the relation's table; 0 if none. */
    public interface CommittedLookup {
        long highestCommitted(long relid) throws Exception;
    }

    /** {@code afterCommit} is null when there is nothing to run. */
    private record Txn(List<SinkRecord> records, long commitLsn, long endLsn,
                       Runnable afterCommit) {}

    private final TxnBatcher.Flusher downstream;
    private final CommittedLookup lookup;
    private final Limits limits;
    private final LongSupplier nanoClock;

    private List<Txn> buffer = new ArrayList<>();
    private long rows;
    private long bytes;
    private long firstAddedNanos;
    private final Map<Long, Long> lastCommitted = new HashMap<>();

    /**
     * @param downstream commits and then confirms; called with the batch's last commit LSN and
     *                   last end LSN
     */
    public MiniBatcher(TxnBatcher.Flusher downstream, CommittedLookup lookup, Limits limits) {
        this(downstream, lookup, limits, System::nanoTime);
    }

    MiniBatcher(TxnBatcher.Flusher downstream, CommittedLookup lookup, Limits limits,
                LongSupplier nanoClock) {
        this.downstream = downstream;
        this.lookup = lookup;
        this.limits = limits;
        this.nanoClock = nanoClock;
    }

    /** What {@link TxnBatcher} calls at every Commit. */
    @Override
    public void flush(List<SinkRecord> records, long commitLsn, long endLsn) throws Exception {
        add(records, commitLsn, endLsn, null);
    }

    /** As above; {@code afterCommit} runs once the batch holding this transaction commits. */
    @Override
    public void flush(List<SinkRecord> records, long commitLsn, long endLsn,
                      Runnable afterCommit) throws Exception {
        add(records, commitLsn, endLsn, afterCommit);
    }

    private void add(List<SinkRecord> records, long commitLsn, long endLsn, Runnable afterCommit)
            throws Exception {
        if (buffer.isEmpty() && records.isEmpty()) {
            // A transaction with nothing for us (DDL only, or untracked tables) while nothing is
            // waiting: confirm it now. No records, so no snapshot. If something IS waiting it
            // must queue behind it, since confirming it would confirm the earlier data too.
            downstream.flush(records, commitLsn, endLsn);
            if (afterCommit != null) {
                afterCommit.run();
            }
            return;
        }
        if (buffer.isEmpty()) {
            firstAddedNanos = nanoClock.getAsLong();
        }
        buffer.add(new Txn(List.copyOf(records), commitLsn, endLsn, afterCommit));
        rows += records.size();
        bytes += estimate(records);
        if (rows >= limits.maxRows() || bytes >= limits.maxBytes() || intervalElapsed()) {
            flushBuffered();
        }
    }

    /** The time trigger. Called on every event and from the idle callback. */
    public void tick() throws Exception {
        if (!buffer.isEmpty() && intervalElapsed()) {
            flushBuffered();
        }
    }

    /**
     * Commits whatever is buffered, then confirms it. The barrier the caller invokes before a
     * DDL message, a truncate or a changed relation, and at a clean stop. Does nothing when the
     * buffer is empty.
     */
    public void flushBuffered() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }
        List<Txn> txns = buffer;
        buffer = new ArrayList<>();      // closed even if the commit throws: that is fatal anyway
        rows = 0;
        bytes = 0;
        Txn last = txns.get(txns.size() - 1);

        List<SinkRecord> assembled = new ArrayList<>();
        long dropped = 0;
        for (Txn t : txns) {
            for (SinkRecord r : t.records()) {
                // Keyed by Paimon table: a relation's history table and each of its archives (a
                // negative key, see HistoryKeys) has its own identifier stream, filtered on its own.
                if (t.commitLsn() <= committed(r.relid())) {
                    dropped++;          // this table already holds this transaction
                } else {
                    assembled.add(r);
                }
            }
        }
        if (dropped > 0) {
            LOG.info("dropped " + dropped + " record(s) of transactions already committed to their"
                + " table (a replay, grouped differently from the original)");
        }
        // Commit, then confirm: downstream is the confirming flusher, so a throw here means
        // nothing of this batch was confirmed.
        downstream.flush(assembled, last.commitLsn(), last.endLsn());
        for (SinkRecord r : assembled) {
            lastCommitted.put(r.relid(), last.commitLsn());
        }
        for (Txn t : txns) {
            if (t.afterCommit() != null) {
                t.afterCommit().run();
            }
        }
    }

    /** A clean stop: commit and confirm everything buffered. */
    public void drain() throws Exception {
        flushBuffered();
    }

    /**
     * The connection was lost. Buffered transactions are complete but unconfirmed and will replay
     * from the last confirmed position, so they are simply forgotten.
     */
    public void discard() {
        if (!buffer.isEmpty()) {
            LOG.info("discarding " + buffer.size() + " buffered transaction(s) after connection"
                + " loss; they replay from the last confirmed position");
        }
        buffer = new ArrayList<>();
        rows = 0;
        bytes = 0;
    }

    /** A commit made outside this class (a truncate): later batches must not go at or below it. */
    public void noteCommitted(long relid, long identifier) throws Exception {
        lastCommitted.put(relid, Math.max(committed(relid), identifier));
    }

    /**
     * Forgets every cached identifier, to be re-read from Paimon on demand. After a DDL message a
     * relation id may name a different Paimon table (removed, deleted, registered again).
     */
    public void forgetCommitted() {
        lastCommitted.clear();
    }

    public boolean hasBuffered() {
        return !buffer.isEmpty();
    }

    public long bufferedRows() {
        return rows;
    }

    public long bufferedBytes() {
        return bytes;
    }

    private long committed(long relid) throws Exception {
        Long known = lastCommitted.get(relid);
        if (known == null) {
            known = lookup.highestCommitted(relid);
            lastCommitted.put(relid, known);
        }
        return known;
    }

    private boolean intervalElapsed() {
        long elapsedMs = (nanoClock.getAsLong() - firstAddedNanos) / 1_000_000L;
        return elapsedMs >= limits.maxIntervalMs();
    }

    /**
     * An estimate of the memory the text payload holds: each non-null value's character count
     * (one byte per character for ASCII, which is the common case) plus a fixed charge per record.
     * Not exact -- the point is a bound that scales with the data.
     */
    static long estimate(List<SinkRecord> records) {
        long n = 0;
        for (SinkRecord r : records) {
            n += RECORD_OVERHEAD_BYTES;
            n += chars(r.values());
        }
        return n;
    }

    private static long chars(List<String> values) {
        long n = 0;
        for (String v : values) {
            if (v != null) {
                n += v.length();
            }
        }
        return n;
    }
}
