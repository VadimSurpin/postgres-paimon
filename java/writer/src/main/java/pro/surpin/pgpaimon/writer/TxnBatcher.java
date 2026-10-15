// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Assembles complete transactions: a batch is one committed PostgreSQL transaction or nothing.
 *
 * <p>Never collapses, reorders or deduplicates within a batch: correctness rests on LSN order
 * surviving intact, which is what lets an interleaved "UPDATE t SET id = id + 1" converge.
 * {@code _seq} is the per-change LSN supplied by the caller; {@code _op_seq} is an ordinal
 * assigned per emitted record (restarting at 0 each transaction), so a -U, which shares its
 * LSN with the +U it precedes, always sorts before it.
 *
 * <p>Relation, TypeMsg, Origin and Message contribute no records and are ignored here. An R
 * message reports LSN 0/0; letting one reach {@code _seq} or the flush position would sort a
 * record before all history or move the slot to a position matching no committed change.
 *
 * <p>Truncate contributes no record either and is not forwarded to the flusher (applying the
 * overwrite is the caller's job, at the Commit, before this transaction's records), but it IS
 * shown to this class, because it decides which of the transaction's records survive. Paimon
 * expresses a truncate as an empty overwrite commit, not a record, so the overwrite and the
 * transaction's appends are two commits in a fixed order: overwrite first. That order is right
 * for every record that came AFTER the truncate, and a record that came BEFORE it is exactly
 * what the truncate removes -- so it is dropped here, and the table ends as PostgreSQL's does.
 * (This used to throw for any mix of the two, and since the transaction was already committed
 * the throw replayed on every restart: BEGIN; TRUNCATE t; INSERT INTO t ...; COMMIT -- the usual
 * way to reload a table -- stopped replication for good. Measured on PostgreSQL 18.)
 *
 * <p><b>Operation records</b> (spec 2026-10-14). Besides the changelog records of a primary-key
 * table, every change of a tracked relation also yields operation records: for its history table
 * one per row image -- {@code +I}; {@code -U} then {@code +U}; {@code -D} -- each with its own
 * {@code _op_seq} and the change's LSN, the transaction's xid and commit time and, stamped at the
 * Commit, the commit LSN; and, for a relation with no primary key, one row per change for its
 * main table, which is an operation log ({@code I}, {@code U}, {@code D}). Nothing is ever merged:
 * two updates of one row in one transaction are four history rows. A TRUNCATE is one {@code T}
 * record, and it removes nothing from the history or from a log: the records before it stay,
 * because they happened. (Only a primary-key table's changelog records before a truncate are
 * dropped, as described above, since the overwrite would delete them anyway.) Each history record
 * goes to the history table of the schema its change was decoded under (see
 * {@link TableTracker#historyKey}), so a transaction that straddles a DDL writes to two.
 */
public final class TxnBatcher {

    private static final Logger LOG = Logger.getLogger(TxnBatcher.class.getName());

    /**
     * Builds the Paimon row for one record, purely to find out whether its values can be
     * represented. Only ever called for the single configured skip LSN, so it adds nothing to
     * the normal path -- one {@code long} comparison per event.
     */
    public interface RowCheck {
        void check(SinkRecord record, RelationMeta meta);
    }

    public interface Flusher {
        /**
         * @param commitLsn becomes Paimon's commitIdentifier
         * @param endLsn    the position confirmed to the server (after the commit record)
         */
        void flush(List<SinkRecord> records, long commitLsn, long endLsn) throws Exception;

        /**
         * As {@link #flush(List, long, long)}, and runs {@code afterCommit} once these records
         * have actually been committed to Paimon -- which, for a flusher that buffers (see
         * {@link MiniBatcher}), is not when this method returns. Never run if the commit fails
         * or the transaction is discarded unconfirmed. The default suits a flusher that commits
         * before returning.
         */
        default void flush(List<SinkRecord> records, long commitLsn, long endLsn,
                           Runnable afterCommit) throws Exception {
            flush(records, commitLsn, endLsn);
            afterCommit.run();
        }
    }

    private final ToastResolver resolver;
    private final TableTracker registry;     // null: every relation is tracked, no history
    private final Flusher flusher;
    private final long skipLsn;              // 0 = no escape configured
    private final RowCheck rowCheck;         // null = cannot check, so never skip

    private List<SinkRecord> open;
    private long xid;
    private long commitMicros;               // the open transaction's commit time, Unix epoch
    private int opSeq;
    private long maxBytes = Long.MAX_VALUE;  // PGPAIMON_TXN_MAX_BYTES
    private long openBytes;                  // the open transaction's estimate
    private final Set<Long> buffered = new HashSet<>();    // relids with records this txn
    private final List<String> skipped = new ArrayList<>();  // audit lines for this txn

    public TxnBatcher(ToastResolver resolver, TableTracker registry, Flusher flusher) {
        this(resolver, registry, flusher, 0, null);
    }

    /**
     * As above with the poison-record escape armed. {@code skipLsn} is one change LSN, the one
     * the POISON RECORD error printed; {@code rowCheck} decides whether the record at that LSN
     * really is unrepresentable.
     */
    public TxnBatcher(ToastResolver resolver, TableTracker registry, Flusher flusher,
                      long skipLsn, RowCheck rowCheck) {
        this.resolver = resolver;
        this.registry = registry;
        this.flusher = flusher;
        this.skipLsn = skipLsn;
        this.rowCheck = rowCheck;
    }

    /**
     * Bounds the memory one open transaction may take (estimated as {@link MiniBatcher#estimate}
     * does). A transaction is held whole until its Commit, because it is never written in part
     * (pgoutput streaming is off on purpose: a transaction's records are built and committed
     * together, after every DDL inside it is known). Without a bound, a large enough transaction
     * -- a bulk load, an UPDATE of a whole table -- ended in an OutOfMemoryError deep inside the
     * decoder or the JVM, on every replay, with no word about why. With it, the writer stops with
     * {@link TransactionTooLargeException}, which names the transaction and the remedy.
     *
     * <p>Why not spill it to disk: every later step (the truncate rule above, the history
     * rerouting at the Commit, the per-table filter in {@link MiniBatcher}, the sink grouping a
     * flush by table) takes the transaction as one list in memory, and rewriting each to stream
     * from disk is a redesign with every invariant of this pipeline at stake. Nothing is lost by
     * stopping instead: the transaction stays in the slot until the writer can hold it.
     */
    public TxnBatcher withMaxBytes(long maxBytes) {
        if (maxBytes < 1) {
            throw new IllegalArgumentException("maxBytes must be at least 1: " + maxBytes);
        }
        this.maxBytes = maxBytes;
        return this;
    }

    /** Thrown when one transaction outgrows {@code PGPAIMON_TXN_MAX_BYTES}. Fatal. */
    public static final class TransactionTooLargeException extends IllegalStateException {
        TransactionTooLargeException(String msg) {
            super(msg);
        }
    }

    public boolean hasOpenTransaction() {
        return open != null;
    }

    public void discardOpenTransaction() {
        open = null;
        opSeq = 0;
        openBytes = 0;
        buffered.clear();
        skipped.clear();   // a transaction that never committed skipped nothing: it replays
    }

    /**
     * @param changeLsn the LSN of this specific message; becomes {@code _seq} of every record
     *                  the event yields. The caller supplies it; it is never the commit LSN.
     * @param meta      the relation of a row event; may be null for other events
     */
    public void onEvent(PgEvent event, long changeLsn, RelationMeta meta) throws Exception {
        if (event instanceof PgEvent.Begin b) {
            if (open != null) {
                // The first transaction's Commit was never seen, so its LSNs are unknown and
                // the batch cannot be trusted; discard it rather than keep it.
                String msg = "Begin arrived while transaction " + xid
                    + " is still open; its Commit was never seen";
                discardOpenTransaction();
                throw new IllegalStateException(msg);
            }
            open = new ArrayList<>();
            openBytes = 0;
            xid = b.xid();
            commitMicros = LogRowBuilder.pgToUnixMicros(b.commitTimestamp());
            opSeq = 0;
            return;
        }
        if (event instanceof PgEvent.Commit c) {
            require();
            List<SinkRecord> batch = open;
            List<String> audit = List.copyOf(skipped);
            discardOpenTransaction();   // the transaction is closed even if flush throws
            dropUntracked(batch);
            // Operation records learn their transaction's commit LSN only now.
            batch.replaceAll(r -> r.withCommitLsn(c.commitLsn()));
            rerouteHistory(batch);
            if (audit.isEmpty()) {
                flusher.flush(batch, c.commitLsn(), c.endLsn());   // always, even when empty
            } else {
                // The audit is logged only once the rest of the transaction is committed to
                // Paimon, which with commit batching is NOT when flush returns: the transaction
                // may only have been buffered. Logged earlier, "unset PGPAIMON_SKIP_LSN" would
                // be advice an operator could follow before the skip had taken effect, and a
                // writer killed inside the batch interval would then replay the poison record
                // and fail on it again. The commit LSN is not known before the Commit arrives,
                // and the audit line is worthless without it.
                long commitLsn = c.commitLsn();
                flusher.flush(batch, commitLsn, c.endLsn(), () -> logSkipped(audit, commitLsn));
            }
            return;
        }
        if (event instanceof PgEvent.Relation || event instanceof PgEvent.TypeMsg
            || event instanceof PgEvent.Origin || event instanceof PgEvent.Message) {
            return;    // handled elsewhere; contributes no records
        }

        // Invariant (unconditional, for every path of onEvent): after any exception no
        // transaction is open. A throw partway through must never leave earlier records
        // buffered, or a later Commit would flush a partial batch under the full
        // transaction's commitLsn/endLsn and the slot would advance past the missing records.
        try {
            if (event instanceof PgEvent.Truncate t) {
                require();
                for (long relid : t.relids()) {
                    if (registry != null && !registry.tracked(relid)) {
                        continue;
                    }
                    boolean log = registry != null && registry.keyless(relid);
                    if (!log && buffered.remove(relid)) {
                        // Wiped by this truncate in PostgreSQL; the overwrite is applied before
                        // the transaction's appends, so keeping them would resurrect them.
                        int before = open.size();
                        open.removeIf(r -> r.relid() == relid && r.op() == null);
                        LOG.fine("transaction " + xid + ": TRUNCATE of relation " + relid
                            + " removes the " + (before - open.size()) + " record(s) it made"
                            + " earlier in the same transaction");
                    }
                    // One T operation record: the history keeps the truncate as an event, and a
                    // key-less table's log is not erased by it (it is an audit log).
                    boolean any = false;
                    if (log) {
                        addOp(SinkRecord.op(relid, "T", null, changeLsn, opSeq, xid,
                                            commitMicros, null), null);
                        any = true;
                    }
                    any |= addHistory(relid, "T", null, changeLsn, opSeq, null);
                    if (any) {
                        opSeq++;
                    }
                }
                return;
            }

            require();
            long relid = relidOf(event);
            if (registry != null && !registry.tracked(relid)) {
                return;    // traffic for a table this writer does not replicate
            }
            List<PartialRecord> parts = resolver.resolve(event, meta);
            boolean log = registry != null && registry.keyless(relid);
            int changeOpSeq = opSeq;
            if (!log) {
                for (PartialRecord p : parts) {
                    SinkRecord record = new SinkRecord(p.relid(), p.kind(), p.values(), changeLsn,
                                                       opSeq++, xid, meta);
                    if (skipLsn != 0 && changeLsn == skipLsn && skip(record, meta)) {
                        continue;   // see skip(): the transaction stays open, deliberately
                    }
                    buffer(record);
                    buffered.add(p.relid());
                }
            } else {
                // An operation log has one row per change, the history one per row image; the
                // _op_seq numbers are the history's, the log row takes its change's first.
                opSeq += parts.size();
                String code = event instanceof PgEvent.Insert ? "I"
                    : event instanceof PgEvent.Update ? "U" : "D";
                List<String> image = event instanceof PgEvent.Update ? parts.get(1).values()
                    : parts.get(0).values();
                addOp(SinkRecord.op(relid, code, image, changeLsn, changeOpSeq, xid, commitMicros,
                                    meta), meta);
            }
            // The history: one row per row image, with the row kind of the change as it happened.
            // An UPDATE resolves to the old row then the new one (-U/+U, or -D/+I when a key
            // moved); the history records an UPDATE as -U then +U either way. Each image takes the
            // _op_seq of the changelog record of the same image.
            if (event instanceof PgEvent.Insert) {
                addHistory(relid, "+I", parts.get(0).values(), changeLsn, changeOpSeq, meta);
            } else if (event instanceof PgEvent.Update) {
                addHistoryPair(relid, parts.get(0).values(), parts.get(1).values(), changeLsn,
                               changeOpSeq, meta);
            } else if (event instanceof PgEvent.Delete) {
                addHistory(relid, "-D", parts.get(0).values(), changeLsn, changeOpSeq, meta);
            }
        } catch (Throwable t) {
            discardOpenTransaction();
            throw t;
        }
    }

    /**
     * Adds a change's operation records: one for the relation's history table, and one for its
     * main table when that is an operation log (no primary key). Each goes through the same
     * poison-record escape as a changelog record.
     */
    /**
     * A DDL message inside this transaction rotated a history table after some of its rows were
     * recorded: those rows were decoded under the old schema and belong to the archive now. Each
     * history row is therefore routed again at the Commit, when every DDL of the transaction has
     * been applied, by its change LSN.
     */
    private void rerouteHistory(List<SinkRecord> batch) {
        if (registry == null) {
            return;
        }
        List<SinkRecord> out = new ArrayList<>(batch.size());
        for (SinkRecord r : batch) {
            if (r.op() != null && HistoryKeys.isHist(r.relid())) {
                long relid = HistoryKeys.relidOf(r.relid());
                long key = registry.historyKey(relid, r.seq());
                if (key == 0) {
                    continue;
                }
                if (key != r.relid()) {
                    r = new SinkRecord(key, r.kind(), r.values(), r.seq(), r.opSeq(), r.xid(),
                                       r.meta(), r.op());
                }
            }
            out.add(r);
        }
        batch.clear();
        batch.addAll(out);
    }

    /**
     * Adds one row image to the relation's history: to the history table of the schema the change
     * was decoded under, which {@link TableTracker#historyKey} picks by the change's LSN. Returns
     * whether there was a history table to write.
     */
    private boolean addHistory(long relid, String code, List<String> image, long changeLsn,
                               int seq, RelationMeta meta) {
        if (registry == null) {
            return false;   // no registry, no history: the unit-test configuration
        }
        long key = registry.historyKey(relid, changeLsn);
        if (key == 0) {
            return false;
        }
        addOp(SinkRecord.op(key, code, image, changeLsn, seq, xid, commitMicros, meta), meta);
        return true;
    }

    /**
     * An UPDATE's two history images, -U then +U, which go together or not at all: the
     * poison-record escape used to apply per image, so with only the new image unrepresentable
     * the history kept a -U with no +U (review of the history tables, m8), which no reader can
     * interpret. With {@code PGPAIMON_SKIP_LSN} at this change, both images are checked and, if
     * either is dropped, the other is dropped with it and says so.
     */
    private void addHistoryPair(long relid, List<String> before, List<String> after, long changeLsn,
                                int seq, RelationMeta meta) {
        if (registry == null) {
            return;
        }
        long key = registry.historyKey(relid, changeLsn);
        if (key == 0) {
            return;
        }
        SinkRecord mu = SinkRecord.op(key, "-U", before, changeLsn, seq, xid, commitMicros, meta);
        SinkRecord pu = SinkRecord.op(key, "+U", after, changeLsn, seq + 1, xid, commitMicros, meta);
        if (skipLsn != 0 && changeLsn == skipLsn) {
            boolean a = skip(mu.withCommitLsn(Long.MAX_VALUE), meta);
            boolean b = skip(pu.withCommitLsn(Long.MAX_VALUE), meta);
            if (a || b) {
                if (a != b) {
                    skipped.add("history key " + key + " " + (a ? "+U" : "-U") + " at _seq "
                        + SlotConsumer.formatLsn(changeLsn) + ": representable, but dropped with"
                        + " its " + (a ? "-U" : "+U") + " so that the history keeps no half of an"
                        + " UPDATE; its raw values were " + raw((a ? pu : mu).values()));
                }
                return;
            }
        }
        buffer(mu);
        buffer(pu);
    }

    private void addOp(SinkRecord record, RelationMeta meta) {
        if (skipLsn != 0 && record.seq() == skipLsn && !"T".equals(record.op().code())
            && skip(record.withCommitLsn(Long.MAX_VALUE), meta)) {
            return;
        }
        buffer(record);
    }

    /**
     * Every record of the open transaction is added here, and the transaction's size is checked
     * as it grows (the records a TRUNCATE later removes stay counted: a bound, not an exact sum).
     * The throw happens inside onEvent's try, so the transaction is discarded and nothing of it
     * was flushed or confirmed: it replays, whole, on the next start.
     */
    private void buffer(SinkRecord record) {
        open.add(record);
        openBytes += MiniBatcher.RECORD_OVERHEAD_BYTES + MiniBatcher.chars(record.values());
        if (openBytes > maxBytes) {
            throw new TransactionTooLargeException("TRANSACTION TOO LARGE: transaction " + xid
                + " has reached " + open.size() + " change record(s) and an estimated "
                + openBytes + " bytes (at _seq " + SlotConsumer.formatLsn(record.seq())
                + "), over PGPAIMON_TXN_MAX_BYTES=" + maxBytes + ". The writer holds a whole"
                + " transaction in memory until its commit and never writes part of one, so it"
                + " stops here rather than run out of memory. Nothing is lost: the transaction"
                + " stays in the slot and replays, whole, on the next start. Give the writer more"
                + " heap (java -Xmx...; the default limit is a quarter of the maximum heap) or"
                + " raise PGPAIMON_TXN_MAX_BYTES, then restart. Avoid such transactions where you"
                + " can: load and update in batches of bounded size (docs, section 3).");
        }
    }

    /**
     * The poison-record escape, and the one exemption from this class's "after any exception no
     * transaction is open" invariant.
     *
     * <p>A value PostgreSQL stores and Paimon cannot (a {@code numeric} NaN, say) is in the slot,
     * and the slot advances only after a successful Paimon commit, so it replays on every restart
     * for ever. Without this, the only recovery is a re-bootstrap that discards all Paimon-side
     * changelog history for every tracked table -- a brutal price for one bad value. So an
     * operator who has read the POISON RECORD error and inspected the row may set
     * {@code PGPAIMON_SKIP_LSN} to that record's change LSN and restart.
     *
     * <p>Why leaving the transaction open is safe here, when nothing else may: the invariant
     * exists so that a throw part-way through cannot leave earlier records buffered for a later
     * Commit to flush under the full transaction's LSNs, advancing the slot past records that
     * were never written. Here nothing is thrown and nothing is lost unknowingly -- one record is
     * dropped, deliberately, by explicit operator instruction, and logged in full. The rest of
     * the transaction is applied, so the loss is one row rather than everything committed
     * alongside it.
     *
     * <p>What keeps it from becoming an "ignore errors" switch, which is the whole design
     * problem: it is one LSN and never a range; it fires only for
     * {@link TextDecoder.UnrepresentableValueException} and nothing else, so a commit failure, a
     * schema error, an arity mismatch or a parse failure at the very same LSN stays fatal; and
     * the complete raw text of the dropped record is logged at WARNING so the row can be put
     * back by hand.
     *
     * <p>An UPDATE decomposes to two records sharing one change LSN, so skipping the change to
     * one row may drop both images of it. Each is logged.
     */
    private boolean skip(SinkRecord record, RelationMeta meta) {
        if (rowCheck == null) {
            return false;
        }
        try {
            rowCheck.check(record, meta);
            return false;       // it builds after all: nothing to skip
        } catch (TextDecoder.UnrepresentableValueException e) {
            String table = meta == null
                ? "relation " + record.relid()
                : meta.namespace() + "." + meta.relName() + " (relation " + record.relid() + ")";
            skipped.add(table + " " + record.kind() + " at _seq "
                + SlotConsumer.formatLsn(record.seq()) + ", xid " + record.xid()
                + ": " + e.getMessage()
                + "; the record's complete raw values, in relation column order "
                + (meta == null ? "(unknown)" : meta.columnNames()) + ", were "
                + raw(record.values()));
            return true;
        } catch (RuntimeException other) {
            // Any other failure at this LSN is NOT what the escape is for. Leave the record in
            // the batch so the sink raises it as it always would, fatally.
            return false;
        }
    }

    private void logSkipped(List<String> audit, long commitLsn) {
        for (String line : audit) {
            LOG.warning("SKIPPED POISON RECORD (PGPAIMON_SKIP_LSN="
                + SlotConsumer.formatLsn(skipLsn) + "): " + line
                + ", in the transaction committed at " + SlotConsumer.formatLsn(commitLsn)
                + ". The rest of that transaction WAS committed to Paimon. This row is now missing"
                + " from Paimon and nothing will replay it: reinsert it by hand from the values"
                + " above, with the offending column corrected. Unset PGPAIMON_SKIP_LSN"
                + " before the next restart.");
        }
    }

    /** Raw text values, NULLs and empty strings distinguishable, nothing elided. */
    private static String raw(List<String> values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            String v = values.get(i);
            sb.append(i == 0 ? "" : ", ").append(v == null ? "NULL" : "\"" + v + "\"");
        }
        return sb.append(']').toString();
    }

    /**
     * Drops the records of a relation that stopped being tracked inside this very transaction:
     * {@code BEGIN; INSERT INTO t ...; DROP TABLE t; COMMIT}. The drop_table message arrives
     * between the INSERT and the Commit and purges the Paimon table, so those rows have nowhere to
     * go and are meant to be gone. Without this the commit asked Paimon for a table it had just
     * forgotten ("relation ... is not tracked") and the writer stopped -- measured, on an
     * ordinary transaction PostgreSQL had committed. Records are only ever added for tracked
     * relations, so an untracked one here can only have been dropped since.
     */
    private void dropUntracked(List<SinkRecord> batch) {
        if (registry == null) {
            return;
        }
        int before = batch.size();
        Set<Long> gone = new HashSet<>();
        batch.removeIf(r -> {
            boolean drop = !registry.tracked(r.relid());
            if (drop) {
                gone.add(r.relid());
            }
            return drop;
        });
        if (batch.size() != before) {
            LOG.info("dropped " + (before - batch.size()) + " record(s) of relation(s) " + gone
                + ", dropped later in the same transaction (transaction " + xid + ")");
        }
    }

    private void require() {
        if (open == null) {
            throw new IllegalStateException(
                "event arrived outside a transaction; the stream began mid-transaction");
        }
    }

    private static long relidOf(PgEvent e) {
        if (e instanceof PgEvent.Insert i) return i.relid();
        if (e instanceof PgEvent.Update u) return u.relid();
        if (e instanceof PgEvent.Delete d) return d.relid();
        throw new IllegalStateException("no relid on " + e);
    }
}
