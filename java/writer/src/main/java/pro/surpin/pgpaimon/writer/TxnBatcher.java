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
 * overwrite is the caller's job), but it IS shown to this class so it can refuse an ambiguous
 * transaction. Paimon expresses a truncate as an empty overwrite commit, not a record, so an
 * overwrite and an append are two separate commits and cannot be interleaved inside one
 * atomic transaction commit. The two orders differ in result (INSERT then TRUNCATE ends empty;
 * TRUNCATE then INSERT keeps the row), so a Truncate of table X together with any record for
 * X in the same transaction throws, naming the table. A Truncate alone, or alongside DML on
 * other tables, proceeds.
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
    private final SchemaRegistry registry;   // null means every relation is tracked
    private final Flusher flusher;
    private final long skipLsn;              // 0 = no escape configured
    private final RowCheck rowCheck;         // null = cannot check, so never skip

    private List<SinkRecord> open;
    private long xid;
    private int opSeq;
    private final Set<Long> buffered = new HashSet<>();    // relids with records this txn
    private final Set<Long> truncated = new HashSet<>();   // relids truncated this txn
    private final List<String> skipped = new ArrayList<>();  // audit lines for this txn

    public TxnBatcher(ToastResolver resolver, SchemaRegistry registry, Flusher flusher) {
        this(resolver, registry, flusher, 0, null);
    }

    /**
     * As above with the poison-record escape armed. {@code skipLsn} is one change LSN, the one
     * the POISON RECORD error printed; {@code rowCheck} decides whether the record at that LSN
     * really is unrepresentable.
     */
    public TxnBatcher(ToastResolver resolver, SchemaRegistry registry, Flusher flusher,
                      long skipLsn, RowCheck rowCheck) {
        this.resolver = resolver;
        this.registry = registry;
        this.flusher = flusher;
        this.skipLsn = skipLsn;
        this.rowCheck = rowCheck;
    }

    public boolean hasOpenTransaction() {
        return open != null;
    }

    public void discardOpenTransaction() {
        open = null;
        opSeq = 0;
        buffered.clear();
        truncated.clear();
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
            xid = b.xid();
            opSeq = 0;
            return;
        }
        if (event instanceof PgEvent.Commit c) {
            require();
            List<SinkRecord> batch = open;
            List<String> audit = List.copyOf(skipped);
            discardOpenTransaction();   // the transaction is closed even if flush throws
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
                    if (buffered.contains(relid)) {
                        throw ambiguous(relid, "TRUNCATE after earlier changes to the same table");
                    }
                    truncated.add(relid);
                }
                return;
            }

            require();
            long relid = relidOf(event);
            if (registry != null && !registry.tracked(relid)) {
                return;    // traffic for a table this writer does not replicate
            }
            if (truncated.contains(relid)) {
                throw ambiguous(relid, "change after TRUNCATE of the same table");
            }
            for (PartialRecord p : resolver.resolve(event, meta)) {
                SinkRecord record =
                    new SinkRecord(p.relid(), p.kind(), p.values(), changeLsn, opSeq++, xid, meta);
                if (skipLsn != 0 && changeLsn == skipLsn && skip(record, meta)) {
                    continue;   // see skip(): the transaction stays open, deliberately
                }
                open.add(record);
                buffered.add(p.relid());
            }
        } catch (Throwable t) {
            discardOpenTransaction();
            throw t;
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

    private IllegalStateException ambiguous(long relid, String what) {
        return new IllegalStateException("transaction " + xid + " mixes TRUNCATE and row changes"
            + " for relation " + relid + " (" + what + "); Paimon applies a truncate as a separate"
            + " overwrite commit, so the order cannot be preserved and the result would differ");
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
