// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    public interface Flusher {
        /**
         * @param commitLsn becomes Paimon's commitIdentifier
         * @param endLsn    the position confirmed to the server (after the commit record)
         */
        void flush(List<SinkRecord> records, long commitLsn, long endLsn) throws Exception;
    }

    private final ToastResolver resolver;
    private final SchemaRegistry registry;   // null means every relation is tracked
    private final Flusher flusher;

    private List<SinkRecord> open;
    private long xid;
    private int opSeq;
    private final Set<Long> buffered = new HashSet<>();    // relids with records this txn
    private final Set<Long> truncated = new HashSet<>();   // relids truncated this txn

    public TxnBatcher(ToastResolver resolver, SchemaRegistry registry, Flusher flusher) {
        this.resolver = resolver;
        this.registry = registry;
        this.flusher = flusher;
    }

    public boolean hasOpenTransaction() {
        return open != null;
    }

    public void discardOpenTransaction() {
        open = null;
        opSeq = 0;
        buffered.clear();
        truncated.clear();
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
            discardOpenTransaction();   // the transaction is closed even if flush throws
            flusher.flush(batch, c.commitLsn(), c.endLsn());   // always, even when empty
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
                open.add(new SinkRecord(p.relid(), p.kind(), p.values(), changeLsn, opSeq++, xid));
                buffered.add(p.relid());
            }
        } catch (Throwable t) {
            discardOpenTransaction();
            throw t;
        }
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
