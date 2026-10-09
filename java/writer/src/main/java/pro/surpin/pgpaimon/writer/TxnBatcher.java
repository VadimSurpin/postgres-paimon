// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.List;

/**
 * Assembles complete transactions: a batch is one committed PostgreSQL transaction or nothing.
 *
 * <p>Never collapses, reorders or deduplicates within a batch: correctness rests on LSN order
 * surviving intact, which is what lets an interleaved "UPDATE t SET id = id + 1" converge.
 * {@code _seq} is the per-change LSN supplied by the caller; {@code _op_seq} is an ordinal
 * assigned per emitted record (restarting at 0 each transaction), so a -U, which shares its
 * LSN with the +U it precedes, always sorts before it.
 *
 * <p>Relation, TypeMsg, Origin, Message and Truncate contribute no records and are ignored
 * here. An R message reports LSN 0/0; letting one reach {@code _seq} or the flush position
 * would sort a record before all history or move the slot to a position matching no committed
 * change. Truncate is not a row change; Main's event loop handles it before this class sees it
 * (calling PaimonSink.truncate for each relid).
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
    }

    /**
     * @param changeLsn the LSN of this specific message; becomes {@code _seq} of every record
     *                  the event yields. The caller supplies it; it is never the commit LSN.
     * @param meta      the relation of a row event; may be null for other events
     */
    public void onEvent(PgEvent event, long changeLsn, RelationMeta meta) throws Exception {
        if (event instanceof PgEvent.Begin b) {
            if (open != null) {
                throw new IllegalStateException("Begin arrived while transaction " + xid
                    + " is still open; its Commit was never seen");
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
            || event instanceof PgEvent.Origin || event instanceof PgEvent.Message
            || event instanceof PgEvent.Truncate) {
            return;    // handled elsewhere; contributes no records
        }

        require();
        if (registry != null && !registry.tracked(relidOf(event))) {
            return;    // traffic for a table this writer does not replicate
        }
        for (PartialRecord p : resolver.resolve(event, meta)) {
            open.add(new SinkRecord(p.relid(), p.kind(), p.values(), changeLsn, opSeq++, xid));
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
