// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;
import org.apache.paimon.types.RowKind;

/**
 * One record bound for one Paimon table.
 *
 * <p>relid: a PostgreSQL OID held as long; see package-info. For an operation record (below)
 * bound for a history table it is the history key ({@link HistoryKeys#histKey}), never a
 * PostgreSQL OID: the sink, the batcher and the offset store are all keyed by the Paimon table
 * a record goes to, and a relation has two of them.
 *
 * <p>meta is the relation description the values were decoded under, or null when the producer
 * has none (the initial load). A transaction can change a table's columns between two of its own
 * changes (BEGIN; INSERT; ALTER TABLE ADD COLUMN; INSERT; COMMIT), and records are built at flush
 * time, so each must be built against its own description and not the latest one.
 *
 * <p>{@code op} is null for an ordinary record of a primary-key table (a changelog record:
 * kind, values). It is set for an <em>operation record</em>: one row of a history table, or of
 * the operation log a key-less table is replicated as. An operation record is one row-level
 * change, never split into -U/+U and never collapsed, and carries both images.
 */
public record SinkRecord(long relid, RowKind kind, List<String> values,
                         long seq, int opSeq, long xid, RelationMeta meta, Op op) {

    /**
     * One row-level change as the operation log and the history table record it.
     *
     * @param code         {@code I} insert, {@code U} update, {@code D} delete, {@code T}
     *                     truncate, {@code R} a row of the initial load
     * @param before       the old row, null for I, T and R
     * @param after        the new row, null for D and T
     * @param commitLsn    the LSN of the transaction's commit record; 0 until the Commit arrives
     * @param commitMicros the transaction's commit time, microseconds since the Unix epoch
     */
    public record Op(char code, List<String> before, List<String> after,
                     long commitLsn, long commitMicros) {}

    public SinkRecord(long relid, RowKind kind, List<String> values,
                      long seq, int opSeq, long xid, RelationMeta meta) {
        this(relid, kind, values, seq, opSeq, xid, meta, null);
    }

    public SinkRecord(long relid, RowKind kind, List<String> values,
                      long seq, int opSeq, long xid) {
        this(relid, kind, values, seq, opSeq, xid, null, null);
    }

    /** An operation record for {@code key} (a relid, or a history key). */
    public static SinkRecord op(long key, char code, List<String> before, List<String> after,
                                long changeLsn, int opSeq, long xid, long commitMicros,
                                RelationMeta meta) {
        List<String> shown = after != null ? after : before != null ? before : List.of();
        return new SinkRecord(key, RowKind.INSERT, shown, changeLsn, opSeq, xid, meta,
                              new Op(code, before, after, 0, commitMicros));
    }

    /** This operation record with its transaction's commit LSN; ordinary records unchanged. */
    public SinkRecord withCommitLsn(long commitLsn) {
        if (op == null) {
            return this;
        }
        return new SinkRecord(relid, kind, values, seq, opSeq, xid, meta,
            new Op(op.code(), op.before(), op.after(), commitLsn, op.commitMicros()));
    }
}
