// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;
import org.apache.paimon.types.RowKind;

/**
 * relid: a PostgreSQL OID held as long; see package-info.
 *
 * <p>meta is the relation description the values were decoded under, or null when the producer
 * has none (the initial load). A transaction can change a table's columns between two of its own
 * changes (BEGIN; INSERT; ALTER TABLE ADD COLUMN; INSERT; COMMIT), and records are built at flush
 * time, so each must be built against its own description and not the latest one.
 */
public record SinkRecord(long relid, RowKind kind, List<String> values,
                         long seq, int opSeq, long xid, RelationMeta meta) {
    public SinkRecord(long relid, RowKind kind, List<String> values,
                      long seq, int opSeq, long xid) {
        this(relid, kind, values, seq, opSeq, xid, null);
    }
}
