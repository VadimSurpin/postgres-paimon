// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

/**
 * Builds a Paimon row from a SinkRecord by column name.
 *
 * Name mapping rather than position mapping: the pgoutput Relation message lists
 * live columns in attnum order, while the Paimon schema also carries _seq, _op_seq
 * and optionally _xid, and a DROP COLUMN shifts positions on one side only.
 */
public final class RowBuilder {

    private final RowType rowType;
    private final int[] paimonIndexForPgColumn;
    private final List<String> pgColumnOrder;
    private final int seqIndex;
    private final int opSeqIndex;
    private final int xidIndex;

    public RowBuilder(RowType rowType, List<String> pgColumnOrder,
                      String seqField, String opSeqField, String xidField) {
        this.rowType = rowType;
        this.pgColumnOrder = List.copyOf(pgColumnOrder);
        this.paimonIndexForPgColumn = new int[pgColumnOrder.size()];
        for (int i = 0; i < pgColumnOrder.size(); i++) {
            this.paimonIndexForPgColumn[i] = rowType.getFieldNames().indexOf(pgColumnOrder.get(i));
        }
        this.seqIndex = required(rowType, seqField);
        this.opSeqIndex = required(rowType, opSeqField);
        this.xidIndex = xidField == null ? -1 : required(rowType, xidField);
    }

    public GenericRow build(SinkRecord r) {
        if (r.values().size() != paimonIndexForPgColumn.length) {
            throw new IllegalStateException("record has " + r.values().size()
                + " values but the relation has " + paimonIndexForPgColumn.length
                + " columns " + pgColumnOrder);
        }
        GenericRow row = new GenericRow(rowType.getFieldCount());
        row.setRowKind(r.kind());
        List<DataField> fields = rowType.getFields();

        for (int i = 0; i < paimonIndexForPgColumn.length; i++) {
            int target = paimonIndexForPgColumn[i];
            if (target < 0) {
                throw new IllegalStateException(
                    "PostgreSQL column '" + pgColumnOrder.get(i)
                    + "' has no counterpart in the Paimon schema " + rowType
                    + "; a DDL message was missed");
            }
            DataField f = fields.get(target);
            row.setField(target, TextDecoder.toPaimon(f.name(), f.type(), r.values().get(i)));
        }

        row.setField(seqIndex, r.seq());
        row.setField(opSeqIndex, r.opSeq());
        if (xidIndex >= 0) {
            row.setField(xidIndex, r.xid());
        }
        return row;
    }

    private static int required(RowType rowType, String name) {
        int i = rowType.getFieldNames().indexOf(name);
        if (i < 0) {
            throw new IllegalArgumentException(
                "Paimon schema has no metadata column '" + name + "': " + rowType);
        }
        return i;
    }
}
