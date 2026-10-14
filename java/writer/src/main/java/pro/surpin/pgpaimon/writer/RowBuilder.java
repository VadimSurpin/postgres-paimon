// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
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
public final class RowBuilder implements RowMaker {

    private static final Logger LOG = Logger.getLogger(RowBuilder.class.getName());

    private final RowType rowType;
    private final int[] paimonIndexForPgColumn;
    private final List<String> pgColumnOrder;
    private final int seqIndex;
    private final int opSeqIndex;
    private final int xidIndex;

    public RowBuilder(RowType rowType, List<String> pgColumnOrder,
                      String seqField, String opSeqField, String xidField) {
        this(null, rowType, pgColumnOrder, seqField, opSeqField, xidField);
    }

    /** {@code tableName} is used only to name the table in the dropped-column warning. */
    public RowBuilder(String tableName, RowType rowType, List<String> pgColumnOrder,
                      String seqField, String opSeqField, String xidField) {
        this.rowType = rowType;
        this.pgColumnOrder = List.copyOf(pgColumnOrder);
        this.paimonIndexForPgColumn = new int[pgColumnOrder.size()];
        for (int i = 0; i < pgColumnOrder.size(); i++) {
            this.paimonIndexForPgColumn[i] = rowType.getFieldNames().indexOf(pgColumnOrder.get(i));
        }
        warnAboutIgnoredColumns(tableName);
        this.seqIndex = required(rowType, seqField);
        this.opSeqIndex = required(rowType, opSeqField);
        this.xidIndex = xidField == null ? -1 : required(rowType, xidField);
    }

    /**
     * A PostgreSQL column with no Paimon counterpart means the Paimon schema has moved on,
     * which happens two ways, and ignoring the value is right in both. The drop_col DDL was
     * earlier in the stream: the column is gone and its value unwanted. Or it is later in
     * the stream (a replay): the registry was rebuilt from paimon.table_registry and reflects
     * the CURRENT schema, so the column is already absent and cannot be restored; the value
     * is about to be dropped anyway. Throwing would kill the writer on a replay it was
     * designed to perform. (The opposite case, a Paimon column with no PostgreSQL
     * counterpart, stays null: that is an added column.) Builders are cached per
     * (relid, column list), so this warns once per schema, not once per row.
     */
    private void warnAboutIgnoredColumns(String tableName) {
        List<String> ignored = new ArrayList<>();
        for (int i = 0; i < paimonIndexForPgColumn.length; i++) {
            if (paimonIndexForPgColumn[i] < 0) {
                ignored.add(pgColumnOrder.get(i));
            }
        }
        if (!ignored.isEmpty()) {
            // Deliberately not "likely a replay". It is likely a replay, and on a replay the
            // values are genuinely unwanted -- but the other cause of this warning is a Paimon
            // schema left behind the registry, where the same line reports permanent, silent
            // loss of a column's values. Saying which it is is impossible here, so say both.
            LOG.warning("table " + (tableName == null ? "(unnamed)" : tableName)
                + ": DISCARDING the values of PostgreSQL column(s) " + ignored
                + ", which have no counterpart in the Paimon schema. Expected if their drop_col"
                + " came earlier in the stream, or on a replay of changes from before it -- the"
                + " values are unwanted then. But if the Paimon schema is simply BEHIND (a DDL"
                + " message lost with a dropped slot), this line is permanent data loss, one per"
                + " row: compare the Paimon schema with paimon.table_registry and re-register"
                + " the table if they disagree");
        }
    }

    @Override
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
                continue;   // see warnAboutIgnoredColumns
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
