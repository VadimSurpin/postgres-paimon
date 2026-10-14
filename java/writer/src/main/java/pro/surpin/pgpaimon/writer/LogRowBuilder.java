// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

/**
 * Builds the row of an append-only table from an operation record ({@link SinkRecord#op()}):
 * one row per row-level change, always {@code +I}, nothing merged or collapsed.
 *
 * <p>Two layouts, both mapped by column name (as {@link RowBuilder} is, for the same reasons):
 * <ul>
 *   <li>{@link Layout#HISTORY}, the {@code <table>_hist} of every tracked table: the metadata
 *       columns, then {@code before_<col>} and {@code after_<col>} for every column;</li>
 *   <li>{@link Layout#LOG}, the main table of a table with no primary key: the user columns
 *       unprefixed, holding the new row (the deleted row for {@code D}, NULL for {@code T}),
 *       then the metadata columns.</li>
 * </ul>
 * The metadata columns are fixed by the design (spec 2026-10-14 section 2): {@code _op},
 * {@code _commit_lsn}, {@code _lsn}, {@code _op_seq}, {@code _xid}, {@code _ts}. They are not
 * configurable as the primary-key tables' {@code _seq}/{@code _op_seq} are: a history table is
 * read by people and tools that need one fixed shape.
 */
public final class LogRowBuilder implements RowMaker {

    private static final Logger LOG = Logger.getLogger(LogRowBuilder.class.getName());

    public enum Layout { HISTORY, LOG }

    public static final String OP = "_op";
    public static final String COMMIT_LSN = "_commit_lsn";
    public static final String LSN = "_lsn";
    public static final String OP_SEQ = "_op_seq";
    public static final String XID = "_xid";
    public static final String TS = "_ts";
    /** In this order, which is the order a history table declares them in. */
    public static final List<String> META = List.of(OP, COMMIT_LSN, LSN, OP_SEQ, XID, TS);

    public static final String BEFORE = "before_";
    public static final String AFTER = "after_";

    /** PostgreSQL's epoch, 2000-01-01, in microseconds after the Unix epoch. */
    public static final long PG_EPOCH_MICROS = 946_684_800_000_000L;

    private final Layout layout;
    private final RowType rowType;
    private final List<String> pgColumnOrder;
    private final int[] beforeIndex;   // HISTORY only
    private final int[] valueIndex;    // HISTORY: after_<col>; LOG: <col>
    private final int op;
    private final int commitLsn;
    private final int lsn;
    private final int opSeq;
    private final int xid;
    private final int ts;

    public LogRowBuilder(String tableName, Layout layout, RowType rowType,
                         List<String> pgColumnOrder) {
        this.layout = layout;
        this.rowType = rowType;
        this.pgColumnOrder = List.copyOf(pgColumnOrder);
        List<String> names = rowType.getFieldNames();
        int n = pgColumnOrder.size();
        this.valueIndex = new int[n];
        this.beforeIndex = layout == Layout.HISTORY ? new int[n] : null;
        List<String> ignored = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String c = pgColumnOrder.get(i);
            if (layout == Layout.HISTORY) {
                beforeIndex[i] = names.indexOf(BEFORE + c);
                valueIndex[i] = names.indexOf(AFTER + c);
            } else {
                valueIndex[i] = names.indexOf(c);
            }
            if (valueIndex[i] < 0) {
                ignored.add(c);
            }
        }
        if (!ignored.isEmpty()) {
            // Same two causes as RowBuilder's warning, same honesty about not knowing which.
            LOG.warning("table " + tableName + ": DISCARDING the values of PostgreSQL column(s) "
                + ignored + ", which have no counterpart in the Paimon schema. Expected on a"
                + " replay of changes from before their drop_col; permanent loss if the Paimon"
                + " schema is behind paimon.table_registry (compare them)");
        }
        this.op = required(names, OP);
        this.commitLsn = required(names, COMMIT_LSN);
        this.lsn = required(names, LSN);
        this.opSeq = required(names, OP_SEQ);
        this.xid = required(names, XID);
        this.ts = required(names, TS);
    }

    /** A PostgreSQL commit timestamp (microseconds after 2000-01-01) as Unix-epoch micros. */
    public static long pgToUnixMicros(long pgMicros) {
        return pgMicros + PG_EPOCH_MICROS;
    }

    @Override
    public GenericRow build(SinkRecord r) {
        SinkRecord.Op o = r.op();
        if (o == null) {
            throw new IllegalStateException("an append-only " + layout + " table takes operation"
                + " records only, got a " + r.kind() + " changelog record");
        }
        if (o.commitLsn() == 0) {
            throw new IllegalStateException("operation record at _lsn "
                + SlotConsumer.formatLsn(r.seq()) + " has no commit LSN");
        }
        GenericRow row = new GenericRow(rowType.getFieldCount());
        row.setRowKind(RowKind.INSERT);
        List<DataField> fields = rowType.getFields();
        switch (o.code()) {
            case 'I', 'R' -> set(row, fields, valueIndex, o.after());
            case 'U' -> {
                if (layout == Layout.HISTORY) {
                    set(row, fields, beforeIndex, o.before());
                }
                set(row, fields, valueIndex, o.after());
            }
            case 'D' -> set(row, fields, layout == Layout.HISTORY ? beforeIndex : valueIndex,
                            o.before());
            case 'T' -> { }   // no row: every user column NULL
            default -> throw new IllegalStateException("unknown operation '" + o.code() + "'");
        }
        row.setField(op, BinaryString.fromString(String.valueOf(o.code())));
        row.setField(commitLsn, o.commitLsn());
        row.setField(lsn, r.seq());
        row.setField(opSeq, r.opSeq());
        row.setField(xid, r.xid());
        row.setField(ts, Timestamp.fromMicros(o.commitMicros()));
        return row;
    }

    private void set(GenericRow row, List<DataField> fields, int[] index, List<String> values) {
        if (values == null) {
            throw new IllegalStateException("operation record lacks the image it needs");
        }
        if (values.size() != index.length) {
            throw new IllegalStateException("record has " + values.size()
                + " values but the relation has " + index.length + " columns " + pgColumnOrder);
        }
        for (int i = 0; i < index.length; i++) {
            int target = index[i];
            if (target < 0) {
                continue;
            }
            DataField f = fields.get(target);
            row.setField(target, TextDecoder.toPaimon(f.name(), f.type(), values.get(i)));
        }
    }

    private static int required(List<String> names, String name) {
        int i = names.indexOf(name);
        if (i < 0) {
            throw new IllegalArgumentException("Paimon schema has no metadata column '" + name
                + "': " + names);
        }
        return i;
    }
}
