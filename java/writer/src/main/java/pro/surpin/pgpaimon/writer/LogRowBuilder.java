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
 * Builds the row of an append-only table from an operation record ({@link SinkRecord#op()}): one
 * row per row image, always stored as {@code +I}, nothing merged or collapsed.
 *
 * <p>Both append-only layouts are the user columns, unprefixed and mapped by name (as
 * {@link RowBuilder} does, for the same reasons), then the metadata columns (spec 2026-10-14,
 * section 2): a history table ({@code <table>_hist} and its archives) has
 * {@code _op, _commit_lsn, _lsn, _op_seq, _xid, _ts, _dt}; a key-less table's operation log has
 * the same without {@code _dt}. The names are fixed, not configurable as the primary-key tables'
 * {@code _seq}/{@code _op_seq} are: a history table is read by people and tools that need one shape.
 *
 * <p>{@code _dt} is the UTC calendar date of {@code _ts}, the transaction's commit timestamp as
 * pgoutput sent it. It is computed here from the epoch microseconds, never through a time zone,
 * so neither the JVM's nor any session's zone can move a row to another partition, and a replay
 * writes the same partition.
 */
public final class LogRowBuilder implements RowMaker {

    private static final Logger LOG = Logger.getLogger(LogRowBuilder.class.getName());

    public static final String OP = "_op";
    public static final String COMMIT_LSN = "_commit_lsn";
    public static final String LSN = "_lsn";
    public static final String OP_SEQ = "_op_seq";
    public static final String XID = "_xid";
    public static final String TS = "_ts";
    public static final String DT = "_dt";
    /** The operation log's metadata, in order. */
    public static final List<String> META = List.of(OP, COMMIT_LSN, LSN, OP_SEQ, XID, TS);
    /** A history table's metadata, in order: the log's plus the partition column. */
    public static final List<String> HIST_META = List.of(OP, COMMIT_LSN, LSN, OP_SEQ, XID, TS, DT);

    /** PostgreSQL's epoch, 2000-01-01, in microseconds after the Unix epoch. */
    public static final long PG_EPOCH_MICROS = 946_684_800_000_000L;
    static final long MICROS_PER_DAY = 86_400_000_000L;

    private final RowType rowType;
    private final List<String> pgColumnOrder;
    private final int[] valueIndex;
    private final int op;
    private final int commitLsn;
    private final int lsn;
    private final int opSeq;
    private final int xid;
    private final int ts;
    private final int dt;          // -1 for an operation log

    public LogRowBuilder(String tableName, RowType rowType, List<String> pgColumnOrder) {
        this.rowType = rowType;
        this.pgColumnOrder = List.copyOf(pgColumnOrder);
        List<String> names = rowType.getFieldNames();
        int n = pgColumnOrder.size();
        this.valueIndex = new int[n];
        List<String> ignored = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            valueIndex[i] = names.indexOf(pgColumnOrder.get(i));
            if (valueIndex[i] < 0) {
                ignored.add(pgColumnOrder.get(i));
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
        this.dt = names.indexOf(DT);
    }

    /** A PostgreSQL commit timestamp (microseconds after 2000-01-01) as Unix-epoch micros. */
    public static long pgToUnixMicros(long pgMicros) {
        return pgMicros + PG_EPOCH_MICROS;
    }

    /** The UTC calendar date of an instant, as days since 1970-01-01 (Paimon's DATE). */
    public static int utcDay(long unixMicros) {
        return (int) Math.floorDiv(unixMicros, MICROS_PER_DAY);
    }

    @Override
    public GenericRow build(SinkRecord r) {
        SinkRecord.Op o = r.op();
        if (o == null) {
            throw new IllegalStateException("an append-only table takes operation records only,"
                + " got a " + r.kind() + " changelog record");
        }
        if (o.commitLsn() == 0) {
            throw new IllegalStateException("operation record at _lsn "
                + SlotConsumer.formatLsn(r.seq()) + " has no commit LSN");
        }
        GenericRow row = new GenericRow(rowType.getFieldCount());
        row.setRowKind(RowKind.INSERT);
        if (o.image() != null) {
            List<String> values = o.image();
            if (values.size() != valueIndex.length) {
                throw new IllegalStateException("record has " + values.size()
                    + " values but the relation has " + valueIndex.length + " columns "
                    + pgColumnOrder);
            }
            List<DataField> fields = rowType.getFields();
            for (int i = 0; i < valueIndex.length; i++) {
                int target = valueIndex[i];
                if (target < 0) {
                    continue;
                }
                DataField f = fields.get(target);
                row.setField(target, TextDecoder.toPaimon(f.name(), f.type(), values.get(i)));
            }
        } else if (!"T".equals(o.code())) {
            throw new IllegalStateException("operation " + o.code() + " lacks its row image");
        }
        row.setField(op, BinaryString.fromString(o.code()));
        row.setField(commitLsn, o.commitLsn());
        row.setField(lsn, r.seq());
        row.setField(opSeq, r.opSeq());
        row.setField(xid, r.xid());
        row.setField(ts, Timestamp.fromMicros(o.commitMicros()));
        if (dt >= 0) {
            row.setField(dt, utcDay(o.commitMicros()));
        }
        return row;
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
