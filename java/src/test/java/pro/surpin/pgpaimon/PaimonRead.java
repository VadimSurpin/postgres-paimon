// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Read a Paimon table written by paimon-daemon using the standard manifest
 * read path.  The daemon now writes SimpleStats (per-column nullable arrays)
 * so the Paimon 0.9.0 Java reader can deserialise the manifests directly.
 *
 * Bucketed tables (written with a fixed bucket number but no bucket-key) are
 * handled automatically: merge-engine options are stripped and bucket is set
 * to -1 so Paimon reads all files as an unordered append table.
 *
 * Usage:
 *   java -jar paimon-reader.jar [warehouse] [table]
 */
public class PaimonRead {

    public static void main(String[] args) throws Exception {
        String warehouse = args.length > 0 ? args[0] : "/tmp/paimon-warehouse";
        String tableName = args.length > 1 ? args[1] : "test_table";

        Path tablePath = new Path(warehouse + "/" + tableName);
        LocalFileIO fileIO = LocalFileIO.create();

        TableSchema stored = new SchemaManager(fileIO, tablePath)
            .latest()
            .orElseThrow(() -> new RuntimeException("No schema at " + tablePath));

        // Strip merge-engine options and force bucket=-1 so the table is read
        // as an unordered append table regardless of how it was originally written.
        // This handles tables written by paimon_heap which use a fixed bucket-0
        // directory without registering a bucket-key in the schema.
        Map<String, String> opts = new HashMap<>(stored.options());
        opts.put("bucket", "-1");
        for (String k : Arrays.asList("merge-engine", "changelog-producer", "sequence.field"))
            opts.remove(k);
        TableSchema schema = new TableSchema(
            stored.id(), stored.fields(), stored.highestFieldId(),
            stored.partitionKeys(), stored.primaryKeys(), opts, stored.comment());

        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, schema);

        RowType rowType = table.rowType();
        System.out.println("Table : " + tablePath);
        System.out.println("Schema (" + rowType.getFieldCount() + " fields):");
        for (DataField f : rowType.getFields())
            System.out.printf("  %-20s %s%n", f.name(), f.type());

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        List<Split> splits = plan.splits();

        System.out.println("\nSplits: " + splits.size());

        long total = 0;
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(splits)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow r;
                while ((r = batch.next()) != null) total++;
                batch.releaseBatch();
            }
        }
        System.out.println("Total rows: " + total);

        System.out.println("\nFirst 5 rows:");
        printHeader(rowType);
        long printed = 0;
        try (RecordReader<InternalRow> reader2 = readBuilder.newRead().createReader(splits)) {
            RecordReader.RecordIterator<InternalRow> batch;
            outer:
            while ((batch = reader2.readBatch()) != null) {
                InternalRow r;
                while ((r = batch.next()) != null) {
                    if (printed++ >= 5) { batch.releaseBatch(); break outer; }
                    printRow(r, rowType);
                }
                batch.releaseBatch();
            }
        }
    }

    static void printHeader(RowType rt) {
        StringBuilder sb = new StringBuilder("  ");
        for (DataField f : rt.getFields())
            if (!f.name().startsWith("_"))
                sb.append(String.format("%-22s", f.name()));
        System.out.println(sb);
        sb = new StringBuilder("  ");
        for (DataField f : rt.getFields())
            if (!f.name().startsWith("_"))
                sb.append(String.format("%-22s", "-".repeat(20)));
        System.out.println(sb);
    }

    static void printRow(InternalRow row, RowType rowType) {
        StringBuilder sb = new StringBuilder("  ");
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField f = fields.get(i);
            if (f.name().startsWith("_")) continue;
            String val = row.isNullAt(i) ? "NULL" : fieldVal(row, i, f);
            sb.append(String.format("%-22s", val));
        }
        System.out.println(sb);
    }

    static String fieldVal(InternalRow row, int i, DataField f) {
        switch (f.type().getTypeRoot().name()) {
            case "INTEGER":   return String.valueOf(row.getInt(i));
            case "BIGINT":    return String.valueOf(row.getLong(i));
            case "SMALLINT":  return String.valueOf(row.getShort(i));
            case "DOUBLE":    return String.format("%.4f", row.getDouble(i));
            case "FLOAT":     return String.format("%.4f", row.getFloat(i));
            case "BOOLEAN":   return String.valueOf(row.getBoolean(i));
            case "DECIMAL":   return row.getDecimal(i, 38, 18).toString();
            case "VARCHAR":
            case "CHAR":      return row.getString(i).toString();
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
            case "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
                              return row.getTimestamp(i, 6).toString();
            default: return "(" + f.type().getTypeRoot() + ")";
        }
    }
}
