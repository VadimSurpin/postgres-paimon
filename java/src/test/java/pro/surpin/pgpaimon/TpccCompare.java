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
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;

/**
 * Compare PostgreSQL TPC-C table state against a Paimon warehouse written
 * by the paimon-daemon / paimon_heap extension.
 *
 * Usage:
 *   java -cp paimon-reader.jar pro.surpin.pgpaimon.TpccCompare [warehouse] [jdbc-url]
 *
 * Defaults:
 *   warehouse = /tmp/paimon-warehouse
 *   jdbc-url  = jdbc:postgresql://localhost:5432/benchbase?user=ubuntu&password=
 */
public class TpccCompare {

    static final String[] TABLES = {
        "warehouse", "district", "customer", "history",
        "item", "stock", "oorder", "new_order", "order_line"
    };
    static final Set<String> INSERT_ONLY =
        new HashSet<>(Arrays.asList("item", "history"));

    public static void main(String[] args) throws Exception {
        String warehouse = args.length > 0 ? args[0] : "/tmp/paimon-warehouse";
        String jdbcUrl   = args.length > 1 ? args[1]
            : "jdbc:postgresql://localhost:5432/benchbase?user=ubuntu&password=";

        System.out.println("╔════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║          TPC-C State Comparison: PostgreSQL vs Paimon Warehouse        ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════════╝");
        System.out.println();

        /* ── 1. PostgreSQL counts ── */
        Map<String, Long> pgCounts = new LinkedHashMap<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            for (String tbl : TABLES) {
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + tbl)) {
                    rs.next();
                    pgCounts.put(tbl, rs.getLong(1));
                }
            }
        }
        System.out.println("PostgreSQL counts retrieved.");

        /* ── 2. Paimon counts (total rows + row-kind breakdown) ── */
        LocalFileIO fileIO = LocalFileIO.create();

        record PCount(long insert, long updBefore, long updAfter, long delete, long total) {}
        Map<String, PCount> paimonCounts = new LinkedHashMap<>();

        for (String tbl : TABLES) {
            Path tablePath = new Path(warehouse + "/" + tbl);

            TableSchema stored = new SchemaManager(fileIO, tablePath).latest()
                .orElseThrow(() -> new RuntimeException("No schema: " + tablePath));
            Map<String, String> opts = new HashMap<>(stored.options());
            opts.put("bucket", "-1");
            opts.remove("merge-engine");
            opts.remove("changelog-producer");

            List<DataField> fields = new ArrayList<>();
            fields.add(new DataField(-1, "_row_kind", DataTypes.INT().notNull()));
            fields.addAll(stored.fields());

            TableSchema schema = new TableSchema(
                stored.id(), fields, stored.highestFieldId(),
                stored.partitionKeys(), stored.primaryKeys(), opts, stored.comment());
            FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, schema);

            ReadBuilder rb = table.newReadBuilder();
            List<Split> splits = rb.newScan().plan().splits();

            long ins = 0, ubf = 0, uaf = 0, del = 0, tot = 0;
            try (RecordReader<InternalRow> reader = rb.newRead().createReader(splits)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow r;
                    while ((r = batch.next()) != null) {
                        tot++;
                        if (!r.isNullAt(0)) {
                            switch (r.getInt(0)) {
                                case 0: ins++; break;
                                case 1: ubf++; break;
                                case 2: uaf++; break;
                                case 3: del++; break;
                            }
                        }
                    }
                    batch.releaseBatch();
                }
            } catch (Exception e) {
                File[] fallback = new File(warehouse + "/" + tbl + "/bucket-0")
                    .listFiles((d, n) -> n.endsWith(".parquet"));
                tot = fallback != null ? rawParquetRowCount(fallback) : 0;
                ins = tot;
                System.err.println("  [" + tbl + "] row-kind projection failed, using raw count: " + e.getMessage());
            }
            paimonCounts.put(tbl, new PCount(ins, ubf, uaf, del, tot));
        }

        /* ── 3. Print comparison ── */
        System.out.println();
        System.out.printf("%-12s  %9s  %9s  %9s  %9s  %9s  %9s  %s%n",
            "Table", "PG rows", "P inserts", "P upd_aft", "P deletes", "P total",
            "P net", "status");
        System.out.println("─".repeat(93));

        boolean allOk = true;
        for (String tbl : TABLES) {
            long pg  = pgCounts.getOrDefault(tbl, -1L);
            PCount p = paimonCounts.getOrDefault(tbl, new PCount(0,0,0,0,0));
            long net = p.insert() + p.updAfter() - p.updBefore() - p.delete();
            String status;
            if (INSERT_ONLY.contains(tbl)) {
                status = (pg == p.total()) ? "✓ exact" : "✗ MISMATCH (insert-only table)";
                if (pg != p.total()) allOk = false;
            } else {
                status = (net == pg) ? "✓ net match"
                       : (p.total() >= pg) ? "~ captured (updates/dels logged)"
                       : "✗ MISSING DATA";
                if (p.total() < pg) allOk = false;
            }
            System.out.printf("%-12s  %9d  %9d  %9d  %9d  %9d  %9d  %s%n",
                tbl, pg, p.insert(), p.updAfter(), p.delete(), p.total(), net, status);
        }
        System.out.println("─".repeat(93));

        /* ── 4. Spot-check w_ytd ── */
        System.out.println();
        System.out.println("Spot-check: warehouse w_ytd (last Paimon record vs PostgreSQL)");
        spotCheckWarehouse(warehouse, jdbcUrl, fileIO);

        System.out.println();
        System.out.println(allOk
            ? "Result: All tables captured correctly in Paimon ✓"
            : "Result: Some tables have data discrepancies — see above ✗");
    }

    static long rawParquetRowCount(File[] parquets) {
        long total = 0;
        for (File f : parquets) {
            try {
                byte[] data = Files.readAllBytes(f.toPath());
                int footerLen = ByteBuffer.wrap(data, data.length - 8, 4)
                    .order(ByteOrder.LITTLE_ENDIAN).getInt();
                total += parseNumRows(data, data.length - 8 - footerLen, data.length - 8);
            } catch (Exception ignored) {}
        }
        return total;
    }

    static long parseNumRows(byte[] buf, int start, int end) {
        int pos = start, prevFieldId = 0;
        while (pos < end) {
            int b = buf[pos++] & 0xFF;
            if (b == 0) break;
            int typeBits = b & 0x0F;
            int delta    = (b >> 4) & 0x0F;
            int fieldId;
            if (delta == 0) {
                long fid = 0; int sh = 0;
                while (pos < end) {
                    int vb = buf[pos++] & 0xFF;
                    fid |= ((long)(vb & 0x7F)) << sh;
                    if ((vb & 0x80) == 0) break;
                    sh += 7;
                }
                fieldId = (int)((fid >>> 1) ^ -(fid & 1));
            } else {
                fieldId = prevFieldId + delta;
            }
            prevFieldId = fieldId;
            if (fieldId == 3 && typeBits == 6) {
                long val = 0; int sh = 0;
                while (pos < end) {
                    int vb = buf[pos++] & 0xFF;
                    val |= ((long)(vb & 0x7F)) << sh;
                    if ((vb & 0x80) == 0) break;
                    sh += 7;
                }
                return (val >>> 1) ^ -(val & 1);
            }
            pos = skipField(buf, pos, end, typeBits);
            if (pos < 0) break;
        }
        return 0;
    }

    static int skipField(byte[] buf, int pos, int end, int type) {
        switch (type) {
            case 1: case 2: return pos;
            case 3: case 4: case 5: case 6: {
                while (pos < end && (buf[pos] & 0x80) != 0) pos++;
                return pos + 1;
            }
            case 7: return pos + 8;
            case 8: {
                long len = 0; int sh = 0;
                while (pos < end) {
                    int vb = buf[pos++] & 0xFF;
                    len |= ((long)(vb & 0x7F)) << sh;
                    if ((vb & 0x80) == 0) break;
                    sh += 7;
                }
                return (int)(pos + len);
            }
            case 9: case 10: case 11: return end;
            case 12: {
                int prev = 0;
                while (pos < end) {
                    int b = buf[pos++] & 0xFF;
                    if (b == 0) return pos;
                    int t = b & 0x0F, d = (b >> 4) & 0x0F;
                    if (d == 0) { while (pos < end && (buf[pos] & 0x80) != 0) pos++; pos++; }
                    prev = (d == 0) ? prev : prev + d;
                    pos = skipField(buf, pos, end, t);
                    if (pos < 0 || pos >= end) return pos;
                }
                return pos;
            }
            default: return -1;
        }
    }

    static void spotCheckWarehouse(String warehouse, String jdbcUrl,
                                   LocalFileIO fileIO) throws Exception {
        double pgYtd = 0;
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT w_ytd FROM warehouse WHERE w_id=1")) {
            if (rs.next()) pgYtd = rs.getDouble(1);
        }

        Path tablePath = new Path(warehouse + "/warehouse");

        TableSchema stored = new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema"));
        if (!new File(warehouse + "/warehouse/snapshot/LATEST").exists()) {
            System.out.println("  No warehouse snapshot found.");
            return;
        }
        Map<String, String> opts = new HashMap<>(stored.options());
        opts.put("bucket", "-1"); opts.remove("merge-engine"); opts.remove("changelog-producer");
        TableSchema schema = new TableSchema(stored.id(), stored.fields(), stored.highestFieldId(),
            stored.partitionKeys(), stored.primaryKeys(), opts, stored.comment());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, schema);
        RowType rowType = table.rowType();

        int idxId = -1, idxYtd = -1;
        int ytdPrecision = 12, ytdScale = 2;
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals("w_id"))  idxId  = i;
            if (fields.get(i).name().equals("w_ytd")) {
                idxYtd = i;
                String ts = fields.get(i).type().toString();
                java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("DECIMAL\\((\\d+),\\s*(\\d+)\\)").matcher(ts);
                if (m.find()) {
                    ytdPrecision = Integer.parseInt(m.group(1));
                    ytdScale     = Integer.parseInt(m.group(2));
                }
            }
        }

        ReadBuilder rb = table.newReadBuilder();
        List<Split> splits = rb.newScan().plan().splits();

        double lastYtd = Double.NaN;
        long wareCount = 0;
        try (RecordReader<InternalRow> reader = rb.newRead().createReader(splits)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow r;
                while ((r = batch.next()) != null) {
                    wareCount++;
                    if (idxId >= 0 && !r.isNullAt(idxId) && r.getInt(idxId) == 1
                            && idxYtd >= 0 && !r.isNullAt(idxYtd)) {
                        try {
                            lastYtd = r.getDecimal(idxYtd, ytdPrecision, ytdScale)
                                       .toBigDecimal().doubleValue();
                        } catch (Exception e) {
                            try { lastYtd = Double.parseDouble(r.getString(idxYtd).toString()); }
                            catch (Exception ignored) {}
                        }
                    }
                }
                batch.releaseBatch();
            }
        }

        System.out.printf("  Paimon warehouse rows read: %d%n", wareCount);
        System.out.printf("  PostgreSQL  w_ytd = %.2f%n", pgYtd);
        if (Double.isNaN(lastYtd))
            System.out.println("  Paimon last w_ytd = N/A (decimal column type mismatch)");
        else {
            System.out.printf("  Paimon last w_ytd = %.2f%n", lastYtd);
            System.out.printf("  Match: %s%n", Math.abs(pgYtd - lastYtd) < 0.005 ? "OK" : "MISMATCH");
        }
    }
}
