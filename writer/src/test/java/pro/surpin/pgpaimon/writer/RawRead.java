// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;

/**
 * Test helper: every changelog record of a table, as stored. The row kind is read through a
 * streaming scan over {@code 
 * 
<table>
 * $audit_log}, whose first column is the kind as a string ({@code
 * +I}, {@code -U}, {@code +U}, {@code -D}); {@code _row_kind} is a column only main's C++ writer
 * physically emits, Paimon itself stores the reserved {@code _VALUE_KIND}.
 */
final class RawRead {

  private RawRead() {}

  /** Reads from the warehouse path, database {@code default}. */
  static List<RawRecord> read(
      String warehouse,
      String table
  ) throws Exception {
    Options o = new Options();
    o.set("warehouse", warehouse);
    try (Catalog c = CatalogFactory.createCatalog(CatalogContext.create(o))) {
      return read(c, "default", table);
    }
  }

  static List<RawRecord> read(
      Catalog catalog,
      String database,
      String table
  ) throws Exception {
    Table al = catalog.getTable(Identifier.create(database, table + "$audit_log"))
        .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
    int seqIdx = al.rowType().getFieldNames().indexOf("_seq");
    if (seqIdx < 0) {
      throw new IllegalStateException("no _seq column in " + al.rowType());
    }
    StreamTableScan scan = al.newReadBuilder().newStreamScan();
    scan.plan(); // first plan() is always empty: discard
    // Each later plan() carries one snapshot's changelog (an empty one for a snapshot with
    // none, such as an overwrite), so walk past the latest snapshot id.
    Long latest = ((org.apache.paimon.table.FileStoreTable) catalog
        .getTable(Identifier.create(database, table))).snapshotManager().latestSnapshotId();
    List<Split> splits = new ArrayList<>();
    for (long i = 0; i <= (latest == null ? 0 : latest) + 1; i++) {
      splits.addAll(scan.plan().splits());
    }
    List<RawRecord> out = new ArrayList<>();
    try (RecordReader<org.apache.paimon.data.InternalRow> r =
        al.newReadBuilder().newRead().createReader(splits)) {
      r.forEachRemaining(
          row -> out.add(new RawRecord(row.getString(0).toString(), row.getLong(seqIdx))));
    }
    return out;
  }
}
