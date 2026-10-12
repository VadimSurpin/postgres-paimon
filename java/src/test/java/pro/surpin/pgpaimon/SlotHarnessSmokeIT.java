// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.junit.jupiter.api.Test;

/**
 * Proves the acceptance harness's central claim: a JDBC suite can observe changes that PostgreSQL
 * streams to Paimon. Needs the stack from test/slot-acceptance/run_acceptance.sh
 * (PGPAIMON_IT_JDBC_URL, PGPAIMON_IT_WAREHOUSE); skips when run standalone, but fails if the
 * harness set PGPAIMON_HARNESS=1 and the variables are missing, so it can never skip silently
 * there.
 */
class SlotHarnessSmokeIT {

  static final long TIMEOUT_MS = 60_000;

  @Test
  void insertUpdateDeleteAreVisibleInPaimon() throws Exception {
    String jdbc = System.getenv("PGPAIMON_IT_JDBC_URL");
    String wh = System.getenv("PGPAIMON_IT_WAREHOUSE");
    if ("1".equals(System.getenv("PGPAIMON_HARNESS"))) {
      assertNotNull(jdbc, "harness did not export PGPAIMON_IT_JDBC_URL");
      assertNotNull(wh, "harness did not export PGPAIMON_IT_WAREHOUSE");
    }
    assumeTrue(jdbc != null && wh != null,
        "no live stack: run test/slot-acceptance/run_acceptance.sh");

    // setup.sql seeded ids 1 and 2 (backfilled). Stream: insert 3, update 3, delete 2,
    // and a primary-key update 1 -> 10.
    try (Connection c = DriverManager.getConnection(jdbc); Statement st = c.createStatement()) {
      st.execute("INSERT INTO harness_smoke VALUES (3, 'three', 3.00)");
      st.execute("UPDATE harness_smoke SET label = 'three-b', amt = 3.75 WHERE id = 3");
      st.execute("DELETE FROM harness_smoke WHERE id = 2");
      st.execute("UPDATE harness_smoke SET id = 10 WHERE id = 1");
    }
    Map<Integer, String> expected = new TreeMap<>();
    try (Connection c = DriverManager.getConnection(jdbc);
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT id, label, amt FROM harness_smoke")) {
      while (rs.next()) {
        expected.put(rs.getInt(1), rs.getString(2) + "|" + rs.getBigDecimal(3).toPlainString());
      }
    }
    assertEquals(Map.of(10, "one|1.50", 3, "three-b|3.75"), expected, "PostgreSQL itself is wrong");

    Options o = new Options();
    o.set("warehouse", wh);
    try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
      Map<Integer, String> actual = Map.of();
      long deadline = System.currentTimeMillis() + TIMEOUT_MS;
      while (System.currentTimeMillis() < deadline) {
        try {
          actual = mergedContents(catalog);
        } catch (Exception notYet) {
          actual = Map.of(); // table or first snapshot not there yet
        }
        if (actual.equals(expected))
          break;
        Thread.sleep(250);
      }
      assertEquals(expected, actual, "Paimon did not converge to PostgreSQL's contents within "
          + TIMEOUT_MS
          + "ms (waiting for the insert of 3, its update, the delete of 2 and the key update 1 -> 10)");

      // The changelog must show each operation, not only the merged result.
      Set<String> kinds = changelogKinds(catalog);
      assertTrue(kinds.contains("3:+I:three"), "insert of id 3 missing from changelog: " + kinds);
      assertTrue(kinds.contains("2:-D:two"), "delete of id 2 missing from changelog: " + kinds);
      // An update of a non-key column is a genuine -U/+U pair (spec section 5), never
      // -D/+I: the key comes from the registry's primary key, not pgoutput's flags.
      assertTrue(kinds.contains("3:-U:three"),
          "update of id 3 must produce -U with the before-image: " + kinds);
      assertTrue(kinds.contains("3:+U:three-b"),
          "update of id 3 must produce +U with the after-image: " + kinds);
      assertFalse(kinds.contains("3:-D:three"),
          "update of id 3 was decomposed into -D/+I: " + kinds);
      // A primary-key update is a delete of the old key and an insert of the new, never -U/+U.
      assertTrue(kinds.contains("1:-D:one"), "old key 1 must be deleted: " + kinds);
      assertTrue(kinds.contains("10:+I:one"), "new key 10 must be inserted: " + kinds);
      assertFalse(kinds.contains("1:-U:one") || kinds.contains("10:+U:one"),
          "a primary-key update must not be -U/+U: " + kinds);
    }
  }

  private static Map<Integer, String> mergedContents(
      Catalog catalog
  ) throws Exception {
    Table t = catalog.getTable(Identifier.create("default", "harness_smoke"));
    List<String> names = t.rowType().getFieldNames();
    int id = names.indexOf("id"), label = names.indexOf("label"), amt = names.indexOf("amt");
    ReadBuilder rb = t.newReadBuilder();
    Map<Integer, String> out = new TreeMap<>();
    try (RecordReader<org.apache.paimon.data.InternalRow> r =
        rb.newRead().createReader(rb.newScan().plan())) {
      r.forEachRemaining(row -> out.put(row.getInt(id),
          row.getString(label) + "|" + row.getDecimal(amt, 12, 2).toBigDecimal().toPlainString()));
    }
    return out;
  }

  /** "id:kind" for every changelog record, via a streaming scan of $audit_log (as RawRead does). */
  private static Set<String> changelogKinds(
      Catalog catalog
  ) throws Exception {
    Table al = catalog.getTable(Identifier.create("default", "harness_smoke$audit_log"))
        .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
    int idIdx = al.rowType().getFieldNames().indexOf("id");
    StreamTableScan scan = al.newReadBuilder().newStreamScan();
    scan.plan(); // the first plan() is always empty
    Long latest = ((FileStoreTable) catalog.getTable(Identifier.create("default", "harness_smoke")))
        .snapshotManager().latestSnapshotId();
    List<Split> splits = new ArrayList<>();
    for (long i = 0; i <= (latest == null ? 0 : latest) + 1; i++) {
      splits.addAll(scan.plan().splits());
    }
    Set<String> out = new TreeSet<>();
    try (RecordReader<org.apache.paimon.data.InternalRow> r =
        al.newReadBuilder().newRead().createReader(splits)) {
      r.forEachRemaining(row -> out.add(row.getInt(idIdx) + ":" + row.getString(0) + ":"
          + row.getString(al.rowType().getFieldNames().indexOf("label"))));
    }
    return out;
  }
}
