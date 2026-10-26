// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.pgpaimon.writer.dto.*;

/** Every table the writer makes carries the cluster's identity; an existing one is stamped. */
class IdentityStampTest {

  @TempDir
  Path warehouse;

  static final Map<String, String> ID =
      Map.of(ClusterIdentity.SYSID_OPTION, "7000", ClusterIdentity.TIMELINE_OPTION, "1");

  SchemaRegistry registry() {
    Options o = new Options();
    o.set("warehouse", warehouse.toString());
    Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
    Map<String, String> env = new HashMap<>();
    env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost:5432/testdb");
    env.put("PGPAIMON_WAREHOUSE", warehouse.toString());
    return new SchemaRegistry(catalog, WriterConfigUtils.fromEnv(env));
  }

  static final List<DdlMessage.Col> COLS =
      List.of(new DdlMessage.Col(1, "id", "INT NOT NULL"), new DdlMessage.Col(2, "v", "STRING"));

  @Test
  void newTablesAndTheirHistoryCarryTheStamp() {
    SchemaRegistry r = registry();
    r.identityOptions(ID);
    r.createTable(1L, "public", "t", List.of("id"), COLS, "input", 1, 100);
    for (String t : List.of("t", "t_hist")) {
      Map<String, String> o = r.optionsOf("public", t);
      assertEquals("7000", o.get(ClusterIdentity.SYSID_OPTION), t);
      assertEquals("1", o.get(ClusterIdentity.TIMELINE_OPTION), t);
    }
  }

  @Test
  void anExistingTableIsStampedOnceAndAMissingOneIsNoError() {
    SchemaRegistry r = registry();
    r.createTable(1L, "public", "t", List.of("id"), COLS, "input", 1, 100);
    assertNull(r.optionsOf("public", "t").get(ClusterIdentity.SYSID_OPTION));
    assertTrue(r.stampExisting("public", "t", ID));
    assertEquals("7000", r.optionsOf("public", "t").get(ClusterIdentity.SYSID_OPTION));
    assertFalse(r.stampExisting("public", "t", ID), "already stamped: no schema change");
    assertTrue(r.stampExisting("public", "t", Map.of(ClusterIdentity.TIMELINE_OPTION, "2")));
    assertEquals("2", r.optionsOf("public", "t").get(ClusterIdentity.TIMELINE_OPTION));
    assertFalse(r.stampExisting("public", "nope", ID));
    assertNull(r.optionsOf("public", "nope"));
  }
}
