// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.dto;

import java.util.List;
import pro.surpin.pgpaimon.writer.SchemaRegistry;

public record RegistryRow(
    long relid,
    String schema,
    String table,
    List<String> pk,
    List<DdlMessage.Col> cols,
    boolean backfillDue,
    String changelogProducer,
    int buckets,
    String histTable,
    String registration,
    Integer histBuckets,
    boolean histBucketsUser
) {
  public RegistryRow(
      long relid,
      String schema,
      String table,
      List<String> pk,
      List<DdlMessage.Col> cols,
      boolean backfillDue,
      String changelogProducer,
      int buckets
  ) {
    this(relid, schema, table, pk, cols, backfillDue, changelogProducer, buckets, null, null, null,
        false);
  }

  public SchemaRegistry.HistSpec hist() {
    return new SchemaRegistry.HistSpec(histTable, registration, histBuckets, histBucketsUser);
  }
}
