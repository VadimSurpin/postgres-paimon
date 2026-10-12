// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import pro.surpin.pgpaimon.writer.dto.*;

/**
 * What the transaction assembler needs to know about a relation: whether it is replicated, and into
 * which tables. Implemented by {@link SchemaRegistry}; tests supply their own.
 */
public interface TableTracker {
  /** Whether a key (a relation id or a {@link HistoryKeys history key}) names a tracked table. */
  boolean tracked(
      long key
  );

  /** Whether the relation has no primary key, so its main table is an operation log. */
  boolean keyless(
      long relid
  );

  /** Whether the relation has a history table this process writes. */
  boolean hasHistory(
      long relid
  );

  /**
   * The key of the history table a change belongs to: the current history table if the change came
   * after its last rotation, else the archive of the schema it was decoded under (spec 2026-10-14,
   * rule 3). 0 when there is none to write.
   */
  default long historyKey(
      long relid,
      long changeLsn
  ) {
    return hasHistory(relid) ? HistoryKeys.histKey(relid) : 0;
  }
}
