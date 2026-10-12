// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.Collection;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reads back where to resume: the highest {@code commitIdentifier} (a commit LSN) among Paimon
 * snapshots written by this writer's {@code commitUser}.
 *
 * <p>
 * Snapshots by other users (compaction, other writers) are skipped, so the scan walks back from the
 * latest snapshot to the newest one that is ours. Identifiers are monotone per user, so that
 * snapshot carries the maximum.
 */
public final class OffsetStore {

  private final @NotNull SchemaRegistry registry;
  private final @NotNull WriterConfig config;

  public OffsetStore(
      @NotNull SchemaRegistry registry,
      @NotNull WriterConfig config
  ) {
    this.registry = registry;
    this.config = config;
  }

  /**
   * The MINIMUM over the tracked relations of each one's highest committed identifier; 0 if any
   * tracked relation has no snapshot from our commitUser (and 0 for no relations).
   *
   * <p>
   * The sink commits table by table, so a crash can leave one transaction applied to A but not B.
   * Resuming earlier than necessary is always safe: filterAndCommit turns every already-applied
   * commit into a per-table no-op, costing only some re-read WAL. Resuming later than necessary
   * silently drops what B missed. Hence min, not max. Relations not tracked by the registry are
   * skipped.
   */
  public long highestCommittedLsn(
      @NotNull Collection<Long> relids
  ) {
    long lowest = Long.MAX_VALUE;
    boolean any = false;
    for (long relid : relids) {
      if (!registry.tracked(relid)) {
        continue;
      }
      any = true;
      Long ours = ourHighest(relid);
      lowest = Math.min(lowest, ours == null ? 0 : ours);
    }
    return any ? lowest : 0;
  }

  /**
   * Whether this writer's {@code commitUser} has ever committed anything to the relation's Paimon
   * table. This is the only honest answer to "has this system ever run for this table": {@code
   * paimon.table_registry.backfill_due} answers a different question ("does the table hold rows
   * that the initial load must copy"), and a table registered while empty is not due although
   * nothing has ever been written for it. Startup must not confuse the two.
   *
   * <p>
   * An untracked relation answers false.
   */
  public boolean hasCommitted(
      long relid
  ) {
    if (!registry.tracked(relid)) {
      return false;
    }
    try {
      return ourHighest(relid) != null;
    } catch (RuntimeException e) {
      // The relid is registered but its Paimon table does not exist yet. That is the
      // normal state when startup asks this BEFORE creating the tables, which it must do
      // so that a refusal can happen before anything in the warehouse is touched. Nothing
      // has been committed to a table that does not exist.
      return false;
    }
  }

  /** The highest identifier committed by our commitUser to one table, or null if there is none. */
  private @Nullable Long ourHighest(
      long relid
  ) {
    FileStoreTable t = registry.table(relid);
    SnapshotManager sm = t.snapshotManager();
    Long latest = sm.latestSnapshotId();
    Long earliest = sm.earliestSnapshotId();
    if (latest == null || earliest == null) {
      return null;
    }
    for (long id = latest; id >= earliest; id--) {
      if (!sm.snapshotExists(id)) {
        continue;
      }
      Snapshot s = sm.snapshot(id);
      if (config.commitUser().equals(s.commitUser())) {
        return s.commitIdentifier();
      }
    }
    return null;
  }
}
