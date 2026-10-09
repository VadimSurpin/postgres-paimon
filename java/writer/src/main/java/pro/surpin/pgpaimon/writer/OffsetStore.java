// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.Collection;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

/**
 * Reads back where to resume: the highest {@code commitIdentifier} (a commit LSN) among Paimon
 * snapshots written by this writer's {@code commitUser}.
 *
 * <p>Snapshots by other users (compaction, other writers) are
 * skipped, so the scan walks back from the latest snapshot to the newest one that is ours.
 * Identifiers are monotone per user, so that snapshot carries the maximum.
 */
public final class OffsetStore {

    private final SchemaRegistry registry;
    private final WriterConfig config;

    public OffsetStore(SchemaRegistry registry, WriterConfig config) {
        this.registry = registry;
        this.config = config;
    }

    /**
     * The MINIMUM over the tracked relations of each one's highest committed identifier; 0 if any
     * tracked relation has no snapshot from our commitUser (and 0 for no relations).
     *
     * <p>The sink commits table by table, so a crash can leave one transaction applied to A but
     * not B. Resuming earlier than necessary is always safe: filterAndCommit turns every
     * already-applied commit into a per-table no-op, costing only some re-read WAL. Resuming later
     * than necessary silently drops what B missed. Hence min, not max. Relations not tracked by
     * the registry are skipped.
     */
    public long highestCommittedLsn(Collection<Long> relids) {
        long lowest = Long.MAX_VALUE;
        boolean any = false;
        for (long relid : relids) {
            if (!registry.tracked(relid)) {
                continue;
            }
            FileStoreTable t = registry.table(relid);
            SnapshotManager sm = t.snapshotManager();
            Long latest = sm.latestSnapshotId();
            Long earliest = sm.earliestSnapshotId();
            any = true;
            long tableHighest = 0;
            if (latest != null && earliest != null) for (long id = latest; id >= earliest; id--) {
                if (!sm.snapshotExists(id)) {
                    continue;
                }
                Snapshot s = sm.snapshot(id);
                if (config.commitUser().equals(s.commitUser())) {
                    tableHighest = s.commitIdentifier();
                    break;
                }
            }
            lowest = Math.min(lowest, tableHighest);
        }
        return any ? lowest : 0;
    }
}
