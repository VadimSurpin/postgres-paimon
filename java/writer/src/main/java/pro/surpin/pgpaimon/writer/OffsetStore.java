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
 * <p>Snapshots by other users (a truncate goes through a batch committer with its own user) are
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

    /** 0 when nothing has been committed. Relations not tracked by the registry contribute nothing. */
    public long highestCommittedLsn(Collection<Long> relids) {
        long highest = 0;
        for (long relid : relids) {
            if (!registry.tracked(relid)) {
                continue;
            }
            FileStoreTable t = registry.table(relid);
            SnapshotManager sm = t.snapshotManager();
            Long latest = sm.latestSnapshotId();
            Long earliest = sm.earliestSnapshotId();
            if (latest == null || earliest == null) {
                continue;
            }
            for (long id = latest; id >= earliest; id--) {
                if (!sm.snapshotExists(id)) {
                    continue;
                }
                Snapshot s = sm.snapshot(id);
                if (config.commitUser().equals(s.commitUser())) {
                    highest = Math.max(highest, s.commitIdentifier());
                    break;
                }
            }
        }
        return highest;
    }
}
