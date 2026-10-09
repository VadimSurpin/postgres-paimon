// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;

/**
 * Commits decoded transactions to Paimon, once per touched table, with
 * {@code commitIdentifier = commitLsn}.
 *
 * <p>Effectively-once rests on three measured facts (Task 1 spike 3, Paimon 1.4.1):
 * <ul>
 *   <li>only {@link TableCommitImpl#filterAndCommit} consults the committed-identifier set; the
 *       plain {@code commit(identifier, messages)} would write a second snapshot with duplicate
 *       data and changelog on replay, so it is never called here;</li>
 *   <li>that set is scoped to the commit user, so {@code config.commitUser()} must be stable
 *       across restarts (it is derived from the slot name, see {@link WriterConfig});</li>
 *   <li>the table is in dynamic bucket mode (no {@code bucket} option, deliberately), so every
 *       row is written with {@code write(row, 0)}, giving the {@code bucket-0/} layout main uses.</li>
 * </ul>
 *
 * <p>A table or writer fetched before a DDL message for that relation is stale. Each cached
 * entry remembers the {@link FileStoreTable} instance it was built from; {@link SchemaRegistry}
 * drops its own instance on every create/alter/forget, so a changed identity here means the
 * schema changed and the entry is rebuilt. No explicit invalidation call is required, but
 * {@link #invalidate(long)} exists for callers that want it.
 *
 * <p>An exception from {@link #flush} is fatal to the writer. TxnBatcher has already discarded
 * the batch; the process must exit and resume from the last confirmed LSN.
 */
public final class PaimonSink implements TxnBatcher.Flusher, AutoCloseable {

    private static final Logger LOG = Logger.getLogger(PaimonSink.class.getName());

    private record Entry(FileStoreTable table, TableWriteImpl<?> write, TableCommitImpl commit)
            implements AutoCloseable {
        @Override
        public void close() throws Exception {
            Exception first = null;
            try {
                write.close();
            } catch (Exception e) {
                first = e;
            }
            try {
                commit.close();
            } catch (Exception e) {
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
            if (first != null) {
                throw first;
            }
        }
    }

    private final SchemaRegistry registry;
    private final WriterConfig config;
    private final Map<Long, Entry> entries = new LinkedHashMap<>();
    private final Map<Long, RelationMeta> relations = new LinkedHashMap<>();

    public PaimonSink(SchemaRegistry registry, WriterConfig config) {
        this.registry = registry;
        this.config = config;
    }

    /** Supplies the PostgreSQL column order RowBuilder needs; replaces any earlier one. */
    public void setRelation(long relid, RelationMeta meta) {
        relations.put(relid, meta);
    }

    @Override
    public void flush(List<SinkRecord> records, long commitLsn, long endLsn) throws Exception {
        if (records.isEmpty()) {
            return;     // a DDL-only transaction: nothing to commit, no empty snapshot
        }
        Map<Long, List<SinkRecord>> byRelid = new LinkedHashMap<>();
        for (SinkRecord r : records) {
            byRelid.computeIfAbsent(r.relid(), k -> new java.util.ArrayList<>()).add(r);
        }
        for (Map.Entry<Long, List<SinkRecord>> g : byRelid.entrySet()) {
            long relid = g.getKey();
            RelationMeta meta = relations.get(relid);
            if (meta == null) {
                throw new IllegalStateException("no RelationMeta for relation " + relid);
            }
            Entry e = entry(relid);
            try {
                RowBuilder rb = registry.rowBuilder(relid, meta);
                for (SinkRecord r : g.getValue()) {
                    e.write().write(rb.build(r), 0);
                }
                List<CommitMessage> msgs = e.write().prepareCommit(false, commitLsn);
                int applied = e.commit().filterAndCommit(Map.of(commitLsn, msgs));
                if (applied < 1) {
                    LOG.info("commit identifier " + commitLsn + " for relation " + relid
                        + " was already committed; replay skipped (applied " + applied
                        + " of 1)");
                    // The writer already holds this transaction's files in its LSM tree but the
                    // table never saw them; reusing it makes a later compaction delete files the
                    // table does not know ("File deletion conflicts detected"). Dropping the
                    // writer is what fixes that.
                    //
                    // Deliberately NOT calling commit.abort(msgs) to delete the orphan files:
                    // in Paimon 1.4.1 abort deletes each message's newFiles, changelog files and
                    // compactAfter without excluding files also in compactBefore. A compaction
                    // finishing inside prepareCommit as an upgrade in place (same file name,
                    // higher level) puts an already-committed file in compactAfter, and abort
                    // would delete committed data. Unproven, but bounded orphan waste beats a
                    // possible silent loss.
                    dropEntry(relid);
                }
            } catch (Exception | Error ex) {
                dropEntry(relid);   // never reuse a writer holding half-written buffers
                throw ex;
            }
        }
    }

    /**
     * Empties the table with an empty overwrite commit under our own commitUser and
     * {@code commitIdentifier = commitLsn}, through the same {@code filterAndCommit} path as data,
     * so a replayed truncate is filtered and every snapshot this writer creates carries our user.
     *
     * <p>{@code TableCommitImpl.truncateTable()} is deliberately not used: it hard-codes the
     * identifier {@code Long.MAX_VALUE}, which under our user would make filterAndCommit drop every
     * later commit and OffsetStore report an impossible offset.
     */
    public void truncate(long relid, long commitLsn) throws Exception {
        // The cached writer holds pre-truncate LSM files the overwrite is about to delete; reusing
        // it makes the next compaction fail with "File deletion conflicts detected".
        dropEntry(relid);
        FileStoreTable t = registry.table(relid);
        try (TableCommitImpl c = t.newCommit(config.commitUser()).withOverwrite(Map.of())) {
            int applied = c.filterAndCommit(Map.of(commitLsn, List.<CommitMessage>of()));
            if (applied < 1) {
                LOG.info("truncate of relation " + relid + " at " + commitLsn
                    + " was already committed; replay skipped");
            }
        } finally {
            dropEntry(relid);
        }
    }

    /** Closes any cached writer/committer for the relation; the next flush rebuilds them. */
    public void invalidate(long relid) throws Exception {
        dropEntry(relid);
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        for (Entry e : entries.values()) {
            try {
                e.close();
            } catch (Exception ex) {
                if (first == null) {
                    first = ex;
                } else {
                    first.addSuppressed(ex);
                }
            }
        }
        entries.clear();
        if (first != null) {
            throw first;
        }
    }

    private Entry entry(long relid) throws Exception {
        FileStoreTable t = registry.table(relid);
        Entry e = entries.get(relid);
        if (e != null && e.table() == t) {
            return e;
        }
        if (e != null) {
            dropEntry(relid);   // schema changed under us
        }
        Entry fresh = new Entry(t, t.newWrite(config.commitUser()), t.newCommit(config.commitUser()));
        entries.put(relid, fresh);
        return fresh;
    }

    private void dropEntry(long relid) throws Exception {
        Entry e = entries.remove(relid);
        if (e != null) {
            e.close();
        }
    }
}
