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
        flush0(records, commitLsn, endLsn, true);
    }

    /**
     * As {@link #flush}, returning false if any table's commit was filtered as already
     * committed. A replay may expect that; a caller minting fresh identifiers (the Backfiller)
     * must treat it as an error. Records reaching this entry point come from the initial load,
     * not from the slot, which changes only what an undecodable value is diagnosed as.
     */
    public boolean flushApplied(List<SinkRecord> records, long commitLsn, long endLsn)
            throws Exception {
        return flush0(records, commitLsn, endLsn, false);
    }

    private boolean flush0(List<SinkRecord> records, long commitLsn, long endLsn,
                           boolean fromTheSlot) throws Exception {
        boolean allApplied = true;
        if (records.isEmpty()) {
            return true;    // a DDL-only transaction: nothing to commit, no empty snapshot
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
                    org.apache.paimon.data.GenericRow row;
                    try {
                        row = rb.build(r);
                    } catch (IllegalArgumentException bad) {
                        // A VALUE the decoder cannot turn into this Paimon type. Only this is a
                        // poison record; anything else from RowBuilder (an arity mismatch, say)
                        // is a structural fault and is reported as one.
                        throw undecodable(meta, r, commitLsn, fromTheSlot, bad);
                    } catch (RuntimeException other) {
                        throw new IllegalStateException("cannot build a Paimon row for "
                            + where(meta, r) + ": " + other.getMessage()
                            + ". This is a structural mismatch between the record and the Paimon"
                            + " schema, not a value Paimon cannot represent; it will replay"
                            + " identically on restart.", other);
                    }
                    e.write().write(row, 0);
                }
                List<CommitMessage> msgs = e.write().prepareCommit(false, commitLsn);
                int applied = e.commit().filterAndCommit(Map.of(commitLsn, msgs));
                if (applied < 1) {
                    allApplied = false;
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
        return allApplied;
    }

    /**
     * A value PostgreSQL stores happily that Paimon cannot represent -- {@code numeric
     * 'NaN'} or a {@code numeric} outside {@code DECIMAL(38, 18)} -- is a
     * <em>poison record</em>, not a transient failure, and the distinction is the whole point of
     * this message. No guardrail can prevent one (a {@code numeric} column can always hold
     * NaN), the slot only advances after a successful Paimon commit, and the offending WAL
     * record is in the slot, so every restart replays it and fails identically. Deleting or
     * correcting the row in PostgreSQL does not help. Spec section 8's "transaction replayed on
     * retry" does not cover a replay that can never succeed; there is no quarantine mechanism, by
     * decision, so the message has to carry everything an operator needs at three in the morning:
     * the table, the column, the value, the LSN, and what the only recourse is.
     *
     * <p>A record from the initial load is a different situation -- it came from a COPY, not from
     * the slot, and fixing the row does help -- so it is diagnosed differently.
     */
    private static String where(RelationMeta meta, SinkRecord r) {
        return meta.namespace() + "." + meta.relName()
            + " (" + r.kind() + " at _seq " + SlotConsumer.formatLsn(r.seq()) + ")";
    }

    private IllegalStateException undecodable(RelationMeta meta, SinkRecord r, long commitLsn,
                                              boolean fromTheSlot, RuntimeException cause) {
        String where = where(meta, r);
        if (!fromTheSlot) {
            // Two different causes land here and the remedy differs, so name both. The value may
            // be one Paimon cannot represent; or the Paimon column's type may no longer match the
            // PostgreSQL column's, in which case the row is perfectly good and correcting it
            // cannot help -- which is what the old "correct or delete that row" advice sent
            // operators to do.
            return new IllegalStateException("cannot load " + where + " during the initial"
                + " backfill: " + cause.getMessage()
                + ". This value came from the table itself, not from the replication slot, so"
                + " nothing is stuck in the slot. Two things cause it. Either the value is one"
                + " Paimon cannot represent, and correcting that row and reloading fixes it; or"
                + " the Paimon column's type no longer matches the PostgreSQL column's (schema"
                + " drift: compare the type named above with the PostgreSQL column), and no edit"
                + " to the row can help -- re-register the table instead"
                + " (paimon.remove_table, delete the Paimon table, paimon.add_table).", cause);
        }
        return new IllegalStateException("POISON RECORD in " + where
            + ", transaction committing at " + SlotConsumer.formatLsn(commitLsn) + ": "
            + cause.getMessage()
            + ". THIS IS NOT TRANSIENT and will not clear by itself: the change is already in"
            + " replication slot '" + config.slotName() + "', the slot advances only after a"
            + " successful Paimon commit, so every restart replays this same record and fails"
            + " identically. Deleting or correcting the row in PostgreSQL does NOT help -- the"
            + " offending WAL record stays in the slot. The only recourse today is the"
            + " re-bootstrap procedure: stop the writer, SELECT"
            + " pg_drop_replication_slot('" + config.slotName() + "'); UPDATE"
            + " paimon.table_registry SET backfill_due = true; and start the writer again. That"
            + " reloads every tracked table from a fresh snapshot and loses Paimon-side changelog"
            + " history. See docs/logical-slot-architecture.md, 'A record the writer can never"
            + " apply'.", cause);
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
    public boolean truncate(long relid, long commitLsn) throws Exception {
        boolean applied = true;
        // The cached writer holds pre-truncate LSM files the overwrite is about to delete; reusing
        // it makes the next compaction fail with "File deletion conflicts detected".
        dropEntry(relid);
        FileStoreTable t = registry.table(relid);
        try (TableCommitImpl c = t.newCommit(config.commitUser()).withOverwrite(Map.of())) {
            applied = c.filterAndCommit(Map.of(commitLsn, List.<CommitMessage>of())) >= 1;
            if (!applied) {
                LOG.info("truncate of relation " + relid + " at " + commitLsn
                    + " was already committed; replay skipped");
            }
        } finally {
            dropEntry(relid);
        }
        return applied;
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
