// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.types.RowKind;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Turns decoded events into the records of the design spec's write-rules table.
 *
 * The TOAST substitution rests on ExtractReplicaIdentity() in
 * src/backend/access/heap/heapam.c calling toast_flatten_tuple() before the old
 * tuple reaches WAL, in both of its branches.  Under REPLICA IDENTITY FULL the
 * old tuple therefore carries every column in full, TOASTed ones included, and an
 * UPDATE that did not touch a column has that column's current value sitting in
 * the old tuple: unchanged means old equals new.
 *
 * Ownership: every value list returned here is built fresh, handed to the
 * PartialRecord and never retained or mutated by this class.  Lists may contain
 * null elements (SQL NULL), so List.copyOf must not be used on them.
 */
public final class ToastResolver {

    public List<PartialRecord> resolve(PgEvent event, RelationMeta meta) {
        if (event instanceof PgEvent.Insert i) {
            return List.of(new PartialRecord(i.relid(), RowKind.INSERT,
                                             plain(i.newTuple(), null, meta)));
        }
        if (event instanceof PgEvent.Delete d) {
            requireFullOld(d.oldTuple(), d.oldIsKeyOnly(), meta);
            return List.of(new PartialRecord(d.relid(), RowKind.DELETE,
                                             plain(d.oldTuple(), null, meta)));
        }
        if (event instanceof PgEvent.Update u) {
            requireFullOld(u.oldTuple(), u.oldIsKeyOnly(), meta);
            // Separate lists per record: the old values are never shared with the new.
            List<String> oldValues = u.oldTuple() == null
                ? null : plain(u.oldTuple(), null, meta);
            List<String> newValues = plain(u.newTuple(), u.oldTuple(), meta);

            if (oldValues == null) {
                // No old tuple was logged (a no-op UPDATE under RI FULL); an UNCHANGED
                // column would already have thrown in plain().
                return List.of(new PartialRecord(u.relid(), RowKind.UPDATE_AFTER, newValues));
            }
            if (keyChanged(oldValues, newValues, meta)) {
                return List.of(
                    new PartialRecord(u.relid(), RowKind.DELETE, oldValues),
                    new PartialRecord(u.relid(), RowKind.INSERT, newValues));
            }
            return List.of(
                new PartialRecord(u.relid(), RowKind.UPDATE_BEFORE, oldValues),
                new PartialRecord(u.relid(), RowKind.UPDATE_AFTER, newValues));
        }
        return List.of();
    }

    /** A key-only ('K') old tuple means the table is not at REPLICA IDENTITY FULL. */
    private static void requireFullOld(TupleData old, boolean keyOnly, RelationMeta meta) {
        if (old != null && keyOnly) {
            throw new IllegalStateException(
                "received a key-only old tuple for " + meta.namespace() + "." + meta.relName()
                + ": its non-key columns are absent, so no faithful before-image exists;"
                + " the table must be at REPLICA IDENTITY FULL");
        }
    }

    /**
     * Flattens a tuple to plain text values.  A null element means SQL NULL; ""
     * means the empty string, which is a different thing.  An UNCHANGED column is
     * filled from {@code fallback}, and is a hard error when there is none.
     */
    private static List<String> plain(TupleData tuple, TupleData fallback, RelationMeta meta) {
        List<ColumnValue> cols = tuple.columns();
        if (fallback != null && fallback.columns().size() != cols.size()) {
            throw new IllegalStateException("old and new tuples of " + meta.namespace()
                + "." + meta.relName() + " differ in width: " + fallback.columns().size()
                + " vs " + cols.size());
        }
        List<String> out = new ArrayList<>(cols.size());
        for (int i = 0; i < cols.size(); i++) {
            ColumnValue c = cols.get(i);
            switch (c.state()) {
                case NULL -> out.add(null);
                case TEXT -> out.add(c.text());
                case UNCHANGED -> {
                    ColumnValue from = fallback == null ? null : fallback.columns().get(i);
                    if (from == null || from.state() == ColState.UNCHANGED) {
                        throw new IllegalStateException(
                            "column '" + columnName(meta, i) + "' of " + meta.namespace()
                            + "." + meta.relName() + " arrived unchanged (an unreassembled"
                            + " TOAST pointer) and no old tuple carries its value;"
                            + " the table must be at REPLICA IDENTITY FULL");
                    }
                    out.add(from.state() == ColState.NULL ? null : from.text());
                }
            }
        }
        // Not List.copyOf: null elements are SQL NULLs.  Caller owns the list from here.
        return out;
    }

    private static boolean keyChanged(List<String> oldValues, List<String> newValues,
                                      RelationMeta meta) {
        List<Boolean> isKey = meta.isKey();
        for (int i = 0; i < isKey.size() && i < oldValues.size(); i++) {
            if (!isKey.get(i)) {
                continue;
            }
            if (!Objects.equals(oldValues.get(i), newValues.get(i))) {
                return true;
            }
        }
        return false;
    }

    private static String columnName(RelationMeta meta, int index) {
        List<String> names = meta.columnNames();
        return index < names.size() ? names.get(index) : "#" + index;
    }
}
