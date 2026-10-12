// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.paimon.types.RowKind;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.ColState;
import pro.surpin.pgpaimon.writer.dto.ColumnValue;
import pro.surpin.pgpaimon.writer.dto.PartialRecord;
import pro.surpin.pgpaimon.writer.dto.PgEvent;
import pro.surpin.pgpaimon.writer.dto.RelationMeta;
import pro.surpin.pgpaimon.writer.dto.TupleData;


/**
 * Turns decoded events into the records of the design spec's write-rules table.
 *
 * <p>
 * The TOAST substitution rests on ExtractReplicaIdentity() in src/backend/access/heap/heapam.c
 * calling toast_flatten_tuple() before the old tuple reaches WAL, in both of its branches. Under
 * REPLICA IDENTITY FULL the old tuple therefore carries every column in full, TOASTed ones
 * included, and an UPDATE that did not touch a column has that column's current value sitting in
 * the old tuple: unchanged means old equals new.
 *
 * <p>
 * Ownership: every value list returned here is built fresh, handed to the PartialRecord and never
 * retained or mutated by this class. Lists may contain null elements (SQL NULL), so List.copyOf
 * must not be used on them.
 */
public final class ToastResolver {

  /** Shared by every "no faithful before-image" failure so they all point at the same place. */
  private static final @NotNull String REMEDY =
      "(its replica identity is not FULL: check pg_class.relreplident, which must be 'f';"
          + " without it the writer cannot produce a faithful before-image;"
          + " the table must be at REPLICA IDENTITY FULL)";

  public @NotNull List<PartialRecord> resolve(
      @NotNull PgEvent event,
      @Nullable RelationMeta meta
  ) {
    if (event instanceof PgEvent.Insert i) {
      return List.of(new PartialRecord(i.relid(), RowKind.INSERT, plain(i.newTuple(), null, meta)));
    }
    if (event instanceof PgEvent.Delete d) {
      requireFullOld(d.oldTuple(), d.oldIsKeyOnly(), null, meta);
      return List.of(new PartialRecord(d.relid(), RowKind.DELETE, plain(d.oldTuple(), null, meta)));
    }
    if (event instanceof PgEvent.Update u) {
      requireFullOld(u.oldTuple(), u.oldIsKeyOnly(), u.newTuple(), meta);
      // Separate lists per record: the old values are never shared with the new.
      List<String> oldValues = plain(u.oldTuple(), null, meta);
      List<String> newValues = plain(u.newTuple(), u.oldTuple(), meta);

      if (keyChanged(oldValues, newValues, meta)) {
        return List.of(new PartialRecord(u.relid(), RowKind.DELETE, oldValues),
            new PartialRecord(u.relid(), RowKind.INSERT, newValues));
      }
      return List.of(new PartialRecord(u.relid(), RowKind.UPDATE_BEFORE, oldValues),
          new PartialRecord(u.relid(), RowKind.UPDATE_AFTER, newValues));
    }
    return List.of();
  }

  /**
   * Every UPDATE and DELETE of a tracked table must carry a full ('O') old tuple. Under REPLICA
   * IDENTITY FULL, ExtractReplicaIdentity() returns the full tuple unconditionally (the key_changed
   * test that can suppress it applies only to the key-only path), so a missing or key-only ('K')
   * old tuple means the table is not FULL. The DDL guardrail from commit f2495af prevents that on a
   * tracked table, so this is unreachable in a correct configuration; it is defence in depth
   * against drift, not paranoia. Do not remove it or fall back to a lone +U.
   */
  private static void requireFullOld(
      @Nullable TupleData old,
      boolean keyOnly,
      @Nullable TupleData fresh,
      @NotNull RelationMeta meta
  ) {
    if (old == null || keyOnly) {
      StringBuilder unchanged = new StringBuilder();
      if (fresh != null) {
        for (int i = 0; i < fresh.columns().size(); i++) {
          if (fresh.columns().get(i).state() == ColState.UNCHANGED) {
            unchanged.append(unchanged.length() == 0 ? "; unchanged column(s): " : ", ")
                .append(columnName(meta, i));
          }
        }
      }
      throw new IllegalStateException("table " + meta.namespace() + "." + meta.relName() + " sent "
          + (old == null ? "no old tuple" : "a key-only old tuple")
          + " for an UPDATE/DELETE: its replica identity is not FULL" + " " + REMEDY + unchanged);
    }
  }

  /**
   * Flattens a tuple to plain text values. A null element means SQL NULL; "" means the empty
   * string, which is a different thing. An UNCHANGED column is filled from {@code fallback}, and is
   * a hard error when there is none.
   */
  private static @NotNull List<@Nullable String> plain(
      @NotNull TupleData tuple,
      @Nullable TupleData fallback,
      @NotNull RelationMeta meta
  ) {
    List<ColumnValue> cols = tuple.columns();
    if (fallback != null && fallback.columns().size() != cols.size()) {
      throw new IllegalStateException(
          "old and new tuples of " + meta.namespace() + "." + meta.relName() + " differ in width: "
              + fallback.columns().size() + " vs " + cols.size());
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
            throw new IllegalStateException("column '" + columnName(meta, i) + "' of "
                + meta.namespace() + "." + meta.relName() + " arrived unchanged (an unreassembled"
                + " TOAST pointer) and no old tuple carries its value;" + " " + REMEDY);
          }
          out.add(from.state() == ColState.NULL ? null : from.text());
        }
      }
    }
    // Not List.copyOf: null elements are SQL NULLs. Caller owns the list from here.
    return out;
  }

  private static boolean keyChanged(
      @NotNull List<@Nullable String> oldValues,
      @NotNull List<@Nullable String> newValues,
      @NotNull RelationMeta meta
  ) {
    List<Boolean> isKey = meta.isKey();
    if (isKey.size() != oldValues.size() || isKey.size() != newValues.size()) {
      // Concluding "key unchanged" from a width mismatch could hide a key move.
      throw new IllegalStateException("relation " + meta.namespace() + "." + meta.relName()
          + " describes " + isKey.size() + " columns but the tuples have " + oldValues.size()
          + " (old) and " + newValues.size() + " (new)");
    }
    for (int i = 0; i < isKey.size(); i++) {
      if (!isKey.get(i)) {
        continue;
      }
      if (!Objects.equals(oldValues.get(i), newValues.get(i))) {
        return true;
      }
    }
    return false;
  }

  private static @NotNull String columnName(
      @NotNull RelationMeta meta,
      int index
  ) {
    List<String> names = meta.columnNames();
    return index < names.size() ? names.get(index) : "#" + index;
  }
}
