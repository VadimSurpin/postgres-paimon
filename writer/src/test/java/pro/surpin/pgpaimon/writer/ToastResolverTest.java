// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.ColumnValue;
import pro.surpin.pgpaimon.writer.dto.PartialRecord;
import pro.surpin.pgpaimon.writer.dto.PgEvent;
import pro.surpin.pgpaimon.writer.dto.RelationMeta;
import pro.surpin.pgpaimon.writer.dto.TupleData;



class ToastResolverTest {

  static final RelationMeta META = new RelationMeta(16412, "public", "t",
      List.of("id", "n", "body"), List.of(true, false, false));

  static TupleData t(
      ColumnValue... c
  ) {
    return new TupleData(List.of(c));
  }

  static ColumnValue v(
      String s
  ) {
    return ColumnValue.of(s);
  }

  private final ToastResolver r = new ToastResolver();

  @Test
  void insertProducesOneInsertRecord() {
    List<PartialRecord> out =
        r.resolve(new PgEvent.Insert(16412, t(v("1"), v("2"), v("big"))), META);
    assertEquals(1, out.size());
    assertEquals(RowKind.INSERT, out.get(0).kind());
    assertEquals(16412, out.get(0).relid());
    assertEquals(List.of("1", "2", "big"), out.get(0).values());
  }

  @Test
  void updateWithUnchangedKeyProducesBeforeAndAfter() {
    List<PartialRecord> out = r.resolve(new PgEvent.Update(16412, t(v("1"), v("2"), v("big")),
        false, t(v("1"), v("99"), ColumnValue.unchanged())), META);
    assertEquals(2, out.size());
    assertEquals(RowKind.UPDATE_BEFORE, out.get(0).kind());
    assertEquals(List.of("1", "2", "big"), out.get(0).values());
    assertEquals(RowKind.UPDATE_AFTER, out.get(1).kind());
    assertEquals(List.of("1", "99", "big"), out.get(1).values(),
        "the u column must come from the old tuple");
    assertEquals("big", out.get(1).values().get(2));
  }

  @Test
  void updateThatMovesTheKeyBecomesDeleteThenInsert() {
    List<PartialRecord> out = r.resolve(new PgEvent.Update(16412, t(v("1"), v("2"), v("big")),
        false, t(v("5"), v("2"), ColumnValue.unchanged())), META);
    assertEquals(2, out.size());
    assertEquals(RowKind.DELETE, out.get(0).kind());
    assertEquals(List.of("1", "2", "big"), out.get(0).values());
    assertEquals(RowKind.INSERT, out.get(1).kind());
    assertEquals(List.of("5", "2", "big"), out.get(1).values());
  }

  /** An old tuple is present because of id_has_external, but the key did not move. */
  @Test
  void oldTuplePresenceAloneIsNotAKeyChange() {
    List<PartialRecord> out = r.resolve(
        new PgEvent.Update(16412, t(v("1"), v("2"), v("big")), false, t(v("1"), v("2"), v("big"))),
        META);
    assertEquals(2, out.size());
    assertEquals(RowKind.UPDATE_BEFORE, out.get(0).kind());
    assertEquals(RowKind.UPDATE_AFTER, out.get(1).kind());
  }

  @Test
  void deleteProducesOneDeleteRecord() {
    List<PartialRecord> out =
        r.resolve(new PgEvent.Delete(16412, t(v("1"), v("2"), v("big")), false), META);
    assertEquals(1, out.size());
    assertEquals(RowKind.DELETE, out.get(0).kind());
    assertEquals(List.of("1", "2", "big"), out.get(0).values());
  }

  @Test
  void nullsSurviveAsNullsAndEmptyStringsAsEmptyStrings() {
    List<PartialRecord> out =
        r.resolve(new PgEvent.Insert(16412, t(v("1"), ColumnValue.ofNull(), v(""))), META);
    assertNull(out.get(0).values().get(1));
    assertEquals("", out.get(0).values().get(2));
  }

  /** Review Focus 2: a u column with no old tuple is unrecoverable; fail loudly. */
  @Test
  void unchangedColumnWithoutAnOldTupleIsRejected() {
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> r.resolve(
            new PgEvent.Update(16412, null, false, t(v("1"), v("99"), ColumnValue.unchanged())),
            META));
    assertTrue(e.getMessage().contains("REPLICA IDENTITY FULL"),
        "the message must say how to fix it, got: " + e.getMessage());
    assertTrue(e.getMessage().contains("body"), "the message must name the column");
  }

  /** A key-only old tuple cannot fill a u column either. */
  @Test
  void unchangedColumnWithAKeyOnlyOldTupleIsRejected() {
    assertThrows(IllegalStateException.class,
        () -> r.resolve(
            new PgEvent.Update(16412, t(v("1"), ColumnValue.ofNull(), ColumnValue.ofNull()), true,
                t(v("1"), v("99"), ColumnValue.unchanged())),
            META));
  }

  @Test
  void beginCommitRelationAndMessageProduceNoRecords() {
    assertTrue(r.resolve(new PgEvent.Begin(1, 2, 3), META).isEmpty());
    assertTrue(r.resolve(new PgEvent.Commit(1, 2, 3), META).isEmpty());
    assertTrue(r.resolve(new PgEvent.Relation(META), META).isEmpty());
    assertTrue(r.resolve(new PgEvent.Message(true, 1, "paimon", new byte[0]), META).isEmpty());
  }

  // ---- additions beyond the brief ----

  /**
   * Defence in depth: a key-only old tuple must never yield a half-empty -U, even with no u column.
   */
  @Test
  void keyOnlyOldTupleIsRejectedEvenWhenNothingIsUnchanged() {
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> r.resolve(
            new PgEvent.Update(16412, t(v("1"), ColumnValue.ofNull(), ColumnValue.ofNull()), true,
                t(v("1"), v("99"), v("new"))),
            META));
    assertTrue(e.getMessage().contains("REPLICA IDENTITY FULL"), e.getMessage());
  }

  @Test
  void keyOnlyOldTupleOnDeleteIsRejected() {
    assertThrows(IllegalStateException.class,
        () -> r.resolve(
            new PgEvent.Delete(16412, t(v("1"), ColumnValue.ofNull(), ColumnValue.ofNull()), true),
            META));
  }

  /**
   * Unreachable in a correct configuration: under REPLICA IDENTITY FULL, ExtractReplicaIdentity()
   * returns the full old tuple unconditionally (even for a no-op UPDATE, verified on PG 18), and
   * the DDL guardrail (f2495af) stops a tracked table leaving FULL. No old tuple therefore means
   * drift (e.g. REPLICA IDENTITY NOTHING); a lone +U would silently drop the -U, so it must throw.
   */
  @Test
  void updateWithoutOldTupleIsRejectedEvenWhenNothingIsUnchanged() {
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> r.resolve(
        new PgEvent.Update(16412, null, false, t(v("1"), ColumnValue.ofNull(), v(""))), META));
    assertTrue(e.getMessage().contains("relreplident"), e.getMessage());
    assertTrue(e.getMessage().contains("REPLICA IDENTITY FULL"), e.getMessage());
  }

  @Test
  void deleteWithoutOldTupleIsRejected() {
    assertThrows(IllegalStateException.class,
        () -> r.resolve(new PgEvent.Delete(16412, null, false), META));
  }

  /** Nulls and empty strings survive the update path, including when filled from old. */
  @Test
  void updatePreservesNullAndEmptyStringAndFillsEmptyFromOld() {
    List<PartialRecord> out =
        r.resolve(new PgEvent.Update(16412, t(v("1"), ColumnValue.ofNull(), v("")), false,
            t(v("1"), ColumnValue.ofNull(), ColumnValue.unchanged())), META);
    assertEquals(2, out.size());
    for (PartialRecord p : out) {
      assertNull(p.values().get(1));
      assertEquals("", p.values().get(2));
    }
  }

  @Test
  void unchangedKeyColumnIsFilledFromOldSoKeyIsNotMoved() {
    List<PartialRecord> out = r.resolve(new PgEvent.Update(16412, t(v("1"), v("2"), v("big")),
        false, t(ColumnValue.unchanged(), v("3"), v("big"))), META);
    assertEquals(RowKind.UPDATE_BEFORE, out.get(0).kind());
    assertEquals(List.of("1", "3", "big"), out.get(1).values());
  }

  /** Key moving from NULL-free value to a value differing only in emptiness is still a move. */
  @Test
  void keyChangeFromNullToEmptyStringIsAKeyChange() {
    RelationMeta m = new RelationMeta(1, "public", "k", List.of("k", "x"), List.of(true, false));
    List<PartialRecord> out = r.resolve(
        new PgEvent.Update(1, t(ColumnValue.ofNull(), v("a")), false, t(v(""), v("a"))), m);
    assertEquals(RowKind.DELETE, out.get(0).kind());
    assertEquals(RowKind.INSERT, out.get(1).kind());
  }

  @Test
  void unchangedInBothTuplesIsRejected() {
    assertThrows(IllegalStateException.class,
        () -> r.resolve(new PgEvent.Update(16412, t(v("1"), v("2"), ColumnValue.unchanged()), false,
            t(v("1"), v("2"), ColumnValue.unchanged())), META));
  }

  private List<RowKind> kinds(
      List<PartialRecord> out
  ) {
    return out.stream().map(PartialRecord::kind).toList();
  }

  /** The key is the LAST column; "compare column 0" would get both halves wrong. */
  @Test
  void keyColumnChoiceIsHonouredWhenKeyIsNotFirst() {
    RelationMeta m =
        new RelationMeta(2, "public", "k", List.of("a", "b", "id"), List.of(false, false, true));
    assertEquals(List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER),
        kinds(r.resolve(
            new PgEvent.Update(2, t(v("x"), v("y"), v("7")), false, t(v("X"), v("y"), v("7"))),
            m)));
    assertEquals(List.of(RowKind.DELETE, RowKind.INSERT),
        kinds(r.resolve(
            new PgEvent.Update(2, t(v("x"), v("y"), v("7")), false, t(v("x"), v("y"), v("8"))),
            m)));
  }

  /** Only the SECOND key column moves; an "only check the first key column" bug misses it. */
  @Test
  void twoColumnKeyWhereOnlyTheSecondKeyColumnMoves() {
    RelationMeta m =
        new RelationMeta(3, "public", "k2", List.of("a", "b", "c"), List.of(true, true, false));
    assertEquals(List.of(RowKind.DELETE, RowKind.INSERT),
        kinds(r.resolve(
            new PgEvent.Update(3, t(v("1"), v("2"), v("c")), false, t(v("1"), v("3"), v("c"))),
            m)));
    assertEquals(List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER),
        kinds(r.resolve(
            new PgEvent.Update(3, t(v("1"), v("2"), v("c")), false, t(v("1"), v("2"), v("d"))),
            m)));
  }

  /** Real PostgreSQL does not TOAST a NULL, but the branch exists; pin it. */
  @Test
  void unchangedColumnFilledFromAnOldNullStaysNull() {
    List<PartialRecord> out =
        r.resolve(new PgEvent.Update(16412, t(v("1"), v("2"), ColumnValue.ofNull()), false,
            t(v("1"), v("3"), ColumnValue.unchanged())), META);
    assertNull(out.get(1).values().get(2));
  }

  @Test
  void unchangedErrorsPointAtRelreplidentToo() {
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> r.resolve(new PgEvent.Update(16412, t(v("1"), v("2"), ColumnValue.unchanged()), false,
            t(v("1"), v("2"), ColumnValue.unchanged())), META));
    assertTrue(e.getMessage().contains("relreplident"), e.getMessage());
  }

  @Test
  void relationWidthMismatchIsRejectedNotTreatedAsKeyUnchanged() {
    RelationMeta wide = new RelationMeta(16412, "public", "t", List.of("id", "n", "body", "extra"),
        List.of(true, false, false, true));
    assertThrows(IllegalStateException.class,
        () -> r.resolve(
            new PgEvent.Update(16412, t(v("1"), v("2"), v("b")), false, t(v("1"), v("2"), v("b"))),
            wide));
  }
}
