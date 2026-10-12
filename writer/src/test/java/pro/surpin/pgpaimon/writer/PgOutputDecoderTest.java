// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import pro.surpin.pgpaimon.writer.decoder.PgOutputDecoder;
import pro.surpin.pgpaimon.writer.decoder.PgOutputMessageDecoder;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.ColState;
import pro.surpin.pgpaimon.writer.dto.ColumnValue;
import pro.surpin.pgpaimon.writer.dto.PgEvent;



class PgOutputDecoderTest {

  static PgOutputDecoder decoder() {
    try (var context = new AnnotationConfigApplicationContext()) {
      context.scan(PgOutputMessageDecoder.class.getPackageName());
      context.refresh();
      return new PgOutputDecoder(
          List.copyOf(context.getBeansOfType(PgOutputMessageDecoder.class).values()));
    }
  }


  /** Builds a pgoutput message body; mirrors src/backend/replication/logical/proto.c. */
  static class Msg {
    final ByteArrayOutputStream bo = new ByteArrayOutputStream();
    final DataOutputStream d = new DataOutputStream(bo);

    Msg b(
        int v
    ) {
      try {
        d.writeByte(v);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    Msg i16(
        int v
    ) {
      try {
        d.writeShort(v);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    Msg i32(
        int v
    ) {
      try {
        d.writeInt(v);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    Msg i64(
        long v
    ) {
      try {
        d.writeLong(v);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    Msg cstr(
        String s
    ) {
      try {
        d.write(s.getBytes(StandardCharsets.UTF_8));
        d.writeByte(0);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    Msg text(
        String s
    ) {
      byte[] x = s.getBytes(StandardCharsets.UTF_8);
      b('t').i32(x.length);
      try {
        d.write(x);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    ByteBuffer buf() {
      return ByteBuffer.wrap(bo.toByteArray());
    }
  }

  private static ByteBuffer relationOf(
      long relid,
      String... cols
  ) {
    return relationIn(relid, "public", "t", cols);
  }

  private static ByteBuffer relationIn(
      long relid,
      String nsp,
      String rel,
      String... cols
  ) {
    Msg m = new Msg().b('R').i32((int) relid).cstr(nsp).cstr(rel).b('d').i16(cols.length);
    for (int i = 0; i < cols.length; i++) {
      m.b(i == 0 ? 1 : 0).cstr(cols[i]).i32(23).i32(-1);
    }
    return m.buf();
  }

  @Test
  void decodesBeginAndCommit() {
    PgOutputDecoder dec = decoder();
    PgEvent.Begin b =
        (PgEvent.Begin) dec.decode(new Msg().b('B').i64(0x16B374D8L).i64(700000L).i32(4711).buf());
    assertEquals(0x16B374D8L, b.finalLsn());
    assertEquals(700000L, b.commitTimestamp());
    assertEquals(4711L, b.xid());

    PgEvent.Commit c = (PgEvent.Commit) dec
        .decode(new Msg().b('C').b(0).i64(0x16B374D8L).i64(0x16B374F0L).i64(700000L).buf());
    assertEquals(0x16B374D8L, c.commitLsn());
    assertEquals(0x16B374F0L, c.endLsn());
    assertEquals(700000L, c.commitTimestamp());
  }

  @Test
  void decodesRelationAndCachesIt() {
    PgOutputDecoder dec = decoder();
    PgEvent.Relation r = (PgEvent.Relation) dec.decode(relationOf(16412, "id", "v"));
    assertEquals(16412L, r.meta().relid());
    assertEquals("public", r.meta().namespace());
    assertEquals(List.of("id", "v"), r.meta().columnNames());
    assertEquals(List.of(true, false), r.meta().isKey());
    assertSame(r.meta(), dec.relation(16412L));
  }

  @Test
  void decodesInsert() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id", "v"));
    PgEvent.Insert i = (PgEvent.Insert) dec
        .decode(new Msg().b('I').i32(16412).b('N').i16(2).text("1").text("hello").buf());
    assertEquals(16412L, i.relid());
    assertEquals("1", i.newTuple().columns().get(0).text());
    assertEquals("hello", i.newTuple().columns().get(1).text());
  }

  /** Review Focus 1: 'n' is NULL, 't' with length 0 is the empty string. */
  @Test
  void nullAndEmptyStringAreDistinct() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id", "v"));
    PgEvent.Insert i = (PgEvent.Insert) dec
        .decode(new Msg().b('I').i32(16412).b('N').i16(2).text("").b('n').buf());
    ColumnValue empty = i.newTuple().columns().get(0);
    ColumnValue nul = i.newTuple().columns().get(1);
    assertEquals(ColState.TEXT, empty.state());
    assertEquals("", empty.text());
    assertEquals(ColState.NULL, nul.state());
    assertNull(nul.text());
    assertEquals(ColumnValue.of(""), empty);
    assertEquals(ColumnValue.ofNull(), nul);
    assertNotEquals(empty, nul);
  }

  @Test
  void decodesUpdateWithFullOldTupleAndAnUnchangedColumn() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id", "n", "body"));
    PgEvent.Update u = (PgEvent.Update) dec.decode(new Msg().b('U').i32(16412).b('O').i16(3)
        .text("1").text("1").text("big-old-value").b('N').i16(3).text("1").text("99").b('u').buf());
    assertFalse(u.oldIsKeyOnly());
    assertEquals("big-old-value", u.oldTuple().columns().get(2).text());
    assertEquals(ColState.UNCHANGED, u.newTuple().columns().get(2).state());
  }

  @Test
  void decodesUpdateWithKeyOnlyOldTuple() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id", "v"));
    PgEvent.Update u = (PgEvent.Update) dec.decode(new Msg().b('U').i32(16412).b('K').i16(2)
        .text("1").b('n').b('N').i16(2).text("2").text("x").buf());
    assertTrue(u.oldIsKeyOnly());
    assertEquals("1", u.oldTuple().columns().get(0).text());
    assertEquals(ColState.NULL, u.oldTuple().columns().get(1).state());
    assertEquals("2", u.newTuple().columns().get(0).text());
  }

  @Test
  void decodesUpdateWithNoOldTuple() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id", "v"));
    PgEvent.Update u = (PgEvent.Update) dec
        .decode(new Msg().b('U').i32(16412).b('N').i16(2).text("1").text("x").buf());
    assertNull(u.oldTuple());
  }

  @Test
  void decodesDeleteAndTruncate() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id", "v"));
    PgEvent.Delete d = (PgEvent.Delete) dec
        .decode(new Msg().b('D').i32(16412).b('K').i16(2).text("7").b('n').buf());
    assertEquals("7", d.oldTuple().columns().get(0).text());
    assertTrue(d.oldIsKeyOnly());

    PgEvent.Truncate t =
        (PgEvent.Truncate) dec.decode(new Msg().b('T').i32(2).b(0).i32(16412).i32(16500).buf());
    assertEquals(List.of(16412L, 16500L), t.relids());
  }

  @Test
  void decodesLogicalMessage() {
    PgOutputDecoder dec = decoder();
    byte[] body = "{\"v\":1,\"op\":\"create\"}".getBytes(StandardCharsets.UTF_8);
    Msg m = new Msg().b('M').b(1).i64(0x100L).cstr("paimon").i32(body.length);
    try {
      m.d.write(body);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    PgEvent.Message msg = (PgEvent.Message) dec.decode(m.buf());
    assertTrue(msg.transactional());
    assertEquals("paimon", msg.prefix());
    assertEquals(0x100L, msg.lsn());
    assertEquals("{\"v\":1,\"op\":\"create\"}", new String(msg.content(), StandardCharsets.UTF_8));
  }

  @Test
  void binaryColumnStateIsRejected() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id"));
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> dec.decode(new Msg().b('I').i32(16412).b('N').i16(1).b('b').i32(1).b(0x41).buf()));
    assertTrue(e.getMessage().contains("binary"));
  }

  @Test
  void unknownMessageTypeIsRejectedWithTheByte() {
    PgOutputDecoder dec = decoder();
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> dec.decode(new Msg().b('Z').buf()));
    assertTrue(e.getMessage().contains("Z"));
  }

  @Test
  void streamMessagesAreRejectedBecauseStreamingIsOff() {
    PgOutputDecoder dec = decoder();
    for (char kind : new char[] {'S', 'E', 'c', 'A'}) {
      assertThrows(IllegalStateException.class, () -> dec.decode(new Msg().b(kind).buf()),
          "stream message " + kind + " must be refused");
    }
  }

  @Test
  void oidAbove2ToThe31IsNotNegative() {
    long big = 4_000_000_000L;
    PgOutputDecoder dec = decoder();
    PgEvent.Relation r = (PgEvent.Relation) dec.decode(relationOf(big, "id"));
    assertEquals(big, r.meta().relid());
    assertSame(r.meta(), dec.relation(big));
    PgEvent.Insert i =
        (PgEvent.Insert) dec.decode(new Msg().b('I').i32((int) big).b('N').i16(1).text("1").buf());
    assertEquals(big, i.relid());
    PgEvent.Truncate t =
        (PgEvent.Truncate) dec.decode(new Msg().b('T').i32(1).b(3).i32((int) big).buf());
    assertEquals(List.of(big), t.relids());
    assertTrue(t.cascade());
    assertTrue(t.restartIdentity());
    PgEvent.TypeMsg y = (PgEvent.TypeMsg) dec
        .decode(new Msg().b('Y').i32((int) big).cstr("public").cstr("mood").buf());
    assertEquals(big, y.typeOid());
    assertEquals("public", y.namespace());
    assertEquals("mood", y.name());
  }

  @Test
  void nonAsciiIdentifiersDecodeAsUtf8() {
    PgOutputDecoder dec = decoder();
    PgEvent.Relation r =
        (PgEvent.Relation) dec.decode(relationIn(16500, "schéma", "表格", "имя", "café"));
    assertEquals("schéma", r.meta().namespace());
    assertEquals("表格", r.meta().relName());
    assertEquals(List.of("имя", "café"), r.meta().columnNames());
    PgEvent.Insert i = (PgEvent.Insert) dec
        .decode(new Msg().b('I').i32(16500).b('N').i16(2).text("ü€").text("x").buf());
    assertEquals("ü€", i.newTuple().columns().get(0).text());
  }

  /** pgoutput sends an empty namespace for pg_catalog (logicalrep_write_namespace). */
  @Test
  void emptyNamespaceMeansPgCatalog() {
    PgOutputDecoder dec = decoder();
    PgEvent.Relation r = (PgEvent.Relation) dec.decode(relationIn(1259, "", "pg_class", "oid"));
    assertEquals("pg_catalog", r.meta().namespace());
  }

  @Test
  void decodesOrigin() {
    PgOutputDecoder dec = decoder();
    PgEvent.Origin o =
        (PgEvent.Origin) dec.decode(new Msg().b('O').i64(0x77L).cstr("origin1").buf());
    assertEquals(0x77L, o.commitLsn());
    assertEquals("origin1", o.name());
  }

  @Test
  void unknownColumnStateIsRejected() {
    PgOutputDecoder dec = decoder();
    assertThrows(IllegalStateException.class,
        () -> dec.decode(new Msg().b('I').i32(1).b('N').i16(1).b('x').buf()));
  }

  /**
   * Cross-check against bytes recorded from a real server (spike 1, the U message for id=1). Layout
   * is in docs/superpowers/notes/2026-10-09-spike-findings.md; the 128000-char value is
   * string_agg(md5(g::text), '') over generate_series(1,4000), regenerated here rather than stored.
   */
  @Test
  void decodesRecordedServerUpdateWithUnchangedToastColumn() throws Exception {
    StringBuilder sb = new StringBuilder();
    java.security.MessageDigest md5 = java.security.MessageDigest.getInstance("MD5");
    for (int g = 1; g <= 4000; g++) {
      for (byte x : md5.digest(Integer.toString(g).getBytes(StandardCharsets.US_ASCII))) {
        sb.append(Character.forDigit((x >> 4) & 15, 16)).append(Character.forDigit(x & 15, 16));
      }
    }
    byte[] value = sb.toString().getBytes(StandardCharsets.US_ASCII);
    assertEquals(128000, value.length);

    ByteArrayOutputStream bo = new ByteArrayOutputStream();
    byte[] head = {0x55, 0x00, 0x00, 0x40, 0x0b, 0x4f, 0x00, 0x03, 0x74, 0x00, 0x00, 0x00, 0x01,
        0x31, 0x74, 0x00, 0x00, 0x00, 0x01, 0x31, 0x74, 0x00, 0x01, (byte) 0xf4, 0x00};
    byte[] tail = {0x4e, 0x00, 0x03, 0x74, 0x00, 0x00, 0x00, 0x01, 0x31, 0x74, 0x00, 0x00, 0x00,
        0x02, 0x39, 0x39, 0x75};
    bo.write(head);
    bo.write(value);
    bo.write(tail);

    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16395, "id", "n", "body"));
    ByteBuffer wire = ByteBuffer.wrap(bo.toByteArray());
    PgEvent.Update u = (PgEvent.Update) dec.decode(wire);
    assertFalse(wire.hasRemaining(), "decoder must consume exactly one message");
    assertEquals(16395L, u.relid());
    assertFalse(u.oldIsKeyOnly());
    ColumnValue oldBody = u.oldTuple().columns().get(2);
    assertEquals(ColState.TEXT, oldBody.state());
    assertEquals(128000, oldBody.text().length());
    assertEquals(sb.toString(), oldBody.text());
    assertEquals("99", u.newTuple().columns().get(1).text());
    assertEquals(ColState.UNCHANGED, u.newTuple().columns().get(2).state());
  }

  @Test
  void deleteWithFullOldTupleIsNotKeyOnly() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id", "v"));
    PgEvent.Delete d = (PgEvent.Delete) dec
        .decode(new Msg().b('D').i32(16412).b('O').i16(2).text("7").text("x").buf());
    assertFalse(d.oldIsKeyOnly());
    assertEquals("x", d.oldTuple().columns().get(1).text());
  }

  @Test
  void truncateFlagBitsAreIndependent() {
    PgOutputDecoder dec = decoder();
    PgEvent.Truncate cascade =
        (PgEvent.Truncate) dec.decode(new Msg().b('T').i32(1).b(1).i32(5).buf());
    assertTrue(cascade.cascade());
    assertFalse(cascade.restartIdentity());
    PgEvent.Truncate restart =
        (PgEvent.Truncate) dec.decode(new Msg().b('T').i32(1).b(2).i32(5).buf());
    assertFalse(restart.cascade());
    assertTrue(restart.restartIdentity());
  }

  @Test
  void badMarkersAreRejected() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id"));
    assertThrows(IllegalStateException.class,
        () -> dec.decode(new Msg().b('I').i32(16412).b('X').i16(1).text("1").buf()));
    assertThrows(IllegalStateException.class,
        () -> dec.decode(new Msg().b('U').i32(16412).b('X').i16(1).text("1").buf()));
    assertThrows(IllegalStateException.class,
        () -> dec.decode(new Msg().b('D').i32(16412).b('N').i16(1).text("1").buf()));
    assertThrows(java.nio.BufferUnderflowException.class,
        () -> dec.decode(new Msg().b('D').i32(16412).buf()));
  }

  @Test
  void negativeAndOversizedLengthsFailCleanly() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id"));
    for (int len : new int[] {-1, 1000, Integer.MAX_VALUE}) {
      assertThrows(IllegalStateException.class,
          () -> dec.decode(new Msg().b('I').i32(16412).b('N').i16(1).b('t').i32(len).b(0x41).buf()),
          "text len " + len);
      assertThrows(IllegalStateException.class,
          () -> dec.decode(new Msg().b('M').b(1).i64(0).cstr("p").i32(len).b(1).buf()),
          "message len " + len);
    }
    for (int n : new int[] {-1, 2, Integer.MAX_VALUE}) {
      assertThrows(IllegalStateException.class,
          () -> dec.decode(new Msg().b('T').i32(n).b(0).i32(1).buf()), "truncate n " + n);
    }
  }

  @Test
  void tupleWidthMustMatchCachedRelation() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id", "v"));
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> dec.decode(new Msg().b('I').i32(16412).b('N').i16(1).text("1").buf()));
    assertTrue(e.getMessage().contains("16412"));
    assertThrows(IllegalStateException.class,
        () -> dec.decode(new Msg().b('D').i32(16412).b('K').i16(3).text("1").b('n').b('n').buf()));
    // A relation with no cached R is not this decoder's concern.
    PgEvent.Insert i =
        (PgEvent.Insert) dec.decode(new Msg().b('I').i32(99).b('N').i16(1).text("1").buf());
    assertEquals(1, i.newTuple().columns().size());
    // A later R replaces the cached width.
    dec.decode(relationOf(16412, "id"));
    assertEquals(1,
        ((PgEvent.Insert) dec.decode(new Msg().b('I').i32(16412).b('N').i16(1).text("1").buf()))
            .newTuple().columns().size());
  }

  @Test
  void malformedUtf8IsRejectedNotReplaced() {
    PgOutputDecoder dec = decoder();
    dec.decode(relationOf(16412, "id"));
    // 0xC3 0x28 is an invalid two-byte sequence.
    assertThrows(IllegalStateException.class, () -> dec
        .decode(new Msg().b('I').i32(16412).b('N').i16(1).b('t').i32(2).b(0xC3).b(0x28).buf()));
    assertThrows(IllegalStateException.class,
        () -> dec.decode(new Msg().b('R').i32(1).b(0xC3).b(0x28).b(0).buf()));
  }
}
