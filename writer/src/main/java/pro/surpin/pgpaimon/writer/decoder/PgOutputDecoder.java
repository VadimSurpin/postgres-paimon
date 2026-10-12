// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.ColumnValue;
import pro.surpin.pgpaimon.writer.dto.PgEvent;
import pro.surpin.pgpaimon.writer.dto.RelationMeta;
import pro.surpin.pgpaimon.writer.dto.TupleData;
import pro.surpin.pgpaimon.writer.util.ByteBufferUtils;


/**
 * Decodes pgoutput protocol version 2 messages, one message per {@link ByteBuffer}.
 *
 * <p>
 * Column state bytes are the four from src/include/replication/logicalproto.h: {@code 'n'} NULL,
 * {@code 'u'} UNCHANGED (an unreassembled TOAST pointer), {@code 't'} TEXT, {@code 'b'} BINARY. The
 * slot runs with binary 'false', so {@code 'b'} is a protocol violation here, not a case to guess
 * at. NULL ({@code 'n'}) and the empty string ({@code 't'}, length 0) are different states and stay
 * different.
 *
 * <p>
 * Every OID is read as an unsigned 32-bit value into a {@code long}. The per-event lists (tuple
 * columns, truncate relids) are built fresh per message, handed over, and never retained or mutated
 * here. {@link RelationMeta} is the one thing deliberately retained: it is cached per relid until a
 * later {@code R} message replaces it. Tuple widths are checked against that cached meta.
 *
 * <p>
 * Input is validated defensively: lengths and counts larger than the bytes left in the buffer (each
 * buffer holds exactly one message) are rejected before anything is allocated, and text must be
 * well-formed UTF-8 (malformed bytes mean a desynchronised stream or a connection that is not
 * UTF-8, and are reported rather than replaced). The buffer must be big-endian, which is the
 * {@link ByteBuffer} default.
 *
 * <p>
 * Not thread-safe: it holds the relation cache.
 */
public final class PgOutputDecoder {

  private final @NotNull Map<Long, RelationMeta> relations = new HashMap<>();
  private final @NotNull Map<PgOutputMessageType, PgOutputMessageDecoder> decoders;

  public PgOutputDecoder(
      @NotNull List<PgOutputMessageDecoder> decoders
  ) {
    this.decoders = decoders.stream()
        .collect(Collectors.toMap(PgOutputMessageDecoder::type, Function.identity()));
  }

  /** The last {@code R} message decoded for this relid, or {@code null}. */
  public @Nullable RelationMeta relation(
      long relid
  ) {
    return relations.get(relid);
  }

  /** Consumes exactly one message from {@code buf}. */
  public @NotNull PgEvent decode(
      @NotNull ByteBuffer buf
  ) {
    char kind = ByteBufferUtils.readUnsignedChar(buf);
    PgOutputMessageDecoder decoder = decoders.get(PgOutputMessageType.fromCode(kind));
    if (decoder == null) {
      throw new IllegalStateException("unknown pgoutput message type '" + kind + "'");
    }
    return decoder.decode(buf, this);
  }

  void rememberRelation(
      long relid,
      @NotNull RelationMeta meta
  ) {
    relations.put(relid, meta);
  }

  @NotNull
  TupleData tuple(
      @NotNull ByteBuffer buf,
      long relid
  ) {
    int n = buf.getShort() & 0xFFFF;
    RelationMeta meta = relations.get(relid);
    if (meta != null && n != meta.columnNames().size()) {
      throw new IllegalStateException(
          "tuple for relation " + relid + " has " + n + " columns but the last R message declared "
              + meta.columnNames().size() + " (missed or stale Relation message?)");
    }
    List<ColumnValue> cols = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      char state = ByteBufferUtils.readUnsignedChar(buf);
      switch (state) {
        case 'n' -> cols.add(ColumnValue.ofNull());
        case 'u' -> cols.add(ColumnValue.unchanged());
        case 't' -> {
          int len = buf.getInt();
          requireFits(len, buf, "text column length");
          byte[] v = new byte[len];
          buf.get(v);
          cols.add(ColumnValue.of(utf8(v)));
        }
        case 'b' -> throw new IllegalStateException(
            "binary column value received although the slot uses binary 'false'");
        default -> throw new IllegalStateException("unknown column state '" + state + "'");
      }
    }
    return new TupleData(cols);
  }

  /** Reads a NUL-terminated UTF-8 string, consuming the terminator. */
  static @NotNull String cstring(
      @NotNull ByteBuffer buf
  ) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(32);
    byte b;
    while ((b = buf.get()) != 0) {
      bytes.write(b);
    }
    return utf8(bytes.toByteArray());
  }

  /** Strict UTF-8: malformed input throws instead of becoming U+FFFD. */
  private static @NotNull String utf8(
      byte @NotNull [] bytes
  ) {
    try {
      return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes))
          .toString();
    } catch (CharacterCodingException e) {
      throw new IllegalStateException("malformed UTF-8 in pgoutput message", e);
    }
  }

  /** A length must be non-negative and cannot exceed what is left in the one-message buffer. */
  static void requireFits(
      int len,
      @NotNull ByteBuffer buf,
      @NotNull String what
  ) {
    if (len < 0 || len > buf.remaining()) {
      throw new IllegalStateException(
          what + " " + len + " does not fit in the remaining " + buf.remaining() + " bytes");
    }
  }

  static void expect(
      byte actual,
      char want,
      @NotNull String what
  ) {
    if (actual != (byte) want) {
      throw new IllegalStateException(
          what + ": expected '" + want + "', got '" + (char) (actual & 0xFF) + "'");
    }
  }
}
