// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

/** Message codes of the pgoutput protocol. */
public enum PgOutputMessageType {
  BEGIN('B'),
  COMMIT('C'),
  RELATION('R'),
  INSERT('I'),
  UPDATE('U'),
  DELETE('D'),
  TRUNCATE('T'),
  MESSAGE('M'),
  ORIGIN('O'),
  TYPE('Y'),
  STREAM_START('S'),
  STREAM_STOP('E'),
  STREAM_COMMIT('c'),
  STREAM_ABORT('A');

  private static final Map<Character, PgOutputMessageType> BY_CODE = Arrays.stream(values())
      .collect(Collectors.toMap(PgOutputMessageType::code, Function.identity()));

  private final char code;

  PgOutputMessageType(
      char code
  ) {
    this.code = code;
  }

  public char code() {
    return code;
  }

  public static @NotNull PgOutputMessageType fromCode(
      char code
  ) {
    PgOutputMessageType type = BY_CODE.get(code);
    if (type == null) {
      throw new IllegalStateException("unknown pgoutput message type '" + code + "'");
    }
    return type;
  }
}
