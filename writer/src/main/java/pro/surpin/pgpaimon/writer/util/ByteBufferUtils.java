// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.util;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;

public final class ByteBufferUtils {

  private ByteBufferUtils() {}

  /** Reads one unsigned byte as a character and advances the buffer position. */
  public static char readUnsignedChar(
      @NotNull ByteBuffer buffer
  ) {
    return (char) Byte.toUnsignedInt(buffer.get());
  }
}
