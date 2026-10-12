// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.PgEvent;

@Component
public final class DeleteMessageDecoder implements PgOutputMessageDecoder {

  @Override
  public @NotNull PgOutputMessageType type() {
    return PgOutputMessageType.DELETE;
  }

  @Override
  public @NotNull PgEvent decode(
      @NotNull ByteBuffer buf,
      @NotNull PgOutputDecoder context
  ) {
    long relid = Integer.toUnsignedLong(buf.getInt());
    byte marker = buf.get();
    if (marker != 'O' && marker != 'K') {
      throw new IllegalStateException(
          "delete without an old tuple marker: got '" + (char) (marker & 0xFF) + "'");
    }
    return new PgEvent.Delete(relid, context.tuple(buf, relid), marker == 'K');
  }
}
