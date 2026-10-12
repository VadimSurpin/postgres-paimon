// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.PgEvent;

@Component
public final class TruncateMessageDecoder implements PgOutputMessageDecoder {

  @Override
  public @NotNull PgOutputMessageType type() {
    return PgOutputMessageType.TRUNCATE;
  }

  @Override
  public @NotNull PgEvent decode(
      @NotNull ByteBuffer buf,
      @NotNull PgOutputDecoder context
  ) {
    int n = buf.getInt();
    int flags = buf.get();
    if (n < 0 || n > buf.remaining() / 4) {
      throw new IllegalStateException("truncate relation count " + n
          + " does not fit in the remaining " + buf.remaining() + " bytes");
    }
    var relids = new java.util.ArrayList<Long>(n);
    for (int i = 0; i < n; i++) {
      relids.add(Integer.toUnsignedLong(buf.getInt()));
    }
    return new PgEvent.Truncate(relids, (flags & 1) != 0, (flags & 2) != 0);
  }
}
