// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.PgEvent;

@Component
public final class MessageMessageDecoder implements PgOutputMessageDecoder {

  @Override
  public @NotNull PgOutputMessageType type() {
    return PgOutputMessageType.MESSAGE;
  }

  @Override
  public @NotNull PgEvent decode(
      @NotNull ByteBuffer buf,
      @NotNull PgOutputDecoder context
  ) {
    boolean transactional = (buf.get() & 1) != 0;
    long lsn = buf.getLong();
    String prefix = PgOutputDecoder.cstring(buf);
    int len = buf.getInt();
    PgOutputDecoder.requireFits(len, buf, "logical message length");
    byte[] content = new byte[len];
    buf.get(content);
    return new PgEvent.Message(transactional, lsn, prefix, content);
  }
}
