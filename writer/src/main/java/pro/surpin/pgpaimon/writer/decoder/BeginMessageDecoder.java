// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.PgEvent;

@Component
public final class BeginMessageDecoder implements PgOutputMessageDecoder {

  @Override
  public @NotNull PgOutputMessageType type() {
    return PgOutputMessageType.BEGIN;
  }

  @Override
  public @NotNull PgEvent decode(
      @NotNull ByteBuffer buf,
      @NotNull PgOutputDecoder context
  ) {
    long finalLsn = buf.getLong();
    long ts = buf.getLong();
    long xid = Integer.toUnsignedLong(buf.getInt());
    return new PgEvent.Begin(finalLsn, ts, xid);
  }
}
