// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.PgEvent;

@Component
public final class StreamStopMessageDecoder implements PgOutputMessageDecoder {

  @Override
  public @NotNull PgOutputMessageType type() {
    return PgOutputMessageType.STREAM_STOP;
  }

  @Override
  public @NotNull PgEvent decode(
      @NotNull ByteBuffer buf,
      @NotNull PgOutputDecoder context
  ) {
    throw new IllegalStateException("stream message 'E' received although streaming is off");
  }
}
