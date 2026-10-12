// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import pro.surpin.pgpaimon.writer.dto.PgEvent;

/** Decodes one pgoutput message kind. */
public interface PgOutputMessageDecoder {

  @NotNull
  PgOutputMessageType type();

  @NotNull
  PgEvent decode(
      @NotNull ByteBuffer buf,
      @NotNull PgOutputDecoder context
  );
}
