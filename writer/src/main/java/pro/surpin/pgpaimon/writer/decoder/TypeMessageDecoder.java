// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import pro.surpin.pgpaimon.writer.dto.PgEvent;

@Component
public final class TypeMessageDecoder implements PgOutputMessageDecoder {

  @Override
  public @NotNull PgOutputMessageType type() {
    return PgOutputMessageType.TYPE;
  }

  @Override
  public @NotNull PgEvent decode(
      @NotNull ByteBuffer buf,
      @NotNull PgOutputDecoder context
  ) {
    long oid = Integer.toUnsignedLong(buf.getInt());
    String nsp = PgOutputDecoder.cstring(buf);
    String name = PgOutputDecoder.cstring(buf);
    return new PgEvent.TypeMsg(oid, nsp, name);
  }
}
