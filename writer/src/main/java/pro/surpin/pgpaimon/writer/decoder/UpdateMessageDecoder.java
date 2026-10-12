// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.PgEvent;
import pro.surpin.pgpaimon.writer.dto.TupleData;

@Component
public final class UpdateMessageDecoder implements PgOutputMessageDecoder {

  @Override
  public @NotNull PgOutputMessageType type() {
    return PgOutputMessageType.UPDATE;
  }

  @Override
  public @NotNull PgEvent decode(
      @NotNull ByteBuffer buf,
      @NotNull PgOutputDecoder context
  ) {
    long relid = Integer.toUnsignedLong(buf.getInt());
    TupleData oldTuple = null;
    boolean keyOnly = false;
    byte marker = buf.get();
    if (marker == 'O' || marker == 'K') {
      keyOnly = marker == 'K';
      oldTuple = context.tuple(buf, relid);
      marker = buf.get();
    }
    PgOutputDecoder.expect(marker, 'N', "update new-tuple marker");
    return new PgEvent.Update(relid, oldTuple, keyOnly, context.tuple(buf, relid));
  }
}
