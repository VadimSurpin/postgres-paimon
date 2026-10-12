// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import pro.surpin.pgpaimon.writer.dto.PgEvent;

@Component
public final class CommitMessageDecoder implements PgOutputMessageDecoder {

  @Override
  public @NotNull PgOutputMessageType type() {
    return PgOutputMessageType.COMMIT;
  }

  @Override
  public @NotNull PgEvent decode(
      @NotNull ByteBuffer buf,
      @NotNull PgOutputDecoder context
  ) {
    buf.get(); // flags, unused
    long commitLsn = buf.getLong();
    long endLsn = buf.getLong();
    long ts = buf.getLong();
    return new PgEvent.Commit(commitLsn, endLsn, ts);
  }
}
