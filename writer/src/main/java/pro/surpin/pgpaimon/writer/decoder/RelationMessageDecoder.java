// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.decoder;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import pro.surpin.pgpaimon.writer.dto.PgEvent;
import pro.surpin.pgpaimon.writer.dto.RelationMeta;

@Component
public final class RelationMessageDecoder implements PgOutputMessageDecoder {

  @Override
  public @NotNull PgOutputMessageType type() {
    return PgOutputMessageType.RELATION;
  }

  @Override
  public @NotNull PgEvent decode(
      @NotNull ByteBuffer buf,
      @NotNull PgOutputDecoder context
  ) {
    long relid = Integer.toUnsignedLong(buf.getInt());
    String nsp = PgOutputDecoder.cstring(buf);
    String rel = PgOutputDecoder.cstring(buf);
    buf.get(); // replica identity setting
    int n = buf.getShort() & 0xFFFF;
    var names = new java.util.ArrayList<String>(n);
    var keys = new java.util.ArrayList<Boolean>(n);
    for (int i = 0; i < n; i++) {
      keys.add((buf.get() & 1) == 1);
      names.add(PgOutputDecoder.cstring(buf));
      buf.getInt(); // type oid
      buf.getInt(); // type modifier
    }
    // pgoutput sends an empty namespace for pg_catalog (logicalrep_write_namespace).
    RelationMeta meta =
        new RelationMeta(relid, nsp.isEmpty() ? "pg_catalog" : nsp, rel, names, keys);
    context.rememberRelation(relid, meta);
    return new PgEvent.Relation(meta);
  }
}
