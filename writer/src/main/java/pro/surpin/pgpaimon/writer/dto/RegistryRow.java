// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.dto;

import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import pro.surpin.pgpaimon.writer.dto.DdlMessage.Col;

/** One row of {@code paimon.table_registry}, as much of it as startup needs. */
public record RegistryRow(
    long relid,
    @NotNull String schema,
    @NotNull String table,
    @NotNull List<@NotNull String> pk,
    @NotNull List<@NotNull Col> cols,
    boolean backfillDue,
    @Nullable String changelogProducer
) {
}
