// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.dto;

import java.util.List;

import org.jetbrains.annotations.NotNull;

/**
 * relid: an unsigned 32-bit PostgreSQL OID held as long.
 */
public record RelationMeta(
    long relid,
    @NotNull String namespace,
    @NotNull String relName,
    @NotNull List<@NotNull String> columnNames,
    @NotNull List<@NotNull Boolean> isKey
) {
}
