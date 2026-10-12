// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.dto;

import java.util.List;

import org.apache.paimon.types.RowKind;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * relid: an unsigned 32-bit PostgreSQL OID held as long. values: a null element is SQL NULL.
 */
public record PartialRecord(
    long relid,
    @NotNull RowKind kind,
    @NotNull List<@Nullable String> values
) {
}
