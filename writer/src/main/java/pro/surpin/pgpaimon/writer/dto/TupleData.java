// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.dto;

import java.util.List;
import org.jetbrains.annotations.NotNull;

public record TupleData(
    @NotNull List<@NotNull ColumnValue> columns
) {
}
