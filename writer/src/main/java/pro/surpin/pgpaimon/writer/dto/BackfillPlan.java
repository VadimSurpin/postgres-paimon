// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.dto;

import org.jetbrains.annotations.NotNull;

/** The exported snapshot's opaque name and the slot's consistent point as an LSN. */
public record BackfillPlan(
    @NotNull String snapshotName,
    long consistentPoint
) {
}
