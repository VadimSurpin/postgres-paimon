// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;
import org.apache.paimon.types.RowKind;

/** values: a null element is SQL NULL. */
public record PartialRecord(int relid, RowKind kind, List<String> values) {}
