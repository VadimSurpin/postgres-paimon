// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;
import org.apache.paimon.types.RowKind;

/** relid: unsigned 32-bit OID widened to long; see RelationMeta.
 *  values: a null element is SQL NULL. */
public record PartialRecord(long relid, RowKind kind, List<String> values) {}
