// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;
import org.apache.paimon.types.RowKind;

/** relid: a PostgreSQL OID held as long; see package-info.
 *  values: a null element is SQL NULL. */
public record PartialRecord(long relid, RowKind kind, List<String> values) {}
