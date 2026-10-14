// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.data.GenericRow;

/**
 * Builds the Paimon row for one record of one table. {@link RowBuilder} does it for a
 * primary-key table's changelog records, {@link LogRowBuilder} for the operation records of a
 * history table or of a key-less table's operation log.
 */
public interface RowMaker {
    GenericRow build(SinkRecord r);
}
