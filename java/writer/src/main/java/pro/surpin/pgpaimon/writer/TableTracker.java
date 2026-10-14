// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

/**
 * What the transaction assembler needs to know about a relation: whether it is replicated, and
 * into which shape. Implemented by {@link SchemaRegistry}; tests supply their own.
 */
public interface TableTracker {
    /** Whether a key (a relation id or a {@link HistoryKeys history key}) names a tracked table. */
    boolean tracked(long key);

    /** Whether the relation has no primary key, so its main table is an operation log. */
    boolean keyless(long relid);

    /** Whether the relation has a history table this process writes. */
    boolean hasHistory(long relid);
}
