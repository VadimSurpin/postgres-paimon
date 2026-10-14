// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

/**
 * Every tracked relation writes two Paimon tables: its main table and its history table
 * ({@code <table>_hist}). The writer's per-table machinery -- the sink's writers, the
 * mini-batcher's committed identifiers, the offset store, the schema registry -- is keyed by a
 * {@code long}, which used to be the relation's OID. The history table needs a key of its own,
 * so it gets the bitwise complement of the OID.
 *
 * <p>Why that is unambiguous: an OID is an unsigned 32-bit value held in a {@code long}
 * (package-info), so it is never negative, and {@code ~oid} always is. The mapping is its own
 * inverse. A key is a history key exactly when it is negative.
 */
public final class HistoryKeys {

    private HistoryKeys() {
    }

    /** The key of a relation's history table. */
    public static long histKey(long relid) {
        if (relid < 0) {
            throw new IllegalArgumentException("not a relation id: " + relid);
        }
        return ~relid;
    }

    /** Whether a key names a history table rather than a relation's main table. */
    public static boolean isHist(long key) {
        return key < 0;
    }

    /** The relation a key belongs to, whichever of its two tables it names. */
    public static long relidOf(long key) {
        return key < 0 ? ~key : key;
    }
}
