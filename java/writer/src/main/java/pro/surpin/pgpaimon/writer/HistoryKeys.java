// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Every tracked relation writes several Paimon tables: its main table, its current history table
 * ({@code <table>_hist}) and the archived history tables a DDL rotation left behind
 * ({@code <table>_hist_<lsn>}). The writer's per-table machinery -- the sink's writers, the
 * mini-batcher's committed identifiers, the offset store, the schema registry -- is keyed by a
 * {@code long}, which is the relation's OID for its main table.
 *
 * <ul>
 *   <li>The CURRENT history table is keyed by {@code ~oid}. An OID is an unsigned 32-bit value held
 *       in a {@code long} (package-info), so {@code ~oid} lies in {@code [-2^32, -1]}, never an OID,
 *       and the mapping is its own inverse.</li>
 *   <li>An ARCHIVED history table is keyed by a number below {@code -2^32}, handed out here once
 *       per (relation, rotation LSN) for the life of the process. It is stable within the process
 *       and recomputed after a restart from the same pair, so nothing persists it.</li>
 * </ul>
 * A key names some history table exactly when it is negative.
 */
public final class HistoryKeys {

    private static final long CURRENT_FLOOR = -(1L << 32);

    private record Archive(long relid, long lsn) {}

    private static final Map<Archive, Long> KEYS = new ConcurrentHashMap<>();
    private static final Map<Long, Archive> BY_KEY = new ConcurrentHashMap<>();
    private static long next = Long.MIN_VALUE;

    private HistoryKeys() {
    }

    /** The key of a relation's current history table. */
    public static long histKey(long relid) {
        if (relid < 0) {
            throw new IllegalArgumentException("not a relation id: " + relid);
        }
        return ~relid;
    }

    /** The key of the history table archived at rotation LSN {@code lsn}. */
    public static synchronized long archiveKey(long relid, long lsn) {
        Archive a = new Archive(relid, lsn);
        Long k = KEYS.get(a);
        if (k == null) {
            k = next++;
            KEYS.put(a, k);
            BY_KEY.put(k, a);
        }
        return k;
    }

    /** Whether a key names a history table (current or archived) rather than a main table. */
    public static boolean isHist(long key) {
        return key < 0;
    }

    /** Whether a key names an archived history table. */
    public static boolean isArchive(long key) {
        return key < CURRENT_FLOOR;
    }

    /** The relation a key belongs to, whichever of its tables it names. */
    public static long relidOf(long key) {
        if (key >= 0) {
            return key;
        }
        if (key >= CURRENT_FLOOR) {
            return ~key;
        }
        Archive a = BY_KEY.get(key);
        if (a == null) {
            throw new IllegalArgumentException("unknown history key " + key);
        }
        return a.relid();
    }

    /** The table name an archive gets: {@code <hist>_<16 upper-case hex digits of the LSN>}. */
    public static String archiveName(String hist, long lsn) {
        return hist + "_" + String.format("%016X", lsn);
    }

    /** The rotation LSN in an archive's name, or -1 if the name is not one of {@code hist}'s. */
    public static long archiveLsn(String name, String hist) {
        if (name.length() != hist.length() + 17 || !name.startsWith(hist + "_")) {
            return -1;
        }
        String hex = name.substring(hist.length() + 1);
        if (!hex.matches("[0-9A-F]{16}")) {
            return -1;
        }
        return Long.parseUnsignedLong(hex, 16);
    }
}
