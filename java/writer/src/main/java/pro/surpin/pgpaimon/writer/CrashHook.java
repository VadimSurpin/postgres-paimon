// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.paimon.table.FileStoreTable;

/**
 * TEST-ONLY: stops the process dead right after one named Paimon table's commit, to prove that a
 * crash between a relation's main-table commit and its history-table commit (which are not
 * atomic with each other) loses and duplicates nothing.
 *
 * <p><b>Off by default</b>, and inert unless {@code PGPAIMON_TEST_HALT_AFTER_COMMIT} is set to a
 * Paimon table as {@code <database>.<table>} (for example {@code public.t_hist}). Then, the first
 * time that table's commit returns, the process halts with exit code 137 -- {@link
 * Runtime#halt}, so no shutdown hook runs, no buffer is drained and nothing is confirmed: what a
 * {@code kill -9} at that instant would leave. The commit order inside one flush is the main
 * table, then its history table; when the named table is a history table, history tables are
 * committed first instead, so the halt always lands between the two commits of the same
 * transactions whichever side is named. The acceptance suite (PaimonHistoryTest) sets it on one
 * writer start and restarts without it. Never set it in production.
 */
final class CrashHook {

    private static final Logger LOG = Logger.getLogger(CrashHook.class.getName());
    static final String ENV = "PGPAIMON_TEST_HALT_AFTER_COMMIT";

    static final CrashHook OFF = new CrashHook(null, false);

    private final String table;        // <database>.<table>, or null
    private final boolean historyFirst;

    private CrashHook(String table, boolean historyFirst) {
        this.table = table;
        this.historyFirst = historyFirst;
    }

    static CrashHook fromEnv(Map<String, String> env) {
        String t = env.get(ENV);
        if (t == null || t.isBlank()) {
            return OFF;
        }
        LOG.warning(ENV + "=" + t + " is set: this writer will HALT right after its first commit"
            + " to that table. This is a crash-test hook; never set it in production.");
        return new CrashHook(t.trim(), t.trim().endsWith("_hist"));
    }

    /** The order in which one flush commits its tables. Identity unless the hook is armed. */
    <V> Map<Long, V> order(Map<Long, V> byKey) {
        if (table == null || !historyFirst) {
            return byKey;
        }
        Map<Long, V> out = new LinkedHashMap<>();
        List<Long> keys = new ArrayList<>(byKey.keySet());
        for (long k : keys) {
            if (HistoryKeys.isHist(k)) {
                out.put(k, byKey.get(k));
            }
        }
        for (long k : keys) {
            if (!HistoryKeys.isHist(k)) {
                out.put(k, byKey.get(k));
            }
        }
        return out;
    }

    /** Called after each table's commit (applied or filtered as a replay). */
    void afterCommit(FileStoreTable t) {
        if (table != null && table.equals(t.fullName())) {
            LOG.severe(ENV + ": halting right after the commit to " + table);
            System.err.flush();
            Runtime.getRuntime().halt(137);
        }
    }
}
