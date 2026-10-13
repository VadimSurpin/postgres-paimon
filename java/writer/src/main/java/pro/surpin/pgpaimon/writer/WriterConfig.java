// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.*;

/**
 * The writer's whole configuration, read from the environment once.
 *
 * <p>There is deliberately no Paimon database setting either. The Paimon database of a table is
 * its PostgreSQL schema, so a configured one would have nothing to name; a
 * {@code PGPAIMON_DATABASE} that is parsed and then ignored is the failure mode this branch has
 * already had to fix twice.
 *
 * <p>Two different knobs are easy to conflate and must not be. {@code flushMaxRecords}
 * ({@code PGPAIMON_FLUSH_MAX_RECORDS}) is the chunk size of the <em>initial load</em> only, where
 * each COPY chunk is a commit of its own. {@code batchMaxRows}, {@code batchMaxBytes} and
 * {@code batchMaxIntervalMs} ({@code PGPAIMON_BATCH_*}) size the <em>streaming</em> commit: how
 * many completed PostgreSQL transactions {@link MiniBatcher} accumulates into one Paimon commit.
 * Neither can split a transaction -- a limit is only checked between whole transactions -- so the
 * limits are "at least this much", never "exactly this much".
 */
public record WriterConfig(String jdbcUrl, String replicationUrl, String user,
                           String password, String warehouse,
                           String slotName, String publicationName,
                           String messagePrefix, String commitUser,
                           String seqField, String opSeqField, String xidField,
                           int flushMaxRecords, long skipLsn,
                           long batchMaxRows, long batchMaxBytes, long batchMaxIntervalMs,
                           long healthIntervalMs,
                           Map<String, String> catalogOptions) {

    /** Defaults for the streaming commit batch; see {@link MiniBatcher}. */
    public static final String DEFAULT_BATCH_MAX_ROWS = "10000";
    public static final String DEFAULT_BATCH_MAX_BYTES = "16777216";    // 16 MiB
    public static final String DEFAULT_BATCH_MAX_INTERVAL_MS = "2000";
    /** How often the running writer re-runs paimon.health(); see {@link HealthCheck}. */
    public static final String DEFAULT_HEALTH_INTERVAL_MS = "60000";

    /**
     * The publication and message prefix the extension uses. Both are fixed in
     * paimon_slot--1.0.sql (add_table adds tables to paimon_pub, paimon.emit uses the prefix),
     * so the writer configured with anything else would receive no tables or no DDL at all, and
     * say nothing. The settings are kept so that a deployment naming them explicitly still
     * starts, but only with these values.
     */
    static final String PUBLICATION = "paimon_pub";
    static final String MESSAGE_PREFIX = "paimon";

    public static WriterConfig fromEnv(Map<String, String> env) {
        String jdbc = require(env, "PGPAIMON_JDBC_URL");
        String wh   = require(env, "PGPAIMON_WAREHOUSE");

        Map<String, String> cat = new LinkedHashMap<>();
        cat.put("warehouse", wh);
        put(cat, "s3.endpoint",   get(env, "PGPAIMON_S3_ENDPOINT"));
        put(cat, "s3.access-key", get(env, "PGPAIMON_S3_ACCESS_KEY"));
        put(cat, "s3.secret-key", get(env, "PGPAIMON_S3_SECRET_KEY"));
        if (get(env, "PGPAIMON_S3_ENDPOINT") != null) {
            cat.put("s3.path.style.access", "true");
        }

        return new WriterConfig(
            jdbc,
            jdbc + (jdbc.contains("?") ? "&" : "?")
                 + "replication=database&preferQueryMode=simple&assumeMinServerVersion=18",
            getOrDefault(env, "PGPAIMON_USER", "postgres"),
            getOrDefault(env, "PGPAIMON_PASSWORD", ""),
            wh,
            getOrDefault(env, "PGPAIMON_SLOT", "paimon"),
            fixed(env, "PGPAIMON_PUBLICATION", PUBLICATION),
            fixed(env, "PGPAIMON_MESSAGE_PREFIX", MESSAGE_PREFIX),
            getOrDefault(env, "PGPAIMON_COMMIT_USER", "paimon-writer-" + getOrDefault(env, "PGPAIMON_SLOT", "paimon")),
            getOrDefault(env, "PGPAIMON_SEQ_FIELD", "_seq"),
            getOrDefault(env, "PGPAIMON_OP_SEQ_FIELD", "_op_seq"),
            get(env, "PGPAIMON_XID_FIELD"),
            (int) parseNumber(env, "PGPAIMON_FLUSH_MAX_RECORDS", "10000", Integer.MAX_VALUE),
            parseLsn(env, "PGPAIMON_SKIP_LSN"),
            parseMin(env, "PGPAIMON_BATCH_MAX_ROWS", DEFAULT_BATCH_MAX_ROWS, 1),
            parseMin(env, "PGPAIMON_BATCH_MAX_BYTES", DEFAULT_BATCH_MAX_BYTES, 1),
            parseMin(env, "PGPAIMON_BATCH_MAX_INTERVAL_MS", DEFAULT_BATCH_MAX_INTERVAL_MS, 0),
            parseMin(env, "PGPAIMON_HEALTH_INTERVAL_MS", DEFAULT_HEALTH_INTERVAL_MS, 0),
            Map.copyOf(cat));
    }

    /** A setting that may only take the one value the extension uses; see {@link #PUBLICATION}. */
    private static String fixed(Map<String, String> env, String key, String only) {
        String v = getOrDefault(env, key, only);
        if (!only.equals(v)) {
            throw new IllegalArgumentException(key + " must be '" + only + "' (got '" + v
                + "'): the paimon_slot extension publishes tracked tables and emits DDL under"
                + " that name only, so any other value would replicate nothing and say nothing");
        }
        return v;
    }

    /** A blank value is treated as unset: null. */
    private static String get(Map<String, String> env, String key) {
        String v = env.get(key);
        return (v == null || v.isBlank()) ? null : v;
    }

    private static String getOrDefault(Map<String, String> env, String key, String dflt) {
        String v = get(env, key);
        return v == null ? dflt : v;
    }

    /**
     * One LSN, or 0 for unset: the change LSN of a single poison record to skip. Accepts the
     * {@code X/XXXXXXXX} form the writer's own POISON RECORD error prints, and a plain decimal.
     *
     * <p>Deliberately one value and never a range: a range would let one mistyped digit discard
     * arbitrary amounts of committed data, silently. See {@link TxnBatcher} for what it does.
     */
    private static long parseLsn(Map<String, String> env, String key) {
        String v = get(env, key);
        if (v == null) {
            return 0;
        }
        v = v.trim();
        try {
            long lsn = v.indexOf('/') >= 0 ? SlotConsumer.parseLsn(v) : Long.parseUnsignedLong(v);
            if (lsn == 0) {
                throw new NumberFormatException("0 is not an LSN");
            }
            return lsn;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(key + " must be one LSN, either as the"
                + " X/XXXXXXXX form the POISON RECORD error prints or as a decimal, got '"
                + v + "'", e);
        }
    }

    private static long parseNumber(Map<String, String> env, String key, String dflt, long max) {
        String v = getOrDefault(env, key, dflt);
        try {
            long n = Long.parseLong(v.trim());
            if (n > max) {
                throw new NumberFormatException("out of range");
            }
            return n;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a number, got '" + v + "'", e);
        }
    }

    /** A number of at least {@code min}: a limit below its minimum would be a typo, not a choice. */
    private static long parseMin(Map<String, String> env, String key, String dflt, long min) {
        long n = parseNumber(env, key, dflt, Long.MAX_VALUE);
        if (n < min) {
            throw new IllegalArgumentException(key + " must be at least " + min + ", got " + n);
        }
        return n;
    }

    private static String require(Map<String, String> env, String key) {
        String v = get(env, key);
        if (v == null) {
            throw new IllegalArgumentException(key + " is required");
        }
        return v;
    }

    private static void put(Map<String, String> m, String k, String v) {
        if (v != null) {
            m.put(k, v);
        }
    }
}
