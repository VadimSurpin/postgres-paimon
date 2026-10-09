// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.*;

public record WriterConfig(String jdbcUrl, String replicationUrl, String user,
                           String password, String warehouse, String database,
                           String slotName, String publicationName,
                           String messagePrefix, String commitUser,
                           String seqField, String opSeqField, String xidField,
                           int flushMaxRecords, long flushMaxIntervalMs,
                           Map<String, String> catalogOptions) {

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
            getOrDefault(env, "PGPAIMON_DATABASE", "default"),
            getOrDefault(env, "PGPAIMON_SLOT", "paimon"),
            getOrDefault(env, "PGPAIMON_PUBLICATION", "paimon_pub"),
            getOrDefault(env, "PGPAIMON_MESSAGE_PREFIX", "paimon"),
            getOrDefault(env, "PGPAIMON_COMMIT_USER", "paimon-writer-" + getOrDefault(env, "PGPAIMON_SLOT", "paimon")),
            getOrDefault(env, "PGPAIMON_SEQ_FIELD", "_seq"),
            getOrDefault(env, "PGPAIMON_OP_SEQ_FIELD", "_op_seq"),
            get(env, "PGPAIMON_XID_FIELD"),
            (int) parseNumber(env, "PGPAIMON_FLUSH_MAX_RECORDS", "10000", Integer.MAX_VALUE),
            parseNumber(env, "PGPAIMON_FLUSH_MAX_INTERVAL_MS", "1000", Long.MAX_VALUE),
            Map.copyOf(cat));
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
