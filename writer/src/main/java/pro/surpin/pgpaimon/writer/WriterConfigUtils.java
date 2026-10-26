// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.*;
import org.jetbrains.annotations.*;

/** Parsing and compatibility helpers for the Spring-bound writer configuration. */
public final class WriterConfigUtils {
  private WriterConfigUtils() {}

  /** Defaults for the streaming commit batch; see {@link MiniBatcher}. */
  public static final String DEFAULT_BATCH_MAX_ROWS = "10000";
  public static final String DEFAULT_BATCH_MAX_BYTES = "16777216"; // 16 MiB
  public static final String DEFAULT_BATCH_MAX_INTERVAL_MS = "2000";
  /** How often the running writer re-runs paimon.health(); see {@link HealthCheck}. */
  public static final String DEFAULT_HEALTH_INTERVAL_MS = "60000";

  /**
   * The default {@code PGPAIMON_TXN_MAX_BYTES}: a quarter of the JVM's maximum heap. The estimate
   * (see {@link MiniBatcher#estimate}) undercounts what a buffered transaction really holds:
   * measured, a 500,000-row INSERT (1,000,000 records with its history, an estimated 153 MB) held
   * about 340 MB live, and the flush that followed peaked at 1 GB of heap with Paimon's write
   * buffers. A quarter leaves room for both; see {@link TxnBatcher}.
   */
  static long defaultTxnMaxBytes() {
    return Math.max(1, Runtime.getRuntime().maxMemory() / 4);
  }

  /**
   * The publication and message prefix the extension uses. Both are fixed in paimon_slot--1.0.sql
   * (add_table adds tables to paimon_pub, paimon.emit uses the prefix), so the writer configured
   * with anything else would receive no tables or no DDL at all, and say nothing. The settings are
   * kept so that a deployment naming them explicitly still starts, but only with these values.
   */
  static final String PUBLICATION = "paimon_pub";
  static final String MESSAGE_PREFIX = "paimon";

  public static WriterConfig fromEnv(
      Map<String, String> env
  ) {
    String jdbc = require(env, "PGPAIMON_JDBC_URL");
    String wh = require(env, "PGPAIMON_WAREHOUSE");

    return new WriterConfig(jdbc, wh, getOrDefault(env, "PGPAIMON_USER", "postgres"),
        getOrDefault(env, "PGPAIMON_PASSWORD", ""), getOrDefault(env, "PGPAIMON_SLOT", "paimon"),
        fixed(env, "PGPAIMON_PUBLICATION", PUBLICATION),
        fixed(env, "PGPAIMON_MESSAGE_PREFIX", MESSAGE_PREFIX), get(env, "PGPAIMON_COMMIT_USER"),
        getOrDefault(env, "PGPAIMON_SEQ_FIELD", "_seq"),
        getOrDefault(env, "PGPAIMON_OP_SEQ_FIELD", "_op_seq"), get(env, "PGPAIMON_XID_FIELD"),
        (int) parseNumber(env, "PGPAIMON_FLUSH_MAX_RECORDS", "10000", Integer.MAX_VALUE),
        get(env, "PGPAIMON_SKIP_LSN"),
        new WriterConfig.S3(get(env, "PGPAIMON_S3_ENDPOINT"), get(env, "PGPAIMON_S3_ACCESS_KEY"),
            get(env, "PGPAIMON_S3_SECRET_KEY")),
        parseMin(env, "PGPAIMON_BATCH_MAX_ROWS", DEFAULT_BATCH_MAX_ROWS, 1),
        parseMin(env, "PGPAIMON_BATCH_MAX_BYTES", DEFAULT_BATCH_MAX_BYTES, 1),
        parseMin(env, "PGPAIMON_BATCH_MAX_INTERVAL_MS", DEFAULT_BATCH_MAX_INTERVAL_MS, 0),
        parseMin(env, "PGPAIMON_HEALTH_INTERVAL_MS", DEFAULT_HEALTH_INTERVAL_MS, 0),
        parseMin(env, "PGPAIMON_TXN_MAX_BYTES", String.valueOf(defaultTxnMaxBytes()), 1),
        systemIdentifier(env, "PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER"));
  }

  /** A setting that may only take the one value the extension uses; see {@link #PUBLICATION}. */
  private static String fixed(
      Map<String, String> env,
      String key,
      String only
  ) {
    String v = getOrDefault(env, key, only);
    if (!only.equals(v)) {
      throw new IllegalArgumentException(key + " must be '" + only + "' (got '" + v
          + "'): the paimon_slot extension publishes tracked tables and emits DDL under"
          + " that name only, so any other value would replicate nothing and say nothing");
    }
    return v;
  }

  /** A blank value is treated as unset: null. */
  private static @Nullable String get(
      @NotNull Map<String, String> env,
      @NotNull String key
  ) {
    String v = env.get(key);
    return (v == null || v.isBlank()) ? null : v;
  }

  private static @NotNull String getOrDefault(
      @NotNull Map<String, String> env,
      @NotNull String key,
      @NotNull String dflt
  ) {
    String v = get(env, key);
    return v == null ? dflt : v;
  }

  /**
   * One LSN, or 0 for unset: the change LSN of a single poison record to skip. Accepts the {@code
   * X/XXXXXXXX} form the writer's own POISON RECORD error prints, and a plain decimal.
   *
   * <p>
   * Deliberately one value and never a range: a range would let one mistyped digit discard
   * arbitrary amounts of committed data, silently. See {@link TxnBatcher} for what it does.
   */
  private static long parseLsn(
      @NotNull Map<String, String> env,
      @NotNull String key
  ) {
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
      throw new IllegalArgumentException(
          key + " must be one LSN, either as the"
              + " X/XXXXXXXX form the POISON RECORD error prints or as a decimal, got '" + v + "'",
          e);
    }
  }

  /** A PostgreSQL system identifier (a decimal 64-bit number), or null for unset. */
  private static String systemIdentifier(
      Map<String, String> env,
      String key
  ) {
    String v = get(env, key);
    if (v == null) {
      return null;
    }
    v = v.trim();
    try {
      Long.parseUnsignedLong(v);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be the decimal system identifier the"
          + " writer's refusal printed (SELECT system_identifier FROM pg_control_system()),"
          + " got '" + v + "'", e);
    }
    return v;
  }

  private static long parseNumber(
      @NotNull Map<String, String> env,
      @NotNull String key,
      @NotNull String dflt,
      long max
  ) {
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
  private static long parseMin(
      Map<String, String> env,
      String key,
      String dflt,
      long min
  ) {
    long n = parseNumber(env, key, dflt, Long.MAX_VALUE);
    if (n < min) {
      throw new IllegalArgumentException(key + " must be at least " + min + ", got " + n);
    }
    return n;
  }

  private static String require(
      Map<String, String> env,
      String key
  ) {
    String v = get(env, key);
    if (v == null) {
      throw new IllegalArgumentException(key + " is required");
    }
    return v;
  }

  static void put(
      @NotNull Map<String, String> m,
      @NotNull String k,
      @Nullable String v
  ) {
    if (v != null) {
      m.put(k, v);
    }
  }

  static long parseLsn(
      String value
  ) {
    return parseLsn(value == null ? Map.of() : Map.of("PGPAIMON_SKIP_LSN", value),
        "PGPAIMON_SKIP_LSN");
  }

  static String blankToNull(
      String value
  ) {
    return value == null || value.isBlank() ? null : value;
  }

  static void validateFixed(
      String key,
      String value,
      String expected
  ) {
    fixed(Map.of(key, value), key, expected);
  }

  static void validateMin(
      String key,
      long value,
      long minimum
  ) {
    if (value < minimum)
      throw new IllegalArgumentException(key + " must be at least " + minimum + ", got " + value);
  }

  static String acceptSystemIdentifier(
      String value
  ) {
    return systemIdentifier(
        value == null ? Map.of() : Map.of("PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER", value),
        "PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER");
  }

}
