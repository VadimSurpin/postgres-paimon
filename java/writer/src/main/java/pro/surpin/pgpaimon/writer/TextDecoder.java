// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;

/**
 * Converts PostgreSQL text output into Paimon internal values.
 *
 * <p>Dispatch is on the Paimon {@link DataType}, never on a PostgreSQL type OID: the
 * mapping lives once, in SQL (paimon.pg_type_to_paimon).
 *
 * <p>The slot runs with binary 'false' because text output is round-trip exact for every
 * type in the mapping: extra_float_digits = 3 is the default since PG 12, numeric is exact
 * as text, and bytea is hex.
 *
 * <p>A null {@code text} is SQL NULL; "" is the empty string. They never map to the same
 * Paimon value. Values with no Paimon representation (date/timestamp infinity,
 * numeric NaN/Infinity, decimals that would lose digits) are rejected, never clamped.
 *
 * <p>A timestamptz's offset is rendered in the walsender session's TimeZone at decode
 * time; the replication connection must be pinned to UTC (all offset forms still parse).
 */
public final class TextDecoder {

    private static final DateTimeFormatter TS = new DateTimeFormatterBuilder()
        .appendPattern("uuuu-MM-dd HH:mm:ss")
        .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
        .toFormatter()
        .withResolverStyle(ResolverStyle.STRICT);

    /**
     * "+HH:mm:ss" reads hours, then optional minutes, then optional seconds, which covers
     * all four forms PostgreSQL prints: +00, +03, +05:45, +05:41:16. "+00" is also the
     * literal for a zero offset.
     */
    private static final DateTimeFormatter TS_TZ = new DateTimeFormatterBuilder()
        .append(TS)
        .appendOffset("+HH:mm:ss", "+00")
        .toFormatter()
        .withResolverStyle(ResolverStyle.STRICT);

    private TextDecoder() {
    }

    public static Object toPaimon(DataType type, String text) {
        return toPaimon(null, type, text);
    }

    /** {@code column} is used only to make error messages name the offending column. */
    public static Object toPaimon(String column, DataType type, String text) {
        if (text == null) {
            if (!type.isNullable()) {
                throw new IllegalArgumentException(where(column)
                    + "NULL for a " + type + " column");
            }
            return null;
        }
        try {
            return convert(type, text);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(where(column)
                + "cannot parse " + type + " from \"" + text + "\""
                + (e.getMessage() == null ? "" : ": " + e.getMessage()), e);
        }
    }

    private static String where(String column) {
        return column == null ? "" : "column '" + column + "': ";
    }

    private static Object convert(DataType type, String s) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return switch (s) {
                    case "t", "true", "1" -> Boolean.TRUE;
                    case "f", "false", "0" -> Boolean.FALSE;
                    default -> throw new IllegalArgumentException("not a boolean");
                };
            case SMALLINT:
                return Short.parseShort(s);
            case INTEGER:
                return Integer.parseInt(s);
            case BIGINT:
                return Long.parseLong(s);
            case FLOAT:
                return Float.parseFloat(s);     // accepts NaN, Infinity, -Infinity
            case DOUBLE:
                return Double.parseDouble(s);
            case DECIMAL:
                return decimal((DecimalType) type, s);
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString(s);
            case BINARY:
            case VARBINARY:
                return fromHex(s);
            case DATE:
                return (int) LocalDate.parse(temporal(s)).toEpochDay();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Timestamp.fromLocalDateTime(LocalDateTime.parse(temporal(s), TS));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.fromLocalDateTime(
                    OffsetDateTime.parse(temporal(s), TS_TZ)
                                  .withOffsetSameInstant(ZoneOffset.UTC)
                                  .toLocalDateTime());
            default:
                throw new UnsupportedOperationException("no text decoding for " + type);
        }
    }

    private static String temporal(String s) {
        if (s.equals("infinity") || s.equals("-infinity")) {
            throw new IllegalArgumentException("infinity has no Paimon representation");
        }
        return s.endsWith(" BC") ? bcToProleptic(s) : s;
    }

    /**
     * PostgreSQL prints BC values as "0044-03-15 BC" / "0044-03-15 10:20:30+00 BC"; the
     * suffix is always last. Java counts proleptic years, where 1 BC is year 0, so the
     * year becomes 1 - bcYear and the rest of the text is parsed unchanged. (Paimon's int
     * epoch-day and long micros both cover these values.)
     */
    private static String bcToProleptic(String s) {
        String body = s.substring(0, s.length() - 3);
        int dash = body.indexOf('-');
        if (dash < 1) {
            throw new IllegalArgumentException("malformed BC value");
        }
        long bcYear = Long.parseLong(body.substring(0, dash));
        if (bcYear < 1 || body.charAt(0) == '+') {
            throw new IllegalArgumentException("malformed BC value");
        }
        long proleptic = 1 - bcYear;
        String year = proleptic == 0 ? "0000"
            : "-" + String.format("%04d", -proleptic);
        return year + body.substring(dash);
    }

    private static Decimal decimal(DecimalType t, String s) {
        BigDecimal v;
        try {
            v = new BigDecimal(s);      // NaN / Infinity throw NumberFormatException
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "numeric NaN and Infinity have no Paimon representation", e);
        }
        // Exact: throws ArithmeticException rather than rounding if digits would be lost.
        v = v.setScale(t.getScale(), RoundingMode.UNNECESSARY);
        if (v.precision() > t.getPrecision()) {
            throw new IllegalArgumentException("value exceeds precision " + t.getPrecision());
        }
        Decimal d = Decimal.fromBigDecimal(v, t.getPrecision(), t.getScale());
        if (d == null) {
            throw new IllegalArgumentException("value exceeds precision " + t.getPrecision());
        }
        return d;
    }

    /** bytea_output = hex is the default since PG 9.0: "\x" followed by pairs. */
    private static byte[] fromHex(String s) {
        if (!s.startsWith("\\x")) {
            throw new IllegalArgumentException("not hex-format bytea (expected \\x prefix)");
        }
        int n = s.length() - 2;
        if (n % 2 != 0) {
            throw new IllegalArgumentException("odd number of hex digits");
        }
        byte[] out = new byte[n / 2];
        for (int i = 0; i < out.length; i++) {
            int hi = Character.digit(s.charAt(2 + i * 2), 16);
            int lo = Character.digit(s.charAt(3 + i * 2), 16);
            if (hi < 0 || lo < 0) {
                throw new IllegalArgumentException("non-hex digit");
            }
            out[i] = (byte) (hi << 4 | lo);
        }
        return out;
    }
}
