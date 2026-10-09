// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.data.*;
import org.apache.paimon.types.*;
import org.junit.jupiter.api.Test;
import java.time.LocalDate;
import static org.junit.jupiter.api.Assertions.*;

class TextDecoderTest {

    @Test
    void nullTextBecomesNullForEveryType() {
        assertNull(TextDecoder.toPaimon(DataTypes.INT(), null));
        assertNull(TextDecoder.toPaimon(DataTypes.STRING(), null));
        assertNull(TextDecoder.toPaimon(DataTypes.TIMESTAMP(), null));
    }

    @Test
    void emptyStringIsAnEmptyStringNotNull() {
        Object v = TextDecoder.toPaimon(DataTypes.STRING(), "");
        assertEquals(BinaryString.fromString(""), v);
        assertNotNull(v);
    }

    @Test
    void booleans() {
        assertEquals(Boolean.TRUE,  TextDecoder.toPaimon(DataTypes.BOOLEAN(), "t"));
        assertEquals(Boolean.FALSE, TextDecoder.toPaimon(DataTypes.BOOLEAN(), "f"));
        assertEquals(Boolean.TRUE,  TextDecoder.toPaimon(DataTypes.BOOLEAN(), "true"));
    }

    @Test
    void integers() {
        assertEquals((short) -7, TextDecoder.toPaimon(DataTypes.SMALLINT(), "-7"));
        assertEquals(42, TextDecoder.toPaimon(DataTypes.INT(), "42"));
        assertEquals(9223372036854775807L,
                     TextDecoder.toPaimon(DataTypes.BIGINT(), "9223372036854775807"));
    }

    @Test
    void floatsRoundTripAtExtraFloatDigitsThree() {
        assertEquals(0.1f, (Float) TextDecoder.toPaimon(DataTypes.FLOAT(), "0.1"));
        assertEquals(1.2345678901234567e300,
                     (Double) TextDecoder.toPaimon(DataTypes.DOUBLE(), "1.2345678901234567e+300"));
        assertTrue(Double.isNaN((Double) TextDecoder.toPaimon(DataTypes.DOUBLE(), "NaN")));
        assertEquals(Double.POSITIVE_INFINITY,
                     TextDecoder.toPaimon(DataTypes.DOUBLE(), "Infinity"));
        assertEquals(Double.NEGATIVE_INFINITY,
                     TextDecoder.toPaimon(DataTypes.DOUBLE(), "-Infinity"));
    }

    @Test
    void decimalKeepsPrecisionAndScale() {
        Decimal d = (Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(10, 2), "123.45");
        assertEquals("123.45", d.toBigDecimal().toPlainString());
        assertEquals(10, d.precision());
        assertEquals(2, d.scale());
        Decimal z = (Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(10, 2), "0.1");
        assertEquals("0.10", z.toBigDecimal().toPlainString());
    }

    @Test
    void byteaHexBecomesBytes() {
        assertArrayEquals(new byte[] { 0x00, (byte) 0xff, 0x41 },
                          (byte[]) TextDecoder.toPaimon(DataTypes.BYTES(), "\\x00ff41"));
        assertArrayEquals(new byte[0], (byte[]) TextDecoder.toPaimon(DataTypes.BYTES(), "\\x"));
    }

    @Test
    void dateBecomesEpochDay() {
        assertEquals((int) LocalDate.of(2026, 10, 9).toEpochDay(),
                     TextDecoder.toPaimon(DataTypes.DATE(), "2026-10-09"));
    }

    @Test
    void timestampKeepsMicroseconds() {
        Timestamp ts = (Timestamp) TextDecoder.toPaimon(
            DataTypes.TIMESTAMP(6), "2026-10-09 13:46:40.123456");
        assertEquals("2026-10-09T13:46:40.123456", ts.toLocalDateTime().toString());
    }

    @Test
    void timestampWithoutFractionalPartWorks() {
        Timestamp ts = (Timestamp) TextDecoder.toPaimon(
            DataTypes.TIMESTAMP(6), "2026-10-09 13:46:40");
        assertEquals("2026-10-09T13:46:40", ts.toLocalDateTime().toString());
    }

    @Test
    void timestampWithLocalTimeZoneIsNormalisedToUtc() {
        Timestamp ts = (Timestamp) TextDecoder.toPaimon(
            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), "2026-10-09 13:46:40+02");
        assertEquals("2026-10-09T11:46:40", ts.toLocalDateTime().toString());
    }

    @Test
    void anUnparseableValueNamesTheTypeAndTheText() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> TextDecoder.toPaimon(DataTypes.INT(), "not-a-number"));
        assertTrue(e.getMessage().contains("INT"));
        assertTrue(e.getMessage().contains("not-a-number"));
    }

    // ---- additions beyond the brief ----

    private static void assertRejected(DataType t, String text, String column) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> TextDecoder.toPaimon(column, t, text));
        assertTrue(e.getMessage().contains(column), e.getMessage());
        assertTrue(e.getMessage().contains(text), e.getMessage());
    }

    private static String tstz(String text) {
        Timestamp ts = (Timestamp) TextDecoder.toPaimon(
            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), text);
        return ts.toLocalDateTime().toString();
    }

    @Test
    void offsetPlus00() {
        assertEquals("2024-03-01T12:34:56.789", tstz("2024-03-01 12:34:56.789+00"));
    }

    @Test
    void offsetPlus03() {
        assertEquals("2024-03-01T09:34:56", tstz("2024-03-01 12:34:56+03"));
    }

    @Test
    void offsetPlus0545() {
        assertEquals("2024-06-30T18:15", tstz("2024-07-01 00:00:00+05:45"));
    }

    @Test
    void offsetPlus054116() {
        // LMT +05:41:16 -> 1899-12-31T18:18:44
        assertEquals("1899-12-31T18:18:44", tstz("1900-01-01 00:00:00+05:41:16"));
    }

    @Test
    void negativeOffsetsParse() {
        assertEquals("2024-03-01T17:00", tstz("2024-03-01 12:00:00-05"));
        assertEquals("2024-03-01T15:30", tstz("2024-03-01 12:00:00-03:30"));
    }

    @Test
    void nullAndEmptyStringAreDifferentInBothDirections() {
        Object nul = TextDecoder.toPaimon(DataTypes.STRING(), null);
        Object empty = TextDecoder.toPaimon(DataTypes.STRING(), "");
        assertNull(nul);
        assertNotNull(empty);
        assertNotEquals(nul, empty);
        assertEquals(0, ((BinaryString) empty).numChars());
        // a non-empty value is neither
        assertNotEquals(empty, TextDecoder.toPaimon(DataTypes.STRING(), " "));
    }

    @Test
    void nullForNotNullTypeIsRejectedNamingTheColumn() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> TextDecoder.toPaimon("id", DataTypes.INT().notNull(), null));
        assertTrue(e.getMessage().contains("id"), e.getMessage());
    }

    @Test
    void byteaDecodedBytesFromTheWireStrings() {
        assertArrayEquals(new byte[] { 0x41, 0x42, 0x0a },
            (byte[]) TextDecoder.toPaimon(DataTypes.BYTES(), "\\x41420a"));
        assertArrayEquals(new byte[0], (byte[]) TextDecoder.toPaimon(DataTypes.BYTES(), "\\x"));
        assertArrayEquals(new byte[] { (byte) 0xAB, (byte) 0xCD },
            (byte[]) TextDecoder.toPaimon(DataTypes.BYTES(), "\\xABcd"));
    }

    @Test
    void malformedByteaIsRejected() {
        assertRejected(DataTypes.BYTES(), "\\x4", "b");
        assertRejected(DataTypes.BYTES(), "\\x+1", "b");
        assertRejected(DataTypes.BYTES(), "4142", "b");
    }

    @Test
    void booleanGarbageIsRejectedNotFalse() {
        assertRejected(DataTypes.BOOLEAN(), "maybe", "flag");
        assertEquals(Boolean.FALSE, TextDecoder.toPaimon(DataTypes.BOOLEAN(), "false"));
    }

    @Test
    void numericWireStrings() {
        assertEquals("12345.6789", ((Decimal) TextDecoder.toPaimon(
            DataTypes.DECIMAL(10, 4), "12345.6789")).toBigDecimal().toPlainString());
        assertEquals("-0.50", ((Decimal) TextDecoder.toPaimon(
            DataTypes.DECIMAL(10, 2), "-0.5")).toBigDecimal().toPlainString());
        assertEquals("0.00", ((Decimal) TextDecoder.toPaimon(
            DataTypes.DECIMAL(10, 2), "0")).toBigDecimal().toPlainString());
    }

    @Test
    void highPrecisionDecimalIsNotRoutedThroughDouble() {
        String s = "12345678901234567890.123456789012345678";
        Decimal d = (Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(38, 18), s);
        assertEquals(s, d.toBigDecimal().toPlainString());
    }

    @Test
    void decimalThatWouldLoseDigitsIsRejected() {
        assertRejected(DataTypes.DECIMAL(10, 2), "1.234", "amt");      // scale loss
        assertRejected(DataTypes.DECIMAL(5, 2), "12345.67", "amt");    // precision overflow
    }

    @Test
    void decimalNaNAndInfinityAreRejected() {
        assertRejected(DataTypes.DECIMAL(10, 2), "NaN", "amt");
        assertRejected(DataTypes.DECIMAL(10, 2), "Infinity", "amt");
        assertRejected(DataTypes.DECIMAL(10, 2), "-Infinity", "amt");
    }

    @Test
    void dateAndTimestampInfinityAreRejected() {
        for (String v : new String[] { "infinity", "-infinity" }) {
            assertRejected(DataTypes.DATE(), v, "d");
            assertRejected(DataTypes.TIMESTAMP(6), v, "ts");
            assertRejected(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), v, "tstz");
        }
    }

    @Test
    void bcDatesAreRejectedNotStripped() {
        assertRejected(DataTypes.DATE(), "0044-03-15 BC", "d");
        assertRejected(DataTypes.TIMESTAMP(6), "0044-03-15 10:00:00 BC", "ts");
        assertRejected(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), "0044-03-15 10:00:00+00 BC", "tstz");
    }

    @Test
    void floatSpecialsPassThroughForFloatAndDouble() {
        assertTrue(Float.isNaN((Float) TextDecoder.toPaimon(DataTypes.FLOAT(), "NaN")));
        assertEquals(Float.POSITIVE_INFINITY, TextDecoder.toPaimon(DataTypes.FLOAT(), "Infinity"));
        assertEquals(Float.NEGATIVE_INFINITY, TextDecoder.toPaimon(DataTypes.FLOAT(), "-Infinity"));
    }

    @Test
    void columnNameAppearsInGenericParseErrors() {
        assertRejected(DataTypes.INT(), "zzz", "qty");
    }
}
