// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.List;
import org.apache.paimon.data.*;
import org.apache.paimon.types.*;
import org.junit.jupiter.api.Test;

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
    assertEquals(Boolean.TRUE, TextDecoder.toPaimon(DataTypes.BOOLEAN(), "t"));
    assertEquals(Boolean.FALSE, TextDecoder.toPaimon(DataTypes.BOOLEAN(), "f"));
    assertEquals(Boolean.TRUE, TextDecoder.toPaimon(DataTypes.BOOLEAN(), "true"));
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
    assertEquals(Double.POSITIVE_INFINITY, TextDecoder.toPaimon(DataTypes.DOUBLE(), "Infinity"));
    assertEquals(Double.NEGATIVE_INFINITY, TextDecoder.toPaimon(DataTypes.DOUBLE(), "-Infinity"));
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
    assertArrayEquals(new byte[] {0x00, (byte) 0xff, 0x41},
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
    Timestamp ts =
        (Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "2026-10-09 13:46:40.123456");
    assertEquals("2026-10-09T13:46:40.123456", ts.toLocalDateTime().toString());
  }

  @Test
  void timestampWithoutFractionalPartWorks() {
    Timestamp ts = (Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "2026-10-09 13:46:40");
    assertEquals("2026-10-09T13:46:40", ts.toLocalDateTime().toString());
  }

  @Test
  void timestampWithLocalTimeZoneIsNormalisedToUtc() {
    Timestamp ts = (Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6),
        "2026-10-09 13:46:40+02");
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

  private static void assertRejected(
      DataType t,
      String text,
      String column
  ) {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> TextDecoder.toPaimon(column, t, text));
    assertTrue(e.getMessage().contains(column), e.getMessage());
    assertTrue(e.getMessage().contains(text), e.getMessage());
  }

  private static String tstz(
      String text
  ) {
    Timestamp ts =
        (Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), text);
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
    assertArrayEquals(new byte[] {0x41, 0x42, 0x0a},
        (byte[]) TextDecoder.toPaimon(DataTypes.BYTES(), "\\x41420a"));
    assertArrayEquals(new byte[0], (byte[]) TextDecoder.toPaimon(DataTypes.BYTES(), "\\x"));
    assertArrayEquals(new byte[] {(byte) 0xAB, (byte) 0xCD},
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
    assertEquals("12345.6789",
        ((Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(10, 4), "12345.6789")).toBigDecimal()
            .toPlainString());
    assertEquals("-0.50", ((Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(10, 2), "-0.5"))
        .toBigDecimal().toPlainString());
    assertEquals("0.00", ((Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(10, 2), "0"))
        .toBigDecimal().toPlainString());
  }

  @Test
  void highPrecisionDecimalIsNotRoutedThroughDouble() {
    String s = "12345678901234567890.123456789012345678";
    Decimal d = (Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(38, 18), s);
    assertEquals(s, d.toBigDecimal().toPlainString());
  }

  @Test
  void decimalThatWouldLoseDigitsIsRejected() {
    assertRejected(DataTypes.DECIMAL(10, 2), "1.234", "amt"); // scale loss
    assertRejected(DataTypes.DECIMAL(5, 2), "12345.67", "amt"); // precision overflow
  }

  @Test
  void decimalNaNAndInfinityAreRejected() {
    assertRejected(DataTypes.DECIMAL(10, 2), "NaN", "amt");
    assertRejected(DataTypes.DECIMAL(10, 2), "Infinity", "amt");
    assertRejected(DataTypes.DECIMAL(10, 2), "-Infinity", "amt");
  }

  /*
   * "infinity" / "-infinity" are what PostgreSQL 18 prints for these values. Every bound below was
   * measured, not chosen: PostgreSQL's own extremes come from the live server ('4714-11-23
   * BC'::date is "date out of range", so 4714-11-24 BC is the floor), and the writable timestamp
   * maximum was computed from Paimon's toMicros() arithmetic.
   */

  /** The largest instant Paimon can WRITE: Long.MAX_VALUE micros after the epoch. */
  private static final Timestamp MAX_TS =
      Timestamp.fromLocalDateTime(java.time.LocalDateTime.of(294247, 1, 10, 4, 0, 54, 775_807_000));

  /** PostgreSQL's own minimum, 4714-11-24 BC, which is proleptic year -4713. */
  private static final Timestamp MIN_TS =
      Timestamp.fromLocalDateTime(java.time.LocalDateTime.of(-4713, 11, 24, 0, 0));

  @Test
  void timestampInfinityMapsToTheExtremeWritableTimestamps() {
    for (DataType t : new DataType[] {DataTypes.TIMESTAMP(6),
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)}) {
      assertEquals(MAX_TS, TextDecoder.toPaimon(t, "infinity"));
      assertEquals(MIN_TS, TextDecoder.toPaimon(t, "-infinity"));
      assertEquals("+294247-01-10T04:00:54.775807",
          ((Timestamp) TextDecoder.toPaimon(t, "infinity")).toLocalDateTime().toString());
      assertEquals("-4713-11-24T00:00",
          ((Timestamp) TextDecoder.toPaimon(t, "-infinity")).toLocalDateTime().toString());
    }
  }

  /**
   * The bound itself, from Paimon's arithmetic: {@code toMicros()} is {@code
   * multiplyExact(millisecond, 1000) + nanoOfMillisecond / 1000}, so the largest writable instant
   * is Long.MAX_VALUE micros after the epoch. One microsecond later the unchecked add WRAPS -- it
   * does not throw -- which is exactly why the earlier verification of this mapping passed while
   * the writer could not store the value.
   */
  @Test
  void theEncodedInfinityIsTheLargestInstantToMicrosCanReturn() {
    assertEquals(Long.MAX_VALUE, MAX_TS.toMicros());
    assertEquals("+294247-01-10T04:00:54.775807", MAX_TS.toLocalDateTime().toString());

    Timestamp oneMicroMore = Timestamp.fromLocalDateTime(MAX_TS.toLocalDateTime().plusNanos(1000));
    long wrapped = oneMicroMore.toMicros();
    assertTrue(wrapped < 0,
        "one micro past the bound wraps silently rather than throwing, which is the trap:" + " got "
            + wrapped);

    // and a second past it throws outright, so both failure shapes are real
    Timestamp oneSecondMore = Timestamp.fromLocalDateTime(MAX_TS.toLocalDateTime().plusSeconds(1));
    assertThrows(ArithmeticException.class, oneSecondMore::toMicros);
  }

  /**
   * PostgreSQL's maximum is about thirty years ABOVE what Paimon can write, so those timestamps
   * must be refused here -- as the skippable, diagnosable type -- rather than failing inside
   * Parquet's writer where nothing catches them.
   */
  @Test
  void aTimestampAbovePaimonsRangeIsRefusedAndNotPassedOn() {
    for (DataType t : new DataType[] {DataTypes.TIMESTAMP(6),
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)}) {
      for (String tooBig : new String[] {"294276-12-31 23:59:59.999999", // PG's own max
          "294247-01-10 04:00:54.775808", // one micro over
          "294247-01-10 04:00:55"}) {
        String text =
            t.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE ? tooBig + "+00"
                : tooBig;
        TextDecoder.UnrepresentableValueException e =
            assertThrows(TextDecoder.UnrepresentableValueException.class,
                () -> TextDecoder.toPaimon("ts", t, text), tooBig + " must be refused for " + t);
        assertTrue(e.getMessage().contains("294247-01-10 04:00:54.775807"), e.getMessage());
      }
    }
    // and the bound itself is accepted
    assertEquals(MAX_TS,
        TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "294247-01-10 04:00:54.775807"));
  }

  @Test
  void dateInfinityMapsToPostgresOwnExtremeDates() {
    assertEquals(2145042905, TextDecoder.toPaimon(DataTypes.DATE(), "infinity"));
    assertEquals(-2440588, TextDecoder.toPaimon(DataTypes.DATE(), "-infinity"));
    assertEquals((int) LocalDate.of(-4713, 11, 24).toEpochDay(),
        TextDecoder.toPaimon(DataTypes.DATE(), "-infinity"));
    // PostgreSQL's whole date range fits in Paimon's int, with room to spare
    assertTrue(2145042905 < Integer.MAX_VALUE);
  }

  /**
   * The invariant the encoding exists for, asserted against the TRUE extremes rather than against
   * an arbitrary modern date. The previous version of this test compared only with 2026-10-09, so
   * it passed with a -infinity constant 38 days above PostgreSQL's real minimum -- 38 legitimate
   * dates sorted the wrong side of it.
   */
  @Test
  void infinitySortsOutsideEveryValuePostgresCanHold() {
    // dates: PostgreSQL's extremes are the encoding's targets, so use the adjacent values
    int posD = (int) TextDecoder.toPaimon(DataTypes.DATE(), "infinity");
    int negD = (int) TextDecoder.toPaimon(DataTypes.DATE(), "-infinity");
    assertEquals(posD, (int) TextDecoder.toPaimon(DataTypes.DATE(), "5874897-12-31"),
        "the encoding IS the maximum, so they coincide: that is the lossiness");
    assertEquals(negD, (int) TextDecoder.toPaimon(DataTypes.DATE(), "4714-11-24 BC"));
    assertTrue(posD > (int) TextDecoder.toPaimon(DataTypes.DATE(), "5874897-12-30"));
    assertTrue(negD < (int) TextDecoder.toPaimon(DataTypes.DATE(), "4714-11-25 BC"));
    assertTrue(negD < (int) TextDecoder.toPaimon(DataTypes.DATE(), "4714-12-31 BC"),
        "4714-12-31 BC is earlier than the old constant and must still sort above" + " -infinity");
    assertTrue(negD < (int) TextDecoder.toPaimon(DataTypes.DATE(), "4713-01-01 BC"));

    // timestamps: everything above the positive target is refused, so nothing in Paimon can
    // outrank it; below, PostgreSQL's own minimum is the target
    Timestamp pos = (Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "infinity");
    Timestamp neg = (Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "-infinity");
    for (String v : new String[] {"2026-10-09 13:46:40", "294247-01-10 04:00:54.775806",
        "4714-11-24 00:00:00.000001 BC", "4714-12-31 00:00:00 BC", "4713-01-01 00:00:00 BC",
        "0001-01-01 00:00:00"}) {
      Timestamp t = (Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), v);
      assertTrue(pos.compareTo(t) > 0, v + " must sort below +infinity");
      assertTrue(neg.compareTo(t) < 0, v + " must sort above -infinity");
    }
    assertEquals(neg, TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "4714-11-24 00:00:00 BC"),
        "the encoding IS the minimum, so they coincide: the lossiness again");
  }

  @Test
  void theFiniteExtremesParseAsThemselvesSoBothPathsAgree() {
    assertEquals(MAX_TS,
        TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "294247-01-10 04:00:54.775807"));
    assertEquals(MIN_TS, TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "4714-11-24 00:00:00 BC"));
    assertEquals(TextDecoder.toPaimon(DataTypes.DATE(), "infinity"),
        TextDecoder.toPaimon(DataTypes.DATE(), "5874897-12-31"));
    assertEquals(TextDecoder.toPaimon(DataTypes.DATE(), "-infinity"),
        TextDecoder.toPaimon(DataTypes.DATE(), "4714-11-24 BC"));
  }

  // Strings below were produced by PostgreSQL 18 (::text), not composed by hand.
  @Test
  void bcDatesBecomeProlepticEpochDays() {
    assertEquals(-735160, TextDecoder.toPaimon(DataTypes.DATE(), "0044-03-15 BC"));
    assertEquals((int) LocalDate.of(-43, 3, 15).toEpochDay(),
        TextDecoder.toPaimon(DataTypes.DATE(), "0044-03-15 BC"));
    assertEquals((int) LocalDate.of(-4712, 1, 1).toEpochDay(),
        TextDecoder.toPaimon(DataTypes.DATE(), "4713-01-01 BC"));
  }

  @Test
  void yearZeroBoundaryIsPinnedFromBothSides() {
    assertEquals((int) LocalDate.of(0, 1, 1).toEpochDay(),
        TextDecoder.toPaimon(DataTypes.DATE(), "0001-01-01 BC"));
    assertEquals((int) LocalDate.of(1, 1, 1).toEpochDay(),
        TextDecoder.toPaimon(DataTypes.DATE(), "0001-01-01"));
    assertEquals(366, (int) TextDecoder.toPaimon(DataTypes.DATE(), "0001-01-01")
        - (int) TextDecoder.toPaimon(DataTypes.DATE(), "0001-01-01 BC"));
  }

  @Test
  void bcLeapDayUsesProlepticLeapRule() {
    // 45 BC = proleptic -44, a leap year; PostgreSQL accepts 0045-02-29 BC
    assertEquals((int) LocalDate.of(-44, 2, 29).toEpochDay(),
        TextDecoder.toPaimon(DataTypes.DATE(), "0045-02-29 BC"));
    assertRejected(DataTypes.DATE(), "0044-02-29 BC", "d"); // 44 BC = -43, not leap
  }

  @Test
  void bcTimestampsKeepTimeAndFraction() {
    assertEquals("-0043-03-15T10:20:30",
        ((Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "0044-03-15 10:20:30 BC"))
            .toLocalDateTime().toString());
    assertEquals("-0043-03-15T10:20:30.500",
        ((Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "0044-03-15 10:20:30.5 BC"))
            .toLocalDateTime().toString());
    assertEquals("0000-01-01T00:00",
        ((Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "0001-01-01 00:00:00 BC"))
            .toLocalDateTime().toString());
    assertEquals("0001-01-01T00:00",
        ((Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "0001-01-01 00:00:00"))
            .toLocalDateTime().toString());
  }

  @Test
  void bcTimestamptzAppliesOffsetAfterTheYearConversion() {
    assertEquals("-0043-03-15T10:20:30", tstz("0044-03-15 10:20:30+00 BC"));
    assertEquals("0000-01-01T00:00", tstz("0001-01-01 00:00:00+00 BC"));
    // server rendering of the same instant in Asia/Kathmandu (LMT +05:41:16)
    assertEquals("-0043-03-15T10:20:30", tstz("0044-03-15 16:01:46+05:41:16 BC"));
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

  @Test
  void nonexistentDateIsRejectedForEveryTemporalType() {
    assertRejected(DataTypes.DATE(), "2024-02-30", "d");
    assertRejected(DataTypes.TIMESTAMP(6), "2024-02-30 00:00:00", "ts");
    assertRejected(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), "2024-02-30 00:00:00+00", "tstz");
    // a real leap day still works
    assertEquals("2024-02-29T00:00",
        ((Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "2024-02-29 00:00:00"))
            .toLocalDateTime().toString());
  }

  @Test
  void decimalBoundaryIsExactlyAtThePrecisionLimit() {
    assertEquals("999.99", ((Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(5, 2), "999.99"))
        .toBigDecimal().toPlainString());
    assertEquals("-999.99", ((Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(5, 2), "-999.99"))
        .toBigDecimal().toPlainString());
    assertRejected(DataTypes.DECIMAL(5, 2), "1000.00", "amt");
  }

  // Server-produced extremes (PostgreSQL 18).
  @Test
  void postgresMaximumDateParses() {
    assertEquals(2145042905, TextDecoder.toPaimon(DataTypes.DATE(), "5874897-12-31"));
    assertEquals((int) LocalDate.of(10000, 1, 1).toEpochDay(),
        TextDecoder.toPaimon(DataTypes.DATE(), "10000-01-01"));
    assertEquals((int) LocalDate.of(9999, 12, 31).toEpochDay(),
        TextDecoder.toPaimon(DataTypes.DATE(), "9999-12-31"));
    assertEquals(-735160, TextDecoder.toPaimon(DataTypes.DATE(), "0044-03-15 BC"));
    assertEquals((int) LocalDate.of(-4713, 11, 24).toEpochDay(),
        TextDecoder.toPaimon(DataTypes.DATE(), "4714-11-24 BC"));
  }

  @Test
  void largeYearTimestampsParse() {
    // Years above 9999, which ISO_LOCAL_DATE and a fixed "uuuu" both refuse, parse here --
    // up to the range Paimon can store. PostgreSQL's own maximum parses as TEXT and is then
    // refused as unstorable, which is a separate test; these are all inside the bound.
    assertEquals("+294247-01-10T04:00:54.775806",
        ((Timestamp) TextDecoder.toPaimon(DataTypes.TIMESTAMP(6), "294247-01-10 04:00:54.775806"))
            .toLocalDateTime().toString());
    assertEquals("+294247-01-10T04:00:54.775806", tstz("294247-01-10 04:00:54.775806+00"));
    // a Kathmandu session renders the same instant in the following local day
    assertEquals("+290000-06-14T23:59:59", tstz("290000-06-15 05:44:59+05:45"));
    assertEquals("+10000-01-01T00:00", tstz("10000-01-01 05:45:00+05:45"));
    // and the offset arithmetic is what decides it: this one crosses the bound only in UTC
    assertThrows(TextDecoder.UnrepresentableValueException.class,
        () -> TextDecoder.toPaimon(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6),
            "294247-01-10 09:45:55+05:45"),
        "04:00:55 UTC is one second past the bound, however it was written");
  }

  @Test
  void invalidHighYearDatesAreStillRejected() {
    assertRejected(DataTypes.DATE(), "10000-02-30", "d");
    assertRejected(DataTypes.DATE(), "+10000-01-01", "d");
  }

  /*
   * The type is the contract PGPAIMON_SKIP_LSN is keyed on, so which failures are
   * UnrepresentableValueException and which are not is as load-bearing as the messages.
   */

  @Test
  void onlyAValuePaimonCannotHoldIsUnrepresentable() {
    DataType dec = DataTypes.DECIMAL(10, 2);
    // a well-formed PostgreSQL numeric with no Paimon counterpart: skippable
    assertThrows(TextDecoder.UnrepresentableValueException.class,
        () -> TextDecoder.toPaimon("v", dec, "NaN"));
    assertThrows(TextDecoder.UnrepresentableValueException.class,
        () -> TextDecoder.toPaimon("v", dec, "Infinity"));
    assertThrows(TextDecoder.UnrepresentableValueException.class,
        () -> TextDecoder.toPaimon("v", dec, "-Infinity"));
    // more integer digits than the column holds
    assertThrows(TextDecoder.UnrepresentableValueException.class,
        () -> TextDecoder.toPaimon("v", dec, "123456789.00"));
    // more decimal places than the column holds: digits would be lost, never rounded
    assertThrows(TextDecoder.UnrepresentableValueException.class,
        () -> TextDecoder.toPaimon("v", dec, "1.005"));
  }

  @Test
  void aParseFailureIsNotUnrepresentableAndSoIsNotSkippable() {
    // Each of these means the text is not of this type at all -- a drifted schema, a session
    // setting, a protocol fault -- which would mis-handle EVERY row of the column. Skipping
    // one would hide the cause, so they must not be the skippable type.
    record Case(
        DataType type,
        String text
    ) {
    }
    for (Case c : List.of(new Case(DataTypes.DECIMAL(10, 2), "not-a-number"),
        new Case(DataTypes.INT(), "abc"), new Case(DataTypes.BIGINT(), ""),
        new Case(DataTypes.BOOLEAN(), "maybe"), new Case(DataTypes.BYTES(), "abc"), // bytea_output
                                                                                    // = 'escape'
        new Case(DataTypes.BYTES(), "\\xabc"), // odd number of hex digits
        new Case(DataTypes.DATE(), "the 5th"), new Case(DataTypes.TIMESTAMP(), "yesterday"))) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> TextDecoder.toPaimon("v", c.type(), c.text()));
      assertFalse(e instanceof TextDecoder.UnrepresentableValueException,
          "a " + c.type() + " from \"" + c.text() + "\" is a parse failure, not an"
              + " unrepresentable value: " + e.getMessage());
    }
    // and NULL for a NOT NULL column is a schema disagreement, not one bad value
    IllegalArgumentException nul = assertThrows(IllegalArgumentException.class,
        () -> TextDecoder.toPaimon("v", DataTypes.INT().notNull(), null));
    assertFalse(nul instanceof TextDecoder.UnrepresentableValueException, nul.getMessage());
  }

  @Test
  void theUnrepresentableMessageStillNamesTheColumnTheTypeAndTheValue() {
    TextDecoder.UnrepresentableValueException e =
        assertThrows(TextDecoder.UnrepresentableValueException.class,
            () -> TextDecoder.toPaimon("amt", DataTypes.DECIMAL(10, 2), "NaN"));
    assertTrue(e.getMessage().contains("column 'amt'"), e.getMessage());
    assertTrue(e.getMessage().contains("DECIMAL(10, 2)"), e.getMessage());
    assertTrue(e.getMessage().contains("\"NaN\""), e.getMessage());
    assertTrue(e.getMessage().contains("no Paimon representation"), e.getMessage());
  }
}
