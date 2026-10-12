// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;
import pro.surpin.pgpaimon.writer.dto.*;

/** The history's bucket count per daily partition: one formula, table-driven. */
class HistBucketsTest {

  static final long GIB = 1L << 30;

  @Test
  void forDayIsClampOneCeilBytesOverOneGibSixtyFour() {
    long[][] cases = {
        // bytes per day, buckets
        {-1, 1}, {0, 1}, {1, 1}, {GIB - 1, 1}, {GIB, 1}, {GIB + 1, 2}, {10 * GIB, 10},
        {10 * GIB + 1, 11}, {64 * GIB, 64}, {64 * GIB + 1, 64}, {Long.MAX_VALUE / 2, 64},};
    for (long[] c : cases) {
      assertEquals((int) c[1], HistBuckets.forDay(c[0]), "bytes per day " + c[0]);
    }
  }

  @Test
  void measuredNeedsThreeFullDaysAndAveragesTheLatestSeven() {
    assertTrue(HistBuckets.measured(Map.of()).isEmpty());
    assertTrue(
        HistBuckets.measured(Map.of(1, 50 * GIB, 2, 50 * GIB, 3, 50 * GIB, 4, 50 * GIB)).isEmpty(),
        "four days are two full ones: not enough to measure");
    assertEquals(3,
        HistBuckets.measured(Map.of(1, 0L, 2, 2 * GIB, 3, 3 * GIB, 4, 4 * GIB, 5, 0L)).getAsInt(),
        "average of the three full days, 3 GiB -> 3");
    Map<Integer, Long> m = new HashMap<>();
    for (int d = 1; d <= 11; d++) {
      m.put(d, d <= 3 ? 64 * GIB : 2 * GIB); // three huge old days fall out of the window
    }
    assertEquals(2, HistBuckets.measured(m).getAsInt(), "only the latest 7 full days count");
  }

  /** Review of the history tables, m5: the partial first and last days do not drag it down. */
  @Test
  void thePartialFirstAndLastDaysAreLeftOut() {
    Map<Integer, Long> m = new HashMap<>();
    m.put(1, GIB / 100); // registered late in the day
    for (int d = 2; d <= 6; d++) {
      m.put(d, 3 * GIB);
    }
    m.put(7, GIB / 100); // the rotation happened early today
    assertEquals(3, HistBuckets.measured(m).getAsInt());
  }
}
