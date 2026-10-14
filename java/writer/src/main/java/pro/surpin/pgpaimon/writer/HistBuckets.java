// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.Map;
import java.util.OptionalInt;
import java.util.TreeMap;

/**
 * The bucket count of a history table, which is PER DAILY PARTITION: a fixed-bucket Paimon table
 * has that many buckets in each {@code _dt} partition, so it is sized from one day's volume,
 * {@code clamp(MIN, ceil(bytes_per_day / TARGET), MAX)}. The same formula is
 * {@code paimon.hist_buckets_for()} in the extension, which add_table() uses; keep them in step.
 *
 * <p>Why these constants (measured on Paimon 1.4.1, 120 commits of 200 rows into a table
 * partitioned by day): after the writer's own compaction a bucket holds about four live files per
 * partition (1 bucket: 4 per day, 4 buckets: 16, 16 buckets: 64), so a table's file count grows as
 * buckets x days, and the writer is single-threaded, so more buckets buy no parallelism. Hence few,
 * full buckets: 1 GiB of history per bucket per day, at least one.
 */
public final class HistBuckets {

    public static final long TARGET_BYTES_PER_BUCKET_PER_DAY = 1L << 30;
    public static final int MIN = 1;
    public static final int MAX = 64;
    /** A measurement needs at least this many days of history, and looks at no more than WINDOW. */
    public static final int MIN_DAYS = 3;
    public static final int WINDOW = 7;

    private HistBuckets() {
    }

    /** The bucket count for a day's worth of {@code bytesPerDay} of history. */
    public static int forDay(long bytesPerDay) {
        long n = bytesPerDay <= 0 ? 0
            : (bytesPerDay + TARGET_BYTES_PER_BUCKET_PER_DAY - 1) / TARGET_BYTES_PER_BUCKET_PER_DAY;
        return (int) Math.max(MIN, Math.min(MAX, n));
    }

    /**
     * The bucket count measured from history already written: {@code bytesByDay} maps a
     * {@code _dt} (days since the epoch) to the data bytes of that day. The average over the most
     * recent {@link #WINDOW} days present; empty when fewer than {@link #MIN_DAYS} days exist.
     */
    public static OptionalInt measured(Map<Integer, Long> bytesByDay) {
        if (bytesByDay.size() < MIN_DAYS) {
            return OptionalInt.empty();
        }
        TreeMap<Integer, Long> sorted = new TreeMap<>(bytesByDay);
        long sum = 0;
        int n = 0;
        for (Long b : sorted.descendingMap().values()) {
            if (n == WINDOW) {
                break;
            }
            sum += b;
            n++;
        }
        return OptionalInt.of(forDay(sum / n));
    }
}
