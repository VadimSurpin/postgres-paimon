// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

/**
 * The TPC-C comparator's history checks against tampered histories, without a warehouse.
 *
 * <p>The review of the history tables (finding I1) rewrote a passing TPC-C history in place and
 * ran the comparator on it: every -U carrying its +U's values, every _ts and _dt a day late, every
 * _xid 0 -- each PASSED. Those three tamperings, and the three it did catch, are replayed here
 * against {@link TpccCompare#verifyHistory}; the untampered history must pass, each tampered one
 * must fail. Runs without a live stack (it is in ALL_SUITES so the harness runs it too).
 */
public class TpccCompareSelfTest {

    static final List<String> COLS = List.of("w_id", "w_ytd");
    static final int[] PK = {0};
    static final long DAY = 86_400_000_000L;
    static final long T0 = 1_760_000_000_000_000L;          // 2025-10-09, microseconds
    static final TpccCompare.Bounds RUN = new TpccCompare.Bounds(T0, T0 + 3_600_000_000L, 1000, 2000);

    static TpccCompare.HistRow row(long commit, int opSeq, long lsn, long xid, String op,
                                   long ts, String... image) {
        return new TpccCompare.HistRow(commit, opSeq, lsn, xid, op, image.length == 0 ? null : image,
                                       ts, (int) Math.floorDiv(ts, DAY));
    }

    /** Insert w 1 and 2; update w 1 twice (two transactions); delete w 2. PostgreSQL: (1, 30). */
    static List<TpccCompare.HistRow> history() {
        List<TpccCompare.HistRow> h = new ArrayList<>();
        h.add(row(100, 0, 90, 1001, "+I", T0 + 1, "1", "10.00"));
        h.add(row(100, 1, 91, 1001, "+I", T0 + 1, "2", "5.00"));
        h.add(row(200, 0, 190, 1002, "-U", T0 + 2, "1", "10.00"));
        h.add(row(200, 1, 190, 1002, "+U", T0 + 2, "1", "20.00"));
        h.add(row(300, 0, 290, 1003, "-U", T0 + 3, "1", "20.00"));
        h.add(row(300, 1, 290, 1003, "+U", T0 + 3, "1", "30.00"));
        h.add(row(300, 2, 291, 1003, "-D", T0 + 3, "2", "5.00"));
        return h;
    }

    static final List<String[]> PG = List.<String[]>of(new String[] {"1", "30.00"});

    static List<String> verify(List<TpccCompare.HistRow> h, TpccCompare.Bounds b) {
        List<String> failures = new ArrayList<>();
        TpccCompare.verifyHistory(h, PG, COLS, PK, Set.of("w_id"), b, 10, failures);
        return failures;
    }

    static List<TpccCompare.HistRow> map(UnaryOperator<TpccCompare.HistRow> f) {
        List<TpccCompare.HistRow> out = new ArrayList<>();
        for (TpccCompare.HistRow r : history()) out.add(f.apply(r));
        return out;
    }

    @Test
    void theTrueHistoryPasses() {
        assertEquals(List.of(), verify(history(), RUN));
    }

    @Test
    void withoutARunWindowItFails() {
        assertFalse(verify(history(), null).isEmpty(), "unchecked _ts/_xid must not pass");
    }

    /** Was PASS: every -U carries its +U's values (the after-image). */
    @Test
    void aBeforeImageThatIsTheAfterImageFails() {
        List<TpccCompare.HistRow> h = history();
        for (int i = 0; i < h.size(); i++) {
            TpccCompare.HistRow r = h.get(i);
            if (r.op().equals("-U")) {
                TpccCompare.HistRow next = h.get(i + 1);
                h.set(i, new TpccCompare.HistRow(r.commitLsn(), r.opSeq(), r.lsn(), r.xid(), "-U",
                    next.image(), r.tsMicros(), r.dt()));
            }
        }
        List<String> f = verify(h, RUN);
        assertTrue(f.stream().anyMatch(x -> x.contains("before-image")), f.toString());
    }

    /** Was PASS: every _ts and _dt one day later (consistent with each other). */
    @Test
    void aCommitTimeOutsideTheRunFails() {
        List<String> f = verify(map(r -> new TpccCompare.HistRow(r.commitLsn(), r.opSeq(), r.lsn(),
            r.xid(), r.op(), r.image(), r.tsMicros() + DAY, r.dt() + 1)), RUN);
        assertTrue(f.stream().anyMatch(x -> x.contains("_ts is outside")), f.toString());
    }

    /** Was PASS: every _xid set to 0. */
    @Test
    void aTransactionIdOutsideTheRunFails() {
        List<String> f = verify(map(r -> new TpccCompare.HistRow(r.commitLsn(), r.opSeq(), r.lsn(),
            0, r.op(), r.image(), r.tsMicros(), r.dt())), RUN);
        assertTrue(f.stream().anyMatch(x -> x.contains("_xid is outside")), f.toString());
    }

    /** One row of a transaction with another _xid or _ts than the rest of it. */
    @Test
    void aTransactionSplitAcrossTwoXidsFails() {
        List<TpccCompare.HistRow> h = history();
        TpccCompare.HistRow r = h.get(6);
        h.set(6, new TpccCompare.HistRow(r.commitLsn(), r.opSeq(), r.lsn(), 1500, r.op(), r.image(),
            r.tsMicros(), r.dt()));
        List<String> f = verify(h, RUN);
        assertTrue(f.stream().anyMatch(x -> x.contains("differs from another row of the same commit")),
            f.toString());
    }

    /** Caught before too: _op_seq of a -U/+U pair swapped. */
    @Test
    void aSwappedPairFails() {
        List<TpccCompare.HistRow> h = history();
        TpccCompare.HistRow mu = h.get(2), pu = h.get(3);
        h.set(2, new TpccCompare.HistRow(mu.commitLsn(), pu.opSeq(), mu.lsn(), mu.xid(), "-U",
            mu.image(), mu.tsMicros(), mu.dt()));
        h.set(3, new TpccCompare.HistRow(pu.commitLsn(), mu.opSeq(), pu.lsn(), pu.xid(), "+U",
            pu.image(), pu.tsMicros(), pu.dt()));
        assertFalse(verify(h, RUN).isEmpty());
    }

    /** Caught before too: one -U/+U pair dropped. */
    @Test
    void aDroppedPairFails() {
        List<TpccCompare.HistRow> h = history();
        h.remove(3);
        h.remove(2);
        assertFalse(verify(h, RUN).isEmpty());
    }

    /** An extra no-op pair under a new commit LSN: the full comparator catches it by counts. */
    @Test
    void anExtraNoOpPairIsLeftToTheChangelogCounts() {
        List<TpccCompare.HistRow> h = history();
        h.add(row(301, 0, 300, 1004, "-U", T0 + 4, "1", "30.00"));
        h.add(row(301, 1, 300, 1004, "+U", T0 + 4, "1", "30.00"));
        // a no-op pair still rebuilds PostgreSQL's row, so only the changelog counts in the full
        // comparator catch it; here, at least, it must not break a true history's other checks
        assertEquals(List.of(), verify(h, RUN));
    }

    @Test
    void xidWindowAllowsOneWraparound() {
        TpccCompare.Bounds wrap = new TpccCompare.Bounds(0, 1, 4_294_967_000L, 500);
        assertTrue(wrap.xidOk(4_294_967_100L));
        assertTrue(wrap.xidOk(400));
        assertFalse(wrap.xidOk(1000));
        assertFalse(wrap.xidOk(0), "0 is InvalidTransactionId");
    }
}
