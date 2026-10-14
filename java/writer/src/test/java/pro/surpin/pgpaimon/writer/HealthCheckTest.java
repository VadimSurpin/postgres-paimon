// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** The writer's use of paimon.health(); the checks themselves are pg_regress-tested. */
class HealthCheckTest {

    @Test
    void anyProblemIsFatalAndNamedInFull() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> HealthCheck.requireHealthy(List.of("event trigger paimon_ddl_end is missing",
                "replicated table t does not have REPLICA IDENTITY FULL"), "at startup"));
        assertTrue(e.getMessage().startsWith("paimon.health() reports 2 problem(s) at startup:"),
            e.getMessage());
        assertTrue(e.getMessage().contains("paimon_ddl_end is missing"));
        assertTrue(e.getMessage().contains("REPLICA IDENTITY FULL"));
        assertTrue(e.getMessage().contains("SELECT * FROM paimon.health()"));
    }

    @Test
    void noProblemIsHealthy() {
        assertDoesNotThrow(() -> HealthCheck.requireHealthy(List.of(), "at startup"));
    }

    /** The periodic check opens a connection at most once per interval, and never when off. */
    @Test
    void thePeriodicCheckRunsAtMostOncePerInterval() {
        AtomicLong now = new AtomicLong(0);
        AtomicInteger opened = new AtomicInteger();
        HealthCheck.ConnectionSource src = () -> {
            opened.incrementAndGet();
            throw new SQLException("unreachable", "08006");   // unreachable: only logged
        };
        HealthCheck h = new HealthCheck(src, 1_000, now::get);
        h.maybeCheck();
        assertEquals(0, opened.get(), "not before the first interval");
        now.set(1_000_000_000L);
        h.maybeCheck();
        assertEquals(1, opened.get());
        now.addAndGet(500_000_000L);
        h.maybeCheck();
        assertEquals(1, opened.get(), "not again within the interval");
        now.addAndGet(500_000_000L);
        h.maybeCheck();
        assertEquals(2, opened.get());

        HealthCheck off = new HealthCheck(src, 0, now::get);
        now.addAndGet(1_000_000_000_000L);
        off.maybeCheck();
        assertEquals(2, opened.get(), "interval 0 never checks");
    }

    /** Failing to reach PostgreSQL for the check is not a problem report: it must not kill. */
    @Test
    void anUnreachableDatabaseIsNotAProblemReport() {
        AtomicLong now = new AtomicLong(0);
        HealthCheck h = new HealthCheck(() -> {
            throw new SQLException("connection refused", "08001");
        }, 1, now::get);
        now.set(10_000_000L);
        assertDoesNotThrow(h::maybeCheck);
    }

    /** A problem found while running stops the writer, like one found at startup. */
    @Test
    void aProblemFoundWhileRunningIsFatal() throws Exception {
        AtomicLong now = new AtomicLong(0);
        FakeJdbc db = new FakeJdbc(sql -> sql.contains("paimon.health()")
            ? List.of("event trigger paimon_ddl_end is missing") : null);
        HealthCheck h = new HealthCheck(db::connection, 1, now::get);
        now.set(10_000_000L);
        IllegalStateException e = assertThrows(IllegalStateException.class, h::maybeCheck);
        assertTrue(e.getMessage().contains("while running"));
        assertTrue(e.getMessage().contains("paimon_ddl_end is missing"));
    }

    /**
     * Review finding I1: the check runs on the consumer's thread, and health() blocks behind any
     * ACCESS EXCLUSIVE lock on a replicated table. It must be bounded, and the bounds must be set
     * BEFORE the query on the same session.
     */
    @Test
    void theCheckIsBoundedByALockTimeoutSetBeforeTheQuery() throws Exception {
        FakeJdbc db = new FakeJdbc(sql -> null);
        HealthCheck.problems(db.connection());
        int lock = db.executed.indexOf("SET lock_timeout = '1s'");
        int stmt = db.executed.indexOf("SET statement_timeout = '30s'");
        int query = db.executed.indexOf("SELECT * FROM paimon.health()");
        assertTrue(lock >= 0 && stmt >= 0 && query >= 0, db.executed.toString());
        assertTrue(lock < query && stmt < query, db.executed.toString());
    }

    /** A lock or statement timeout means "could not check this time": not fatal, not a report. */
    @Test
    void aLockTimeoutIsNotAProblemReport() {
        for (String state : List.of("55P03", "57014")) {
            AtomicLong now = new AtomicLong(0);
            FakeJdbc db = new FakeJdbc(sql -> sql.contains("paimon.health()")
                ? new SQLException("canceling statement due to lock timeout", state) : null);
            HealthCheck h = new HealthCheck(db::connection, 1, now::get);
            now.set(10_000_000L);
            assertDoesNotThrow(h::maybeCheck, state);
        }
    }

    /**
     * Review finding I2: DROP EXTENSION under a running writer made every check fail with
     * "function paimon.health() does not exist", which was logged as "could not reach" while
     * the writer went on discarding the values of every column added afterwards.
     */
    @Test
    void aMissingExtensionIsFatal() {
        for (String state : List.of("42883", "3F000", "42P01", "42501")) {
            AtomicLong now = new AtomicLong(0);
            FakeJdbc db = new FakeJdbc(sql -> sql.contains("paimon.health()")
                ? new SQLException("function paimon.health() does not exist", state) : null);
            HealthCheck h = new HealthCheck(db::connection, 1, now::get);
            now.set(10_000_000L);
            IllegalStateException e = assertThrows(IllegalStateException.class, h::maybeCheck,
                state);
            assertTrue(e.getMessage().contains("could not run while running"), e.getMessage());
            assertTrue(e.getMessage().contains("not a connection problem"), e.getMessage());
        }
    }

    @Test
    void whichFailuresAreTransient() {
        for (String s : List.of("08001", "08006", "08P01", "55P03", "57014", "57P01", "53300")) {
            assertTrue(HealthCheck.transientFailure(new SQLException("x", s)), s);
        }
        assertTrue(HealthCheck.transientFailure(new SQLException("io")), "no SQLSTATE: I/O");
        for (String s : List.of("42883", "3F000", "42P01", "42501", "XX000", "22012")) {
            assertFalse(HealthCheck.transientFailure(new SQLException("x", s)), s);
        }
    }

    /** At startup a lock timeout is waited out (there is no stream to hold up yet). */
    @Test
    void theStartupCheckRetriesALockTimeout() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        FakeJdbc db = new FakeJdbc(sql -> !sql.contains("paimon.health()") ? null
            : calls.incrementAndGet() < 3 ? new SQLException("lock timeout", "55P03")
            : List.of("a problem"));
        List<Long> slept = new java.util.ArrayList<>();
        assertEquals(List.of("a problem"),
            HealthCheck.problemsAtStartup(db.connection(), slept::add));
        assertEquals(List.of(2_000L, 2_000L), slept);

        FakeJdbc gone = new FakeJdbc(sql -> sql.contains("paimon.health()")
            ? new SQLException("function paimon.health() does not exist", "42883") : null);
        SQLException e = assertThrows(SQLException.class,
            () -> HealthCheck.problemsAtStartup(gone.connection(), ms -> fail("no retry")));
        assertEquals("42883", e.getSQLState());
    }
}
