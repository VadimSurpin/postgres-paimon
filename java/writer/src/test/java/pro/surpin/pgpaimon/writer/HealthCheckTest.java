// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
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
        Connection conn = (Connection) java.lang.reflect.Proxy.newProxyInstance(
            getClass().getClassLoader(), new Class<?>[] {Connection.class}, (p, m, a) -> {
                switch (m.getName()) {
                    case "prepareStatement":
                        return statementReturning("event trigger paimon_ddl_end is missing");
                    case "close":
                        return null;
                    default:
                        throw new UnsupportedOperationException(m.getName());
                }
            });
        HealthCheck h = new HealthCheck(() -> conn, 1, now::get);
        now.set(10_000_000L);
        IllegalStateException e = assertThrows(IllegalStateException.class, h::maybeCheck);
        assertTrue(e.getMessage().contains("while running"));
        assertTrue(e.getMessage().contains("paimon_ddl_end is missing"));
    }

    private static Object statementReturning(String problem) {
        boolean[] served = {false};
        java.sql.ResultSet rs = (java.sql.ResultSet) java.lang.reflect.Proxy.newProxyInstance(
            HealthCheckTest.class.getClassLoader(), new Class<?>[] {java.sql.ResultSet.class},
            (p, m, a) -> switch (m.getName()) {
                case "next" -> { boolean r = !served[0]; served[0] = true; yield r; }
                case "getString" -> problem;
                case "close" -> null;
                default -> throw new UnsupportedOperationException(m.getName());
            });
        return java.lang.reflect.Proxy.newProxyInstance(
            HealthCheckTest.class.getClassLoader(), new Class<?>[] {java.sql.PreparedStatement.class},
            (p, m, a) -> switch (m.getName()) {
                case "executeQuery" -> rs;
                case "close" -> null;
                default -> throw new UnsupportedOperationException(m.getName());
            });
    }
}
