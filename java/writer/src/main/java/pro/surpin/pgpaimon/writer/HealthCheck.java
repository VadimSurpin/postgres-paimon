// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.logging.Logger;

/**
 * Runs {@code paimon.health()} and stops the writer on any problem it reports.
 *
 * <p>The extension's guardrails refuse, before PostgreSQL commits, everything that would become
 * a poison record or silent data loss. What they cannot refuse is their own removal: a superuser
 * can disable or drop an event trigger, edit the publication with the triggers off, or change a
 * table with them disabled. {@code paimon.health()} checks from the catalog that what the
 * guardrails keep true still is (every replicated table published whole, FULL replica identity,
 * value guards in place, the registry matching the live table, the event triggers enabled
 * ALWAYS), and each problem it reports means changes may be reaching PostgreSQL that this writer
 * cannot see or cannot apply. So the writer refuses to start on one, and stops when one appears
 * while it runs: loud and early, rather than a Paimon table that quietly diverges.
 *
 * <p>The periodic check runs on the consumer's own thread from the idle and event callbacks, at
 * most once per interval. A failure to <em>reach</em> PostgreSQL for the check is not a problem
 * report and is only logged: the replication connection has its own, stricter handling.
 */
public final class HealthCheck {

    private static final Logger LOG = Logger.getLogger(HealthCheck.class.getName());

    /** Opens a plain (non-replication) connection. */
    public interface ConnectionSource {
        Connection open() throws SQLException;
    }

    private final ConnectionSource connections;
    private final long intervalNanos;
    private final LongSupplier nanoClock;
    private long lastCheck;

    /** @param intervalMs 0 disables the periodic check; {@link #requireHealthy} still works */
    public HealthCheck(ConnectionSource connections, long intervalMs) {
        this(connections, intervalMs, System::nanoTime);
    }

    HealthCheck(ConnectionSource connections, long intervalMs, LongSupplier nanoClock) {
        this.connections = connections;
        this.intervalNanos = intervalMs * 1_000_000L;
        this.nanoClock = nanoClock;
        this.lastCheck = nanoClock.getAsLong();
    }

    /** The problems {@code paimon.health()} reports, in order; empty when healthy. */
    static List<String> problems(Connection conn) throws SQLException {
        List<String> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM paimon.health()");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }

    /** Throws, naming every problem, unless the extension reports itself healthy. */
    static void requireHealthy(List<String> problems, String when) {
        if (!problems.isEmpty()) {
            throw new IllegalStateException("paimon.health() reports " + problems.size()
                + " problem(s) " + when + ": " + String.join("; ", problems)
                + ". Each means changes may reach PostgreSQL that the writer cannot see or cannot"
                + " apply, because a guardrail of the paimon_slot extension was bypassed (an event"
                + " trigger disabled or dropped, the publication or a table changed with it off)."
                + " Fix each one -- SELECT * FROM paimon.health() lists them -- and check the"
                + " tables named for changes made meanwhile; a table changed while unguarded"
                + " may need re-registering (docs/logical-slot-architecture.md, section 3,"
                + " 'Re-bootstrapping').");
        }
    }

    /** Called from the consumer loop; checks at most once per interval. */
    public void maybeCheck() {
        if (intervalNanos <= 0) {
            return;
        }
        long now = nanoClock.getAsLong();
        if (now - lastCheck < intervalNanos) {
            return;
        }
        lastCheck = now;
        List<String> found;
        try (Connection conn = connections.open()) {
            found = problems(conn);
        } catch (SQLException e) {
            LOG.warning("could not run paimon.health(): " + e + "; will try again in "
                + intervalNanos / 1_000_000L + " ms");
            return;
        }
        requireHealthy(found, "while running");
    }
}
