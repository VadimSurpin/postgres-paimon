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
 * most once per interval, so it must never wait long: {@code health()} reads the catalog of every
 * replicated table and blocks behind any ACCESS EXCLUSIVE lock on one (VACUUM FULL, CLUSTER, a
 * long ALTER, an open TRUNCATE). Measured: a 25 s lock on one table held up a row of an UNRELATED
 * table for the whole 25 s, and a multi-hour VACUUM FULL would stall replication for hours and get
 * the walsender killed by wal_sender_timeout. So the check runs with a {@link #LOCK_TIMEOUT} and a
 * {@link #STATEMENT_TIMEOUT}, and a timeout means "could not check this time", not a problem.
 *
 * <p>What a failure to run the check means is decided by its SQLSTATE ({@link #transientFailure}).
 * Not reaching PostgreSQL (class 08), a lock or statement timeout, a server shutting down or out of
 * resources is transient and only logged: the replication connection has its own, stricter
 * handling. Anything else is fatal. In particular {@code function paimon.health() does not exist}
 * (42883), a missing schema (3F000) or table (42P01) mean the extension was dropped under a
 * running writer: its event triggers and registry are gone, DDL no longer reaches the writer, and
 * treating that as "could not reach PostgreSQL" kept the writer running and discarding the
 * values of every column added afterwards (measured).
 */
public final class HealthCheck {

    private static final Logger LOG = Logger.getLogger(HealthCheck.class.getName());

    /** Opens a plain (non-replication) connection. */
    public interface ConnectionSource {
        Connection open() throws SQLException;
    }

    private final ConnectionSource connections;
    private final String slotName;           // null: the slot is not checked
    private final long intervalNanos;
    private final LongSupplier nanoClock;
    private long lastCheck;

    /** @param intervalMs 0 disables the periodic check; {@link #requireHealthy} still works */
    public HealthCheck(ConnectionSource connections, long intervalMs) {
        this(connections, intervalMs, System::nanoTime);
    }

    /** As above, also checking the replication slot {@code slotName} ({@link #slotProblems}). */
    public HealthCheck(ConnectionSource connections, long intervalMs, String slotName) {
        this(connections, intervalMs, System::nanoTime, slotName);
    }

    HealthCheck(ConnectionSource connections, long intervalMs, LongSupplier nanoClock) {
        this(connections, intervalMs, nanoClock, null);
    }

    HealthCheck(ConnectionSource connections, long intervalMs, LongSupplier nanoClock,
                String slotName) {
        this.connections = connections;
        this.slotName = slotName;
        this.intervalNanos = intervalMs * 1_000_000L;
        this.nanoClock = nanoClock;
        this.lastCheck = nanoClock.getAsLong();
    }

    /** How long one check waits for a lock before giving up for this interval. */
    static final String LOCK_TIMEOUT = "1s";
    /** How long one check may run at all. */
    static final String STATEMENT_TIMEOUT = "30s";

    /**
     * The problems {@code paimon.health()} reports, in order; empty when healthy. Bounded by
     * {@link #LOCK_TIMEOUT} and {@link #STATEMENT_TIMEOUT}, set for this session (the
     * connection is the check's own).
     */
    static List<String> problems(Connection conn) throws SQLException {
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute("SET lock_timeout = '" + LOCK_TIMEOUT + "'");
            st.execute("SET statement_timeout = '" + STATEMENT_TIMEOUT + "'");
        }
        List<String> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM paimon.health()");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute("RESET lock_timeout");
            st.execute("RESET statement_timeout");
        }
        return out;
    }

    /**
     * Whether a failure to run the check says nothing about the extension: PostgreSQL could not be
     * reached (class 08, or no SQLSTATE at all, an I/O failure in the driver), a lock or statement
     * timeout (55P03, 57014), the server shutting down (57P01-57P05) or short of resources (53).
     * Everything else -- above all 42883, 3F000, 42P01: the extension is gone -- is fatal.
     */
    static boolean transientFailure(SQLException e) {
        String state = e.getSQLState();
        if (state == null || state.length() != 5) {
            return true;
        }
        return state.startsWith("08") || state.startsWith("53") || state.startsWith("57P")
            || state.equals("55P03") || state.equals("57014");
    }

    /** Thrown for a failure of the check that is not transient. */
    static IllegalStateException checkFailed(SQLException e, String when) {
        return new IllegalStateException("paimon.health() could not run " + when + ": "
            + e.getMessage() + " (SQLSTATE " + e.getSQLState() + "). This is not a connection"
            + " problem: the paimon_slot extension, or part of it, is missing or not usable by the"
            + " writer's role (DROP EXTENSION, a dropped schema or function, a revoked grant)."
            + " Without it DDL on replicated tables no longer reaches the writer, so it stops"
            + " rather than replicate on blind. Restore the extension (and re-register its tables,"
            + " section 3 'Re-bootstrapping', if it was dropped), or the grants, then restart", e);
    }

    /** Waits between startup attempts that hit a lock; replaceable in tests. */
    interface Sleeper {
        void sleep(long ms) throws InterruptedException;
    }

    /**
     * The startup check. A lock or statement timeout is retried (with a WARNING each time): the
     * writer cannot start without a verdict, and before the stream opens waiting costs nothing
     * but time. Any other failure propagates.
     */
    static List<String> problemsAtStartup(Connection conn) throws SQLException {
        return problemsAtStartup(conn, Thread::sleep);
    }

    static List<String> problemsAtStartup(Connection conn, Sleeper sleeper) throws SQLException {
        while (true) {
            try {
                return problems(conn);
            } catch (SQLException e) {
                if (!"55P03".equals(e.getSQLState()) && !"57014".equals(e.getSQLState())) {
                    throw e;
                }
                LOG.warning("paimon.health() timed out at startup (" + e.getMessage() + "): a"
                    + " replicated table is locked (VACUUM FULL, CLUSTER, a long ALTER?); retrying"
                    + " in 2 s");
                try {
                    sleeper.sleep(2_000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
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

    /**
     * What {@code paimon.slot_health(slot, for_writer => true)} reports about the writer's
     * replication slot, as {@code level|problem}. Runs under the same timeouts as
     * {@link #problems}, which must have set them on this connection, or sets them itself.
     */
    static List<String> slotProblems(Connection conn, String slotName) throws SQLException {
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute("SET lock_timeout = '" + LOCK_TIMEOUT + "'");
            st.execute("SET statement_timeout = '" + STATEMENT_TIMEOUT + "'");
        }
        List<String> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                 "SELECT level || '|' || problem FROM paimon.slot_health(?, for_writer => true)")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getString(1));
                }
            }
        }
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute("RESET lock_timeout");
            st.execute("RESET statement_timeout");
        }
        return out;
    }

    /**
     * Throws on any {@code fatal} finding about the slot (invalidated, or not the writer's slot);
     * logs every {@code warning} (unreserved WAL, an old prepared transaction holding it back).
     * An invalidated slot is otherwise met only as START_REPLICATION's bare "can no longer access
     * replication slot", or, while streaming, as the walsender being killed and that same error
     * on the reconnect; this says why and what to do.
     */
    static void requireUsableSlot(List<String> findings, String slotName, String when) {
        List<String> fatal = new ArrayList<>();
        for (String f : findings) {
            int bar = f.indexOf('|');
            String level = bar < 0 ? "fatal" : f.substring(0, bar);
            String problem = bar < 0 ? f : f.substring(bar + 1);
            if (level.equals("fatal")) {
                fatal.add(problem);
            } else {
                LOG.warning("paimon.slot_health() " + when + ": " + problem);
            }
        }
        if (!fatal.isEmpty()) {
            throw new IllegalStateException("REPLICATION SLOT UNUSABLE " + when + ": "
                + String.join("; ", fatal) + ". The writer stops: nothing it could stream from"
                + " this slot would be complete. Nothing already in Paimon is lost or changed."
                + " Re-bootstrap (docs/logical-slot-architecture.md, section 3,"
                + " 'Re-bootstrapping'; scripts/rebootstrap.sh does it): stop the"
                + " writer, SELECT pg_drop_replication_slot('" + slotName + "'); UPDATE"
                + " paimon.table_registry SET backfill_due = true; and start the writer again."
                + " Changes made while the slot was unusable are not in the history tables or"
                + " the changelog: the reload copies each table's state at the new snapshot.");
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
        List<String> slot = List.of();
        try (Connection conn = connections.open()) {
            found = problems(conn);
            if (slotName != null) {
                slot = slotProblems(conn, slotName);
            }
        } catch (SQLException e) {
            if (!transientFailure(e)) {
                throw checkFailed(e, "while running");
            }
            LOG.warning("could not run paimon.health(): " + e + "; will try again in "
                + intervalNanos / 1_000_000L + " ms");
            return;
        }
        requireHealthy(found, "while running");
        requireUsableSlot(slot, slotName, "while running");
    }
}
