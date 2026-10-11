// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

/**
 * Owns the replication stream: connect, read, confirm, reconnect.
 *
 * <p>Two kinds of failure are kept apart. A lost <em>connection</em> ({@link SQLException} from
 * connecting, opening or reading the stream) calls {@link Handler#onDisconnect()} and reconnects
 * with backoff. Anything else, including every exception from {@link Handler#onEvent} (so from
 * the batcher and from the Paimon flush) and from the decoder, is fatal and propagates out of
 * {@link #run}: continuing would advance the slot past data that was never written.
 */
public final class SlotConsumer {

    public interface Handler {
        void onEvent(PgEvent event, long changeLsn) throws Exception;
        void onDisconnect();
    }

    private static final Logger LOG = Logger.getLogger(SlotConsumer.class.getName());

    private final WriterConfig config;
    private final PgOutputDecoder decoder;
    private final Handler handler;
    private volatile PGReplicationStream stream;
    private volatile boolean running = true;

    public SlotConsumer(WriterConfig config, PgOutputDecoder decoder, Handler handler) {
        this.config = config;
        this.decoder = decoder;
        this.handler = handler;
    }

    public void stop() {
        running = false;
    }

    /** Sets the flushed and applied LSN. Call only after the Paimon commit has returned. */
    public void confirm(long lsn) {
        PGReplicationStream s = stream;
        if (s != null) {
            LogSequenceNumber n = LogSequenceNumber.valueOf(lsn);
            s.setFlushedLSN(n);
            s.setAppliedLSN(n);
        }
    }

    /**
     * @param startLsn 0 imposes no additional constraint: the slot resumes from its own
     *                 {@code confirmed_flush_lsn}. It does not mean "replay all WAL".
     */
    public void run(long startLsn) throws Exception {
        long backoffMs = 500;
        while (running) {
            try (Connection conn = connect()) {
                stream = open(conn, startLsn);
                backoffMs = 500;
                while (running) {
                    // The buffer is big-endian (the ByteBuffer default), which PgOutputDecoder
                    // relies on. It is passed on untouched.
                    ByteBuffer buf = stream.readPending();
                    if (buf == null) {
                        Thread.sleep(10);
                        continue;
                    }
                    long changeLsn = stream.getLastReceiveLSN().asLong();
                    // Wrapped so that nothing from here, a SQLException included, can be taken
                    // for connection loss: failures here are fatal. Decoder errors are already
                    // unchecked and likewise never match the SQLException handler below.
                    PgEvent event = decoder.decode(buf);
                    try {
                        handler.onEvent(event, changeLsn);
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new HandlerFailure(e);
                    }
                }
                // Clean stop: report the confirmed position now rather than at the next interval.
                try {
                    stream.forceUpdateStatus();
                } catch (SQLException e) {
                    LOG.warning("final status update failed: " + e);
                }
            } catch (SQLException e) {
                if (!running) {
                    return;
                }
                if (!isConnectionLoss(e)) {
                    throw e;    // a missing or invalidated slot cannot resolve itself
                }
                LOG.warning("replication stream lost: " + e
                            + "; discarding any open transaction and reconnecting in "
                            + backoffMs + "ms");
                stream = null;
                handler.onDisconnect();
                Thread.sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 30_000);
            } finally {
                stream = null;
            }
        }
    }

    /** Wraps a checked exception from {@link Handler#onEvent}; never retried. */
    static final class HandlerFailure extends RuntimeException {
        HandlerFailure(Throwable cause) {
            super("event handler failed: " + cause, cause);
        }
    }

    /**
     * Whether a failure should reconnect with backoff. Only a {@link SQLException} from the
     * connection itself qualifies, and not every one: a missing slot (42704) is fatal because
     * nothing creates it but the Backfiller, and an invalidated slot (55000, "can no longer get
     * changes") is unrecoverable without a re-bootstrap. Retrying either is a loop, not
     * resilience. The cause chain is deliberately not searched, so a failure wrapped by the
     * handler boundary is never retryable.
     */
    static boolean isConnectionLoss(Throwable t) {
        if (!(t instanceof SQLException e)) {
            return false;
        }
        String state = e.getSQLState();
        return !("42704".equals(state) || "55000".equals(state));
    }

    private Connection connect() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", config.user());
        p.setProperty("password", config.password());
        p.setProperty("replication", "database");
        p.setProperty("preferQueryMode", "simple");
        p.setProperty("assumeMinServerVersion", "18");
        Connection conn = DriverManager.getConnection(config.jdbcUrl(), p);
        try {
            // Pinned, not defaulted. pgjdbc announces the JVM's default zone as TimeZone in the
            // startup packet and the "options" property does not override it (verified against
            // PostgreSQL 18), so the zone is set with SQL, which a replication=database session
            // accepts. client_encoding must be UTF8 because PgOutputDecoder decodes strictly.
            // bytea_output must be hex because TextDecoder accepts only the \x form, and a
            // server (or role, or database) set to 'escape' would otherwise feed it octal
            // escapes; Backfiller pins it on its own connection for the same reason.
            // Each setting is read back: a mismatch is a misconfiguration, not connection loss.
            try (Statement st = conn.createStatement()) {
                st.execute("SET TimeZone = 'UTC'");
                st.execute("SET client_encoding = 'UTF8'");
                st.execute("SET bytea_output = 'hex'");
            }
            requireSetting(conn, "TimeZone", "UTC");
            requireSetting(conn, "client_encoding", "UTF8");
            requireSetting(conn, "bytea_output", "hex");
        } catch (SQLException | RuntimeException e) {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // the original failure is the one that matters
            }
            throw e;
        }
        return conn;
    }

    /** A setting that is not what we pinned is a misconfiguration, never connection loss. */
    private static void requireSetting(Connection conn, String name, String want)
            throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SHOW " + name)) {
            rs.next();
            String got = rs.getString(1);
            if (!want.equalsIgnoreCase(got)) {
                throw new IllegalStateException("replication session has " + name + "=" + got
                    + " but the writer requires " + want);
            }
        }
    }

    private PGReplicationStream open(Connection conn, long startLsn) throws SQLException {
        var b = conn.unwrap(PGConnection.class).getReplicationAPI().replicationStream()
                    .logical()
                    .withSlotName(config.slotName())
                    .withSlotOption("proto_version", "2")
                    .withSlotOption("publication_names", config.publicationName())
                    .withSlotOption("messages", "true")
                    .withSlotOption("binary", "false")
                    .withSlotOption("streaming", "off")
                    .withStatusInterval(10_000, TimeUnit.MILLISECONDS);
        if (startLsn > 0) {
            b = b.withStartPosition(LogSequenceNumber.valueOf(startLsn));
        }
        return b.start();
    }

    public static long parseLsn(String text) {
        int slash = text.indexOf('/');
        long hi = Long.parseUnsignedLong(text.substring(0, slash), 16);
        long lo = Long.parseUnsignedLong(text.substring(slash + 1), 16);
        return (hi << 32) | lo;
    }

    public static String formatLsn(long lsn) {
        return String.format("%X/%08X", lsn >>> 32, lsn & 0xFFFFFFFFL);
    }
}
