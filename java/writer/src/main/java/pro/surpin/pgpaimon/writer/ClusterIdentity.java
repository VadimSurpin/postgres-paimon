// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Which PostgreSQL cluster a warehouse belongs to, and the refusal to stream into it from another.
 *
 * <p>Every Paimon table the writer makes carries {@link #SYSID_OPTION} (the cluster's
 * {@code system_identifier}) and {@link #TIMELINE_OPTION}. The warehouse's commit identifiers and
 * {@code pgpaimon.applied-ddl-lsn} are LSNs, which mean something only on the timeline of the
 * cluster that produced them. Pointed at another cluster, or at this one restored from a backup,
 * the writer would filter real transactions as "already committed" (their LSNs are below what
 * the warehouse holds) and skip real DDL as replayed: silent loss, with nothing to see. So at
 * startup, before anything is written:
 * <ol>
 *   <li><b>Another system identifier</b> (pg_upgrade, a restore into a new cluster, a re-initdb,
 *       a JDBC URL naming another server): refused, unless every table is being reloaded (a
 *       re-bootstrap, which replaces their contents from the new cluster), or the operator names
 *       the new identifier in {@code PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER} (pg_upgrade from
 *       PostgreSQL 17 or later carries logical slots over, and the stream then continues).</li>
 *   <li><b>The server's WAL is behind what the warehouse committed</b>: the cluster was restored
 *       from a backup or recovered to a point in time. Refused, always: even a re-bootstrap would
 *       have its commits filtered below the warehouse's identifiers, so the tables must be moved
 *       aside or a new warehouse used.</li>
 *   <li><b>Another timeline</b> with the slot present: a standby was promoted and the slot came
 *       with it (a failover slot). Logged and re-stamped. With the slot absent, startup refuses
 *       on its own ("stranded" tables) and names the re-bootstrap.</li>
 * </ol>
 * A table carrying no stamp (made before this check existed) is adopted: stamped, with a line in
 * the log. What this cannot see: a point-in-time recovery whose WAL has already grown past the
 * warehouse's last commit when the writer starts, and the slot survived (a file-level snapshot
 * restore). Documented as a residual risk (docs, section 8).
 */
final class ClusterIdentity {

    private static final Logger LOG = Logger.getLogger(ClusterIdentity.class.getName());

    static final String SYSID_OPTION = "pgpaimon.pg-system-identifier";
    static final String TIMELINE_OPTION = "pgpaimon.pg-timeline";

    /** The connected cluster: system identifier, timeline, and its current WAL position. */
    record Identity(String systemId, String timeline, long walLsn) {
        Map<String, String> options() {
            return Map.of(SYSID_OPTION, systemId, TIMELINE_OPTION, timeline);
        }
    }

    /** One Paimon table's stamp: its name, and the two options (null when absent). */
    record Stamp(String table, String systemId, String timeline) {}

    /** What startup does about the stamps. */
    enum Verdict {
        /** Every stamp matches (or there are none): stamp what is unstamped. */
        MATCH,
        /** A new timeline with the slot present: a failover. Re-stamp, with a warning. */
        NEW_TIMELINE,
        /** Another cluster, accepted (re-bootstrap, or PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER). */
        ACCEPTED_OTHER_CLUSTER
    }

    private ClusterIdentity() {
    }

    static Identity read(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                 "SELECT s.system_identifier::text || '|' || c.timeline_id || '|' ||"
                 + " (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()"
                 + " ELSE pg_current_wal_lsn() END)::text"
                 + " FROM pg_control_system() s, pg_control_checkpoint() c");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            String[] f = rs.getString(1).split("\\|");
            return new Identity(f[0], f[1], SlotConsumer.parseLsn(f[2]));
        }
    }

    /**
     * The decision, from the stamps and the facts; throws when startup must refuse.
     *
     * @param highestCommitted the highest commit identifier this writer committed to any tracked
     *                         table, 0 if none
     * @param fullReload       every tracked table is about to be reloaded from a new snapshot
     * @param accept           {@code PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER}, or null
     */
    static Verdict decide(List<Stamp> stamps, Identity now, long highestCommitted,
                          boolean slotExists, boolean fullReload, String accept) {
        if (highestCommitted != 0 && Long.compareUnsigned(highestCommitted, now.walLsn()) > 0) {
            throw new IllegalStateException("CLUSTER MISMATCH: the warehouse holds transactions"
                + " committed at LSN " + SlotConsumer.formatLsn(highestCommitted) + ", but this"
                + " server's WAL ends at " + SlotConsumer.formatLsn(now.walLsn()) + ". Its WAL is"
                + " behind the warehouse: it was restored from a backup, recovered to a point in"
                + " time, or is another cluster. Streaming on would filter its new transactions as"
                + " already committed (their LSNs are lower) and lose them silently, so the writer"
                + " refuses. This needs a re-bootstrap into a NEW warehouse (or with every Paimon"
                + " table of this one moved aside; to keep history, move the <table>_hist tables"
                + " and their archives aside, where they stay readable): docs/logical-slot-"
                + "architecture.md, section 8, 'Rules for developers and administrators'.");
        }
        List<String> foreign = new ArrayList<>();
        boolean otherTimeline = false;
        for (Stamp s : stamps) {
            if (s.systemId() != null && !s.systemId().equals(now.systemId())) {
                foreign.add(s.table() + " (" + s.systemId() + ")");
            } else if (s.timeline() != null && !s.timeline().equals(now.timeline())) {
                otherTimeline = true;
            }
        }
        if (!foreign.isEmpty()) {
            if (fullReload) {
                LOG.warning("the warehouse was written from another PostgreSQL cluster ("
                    + foreign + "; this one is " + now.systemId() + "), and every table is being"
                    + " reloaded from this one (a re-bootstrap): taking it over");
                return Verdict.ACCEPTED_OTHER_CLUSTER;
            }
            if (now.systemId().equals(accept)) {
                LOG.warning("PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER=" + accept + ": continuing a"
                    + " warehouse written from another cluster " + foreign + " on this one, as"
                    + " told (pg_upgrade that carried the slot over). Unset it once started.");
                return Verdict.ACCEPTED_OTHER_CLUSTER;
            }
            throw new IllegalStateException("CLUSTER MISMATCH: this warehouse was written from"
                + " PostgreSQL cluster " + foreign + " (pgpaimon.pg-system-identifier), but the"
                + " writer is connected to cluster " + now.systemId() + ". Its LSNs mean nothing"
                + " here: streaming on could skip real transactions and DDL as already applied."
                + " Causes: pg_upgrade, a restore into a new cluster (pg_dump/pg_restore,"
                + " pg_basebackup of another server), a re-initdb, or PGPAIMON_JDBC_URL naming"
                + " another server. Remedies: re-bootstrap (stop the writer;"
                + " SELECT pg_drop_replication_slot(...) if a slot exists; UPDATE"
                + " paimon.table_registry SET backfill_due = true; start the writer: every table"
                + " is reloaded from this cluster, the history tables keep what they hold); or,"
                + " ONLY after a pg_upgrade from PostgreSQL 17 or later that carried this slot over"
                + " (so its stream continues the old one exactly), start once with"
                + " PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER=" + now.systemId() + ". Docs: section 8.");
        }
        if (otherTimeline && slotExists) {
            LOG.warning("the warehouse was written on another timeline of this cluster; this server"
                + " is on timeline " + now.timeline() + " and the slot exists here (a promoted"
                + " standby with a synchronized failover slot): continuing, and re-stamping");
            return Verdict.NEW_TIMELINE;
        }
        return Verdict.MATCH;
    }
}
