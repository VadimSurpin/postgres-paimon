// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.paimon.types.RowKind;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyOut;
import org.postgresql.replication.LogSequenceNumber;

/**
 * The initial load, and the only code that may create the replication slot.
 *
 * <p>{@code CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT} returns a snapshot taken at exactly the
 * slot's consistent point: every row committed before that point is in the snapshot, every row
 * after it arrives through the stream, with no gap and no overlap. Creating the slot separately
 * and then taking a snapshot leaves a window in which rows are copied twice or missed, and
 * nothing detects it. Hence {@code Main} never creates the slot.
 *
 * <p>The exported snapshot lives only while the replication connection that created it is open
 * <em>and idle</em> (PostgreSQL invalidates it as soon as that connection runs another command).
 * This object owns that connection; {@link #close()} it once every table is loaded.
 *
 * <p>Every backfilled row is {@code +I} with {@code _seq = consistentPoint} and an increasing
 * {@code _op_seq}; any streamed change carries a higher LSN and so wins on merge.
 *
 * <p>Commit identifiers (per table, one commit user): an empty overwrite at
 * {@code consistentPoint - chunks}, which clears whatever an interrupted earlier attempt left,
 * then the chunks at {@code consistentPoint - chunks + 1 .. consistentPoint}. They ascend, are
 * above anything a previous slot's attempt used, and none exceeds the consistent point, so none
 * can collide with a streaming commit (those carry a commit LSN after it). Subtracting is safe;
 * adding would put an identifier past WAL the stream has not replayed, and a restart would then
 * resume beyond it.
 */
public final class Backfiller implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(Backfiller.class.getName());

    /** The exported snapshot's opaque name and the slot's consistent point as an LSN. */
    public record BackfillPlan(String snapshotName, long consistentPoint) {}

    private final WriterConfig config;
    private final SchemaRegistry registry;
    private final PaimonSink sink;
    private Connection replicationConnection;

    public Backfiller(WriterConfig config, SchemaRegistry registry, PaimonSink sink) {
        this.config = config;
        this.registry = registry;
        this.sink = sink;
    }

    /**
     * Creates the slot and exports its snapshot. The replication connection is kept open (and
     * idle) until {@link #close()}.
     */
    public BackfillPlan createSlotAndExportSnapshot() throws SQLException {
        String slot = config.slotName();
        if (!slot.matches("[a-z0-9_]+")) {
            // a replication command takes the name unquoted
            throw new IllegalArgumentException("slot name '" + slot
                + "' must be lower-case letters, digits and underscores");
        }
        if (replicationConnection != null) {
            throw new IllegalStateException("a snapshot is already exported by this Backfiller");
        }
        Properties p = new Properties();
        p.setProperty("user", config.user());
        p.setProperty("password", config.password());
        p.setProperty("replication", "database");
        p.setProperty("preferQueryMode", "simple");
        p.setProperty("assumeMinServerVersion", "18");
        Connection conn = DriverManager.getConnection(config.jdbcUrl(), p);
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                 "CREATE_REPLICATION_SLOT " + slot + " LOGICAL pgoutput (SNAPSHOT 'export')")) {
            if (!rs.next()) {
                throw new SQLException("CREATE_REPLICATION_SLOT returned no row");
            }
            String lsn = rs.getString("consistent_point");
            String snapshot = rs.getString("snapshot_name");
            if (lsn == null || snapshot == null) {
                throw new SQLException("CREATE_REPLICATION_SLOT returned consistent_point="
                    + lsn + " snapshot_name=" + snapshot);
            }
            long cp = LogSequenceNumber.valueOf(lsn).asLong();
            LOG.info("created slot " + slot + " at " + lsn + ", snapshot " + snapshot);
            replicationConnection = conn;
            return new BackfillPlan(snapshot, cp);
        } catch (SQLException | RuntimeException e) {
            closeQuietly(conn);
            throw e;
        }
    }

    /**
     * Copies one table out of the exported snapshot into Paimon and marks it backfilled. The
     * caller must already have given the sink this relation's {@link RelationMeta}, with
     * {@code columns} in the same order.
     */
    public void backfill(BackfillPlan plan, long relid, String schema, String table,
                         List<String> columns) throws Exception {
        if (replicationConnection == null) {
            throw new IllegalStateException(
                "no exported snapshot: call createSlotAndExportSnapshot first, and do not close");
        }
        StringBuilder cols = new StringBuilder();
        for (String c : columns) {
            cols.append(cols.length() == 0 ? "" : ", ").append(quoteIdent(c));
        }
        // Two identities. The PostgreSQL source is resolved from relid at load time, because
        // relid survives ALTER TABLE ... RENAME while the registry's schema/table are the
        // Paimon identity, frozen at registration. `schema` and `table` only name the Paimon
        // side (the sink resolves it by relid too) and appear in messages.
        String qualified;
        int chunk = config.flushMaxRecords();

        try (Connection conn = Main.plainConnection(config)) {
            conn.setAutoCommit(true);
            qualified = currentName(conn, relid);
            try (Statement st = conn.createStatement()) {
                // Text output that depends on session settings is pinned, as on the stream.
                st.execute("SET TimeZone = 'UTC'");
                st.execute("SET client_encoding = 'UTF8'");
                st.execute("SET bytea_output = 'hex'");
                st.execute("SET extra_float_digits = 1");
                st.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
                // must be the first statement of the transaction
                st.execute("SET TRANSACTION SNAPSHOT " + quoteLiteral(plan.snapshotName()));

                long rows;
                try (ResultSet rs = st.executeQuery("SELECT count(*) FROM " + qualified)) {
                    rs.next();
                    rows = rs.getLong(1);
                }
                long chunks = (rows + chunk - 1) / chunk;
                long truncId = truncateIdentifier(plan.consistentPoint(), chunks);
                requireApplied(sink.truncate(relid, truncId), qualified, truncId);

                CopyOut copy = conn.unwrap(PGConnection.class).getCopyAPI().copyOut(
                    "COPY " + qualified + " (" + cols + ") TO STDOUT");
                List<SinkRecord> batch = new ArrayList<>(Math.min(chunk, 1 << 16));
                long opSeq = 0;
                long chunkIndex = 0;
                long loaded = 0;
                byte[] line;
                while ((line = copy.readFromCopy()) != null) {
                    int len = line.length;
                    if (len > 0 && line[len - 1] == '\n') {
                        len--;      // the row terminator; embedded newlines are escaped
                    }
                    List<String> values = parseCopyLine(
                        new String(line, 0, len, StandardCharsets.UTF_8));
                    if (values.size() != columns.size()) {
                        throw new IllegalStateException("COPY of " + qualified + " returned "
                            + values.size() + " fields, expected " + columns.size());
                    }
                    // opSeq is only an ordering within one _seq and primary keys are unique in
                    // a snapshot, so wrapping past Integer.MAX_VALUE (2 billion rows) is harmless.
                    batch.add(new SinkRecord(relid, RowKind.INSERT, values,
                        plan.consistentPoint(), (int) (opSeq++ & Integer.MAX_VALUE), 0L));
                    loaded++;
                    if (batch.size() == chunk) {
                        flushChunk(batch, qualified, plan, chunks, chunkIndex++);
                    }
                }
                if (!batch.isEmpty()) {
                    flushChunk(batch, qualified, plan, chunks, chunkIndex++);
                }
                if (loaded != rows || chunkIndex != chunks) {
                    throw new IllegalStateException("snapshot count " + rows + " but COPY gave "
                        + loaded + " rows in " + chunkIndex + " chunks of " + chunks);
                }
                st.execute("COMMIT");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT paimon.mark_backfilled(?::bigint::oid::regclass)")) {
                ps.setLong(1, relid);     // by oid: immune to a rename mid-load
                ps.execute();
            }
        }
        LOG.info("backfilled " + qualified + " (paimon " + schema + "." + table + ") at " + plan.consistentPoint());
    }

    /** The table's present, quoted, schema-qualified name; looked up before the transaction. */
    static String currentName(Connection conn, long relid) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT n.nspname, c.relname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = ?::bigint::oid")) {
            ps.setLong(1, relid);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("relation with oid " + relid + " no longer exists");
                }
                return quoteIdent(rs.getString(1)) + "." + quoteIdent(rs.getString(2));
            }
        }
    }

    private void flushChunk(List<SinkRecord> batch, String table, BackfillPlan plan, long chunks, long index)
            throws Exception {
        long id = chunkIdentifier(plan.consistentPoint(), chunks, index);
        requireApplied(sink.flushApplied(batch, id, id), table, id);
        batch.clear();
    }

    /**
     * Backfill identifiers are minted for this load, so a commit the sink filtered as already
     * committed means an earlier attempt used an identifier at or above it. Carrying on would
     * load nothing and still mark the table done: stale data, no error.
     */
    private static void requireApplied(boolean applied, String table, long id) {
        if (!applied) {
            throw new IllegalStateException("backfill commit " + id + " for " + table
                + " was filtered as already committed by an earlier attempt; the table holds"
                + " stale data. Delete the Paimon table and retry");
        }
    }

    /** The overwrite that precedes the chunks; below every chunk identifier. */
    static long truncateIdentifier(long consistentPoint, long chunks) {
        long id = consistentPoint - chunks;
        if (id < 1) {
            throw new IllegalStateException("consistent point " + consistentPoint
                + " is too low for " + chunks + " chunks");
        }
        return id;
    }

    /** Chunk {@code index} (0-based) of {@code chunks}: ascending, the last at the consistent point. */
    static long chunkIdentifier(long consistentPoint, long chunks, long index) {
        return consistentPoint - chunks + 1 + index;
    }

    /** Releases the exported snapshot. Call only after every table is backfilled. */
    @Override
    public void close() {
        Connection c = replicationConnection;
        replicationConnection = null;
        if (c != null) {
            closeQuietly(c);
        }
    }

    private static void closeQuietly(Connection c) {
        try {
            c.close();
        } catch (SQLException ignored) {
            // nothing to do about it
        }
    }

    static String quoteIdent(String s) {
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }

    private static String quoteLiteral(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    /**
     * COPY TO STDOUT text format: tab-separated, \N = NULL, and the escapes \t \n \r \\ \b \f
     * \v. Octal (\ddd) and hex (\xhh) escapes are accepted by COPY FROM only: COPY TO writes
     * every other control character (checked: 0x01, 0x1b, 0x7f) as the raw byte.
     */
    static List<String> parseCopyLine(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean sawEscape = false;
        boolean isNull = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (sawEscape) {
                switch (c) {
                    case 'N' -> isNull = cur.isEmpty();
                    case 't' -> cur.append('\t');
                    case 'n' -> cur.append('\n');
                    case 'r' -> cur.append('\r');
                    case 'b' -> cur.append('\u0008');
                    case 'f' -> cur.append('\u000c');
                    case 'v' -> cur.append('\u000b');
                    case '\\' -> cur.append('\\');
                    default -> cur.append(c);
                }
                sawEscape = false;
            } else if (c == '\\') {
                sawEscape = true;
            } else if (c == '\t') {
                out.add(isNull ? null : cur.toString());
                cur.setLength(0);
                isNull = false;
            } else {
                cur.append(c);
            }
        }
        out.add(isNull ? null : cur.toString());
        return out;
    }
}
