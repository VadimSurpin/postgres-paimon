// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.logging.Logger;

/**
 * Applies DDL messages to the {@link SchemaRegistry}, one at a time, in arrival order. Within one
 * statement the emitter orders them drop_col, then rename_col/alter_col, then add_col, so a
 * dropped column's name can be reused by a column added in the same ALTER TABLE; apply them
 * exactly as received.
 */
public final class DdlMessageHandler {

    private static final Logger LOG = Logger.getLogger(DdlMessageHandler.class.getName());
    static final int WIRE_VERSION = 1;

    private final SchemaRegistry registry;
    private final String expectedPrefix;
    private final String token;

    /**
     * @param token the installation's credential, read from {@code paimon.secret} at startup.
     *              Required: a handler that accepted unauthenticated messages would let any role
     *              that can connect drive the writer (see {@link #handle}).
     */
    public DdlMessageHandler(SchemaRegistry registry, String expectedPrefix, String token) {
        if (token == null || token.isEmpty()) {
            throw new IllegalArgumentException("the DDL message credential (paimon.secret) is"
                + " required");
        }
        this.registry = registry;
        this.expectedPrefix = expectedPrefix;
        this.token = token;
    }

    /** Whether {@link #handle} would act on this message (it ignores other prefixes). */
    public boolean isDdl(PgEvent.Message m) {
        return expectedPrefix.equals(m.prefix());
    }

    /**
     * Applies one message, or ignores it with a WARNING when it was not emitted by the extension.
     *
     * <p>{@code pg_logical_emit_message()} is executable by PUBLIC, so anything under the prefix
     * may come from any role that can connect. Before the credential, one
     * {@code SELECT pg_logical_emit_message(true, 'paimon', 'not json')} by a role with no
     * privileges at all stopped the writer for good -- the message is in the slot and replays on
     * every restart, a poison record -- and a well-formed forged {@code drop_table} would have
     * purged a Paimon table. So a message that does not parse, or does not carry this
     * installation's credential, is not ours: it is reported and skipped, never applied and
     * never fatal. Every message the extension emits is valid JSON carrying the credential
     * (paimon.emit), so nothing legitimate can land here. A message that IS authenticated and
     * still wrong is a defect of ours, and stays fatal below.
     */
    public void handle(PgEvent.Message m) {
        if (!expectedPrefix.equals(m.prefix())) {
            return;
        }
        DdlMessage d;
        try {
            d = DdlMessage.parse(m.content());
        } catch (RuntimeException e) {
            LOG.warning(ignored(m, "it is not a JSON object (" + e.getMessage() + ")"));
            return;
        }
        if (d.auth() == null || !java.security.MessageDigest.isEqual(
                d.auth().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                token.getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
            LOG.warning(ignored(m, d.auth() == null ? "it carries no credential"
                                                    : "its credential is not this installation's"));
            return;
        }
        if (d.version() != WIRE_VERSION) {
            throw new IllegalStateException("unsupported DDL message version " + d.version()
                + " (this writer understands " + WIRE_VERSION + ")");
        }
        String op = d.op();
        if (op == null) {
            throw new IllegalStateException("DDL message has no op");
        }
        switch (op) {
            case "create" -> registry.createTable(d.relid(), d.schema(), d.table(),
                need(d.pk(), "pk", op), need(d.cols(), "cols", op), d.changelogProducer(),
                d.buckets(), m.lsn());
            case "add_col", "drop_col", "rename_col", "alter_col", "drop_table" -> {
                if (!registry.tracked(d.relid())) {
                    LOG.warning("ignoring " + op + " for unregistered relation " + d.relid());
                    return;
                }
                // m.lsn() is the message's own position in WAL, from the pgoutput 'M' header.
                // It is stable across replays and is what SchemaRegistry records as the DDL
                // commit identifier, so a replayed message is skipped the way filterAndCommit
                // skips a replayed data commit.
                switch (op) {
                    case "add_col" ->
                        registry.addColumn(d.relid(), need(d.col(), "col", op), m.lsn());
                    case "drop_col" ->
                        registry.dropColumn(d.relid(), need(d.name(), "name", op), m.lsn());
                    case "rename_col" -> registry.renameColumn(d.relid(),
                        need(d.from(), "from", op), need(d.to(), "to", op), m.lsn());
                    case "alter_col" -> registry.alterColumn(d.relid(),
                        need(d.name(), "name", op), need(d.type(), "type", op), m.lsn());
                    default -> registry.dropTable(d.relid(), d.schema(), d.table(), m.lsn());
                }
            }
            default -> throw new IllegalStateException("unknown DDL op '" + op + "'");
        }
    }

    private String ignored(PgEvent.Message m, String why) {
        return "IGNORED a logical message with prefix '" + expectedPrefix + "' at "
            + SlotConsumer.formatLsn(m.lsn()) + ": " + why + ", so it was not emitted by the"
            + " paimon_slot extension. Some role called pg_logical_emit_message() with this"
            + " prefix (it is executable by PUBLIC); nothing was applied. If it was meant to be"
            + " a DDL message, emit it through the extension instead";
    }

    private static <T> T need(T v, String field, String op) {
        if (v == null) {
            throw new IllegalStateException("DDL op '" + op + "' lacks '" + field + "'");
        }
        return v;
    }
}
