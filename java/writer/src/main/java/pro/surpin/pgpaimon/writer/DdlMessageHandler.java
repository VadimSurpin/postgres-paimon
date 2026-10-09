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

    public DdlMessageHandler(SchemaRegistry registry, String expectedPrefix) {
        this.registry = registry;
        this.expectedPrefix = expectedPrefix;
    }

    public void handle(PgEvent.Message m) {
        if (!expectedPrefix.equals(m.prefix())) {
            return;
        }
        DdlMessage d = DdlMessage.parse(m.content());
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
                need(d.pk(), "pk", op), need(d.cols(), "cols", op), d.changelogProducer());
            case "add_col", "drop_col", "rename_col", "alter_col", "drop_table" -> {
                if (!registry.tracked(d.relid())) {
                    LOG.warning("ignoring " + op + " for unregistered relation " + d.relid());
                    return;
                }
                switch (op) {
                    case "add_col" -> registry.addColumn(d.relid(), need(d.col(), "col", op));
                    case "drop_col" -> registry.dropColumn(d.relid(), need(d.name(), "name", op));
                    case "rename_col" -> registry.renameColumn(d.relid(),
                        need(d.from(), "from", op), need(d.to(), "to", op));
                    case "alter_col" -> registry.alterColumn(d.relid(),
                        need(d.name(), "name", op), need(d.type(), "type", op));
                    default -> registry.dropTable(d.relid(), d.schema(), d.table());
                }
            }
            default -> throw new IllegalStateException("unknown DDL op '" + op + "'");
        }
    }

    private static <T> T need(T v, String field, String op) {
        if (v == null) {
            throw new IllegalStateException("DDL op '" + op + "' lacks '" + field + "'");
        }
        return v;
    }
}
