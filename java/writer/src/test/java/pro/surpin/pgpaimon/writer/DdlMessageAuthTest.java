// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Messages under the 'paimon' prefix that the extension did not emit.
 *
 * <p>pg_logical_emit_message() is executable by PUBLIC. Measured before the credential: one
 * {@code SELECT pg_logical_emit_message(true, 'paimon', 'not json')} by a role with no
 * privileges stopped the writer with "DDL message is not valid JSON", on every restart. These
 * tests fail on that writer: it threw for the first, and applied the forged drop.
 */
class DdlMessageAuthTest {

    @TempDir Path wh;
    Catalog catalog;
    SchemaRegistry reg;
    DdlMessageHandler handler;
    final List<LogRecord> logged = new ArrayList<>();
    long lsn = 0x5000;

    static final long RELID = 17001L;

    @BeforeEach
    void setUp() {
        Options o = new Options();
        o.set("warehouse", wh.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
        WriterConfig cfg = WriterConfig.fromEnv(Map.of(
            "PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db",
            "PGPAIMON_WAREHOUSE", wh.toString()));
        reg = new SchemaRegistry(catalog, cfg);
        handler = new DdlMessageHandler(reg, "paimon", TestAuth.TOKEN);
        handler.handle(message(TestAuth.authed("""
            {"v":1,"op":"create","relid":"17001","schema":"public","table":"victim",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"}],
             "options":{"bucket":"1"}}""")));
        assertTrue(reg.tracked(RELID));
        Logger.getLogger(DdlMessageHandler.class.getName()).addHandler(new Handler() {
            public void publish(LogRecord r) { logged.add(r); }
            public void flush() { }
            public void close() { }
        });
    }

    PgEvent.Message message(byte[] content) {
        lsn += 0x10;
        return new PgEvent.Message(true, lsn, "paimon", content);
    }

    static byte[] raw(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    void assertIgnoredWithWarning(String why) {
        assertEquals(1, logged.size(), "exactly one warning");
        assertEquals(Level.WARNING, logged.get(0).getLevel());
        assertTrue(logged.get(0).getMessage().startsWith("IGNORED a logical message with prefix 'paimon'"),
            logged.get(0).getMessage());
        assertTrue(logged.get(0).getMessage().contains(why), logged.get(0).getMessage());
    }

    @Test
    void textThatIsNotJsonIsIgnoredNotFatal() {
        assertDoesNotThrow(() -> handler.handle(message(raw("not json"))));
        assertIgnoredWithWarning("not a JSON object");
    }

    @Test
    void jsonThatIsNotAnObjectIsIgnored() {
        assertDoesNotThrow(() -> handler.handle(message(raw("[1,2]"))));
        assertIgnoredWithWarning("not a JSON object");
    }

    @Test
    void aForgedDropTableWithoutTheCredentialPurgesNothing() throws Exception {
        handler.handle(message(raw("""
            {"v":1,"op":"drop_table","relid":"17001","schema":"public","table":"victim"}""")));
        assertIgnoredWithWarning("carries no credential");
        assertTrue(reg.tracked(RELID), "still tracked");
        assertNotNull(catalog.getTable(Identifier.create("public", "victim")), "Paimon table kept");
    }

    @Test
    void aWrongCredentialIsIgnoredToo() {
        handler.handle(message(raw("""
            {"v":1,"auth":"guess","op":"add_col","relid":"17001",
             "col":{"id":2,"name":"injected","type":"INT"}}""")));
        assertIgnoredWithWarning("not this installation's");
        assertTrue(reg.table(RELID).rowType().getFieldNames().stream()
            .noneMatch("injected"::equals), "no column added");
    }

    @Test
    void anAuthenticatedMessageIsApplied() {
        handler.handle(message(TestAuth.authed("""
            {"v":1,"op":"add_col","relid":"17001","col":{"id":2,"name":"real","type":"INT"}}""")));
        assertTrue(logged.isEmpty());
        assertTrue(reg.table(RELID).rowType().getFieldNames().contains("real"));
    }

    @Test
    void anAuthenticatedButBrokenMessageIsStillFatal() {
        // Ours, and wrong: a defect to stop on, not to step over.
        assertThrows(IllegalStateException.class, () -> handler.handle(message(
            TestAuth.authed("{\"v\":1,\"op\":\"teleport\",\"relid\":\"17001\"}"))));
    }

    @Test
    void theCredentialIsRequired() {
        assertThrows(IllegalArgumentException.class,
            () -> new DdlMessageHandler(reg, "paimon", null));
        assertThrows(IllegalArgumentException.class,
            () -> new DdlMessageHandler(reg, "paimon", ""));
    }
}
