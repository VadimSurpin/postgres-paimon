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
    void aJsonObjectWithoutAnOpIsNotADdlMessageAndIsIgnored() {
        assertDoesNotThrow(() -> handler.handle(message(raw("{\"hello\":\"world\"}"))));
        assertIgnoredWithWarning("no \"op\"");
    }

    /**
     * Review finding B2. A DDL message without the credential may be forged -- or legitimate,
     * emitted under another value of paimon.secret. Ignoring it silently lost a column for good,
     * so it stops the writer, applying nothing, and says what to check.
     */
    @Test
    void aDropTableWithoutTheCredentialStopsTheWriterAndPurgesNothing() throws Exception {
        PgEvent.Message m = message(raw("""
            {"v":1,"op":"drop_table","relid":"17001","schema":"public","table":"victim"}"""));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> handler.handle(m));
        assertTrue(e.getMessage().startsWith("REJECTED DDL MESSAGE"), e.getMessage());
        assertTrue(e.getMessage().contains("carries no credential"), e.getMessage());
        assertTrue(e.getMessage().contains("paimon.secret"), e.getMessage());
        assertTrue(e.getMessage().contains("PGPAIMON_SKIP_LSN=" + SlotConsumer.formatLsn(m.lsn())),
            e.getMessage());
        assertTrue(reg.tracked(RELID), "still tracked");
        assertNotNull(catalog.getTable(Identifier.create("public", "victim")), "Paimon table kept");
    }

    /** The rotation case of B2: a legitimate add_col under an old token is not silently lost. */
    @Test
    void aWrongCredentialStopsTheWriterAndAddsNothing() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> handler.handle(message(raw("""
                {"v":1,"auth":"an-older-token","op":"add_col","relid":"17001",
                 "col":{"id":2,"name":"c","type":"INT"}}"""))));
        assertTrue(e.getMessage().contains("not this installation's paimon.secret"), e.getMessage());
        assertTrue(e.getMessage().contains("(op 'add_col', relation 17001)"), e.getMessage());
        assertTrue(reg.table(RELID).rowType().getFieldNames().stream()
            .noneMatch("c"::equals), "no column added");
        assertTrue(logged.isEmpty(), "not a warning: " + logged);
    }

    /** PGPAIMON_SKIP_LSN at exactly the message's LSN skips it, applying nothing, said loudly. */
    @Test
    void theSkipLsnSkipsExactlyThatRejectedMessage() throws Exception {
        long next = lsn + 0x10;
        DdlMessageHandler armed = new DdlMessageHandler(reg, "paimon", TestAuth.TOKEN, next, null);
        String forged = """
            {"v":1,"op":"drop_table","relid":"17001","schema":"public","table":"victim"}""";
        assertDoesNotThrow(() -> armed.handle(message(raw(forged))));
        assertEquals(1, logged.size());
        assertTrue(logged.get(0).getMessage().startsWith("SKIPPED DDL MESSAGE (PGPAIMON_SKIP_LSN="),
            logged.get(0).getMessage());
        assertTrue(reg.tracked(RELID), "still tracked");
        assertNotNull(catalog.getTable(Identifier.create("public", "victim")), "Paimon table kept");
        // any other LSN stays fatal
        assertThrows(IllegalStateException.class, () -> armed.handle(message(raw(forged))));
    }

    /**
     * A secret rotated while the writer runs: the next DDL carries the NEW value. The handler
     * re-reads paimon.secret, finds it equal, applies the message and keeps the new value.
     */
    @Test
    void aSecretRotatedWhileRunningIsFollowed() {
        int[] reads = {0};
        DdlMessageHandler h = new DdlMessageHandler(reg, "paimon", TestAuth.TOKEN, 0,
            () -> { reads[0]++; return "rotated"; });
        h.handle(message(raw("""
            {"v":1,"auth":"rotated","op":"add_col","relid":"17001",
             "col":{"id":2,"name":"after_rotation","type":"INT"}}""")));
        assertTrue(reg.table(RELID).rowType().getFieldNames().contains("after_rotation"));
        h.handle(message(raw("""
            {"v":1,"auth":"rotated","op":"add_col","relid":"17001",
             "col":{"id":3,"name":"again","type":"INT"}}""")));
        assertEquals(1, reads[0], "re-read once, then the new value is the one in hand");
        // the old value, still in the slot after the rotation, is now the anomaly
        assertThrows(IllegalStateException.class, () -> h.handle(message(TestAuth.authed("""
            {"v":1,"op":"add_col","relid":"17001","col":{"id":4,"name":"old","type":"INT"}}"""))));
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
