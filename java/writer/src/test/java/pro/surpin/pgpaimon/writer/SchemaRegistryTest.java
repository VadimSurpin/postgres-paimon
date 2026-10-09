// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.catalog.*;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.*;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SchemaRegistryTest {

    @TempDir static Path warehouse;
    static Catalog catalog;
    static SchemaRegistry reg;
    static WriterConfig cfg;
    static DdlMessageHandler handler;

    static final long RELID = 16412L;

    @BeforeAll
    static void setUp() {
        Options o = new Options();
        o.set("warehouse", warehouse.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
        Map<String, String> env = new HashMap<>();
        env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost:5432/testdb");
        env.put("PGPAIMON_WAREHOUSE", warehouse.toString());
        cfg = WriterConfig.fromEnv(env);
        reg = new SchemaRegistry(catalog, cfg);
        handler = new DdlMessageHandler(reg, "paimon");
    }

    private static PgEvent.Message msg(String json) {
        return new PgEvent.Message(true, 1L, "paimon", json.getBytes(StandardCharsets.UTF_8));
    }

    @Test @Order(1)
    void createMakesAPrimaryKeyTableWithTheSpecOptions() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"create","relid":"16412","schema":"public","table":"orders",
             "pk":["id"],
             "cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                     {"id":2,"name":"amount","type":"DECIMAL(10, 2)"},
                     {"id":3,"name":"note","type":"STRING"}],
             "options":{"changelog-producer":"input"}}"""));

        FileStoreTable t = reg.table(RELID);
        assertEquals(List.of("id"), t.schema().primaryKeys());
        Map<String, String> opts = t.schema().options();
        assertEquals("deduplicate", opts.get("merge-engine"));
        assertEquals("input", opts.get("changelog-producer"));
        assertEquals("_seq,_op_seq", opts.get("sequence.field"));
        assertEquals("false", opts.get("ignore-update-before"));
        assertEquals(List.of(), t.schema().partitionKeys());

        List<String> names = t.schema().fieldNames();
        assertEquals(List.of("id", "amount", "note", "_seq", "_op_seq"), names);
    }

    @Test @Order(2)
    void createIsIdempotent() throws Exception {
        long before = reg.table(RELID).schema().id();
        handler.handle(msg("""
            {"v":1,"op":"create","relid":"16412","schema":"public","table":"orders",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                     {"id":2,"name":"amount","type":"DECIMAL(10, 2)"},
                     {"id":3,"name":"note","type":"STRING"}],
             "options":{"changelog-producer":"input"}}"""));
        assertEquals(before, reg.table(RELID).schema().id());
    }

    @Test @Order(3)
    void addColumn() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"add_col","relid":"16412",
             "col":{"id":4,"name":"qty","type":"BIGINT"}}"""));
        assertTrue(reg.table(RELID).schema().fieldNames().contains("qty"));
    }

    @Test @Order(4)
    void renameColumn() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"rename_col","relid":"16412","id":4,"from":"qty","to":"quantity"}"""));
        List<String> names = reg.table(RELID).schema().fieldNames();
        assertTrue(names.contains("quantity"));
        assertFalse(names.contains("qty"));
    }

    @Test @Order(5)
    void alterColumnType() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"alter_col","relid":"16412","id":2,
             "name":"amount","type":"DECIMAL(20, 2)"}"""));
        assertEquals("DECIMAL(20, 2)",
            reg.table(RELID).schema().fields().stream()
               .filter(f -> f.name().equals("amount")).findFirst()
               .orElseThrow().type().asSQLString());
    }

    @Test @Order(6)
    void alterColumnToTheSameTypeIsANoOp() throws Exception {
        long before = reg.table(RELID).schema().id();
        handler.handle(msg("""
            {"v":1,"op":"alter_col","relid":"16412","id":3,
             "name":"note","type":"STRING"}"""));
        assertEquals(before, reg.table(RELID).schema().id(),
                     "an identical type must not create a schema version");
    }

    /** gaps.md item 1: DROP COLUMN was never propagated on main. */
    @Test @Order(7)
    void dropColumnIsPropagated() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"drop_col","relid":"16412","name":"quantity"}"""));
        assertFalse(reg.table(RELID).schema().fieldNames().contains("quantity"));
    }

    /** gaps.md item 3: DROP TABLE left warehouse data behind on main. */
    @Test @Order(8)
    void dropTableRemovesTheWarehouseData() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"drop_table","relid":"16412",
             "schema":"public","table":"orders"}"""));
        // Catalog.tableExists does not exist in Paimon 1.4.1; getTable must now fail.
        assertThrows(Catalog.TableNotExistException.class,
            () -> catalog.getTable(Identifier.create(cfg.database(), "orders")));
        assertFalse(java.nio.file.Files.exists(warehouse.resolve(cfg.database() + ".db").resolve("orders")),
            "the table directory must be purged from the warehouse");
        assertFalse(reg.tracked(RELID));
    }

    @Test @Order(9)
    void aMessageWithAForeignPrefixIsIgnored() {
        assertDoesNotThrow(() -> handler.handle(new PgEvent.Message(
            true, 1L, "someone_else", "garbage".getBytes(StandardCharsets.UTF_8))));
    }

    @Test @Order(10)
    void anUnknownOpNamesItself() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> handler.handle(msg("{\"v\":1,\"op\":\"teleport\",\"relid\":\"1\"}")));
        assertTrue(e.getMessage().contains("teleport"));
    }

    @Test @Order(11)
    void aFutureWireVersionIsRefusedRatherThanMisread() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> handler.handle(msg("{\"v\":99,\"op\":\"create\",\"relid\":\"1\"}")));
        assertTrue(e.getMessage().contains("99"));
    }

    /** Spec section 8: DDL for an unregistered table is ignored with a warning. */
    @Test @Order(12)
    void ddlForAnUnregisteredTableIsIgnoredNotFatal() {
        List<LogRecord> logged = new ArrayList<>();
        Logger log = Logger.getLogger(DdlMessageHandler.class.getName());
        Handler h = new Handler() {
            public void publish(LogRecord r) { logged.add(r); }
            public void flush() {}
            public void close() {}
        };
        log.addHandler(h);
        try {
            ignoredBody();
        } finally {
            log.removeHandler(h);
        }
        assertEquals(2, logged.size());
        assertTrue(logged.stream().allMatch(r -> r.getLevel() == Level.WARNING));
        assertTrue(logged.get(0).getMessage().contains("999999"));
    }

    private void ignoredBody() {
        assertDoesNotThrow(() -> handler.handle(msg("""
            {"v":1,"op":"add_col","relid":"999999",
             "col":{"id":1,"name":"ghost","type":"INT"}}""")));
        assertDoesNotThrow(() -> handler.handle(msg("""
            {"v":1,"op":"drop_table","relid":"999999",
             "schema":"public","table":"ghost"}""")));
        assertFalse(reg.tracked(999999));
    }

    // ---- additions beyond the brief ----

    static final String CREATE_FIXTURE = """
        {"v":1,"op":"create","pk":["id"],
         "cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                 {"id":2,"name":"qty","type":"INT"},
                 {"id":3,"name":"price","type":"DECIMAL(10, 2)"},
                 {"id":4,"name":"note","type":"STRING"}],
         "relid":"19228","table":"cap_t","schema":"public",
         "options":{"changelog-producer":"input"}}""";

    @Test @Order(20)
    void realCapturedLifecycle() throws Exception {
        long r = 19228L;
        handler.handle(msg(CREATE_FIXTURE));
        assertEquals(List.of("id", "qty", "price", "note", "_seq", "_op_seq"),
            reg.table(r).schema().fieldNames());
        assertFalse(reg.table(r).schema().options().containsKey("bucket"),
            "no bucket option: dynamic bucket mode, the sink writes bucket 0");
        assertFalse(reg.table(r).rowType().getField("id").type().isNullable());
        assertTrue(reg.table(r).rowType().getField("qty").type().isNullable());

        handler.handle(msg("""
            {"v":1,"op":"add_col","col":{"id":5,"name":"extra","type":"BIGINT"},"relid":"19228"}"""));
        handler.handle(msg("""
            {"v":1,"id":2,"op":"rename_col","to":"quantity","from":"qty","relid":"19228"}"""));
        handler.handle(msg("""
            {"v":1,"id":5,"op":"alter_col","name":"extra","type":"STRING","relid":"19228"}"""));
        handler.handle(msg("""
            {"v":1,"op":"drop_col","name":"note","relid":"19228"}"""));
        assertEquals(List.of("id", "quantity", "price", "_seq", "_op_seq", "extra"),
            reg.table(r).schema().fieldNames());
        // add_col appends after the metadata columns; RowBuilder maps by name so this is harmless.
        assertEquals("STRING", reg.table(r).rowType().getField("extra").type().asSQLString());
        handler.handle(msg("""
            {"v":1,"op":"drop_table","relid":"19228","table":"cap_t","schema":"public"}"""));
        assertFalse(reg.tracked(r));
    }

    /** Emitted order within one ALTER TABLE: drop, rename/alter, add. A name can be reused. */
    @Test @Order(21)
    void droppedNameCanBeReusedByAnAddInTheSameStatement() throws Exception {
        long r = 30001L;
        handler.handle(msg(CREATE_FIXTURE.replace("19228", "30001").replace("cap_t", "reuse_t")));
        handler.handle(msg("{\"v\":1,\"op\":\"drop_col\",\"name\":\"note\",\"relid\":\"30001\"}"));
        handler.handle(msg("""
            {"v":1,"op":"add_col","col":{"id":5,"name":"note","type":"BIGINT"},"relid":"30001"}"""));
        assertEquals("BIGINT",
            reg.table(r).rowType().getField("note").type().asSQLString());
        // out of order would collide:
        long r2 = 30002L;
        handler.handle(msg(CREATE_FIXTURE.replace("19228", "30002").replace("cap_t", "reuse2_t")));
        assertThrows(IllegalStateException.class, () -> handler.handle(msg("""
            {"v":1,"op":"add_col","col":{"id":5,"name":"note","type":"BIGINT"},"relid":"30002"}""")));
    }

    @Test @Order(22)
    void relidAboveIntMaxIsHeldUnsigned() throws Exception {
        long big = 4_000_000_000L;
        handler.handle(msg(CREATE_FIXTURE.replace("\"19228\"", "\"4000000000\"")
                                         .replace("cap_t", "big_t")));
        assertTrue(reg.tracked(big));
        assertEquals("big_t", reg.table(big).name());
    }

    @Test @Order(23)
    void rowBuilderIsCachedPerColumnListAndRebuiltOnChange() throws Exception {
        long r = 30001L;
        RelationMeta m1 = new RelationMeta(r, "public", "reuse_t",
            List.of("id", "qty", "price", "note"), List.of(true, false, false, false));
        RowBuilder a = reg.rowBuilder(r, m1);
        assertSame(a, reg.rowBuilder(r, m1));
        assertSame(a, reg.rowBuilder(r, new RelationMeta(r, "public", "reuse_t",
            List.of("id", "qty", "price", "note"), List.of(true, false, false, false))));
        RelationMeta m2 = new RelationMeta(r, "public", "reuse_t",
            List.of("id", "qty", "price"), List.of(true, false, false));
        assertNotSame(a, reg.rowBuilder(r, m2));
    }

    @Test @Order(24)
    void forgetDropsCachedStateSoTheTableReloads() throws Exception {
        long r = 30001L;
        RelationMeta m = new RelationMeta(r, "public", "reuse_t",
            List.of("id", "qty", "price"), List.of(true, false, false));
        RowBuilder before = reg.rowBuilder(r, m);
        reg.forget(r);
        assertFalse(reg.tracked(r));
        assertThrows(IllegalStateException.class, () -> reg.table(r));
        // re-issued create (as at startup) re-tracks the existing table without touching it
        handler.handle(msg(CREATE_FIXTURE.replace("19228", "30001").replace("cap_t", "reuse_t")));
        assertTrue(reg.tracked(r));
        assertNotSame(before, reg.rowBuilder(r, m));
    }

    @Test @Order(25)
    void aPrecisionQualifiedTimestampIsRefused() {
        assertThrows(IllegalStateException.class, () -> SchemaRegistry.parseType("TIMESTAMP(3)"));
        assertThrows(IllegalStateException.class,
            () -> SchemaRegistry.parseType("TIMESTAMP(3) WITH LOCAL TIME ZONE"));
        assertEquals("TIMESTAMP(6)", SchemaRegistry.parseType("TIMESTAMP").asSQLString());
        assertEquals("TIMESTAMP(6) WITH LOCAL TIME ZONE",
            SchemaRegistry.parseType("TIMESTAMP WITH LOCAL TIME ZONE NOT NULL")
                .copy(true).asSQLString());
    }

    /** Pins the real Paimon 1.4.1 rendering that Task 12 may depend on. */
    @Test @Order(26)
    void asSqlStringRendering() {
        assertEquals("DECIMAL(10, 2)", SchemaRegistry.parseType("DECIMAL(10, 2)").asSQLString());
        assertEquals("DECIMAL(10, 2) NOT NULL",
            SchemaRegistry.parseType("DECIMAL(10, 2) NOT NULL").asSQLString());
        assertEquals("INT NOT NULL", SchemaRegistry.parseType("INT NOT NULL").asSQLString());
        assertEquals(SchemaRegistry.parseType("DECIMAL(10,2)"),
                     SchemaRegistry.parseType("DECIMAL(10, 2)"));
    }

    private static List<LogRecord> captureWarnings(Class<?> c, Runnable r) {
        List<LogRecord> logged = new ArrayList<>();
        Logger log = Logger.getLogger(c.getName());
        Handler h = new Handler() {
            public void publish(LogRecord x) { logged.add(x); }
            public void flush() {}
            public void close() {}
        };
        log.addHandler(h);
        try { r.run(); } finally { log.removeHandler(h); }
        return logged;
    }

    @Test @Order(30)
    void xidColumnIsCreatedWhenConfigured() throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost:5432/testdb");
        env.put("PGPAIMON_WAREHOUSE", warehouse.toString());
        env.put("PGPAIMON_XID_FIELD", "_xid");
        SchemaRegistry xr = new SchemaRegistry(catalog, WriterConfig.fromEnv(env));
        xr.createTable(40001L, "public", "xid_t", List.of("id"),
            List.of(new DdlMessage.Col(1, "id", "INT NOT NULL")));
        assertEquals(List.of("id", "_seq", "_op_seq", "_xid"),
            xr.table(40001L).schema().fieldNames());
        assertEquals("BIGINT", xr.table(40001L).rowType().getField("_xid").type().asSQLString());
    }

    @Test @Order(31)
    void replayedCreateThatDriftsWarnsNamingTheDifference() throws Exception {
        handler.handle(msg(CREATE_FIXTURE.replace("19228", "40002").replace("cap_t", "drift_t")));
        long ver = reg.table(40002L).schema().id();

        List<LogRecord> a = captureWarnings(SchemaRegistry.class, () ->
            handler.handle(msg(CREATE_FIXTURE.replace("19228", "40002").replace("cap_t", "drift_t")
                .replace("DECIMAL(10, 2)", "DECIMAL(12, 2)"))));
        assertEquals(1, a.size());
        assertEquals(Level.WARNING, a.get(0).getLevel());
        assertTrue(a.get(0).getMessage().contains("'price'"), a.get(0).getMessage());
        assertTrue(a.get(0).getMessage().contains("DECIMAL(10, 2)"));

        List<LogRecord> b = captureWarnings(SchemaRegistry.class, () ->
            handler.handle(msg(CREATE_FIXTURE.replace("19228", "40002").replace("cap_t", "drift_t")
                .replace("\"pk\":[\"id\"]", "\"pk\":[\"id\",\"qty\"]"))));
        assertEquals(1, b.size());
        assertTrue(b.get(0).getMessage().contains("primary key"), b.get(0).getMessage());
        assertTrue(b.get(0).getMessage().contains("qty"));

        assertEquals(ver, reg.table(40002L).schema().id(), "drift must not alter the table");

        List<LogRecord> clean = captureWarnings(SchemaRegistry.class, () ->
            handler.handle(msg(CREATE_FIXTURE.replace("19228", "40002").replace("cap_t", "drift_t"))));
        assertTrue(clean.isEmpty(), "an identical replay must not warn");
    }
}
