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

    /**
     * A DDL message with a fresh, ascending LSN, as the stream delivers them. The LSN is the DDL
     * commit identifier (SchemaRegistry.DDL_LSN_OPTION), so reusing one would make the registry
     * treat the second message as a replay; tests that WANT a replay pass the LSN explicitly.
     */
    private static final java.util.concurrent.atomic.AtomicLong NEXT_LSN =
        new java.util.concurrent.atomic.AtomicLong(0x1000);

    private static PgEvent.Message msg(String json) {
        return msg(json, NEXT_LSN.addAndGet(0x10));
    }

    /** The same message at a chosen LSN: this is how a replay is spelled. */
    private static PgEvent.Message msg(String json, long lsn) {
        return new PgEvent.Message(true, lsn, "paimon", json.getBytes(StandardCharsets.UTF_8));
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
        // Out of order, the add does not take effect: the name still belongs to the old column,
        // so there is nothing to add. It is reported and the stored type left alone. (It used to
        // throw. Throwing here is what wedged the writer on "ADD COLUMN num int" followed by
        // "ALTER COLUMN num TYPE bigint": the replayed add sees BIGINT and cannot tell a
        // superseded replay from a real conflict.)
        long r2 = 30002L;
        handler.handle(msg(CREATE_FIXTURE.replace("19228", "30002").replace("cap_t", "reuse2_t")));
        List<LogRecord> warn = captureWarnings(SchemaRegistry.class, () ->
            handler.handle(msg("""
                {"v":1,"op":"add_col","col":{"id":5,"name":"note","type":"BIGINT"},"relid":"30002"}""")));
        assertEquals(1, warn.size(), warn.toString());
        assertTrue(warn.get(0).getMessage().contains("leaving the stored type alone"),
                   warn.get(0).getMessage());
        assertEquals("STRING", reg.table(r2).rowType().getField("note").type().asSQLString(),
                     "an out-of-order add must not change the stored type");
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

    /**
     * Finding 4 of the whole-branch review. A DDL message lost while the writer was down (the
     * re-bootstrap procedure drops the slot, so anything still in it is gone) used to leave the
     * Paimon schema permanently behind: the drift was warned about and RowBuilder then discarded
     * the column's value from every row for ever. A create for an existing table now APPLIES the
     * columns Paimon lacks. A conflicting difference stays a warning, because neither side can be
     * shown to be right.
     */
    @Test @Order(32)
    void replayedCreateAppliesTheColumnsPaimonIsMissing() throws Exception {
        String base = CREATE_FIXTURE.replace("19228", "40003").replace("cap_t", "recon_t");
        handler.handle(msg(base));
        assertEquals(List.of("id", "qty", "price", "note", "_seq", "_op_seq"),
                     reg.table(40003L).schema().fieldNames());

        // the same table, now with a column the writer never saw a DDL message for
        String drifted = base.replace(
            """
            {"id":4,"name":"note","type":"STRING"}""",
            """
            {"id":4,"name":"note","type":"STRING"},
                     {"id":5,"name":"extra","type":"STRING"}""");
        List<LogRecord> logs = captureWarnings(SchemaRegistry.class,
                                               () -> handler.handle(msg(drifted)));
        assertEquals(1, logs.size(), logs.toString());
        assertTrue(logs.get(0).getMessage().contains("'extra'"), logs.get(0).getMessage());
        assertTrue(logs.get(0).getMessage().contains("has been added"), logs.get(0).getMessage());
        assertEquals(List.of("id", "qty", "price", "note", "_seq", "_op_seq", "extra"),
                     reg.table(40003L).schema().fieldNames(),
                     "the missing column must be applied, not merely warned about");

        // and a second replay of the same message is then silent
        assertTrue(captureWarnings(SchemaRegistry.class,
                                   () -> handler.handle(msg(drifted))).isEmpty());

        // a column present in both with a different type is NOT reconciled
        long ver = reg.table(40003L).schema().id();
        List<LogRecord> conflict = captureWarnings(SchemaRegistry.class, () ->
            handler.handle(msg(drifted.replace("DECIMAL(10, 2)", "DECIMAL(12, 2)"))));
        assertEquals(1, conflict.size());
        assertTrue(conflict.get(0).getMessage().contains("'price'"),
                   conflict.get(0).getMessage());
        assertEquals(ver, reg.table(40003L).schema().id(),
                     "a conflicting difference must not alter the table");
    }

    /*
     * Finding 2 of the whole-branch review. A DDL-only transaction produces no records, so
     * filterAndCommit has nothing to filter and up to one status interval (10 s) of applied
     * DDL replays after a hard kill. Every op must therefore be a no-op the second time:
     * before this, add_col raised ColumnAlreadyExistException and drop_col/rename_col raised
     * ColumnNotExistException, and because the slot only advances after a successful commit
     * the writer could never get past it. One test per op; each applies it twice.
     */

    private static final long IDEM = 41000L;

    private static String idem(String json) {
        return json.replace("RELID", String.valueOf(IDEM));
    }

    /** Also the "create" case: a second create must not bump the schema version. */
    @Test @Order(40)
    void replayedCreateIsANoOp() throws Exception {
        String create = idem("""
            {"v":1,"op":"create","relid":"RELID","schema":"public","table":"idem_t",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                 {"id":2,"name":"keep","type":"STRING"},
                                 {"id":3,"name":"n","type":"INT"}],
             "options":{"changelog-producer":"input"}}""");
        handler.handle(msg(create));
        long ver = reg.table(IDEM).schema().id();
        handler.handle(msg(create));
        assertEquals(ver, reg.table(IDEM).schema().id());
    }

    @Test @Order(41)
    void replayedAddColumnIsANoOp() throws Exception {
        String add = idem("""
            {"v":1,"op":"add_col","relid":"RELID","col":{"id":4,"name":"extra","type":"BIGINT"}}""");
        handler.handle(msg(add));
        long ver = reg.table(IDEM).schema().id();
        assertTrue(reg.table(IDEM).schema().fieldNames().contains("extra"));

        assertDoesNotThrow(() -> handler.handle(msg(add)), "replayed add_col must not throw");
        assertEquals(ver, reg.table(IDEM).schema().id(), "replay must not alter the schema");
        assertEquals(1, reg.table(IDEM).schema().fieldNames().stream()
                          .filter("extra"::equals).count());
    }

    /**
     * Same name, different type: reported, never thrown, and the stored type is left alone.
     *
     * This test used to assert the opposite, and that assertion was wrong. An add_col whose
     * column exists with a different type is indistinguishable from a replayed add_col that a
     * replayed alter_col has already superseded -- "ADD COLUMN num int; ALTER COLUMN num TYPE
     * bigint" produces exactly this on replay -- so throwing turned an ordinary two-statement
     * ALTER into a permanent stop. Leaving the stored type alone is also the right outcome in
     * the replay case: it is the later, correct type.
     */
    @Test @Order(42)
    void addColumnOnAColumnOfAnotherTypeIsReportedNotThrown() throws Exception {
        long before = reg.table(IDEM).schema().id();
        List<LogRecord> warn = captureWarnings(SchemaRegistry.class, () ->
            handler.handle(msg(idem("""
                {"v":1,"op":"add_col","relid":"RELID",
                 "col":{"id":4,"name":"extra","type":"STRING"}}"""))));
        assertEquals(1, warn.size(), warn.toString());
        assertTrue(warn.get(0).getMessage().contains("extra"), warn.get(0).getMessage());
        assertTrue(warn.get(0).getMessage().contains("leaving the stored type alone"),
                   warn.get(0).getMessage());
        assertEquals("BIGINT",
            reg.table(IDEM).rowType().getField("extra").type().asSQLString());
        assertEquals(before, reg.table(IDEM).schema().id());
    }

    @Test @Order(43)
    void replayedRenameColumnIsANoOp() throws Exception {
        String rename = idem("""
            {"v":1,"op":"rename_col","relid":"RELID","id":4,"from":"extra","to":"renamed"}""");
        handler.handle(msg(rename));
        long ver = reg.table(IDEM).schema().id();
        assertTrue(reg.table(IDEM).schema().fieldNames().contains("renamed"));

        assertDoesNotThrow(() -> handler.handle(msg(rename)),
                           "replayed rename_col must not throw");
        assertEquals(ver, reg.table(IDEM).schema().id());
        assertFalse(reg.table(IDEM).schema().fieldNames().contains("extra"));
    }

    @Test @Order(44)
    void replayedAlterColumnIsANoOp() throws Exception {
        String alter = idem("""
            {"v":1,"op":"alter_col","relid":"RELID","id":3,"name":"n","type":"BIGINT"}""");
        handler.handle(msg(alter));
        long ver = reg.table(IDEM).schema().id();
        assertEquals("BIGINT", reg.table(IDEM).rowType().getField("n").type().asSQLString());

        assertDoesNotThrow(() -> handler.handle(msg(alter)));
        assertEquals(ver, reg.table(IDEM).schema().id());
    }

    @Test @Order(45)
    void replayedDropColumnIsANoOp() throws Exception {
        String drop = idem("""
            {"v":1,"op":"drop_col","relid":"RELID","name":"renamed"}""");
        handler.handle(msg(drop));
        long ver = reg.table(IDEM).schema().id();
        assertFalse(reg.table(IDEM).schema().fieldNames().contains("renamed"));

        assertDoesNotThrow(() -> handler.handle(msg(drop)), "replayed drop_col must not throw");
        assertEquals(ver, reg.table(IDEM).schema().id());
    }

    @Test @Order(46)
    void replayedDropTableIsANoOp() throws Exception {
        String drop = idem("""
            {"v":1,"op":"drop_table","relid":"RELID","schema":"public","table":"idem_t"}""");
        handler.handle(msg(drop));
        assertFalse(reg.tracked(IDEM));
        assertDoesNotThrow(() -> handler.handle(msg(drop)), "replayed drop_table must not throw");
        assertThrows(Catalog.TableNotExistException.class,
            () -> catalog.getTable(Identifier.create(cfg.database(), "idem_t")));
    }

    /*
     * Finding 2, second layer and the actual mechanism: a commit identifier for DDL.
     *
     * Per-op state inspection cannot make a SEQUENCE safe. "ADD COLUMN num int" then "ALTER
     * COLUMN num TYPE bigint" leaves a replayed add_col looking at a BIGINT column, which is
     * indistinguishable from a real conflict; "ADD COLUMN x" then "RENAME x TO y" leaves a
     * replayed add_col re-creating a spurious x for the replayed rename to collide with; two
     * renames in a row fail either way round. Data replay is safe because filterAndCommit
     * compares a commit IDENTIFIER, and DDL had none.
     *
     * Each DDL message carries its own WAL LSN, stable across replays. The LSN of the last
     * applied one is stored in the Paimon table's own schema options, written in the SAME
     * alterTable call as the column change, and any message at or below it is skipped. The
     * invariant: replaying any suffix of an applied sequence converges to the same schema and
     * never fails.
     */

    private static final long SEQ = 42000L;

    /** One DDL message of a sequence: its JSON and the LSN the stream gave it. */
    private record Step(long lsn, String json) {}

    private static List<Step> sequence() {
        return List.of(
            new Step(0xA100L, """
                {"v":1,"op":"add_col","relid":"42000","col":{"id":2,"name":"num","type":"INT"}}"""),
            new Step(0xA200L, """
                {"v":1,"op":"alter_col","relid":"42000","id":2,"name":"num","type":"BIGINT"}"""),
            new Step(0xA300L, """
                {"v":1,"op":"add_col","relid":"42000","col":{"id":3,"name":"x","type":"STRING"}}"""),
            new Step(0xA400L, """
                {"v":1,"op":"rename_col","relid":"42000","id":3,"from":"x","to":"y"}"""),
            new Step(0xA500L, """
                {"v":1,"op":"add_col","relid":"42000","col":{"id":4,"name":"extra","type":"STRING"}}"""),
            new Step(0xA600L, """
                {"v":1,"op":"rename_col","relid":"42000","id":4,"from":"extra","to":"extra2"}"""),
            new Step(0xA700L, """
                {"v":1,"op":"rename_col","relid":"42000","id":4,"from":"extra2","to":"extra3"}"""),
            new Step(0xA800L, """
                {"v":1,"op":"drop_col","relid":"42000","name":"y"}"""));
    }

    private static void apply(List<Step> steps, int from) throws Exception {
        for (int i = from; i < steps.size(); i++) {
            handler.handle(msg(steps.get(i).json(), steps.get(i).lsn()));
        }
    }

    @Test @Order(49)
    void theAppliedDdlLsnIsRecordedByTheSameAlterTableAsTheColumnChange() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"create","relid":"42000","schema":"public","table":"seq_t",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"}],
             "options":{"changelog-producer":"input"}}"""));
        assertEquals(0L, reg.appliedDdlLsn(SEQ), "a fresh table has no applied DDL LSN");

        handler.handle(msg("""
            {"v":1,"op":"add_col","relid":"42000","col":{"id":9,"name":"probe","type":"INT"}}""",
            0x9000L));
        assertTrue(reg.table(SEQ).schema().fieldNames().contains("probe"));
        assertEquals(0x9000L, reg.appliedDdlLsn(SEQ));
        assertEquals("36864", reg.table(SEQ).schema().options().get(SchemaRegistry.DDL_LSN_OPTION),
                     "the LSN lives in the table's own schema options");

        // a lower or equal LSN is a replay and is skipped outright, whatever the schema looks like
        long ver = reg.table(SEQ).schema().id();
        handler.handle(msg("""
            {"v":1,"op":"drop_col","relid":"42000","name":"probe"}""", 0x8FFFL));
        assertTrue(reg.table(SEQ).schema().fieldNames().contains("probe"),
                   "a message below the applied LSN must not be applied at all");
        assertEquals(ver, reg.table(SEQ).schema().id());

        handler.handle(msg("""
            {"v":1,"op":"drop_col","relid":"42000","name":"probe"}""", 0x9100L));
        assertFalse(reg.table(SEQ).schema().fieldNames().contains("probe"));
        assertEquals(0x9100L, reg.appliedDdlLsn(SEQ));
    }

    @Test @Order(50)
    void replayingAnySuffixOfAnAppliedSequenceConvergesAndNeverThrows() throws Exception {
        List<Step> steps = sequence();
        apply(steps, 0);
        List<String> want = reg.table(SEQ).schema().fieldNames();
        long version = reg.table(SEQ).schema().id();
        assertEquals(List.of("id", "_seq", "_op_seq", "num", "extra3"), want,
                     "added columns append after the metadata columns; RowBuilder maps by name");
        assertEquals("BIGINT", reg.table(SEQ).rowType().getField("num").type().asSQLString());

        for (int from = 0; from < steps.size(); from++) {
            final int f = from;
            assertDoesNotThrow(() -> apply(steps, f),
                "replaying the suffix from step " + f + " must not throw");
            assertEquals(want, reg.table(SEQ).schema().fieldNames(),
                         "suffix from step " + f + " must converge to the same schema");
            assertEquals(version, reg.table(SEQ).schema().id(),
                         "a replayed suffix must not alter the schema at all");
        }
    }

    /**
     * The partial case a crash actually produces: the first op of a sequence was applied and
     * recorded, the second never ran. Replaying the whole sequence must skip the first and apply
     * the second.
     */
    @Test @Order(51)
    void aPartiallyAppliedSequenceResumesAtTheRightStep() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"create","relid":"42001","schema":"public","table":"part_t",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"}],
             "options":{"changelog-producer":"input"}}"""));
        String add = """
            {"v":1,"op":"add_col","relid":"42001","col":{"id":2,"name":"num","type":"INT"}}""";
        String alter = """
            {"v":1,"op":"alter_col","relid":"42001","id":2,"name":"num","type":"BIGINT"}""";

        handler.handle(msg(add, 0xB100L));                 // applied, then the writer died
        assertEquals("INT", reg.table(42001L).rowType().getField("num").type().asSQLString());

        assertDoesNotThrow(() -> {                          // the replay covers both
            handler.handle(msg(add, 0xB100L));
            handler.handle(msg(alter, 0xB200L));
        });
        assertEquals("BIGINT", reg.table(42001L).rowType().getField("num").type().asSQLString());
        assertEquals(0xB200L, reg.appliedDdlLsn(42001L));
    }

    /** The review's three live reproductions, each a permanent wedge before the LSN gate. */
    @Test @Order(52)
    void theThreeReportedSequencesSurviveAReplay() throws Exception {
        // 1. ADD COLUMN num int; ALTER COLUMN num TYPE bigint
        handler.handle(msg("""
            {"v":1,"op":"create","relid":"42002","schema":"public","table":"wedge1_t",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"}],
             "options":{"changelog-producer":"input"}}"""));
        String a1 = """
            {"v":1,"op":"add_col","relid":"42002","col":{"id":2,"name":"num","type":"INT"}}""";
        String a2 = """
            {"v":1,"op":"alter_col","relid":"42002","id":2,"name":"num","type":"BIGINT"}""";
        handler.handle(msg(a1, 0xC100L));
        handler.handle(msg(a2, 0xC200L));
        assertDoesNotThrow(() -> {
            handler.handle(msg(a1, 0xC100L));
            handler.handle(msg(a2, 0xC200L));
        });
        assertEquals("BIGINT", reg.table(42002L).rowType().getField("num").type().asSQLString());
        assertEquals(List.of("id", "_seq", "_op_seq", "num"),
                     reg.table(42002L).schema().fieldNames());

        // 2. ADD COLUMN x text; RENAME COLUMN x TO y
        handler.handle(msg("""
            {"v":1,"op":"create","relid":"42003","schema":"public","table":"wedge2_t",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"}],
             "options":{"changelog-producer":"input"}}"""));
        String b1 = """
            {"v":1,"op":"add_col","relid":"42003","col":{"id":2,"name":"x","type":"STRING"}}""";
        String b2 = """
            {"v":1,"op":"rename_col","relid":"42003","id":2,"from":"x","to":"y"}""";
        handler.handle(msg(b1, 0xD100L));
        handler.handle(msg(b2, 0xD200L));
        assertDoesNotThrow(() -> {
            handler.handle(msg(b1, 0xD100L));
            handler.handle(msg(b2, 0xD200L));
        });
        assertEquals(List.of("id", "_seq", "_op_seq", "y"),
                     reg.table(42003L).schema().fieldNames(),
                     "no spurious 'x' may be re-created by the replayed add");

        // 3. RENAME extra -> extra2; RENAME extra2 -> extra3
        handler.handle(msg("""
            {"v":1,"op":"create","relid":"42004","schema":"public","table":"wedge3_t",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                 {"id":2,"name":"extra","type":"STRING"}],
             "options":{"changelog-producer":"input"}}"""));
        String c1 = """
            {"v":1,"op":"rename_col","relid":"42004","id":2,"from":"extra","to":"extra2"}""";
        String c2 = """
            {"v":1,"op":"rename_col","relid":"42004","id":2,"from":"extra2","to":"extra3"}""";
        handler.handle(msg(c1, 0xE100L));
        handler.handle(msg(c2, 0xE200L));
        assertDoesNotThrow(() -> {
            handler.handle(msg(c1, 0xE100L));
            handler.handle(msg(c2, 0xE200L));
        });
        assertEquals(List.of("id", "extra3", "_seq", "_op_seq"),
                     reg.table(42004L).schema().fieldNames());
    }

}
