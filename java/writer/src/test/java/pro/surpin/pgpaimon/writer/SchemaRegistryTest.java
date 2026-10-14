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
        handler = new DdlMessageHandler(reg, "paimon", TestAuth.TOKEN);
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
        return new PgEvent.Message(true, lsn, "paimon", TestAuth.authed(json));
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
            () -> catalog.getTable(Identifier.create("public", "orders")));
        assertFalse(java.nio.file.Files.exists(warehouse.resolve("public.db").resolve("orders")),
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
        assertEquals(String.valueOf(SchemaRegistry.DEFAULT_BUCKETS),
            reg.table(r).schema().options().get("bucket"),
            "fixed-bucket mode: without it Paimon keeps a key index and the sink's write(row)"
            + " is rejected");
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
        // Out of order the add cannot take effect: the name still belongs to the old column, so
        // there is nothing to add. What happens then depends on whether the table has a recorded
        // DDL LSN. This one does -- its create recorded one -- so a replay was already filtered
        // by the gate and reaching the conflict means genuine drift, which is loud. The stored
        // type is left alone either way.
        long r2 = 30002L;
        handler.handle(msg(CREATE_FIXTURE.replace("19228", "30002").replace("cap_t", "reuse2_t")));
        assertTrue(reg.appliedDdlLsn(r2) != 0);
        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
            handler.handle(msg("""
                {"v":1,"op":"add_col","col":{"id":5,"name":"note","type":"BIGINT"},"relid":"30002"}""")));
        assertTrue(e.getMessage().contains("genuinely disagree"), e.getMessage());
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
        RowMaker a = reg.rowBuilder(r, m1);
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
        RowMaker before = reg.rowBuilder(r, m);
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

    /**
     * What the registry logs about MAIN tables while {@code r} runs. Lines about a history table
     * (they say "history table") are left out: the same DDL is fanned out to it and logs its own
     * lines, which SchemaRegistryHistoryTest asserts.
     */
    private static List<LogRecord> captureWarnings(Class<?> c, Runnable r) {
        List<LogRecord> logged = new ArrayList<>();
        Logger log = Logger.getLogger(c.getName());
        Handler h = new Handler() {
            public void publish(LogRecord x) {
                if (!x.getMessage().contains("history table")) {
                    logged.add(x);
                }
            }
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

        // A different KEY is no drift to warn about: merged on another key, the table would
        // collapse rows PostgreSQL keeps apart. Refused, and nothing is altered (review of the
        // history tables, I4: a table re-registered after its key changed).
        IllegalStateException b = assertThrows(IllegalStateException.class, () ->
            handler.handle(msg(CREATE_FIXTURE.replace("19228", "40002").replace("cap_t", "drift_t")
                .replace("\"pk\":[\"id\"]", "\"pk\":[\"id\",\"qty\"]"))));
        assertTrue(b.getMessage().contains("primary key [id, qty]"), b.getMessage());

        assertEquals(ver, reg.table(40002L).schema().id(), "drift must not alter the table");

        List<LogRecord> clean = captureWarnings(SchemaRegistry.class, () ->
            handler.handle(msg(CREATE_FIXTURE.replace("19228", "40002").replace("cap_t", "drift_t"))));
        assertTrue(clean.isEmpty(), "an identical replay must not warn");
    }

    /**
     * Finding 4 of the whole-branch review, and its own regression.
     *
     * A DDL message lost with the slot (the re-bootstrap procedure drops it) used to leave the
     * Paimon schema permanently behind: the drift was warned about and RowBuilder then discarded
     * the column's value from every row for ever. The bootstrap now APPLIES the columns Paimon
     * lacks -- but only when the slot is ABSENT, which is the only situation in which a message
     * can have been lost. With the slot present the same difference means the message is still
     * in the stream, and fabricating the column made the pending rename_col/add_col collide,
     * wedging the writer on an ordinary RENAME COLUMN done while it was stopped.
     *
     * A conflicting difference stays a warning either way, because neither side can be shown to
     * be right.
     */
    @Test @Order(32)
    void bootstrapAppliesTheMissingColumnsOnlyWhenTheSlotIsAbsent() throws Exception {
        String base = CREATE_FIXTURE.replace("19228", "40003").replace("cap_t", "recon_t");
        handler.handle(msg(base));
        List<String> original = List.of("id", "qty", "price", "note", "_seq", "_op_seq");
        assertEquals(original, reg.table(40003L).schema().fieldNames());

        // The registry row now carries a column Paimon has not got. At bootstrap that always
        // happens when a DDL message is still unconsumed, so it only means "lost" if the slot
        // has gone.
        List<DdlMessage.Col> drifted = List.of(
            new DdlMessage.Col(1, "id", "INT NOT NULL"),
            new DdlMessage.Col(2, "qty", "INT"),
            new DdlMessage.Col(3, "price", "DECIMAL(10, 2)"),
            new DdlMessage.Col(4, "note", "STRING"),
            new DdlMessage.Col(5, "extra", "STRING"));

        // slot PRESENT: reported, never applied -- the pending message will apply it
        long ver = reg.table(40003L).schema().id();
        List<LogRecord> pending = captureWarnings(SchemaRegistry.class, () ->
            reg.bootstrapTable(40003L, "public", "recon_t", List.of("id"), drifted, "input",
                               false));
        assertEquals(1, pending.size(), pending.toString());
        assertTrue(pending.get(0).getMessage().contains("should still be in the stream"),
                   pending.get(0).getMessage());
        assertEquals(original, reg.table(40003L).schema().fieldNames(),
                     "with the slot present the column must NOT be fabricated");
        assertEquals(ver, reg.table(40003L).schema().id());

        // slot ABSENT: the message is lost, so apply it
        List<LogRecord> lost = captureWarnings(SchemaRegistry.class, () ->
            reg.bootstrapTable(40003L, "public", "recon_t", List.of("id"), drifted, "input",
                               true));
        assertEquals(1, lost.size(), lost.toString());
        assertTrue(lost.get(0).getMessage().contains("'extra'"), lost.get(0).getMessage());
        assertTrue(lost.get(0).getMessage().contains("has been added"), lost.get(0).getMessage());
        assertEquals(List.of("id", "qty", "price", "note", "_seq", "_op_seq", "extra"),
                     reg.table(40003L).schema().fieldNames(),
                     "the missing column must be applied, not merely warned about");

        // and a second bootstrap is then silent
        assertTrue(captureWarnings(SchemaRegistry.class, () ->
            reg.bootstrapTable(40003L, "public", "recon_t", List.of("id"), drifted, "input",
                               true)).isEmpty());

        // a column present in both with a different type is NOT reconciled, slot or no slot
        long ver2 = reg.table(40003L).schema().id();
        List<DdlMessage.Col> conflicting = new ArrayList<>(drifted);
        conflicting.set(2, new DdlMessage.Col(3, "price", "DECIMAL(12, 2)"));
        List<LogRecord> conflict = captureWarnings(SchemaRegistry.class, () ->
            reg.bootstrapTable(40003L, "public", "recon_t", List.of("id"), conflicting, "input",
                               true));
        assertEquals(1, conflict.size(), conflict.toString());
        assertTrue(conflict.get(0).getMessage().contains("'price'"),
                   conflict.get(0).getMessage());
        assertEquals(ver2, reg.table(40003L).schema().id(),
                     "a conflicting difference must not alter the table");
    }

    /** A streamed create message never reconciles: only the startup bootstrap can. */
    @Test @Order(33)
    void aStreamedCreateNeverReconciles() throws Exception {
        String base = CREATE_FIXTURE.replace("19228", "40004").replace("cap_t", "nostream_t");
        handler.handle(msg(base));
        long ver = reg.table(40004L).schema().id();
        String drifted = base.replace(
            """
            {"id":4,"name":"note","type":"STRING"}""",
            """
            {"id":4,"name":"note","type":"STRING"},
                     {"id":5,"name":"extra","type":"STRING"}""");
        captureWarnings(SchemaRegistry.class, () -> handler.handle(msg(drifted)));
        assertEquals(ver, reg.table(40004L).schema().id());
        assertFalse(reg.table(40004L).schema().fieldNames().contains("extra"));
    }

    /*
     * Finding 2 of the whole-branch review, first layer: the state-based tolerances.
     *
     * A DDL-only transaction produces no records, so filterAndCommit has nothing to filter, and
     * the resume position is the highest Paimon commit identifier, which such a transaction never
     * moves -- so applied DDL replays. Before this, add_col raised ColumnAlreadyExistException and
     * drop_col/rename_col raised ColumnNotExistException, and because the slot only advances after
     * a successful commit the writer could never get past it.
     *
     * Each test below applies its op twice at DIFFERENT LSNs (msg() mints a fresh one), so the
     * DDL_LSN_OPTION gate does not fire and what is exercised is the state inspection: the second
     * line of defence, which is all a table with no recorded DDL LSN has. The gate itself, and
     * replayed SEQUENCES (which state inspection cannot handle), are covered from @Order(49).
     */

    private static final long IDEM = 41000L;
    /** Explicit ascending LSNs for the IDEM block, so a replay can reuse one exactly. */
    private static final long IDEM_LSN = 0x50000L;

    private static String idem(String json) {
        return json.replace("RELID", String.valueOf(IDEM));
    }

    /**
     * Also the "create" case: a replayed create must not bump the schema version. A replay
     * carries the SAME LSN the message had, so that is how it is spelled here; a create also
     * records that LSN as the table's applied-DDL identifier, which is what lets a stale
     * drop_table be recognised later.
     */
    @Test @Order(40)
    void replayedCreateIsANoOp() throws Exception {
        String create = idem("""
            {"v":1,"op":"create","relid":"RELID","schema":"public","table":"idem_t",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                 {"id":2,"name":"keep","type":"STRING"},
                                 {"id":3,"name":"n","type":"INT"}],
             "options":{"changelog-producer":"input"}}""");
        long createLsn = IDEM_LSN;
        handler.handle(msg(create, createLsn));
        long ver = reg.table(IDEM).schema().id();
        assertEquals(createLsn, reg.appliedDdlLsn(IDEM),
                     "a create records its own LSN, atomically with the table's creation");

        handler.handle(msg(create, createLsn));
        assertEquals(ver, reg.table(IDEM).schema().id());
        assertEquals(createLsn, reg.appliedDdlLsn(IDEM));
    }

    @Test @Order(41)
    void replayedAddColumnIsANoOp() throws Exception {
        String add = idem("""
            {"v":1,"op":"add_col","relid":"RELID","col":{"id":4,"name":"extra","type":"BIGINT"}}""");
        handler.handle(msg(add, IDEM_LSN + 0x100));
        long ver = reg.table(IDEM).schema().id();
        assertTrue(reg.table(IDEM).schema().fieldNames().contains("extra"));

        assertDoesNotThrow(() -> handler.handle(msg(add, IDEM_LSN + 0x100)),
                           "replayed add_col must not throw");
        assertEquals(ver, reg.table(IDEM).schema().id(), "replay must not alter the schema");
        assertEquals(1, reg.table(IDEM).schema().fieldNames().stream()
                          .filter("extra"::equals).count());
    }

    /**
     * Same name, different type, on a table with NO recorded DDL LSN: reported, never thrown,
     * and the stored type left alone.
     *
     * This test used to assert a throw unconditionally, and that was wrong. An add_col whose
     * column exists with a different type is indistinguishable from a replayed add_col that a
     * replayed alter_col has superseded -- "ADD COLUMN num int; ALTER COLUMN num TYPE bigint"
     * produces exactly this on replay -- so throwing turned an ordinary two-statement ALTER into
     * a permanent stop. Leaving the stored type alone is also right in the replay case: it is
     * the later type. But the tolerance is only sound where a replay cannot be ruled out, which
     * is why it is keyed on the absence of an identifier; see the next test.
     */
    @Test @Order(42)
    void addColumnOnAnotherTypeIsToleratedOnlyWithoutARecordedLsn() throws Exception {
        // A table with no recorded DDL LSN: one whose DDL predates the identifier mechanism.
        // The startup bootstrap replays registry rows rather than messages and so has no LSN to
        // record, which is exactly how such a table arises after an upgrade.
        reg.bootstrapTable(41100L, "public", "nolsn_t", List.of("id"),
            List.of(new DdlMessage.Col(1, "id", "INT NOT NULL"),
                    new DdlMessage.Col(2, "n", "BIGINT")), "input", false);
        assertEquals(0L, reg.appliedDdlLsn(41100L));
        long before = reg.table(41100L).schema().id();

        List<LogRecord> warn = captureWarnings(SchemaRegistry.class, () ->
            handler.handle(msg("""
                {"v":1,"op":"add_col","relid":41100,
                 "col":{"id":2,"name":"n","type":"INT"}}""")));
        assertEquals(1, warn.size(), warn.toString());
        assertTrue(warn.get(0).getMessage().contains("leaving the stored type alone"),
                   warn.get(0).getMessage());
        assertTrue(warn.get(0).getMessage().contains("no recorded DDL LSN"),
                   warn.get(0).getMessage());
        assertEquals("BIGINT", reg.table(41100L).rowType().getField("n").type().asSQLString());
        assertEquals(before, reg.table(41100L).schema().id());
    }

    /**
     * The other half, and the signal the tolerance must not swallow: once a table HAS a recorded
     * DDL LSN, a replay is filtered by the gate before any state is inspected, so reaching the
     * tolerance means the Paimon schema and paimon.table_registry genuinely disagree. That has
     * to be loud.
     */
    @Test @Order(42)
    void withARecordedLsnTheSameConflictFailsLoudly() throws Exception {
        assertTrue(reg.appliedDdlLsn(IDEM) != 0, "earlier ops recorded an LSN for this table");
        long before = reg.table(IDEM).schema().id();
        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
            handler.handle(msg(idem("""
                {"v":1,"op":"add_col","relid":"RELID",
                 "col":{"id":4,"name":"extra","type":"STRING"}}"""), IDEM_LSN + 0x180)));
        assertTrue(e.getMessage().contains("not a replay"), e.getMessage());
        assertTrue(e.getMessage().contains("genuinely disagree"), e.getMessage());
        assertTrue(e.getMessage().contains("re-register"), e.getMessage());
        assertEquals("BIGINT",
            reg.table(IDEM).rowType().getField("extra").type().asSQLString());
        assertEquals(before, reg.table(IDEM).schema().id());
    }

    /** The same split for alter_col on a column Paimon does not have. */
    @Test @Order(42)
    void alterColumnOnAMissingColumnIsToleratedOnlyWithoutARecordedLsn() throws Exception {
        reg.bootstrapTable(41101L, "public", "nolsn2_t", List.of("id"),
            List.of(new DdlMessage.Col(1, "id", "INT NOT NULL")), "input", false);
        assertEquals(0L, reg.appliedDdlLsn(41101L));
        List<LogRecord> warn = captureWarnings(SchemaRegistry.class, () ->
            handler.handle(msg("""
                {"v":1,"op":"alter_col","relid":41101,"id":9,"name":"ghost","type":"INT"}""")));
        assertEquals(1, warn.size(), warn.toString());
        assertTrue(warn.get(0).getMessage().contains("no recorded DDL LSN"),
                   warn.get(0).getMessage());

        // with an identifier recorded, the same message is a genuine disagreement
        handler.handle(msg("""
            {"v":1,"op":"add_col","relid":41101,"col":{"id":2,"name":"real","type":"INT"}}"""));
        assertTrue(reg.appliedDdlLsn(41101L) != 0);
        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
            handler.handle(msg("""
                {"v":1,"op":"alter_col","relid":41101,"id":9,"name":"ghost","type":"INT"}""")));
        assertTrue(e.getMessage().contains("genuinely disagree"), e.getMessage());
    }

    @Test @Order(43)
    void replayedRenameColumnIsANoOp() throws Exception {
        String rename = idem("""
            {"v":1,"op":"rename_col","relid":"RELID","id":4,"from":"extra","to":"renamed"}""");
        handler.handle(msg(rename, IDEM_LSN + 0x200));
        long ver = reg.table(IDEM).schema().id();
        assertTrue(reg.table(IDEM).schema().fieldNames().contains("renamed"));

        assertDoesNotThrow(() -> handler.handle(msg(rename, IDEM_LSN + 0x200)),
                           "replayed rename_col must not throw");
        assertEquals(ver, reg.table(IDEM).schema().id());
        assertFalse(reg.table(IDEM).schema().fieldNames().contains("extra"));
    }

    @Test @Order(44)
    void replayedAlterColumnIsANoOp() throws Exception {
        String alter = idem("""
            {"v":1,"op":"alter_col","relid":"RELID","id":3,"name":"n","type":"BIGINT"}""");
        handler.handle(msg(alter, IDEM_LSN + 0x300));
        long ver = reg.table(IDEM).schema().id();
        assertEquals("BIGINT", reg.table(IDEM).rowType().getField("n").type().asSQLString());

        assertDoesNotThrow(() -> handler.handle(msg(alter, IDEM_LSN + 0x300)));
        assertEquals(ver, reg.table(IDEM).schema().id());
    }

    @Test @Order(45)
    void replayedDropColumnIsANoOp() throws Exception {
        String drop = idem("""
            {"v":1,"op":"drop_col","relid":"RELID","name":"renamed"}""");
        handler.handle(msg(drop, IDEM_LSN + 0x400));
        long ver = reg.table(IDEM).schema().id();
        assertFalse(reg.table(IDEM).schema().fieldNames().contains("renamed"));

        assertDoesNotThrow(() -> handler.handle(msg(drop, IDEM_LSN + 0x400)),
                           "replayed drop_col must not throw");
        assertEquals(ver, reg.table(IDEM).schema().id());
    }

    @Test @Order(46)
    void replayedDropTableIsANoOp() throws Exception {
        String drop = idem("""
            {"v":1,"op":"drop_table","relid":"RELID","schema":"public","table":"idem_t"}""");
        handler.handle(msg(drop, IDEM_LSN + 0x500));
        assertFalse(reg.tracked(IDEM));
        assertDoesNotThrow(() -> handler.handle(msg(drop, IDEM_LSN + 0x500)),
                           "replayed drop_table must not throw");
        assertThrows(Catalog.TableNotExistException.class,
            () -> catalog.getTable(Identifier.create("public", "idem_t")));
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
             "options":{"changelog-producer":"input"}}""", 0x8000L));
        assertEquals(0x8000L, reg.appliedDdlLsn(SEQ),
                     "a create records its own LSN, so every table has one from the start");

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


    /*
     * drop_table and create inside the DDL commit identifier.
     *
     * A Paimon table is identified by NAME, so a PostgreSQL table dropped and recreated under
     * the same name resolves to the same Paimon table. If a drop_table message replays after
     * that recreation, applying it would purge the new table's rows and the replayed create
     * would rebuild it empty. The gate: a create records its own LSN on the table it creates,
     * and a drop whose LSN is at or below what the table records is stale and is refused.
     */

    @Test @Order(60)
    void aDropWhoseTableWasCreatedAfterItIsRefusedAsStale() throws Exception {
        // the original table, dropped at LSN A
        handler.handle(msg("""
            {"v":1,"op":"create","relid":43000,"schema":"public","table":"recycled",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"}],
             "options":{"changelog-producer":"input"}}""", 0x60000L));
        String dropA = """
            {"v":1,"op":"drop_table","relid":43000,"schema":"public","table":"recycled"}""";
        handler.handle(msg(dropA, 0x60100L));
        assertFalse(reg.tracked(43000L));

        // recreated under the SAME name at a LATER LSN, as a new relid
        handler.handle(msg("""
            {"v":1,"op":"create","relid":43001,"schema":"public","table":"recycled",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"}],
             "options":{"changelog-producer":"input"}}""", 0x60200L));
        assertEquals(0x60200L, reg.appliedDdlLsn(43001L));
        long ver = reg.table(43001L).schema().id();

        // now the stale drop replays. It cannot arrive under the OLD relid, which is untracked,
        // so this is the case that matters: the recreated table reusing the dropped OID.
        String dropReplay = """
            {"v":1,"op":"drop_table","relid":43001,"schema":"public","table":"recycled"}""";
        List<LogRecord> warn = captureWarnings(SchemaRegistry.class, () ->
            handler.handle(msg(dropReplay, 0x60100L)));
        assertEquals(1, warn.size(), warn.toString());
        assertTrue(warn.get(0).getMessage().contains("STALE"), warn.get(0).getMessage());
        assertTrue(warn.get(0).getMessage().contains("has NOT been applied"),
                   warn.get(0).getMessage());
        assertTrue(reg.tracked(43001L), "the relation must stay tracked");
        assertDoesNotThrow(() -> catalog.getTable(
            Identifier.create("public", "recycled")), "the table must still be there");
        assertEquals(ver, reg.table(43001L).schema().id());
    }

    /** And the gate must not make a real drop unreachable. */
    @Test @Order(61)
    void aGenuineDropStillDropsWithAnIdentifierRecorded() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"create","relid":43002,"schema":"public","table":"goner",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"}],
             "options":{"changelog-producer":"input"}}""", 0x61000L));
        assertEquals(0x61000L, reg.appliedDdlLsn(43002L));

        // a drop ABOVE the recorded LSN is the ordinary case and must take effect
        handler.handle(msg("""
            {"v":1,"op":"drop_table","relid":43002,"schema":"public","table":"goner"}""",
            0x61100L));
        assertFalse(reg.tracked(43002L));
        assertThrows(Catalog.TableNotExistException.class,
            () -> catalog.getTable(Identifier.create("public", "goner")));
    }

    /** A table with no recorded identifier keeps the old behaviour: nothing to compare. */
    @Test @Order(62)
    void aDropOnATableWithNoRecordedLsnStillDrops() throws Exception {
        reg.bootstrapTable(43003L, "public", "oldstyle", List.of("id"),
            List.of(new DdlMessage.Col(1, "id", "INT NOT NULL")), "input", false);
        assertEquals(0L, reg.appliedDdlLsn(43003L));
        // even a LOW drop LSN applies, because refusing on no evidence would make drops
        // unreachable for every table that predates the identifier
        handler.handle(msg("""
            {"v":1,"op":"drop_table","relid":43003,"schema":"public","table":"oldstyle"}""", 1L));
        assertFalse(reg.tracked(43003L));
        assertThrows(Catalog.TableNotExistException.class,
            () -> catalog.getTable(Identifier.create("public", "oldstyle")));
    }

    /**
     * The orphan column left by a lost rename gets its own WARNING, not a clause in a combined
     * line: it reads NULL for every row and every SELECT * downstream returns it.
     */
    @Test @Order(63)
    void anOrphanColumnIsWarnedAboutOnItsOwn() throws Exception {
        reg.bootstrapTable(43004L, "public", "orphan_t", List.of("id"),
            List.of(new DdlMessage.Col(1, "id", "INT NOT NULL"),
                    new DdlMessage.Col(2, "before", "STRING")), "input", false);
        // the registry has renamed it; Paimon still has the old name beside the new one
        List<LogRecord> logs = captureWarnings(SchemaRegistry.class, () ->
            reg.bootstrapTable(43004L, "public", "orphan_t", List.of("id"),
                List.of(new DdlMessage.Col(1, "id", "INT NOT NULL"),
                        new DdlMessage.Col(2, "after", "STRING")), "input", true));
        List<String> orphan = logs.stream().map(LogRecord::getMessage)
            .filter(m -> m.contains("'before'")).toList();
        assertEquals(1, orphan.size(), logs.toString());
        String m = orphan.get(0);
        assertTrue(m.contains("NULL for every row"), m);
        assertTrue(m.contains("SELECT *"), m);
        assertTrue(m.contains("DROP COLUMN `before`"), m);
        assertTrue(m.contains("not maintained") || m.contains("NOT maintained"), m);
    }

    /**
     * The seam the "refuse before touching the warehouse" ordering rests on: register() must
     * establish enough for the startup decision to be taken -- the relid is tracked, and its
     * committed offsets can be asked for -- while creating nothing.
     */
    @Test @Order(64)
    void registerEstablishesTheMappingWithoutTouchingTheWarehouse() throws Exception {
        reg.register(44000L, "public", "never_created");
        assertTrue(reg.tracked(44000L), "the decision needs the mapping");
        assertThrows(Catalog.TableNotExistException.class,
            () -> catalog.getTable(Identifier.create("public", "never_created")),
            "and must not have created anything");
        assertFalse(new OffsetStore(reg, cfg).hasCommitted(44000L),
                    "nothing is committed to a table that does not exist, and asking must not"
                    + " throw -- startup asks before creating");
    }

    /*
     * The PostgreSQL schema is part of the Paimon identity: <warehouse>/<schema>.db/<table>.
     *
     * Before this, Identifier.create(<one configured database>, table) dropped the schema, so
     * public.f and s2.f resolved to ONE Paimon table and interleaved into it -- two tables' rows
     * merging on a shared primary key, which a SQL guardrail had to refuse outright.
     */

    @Test @Order(70)
    void sameNamedTablesInDifferentSchemasAreSeparatePaimonTables() throws Exception {
        String create = """
            {"v":1,"op":"create","relid":RELID,"schema":"NSP","table":"f",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                 {"id":2,"name":"v","type":"STRING"}],
             "options":{"changelog-producer":"input"}}""";
        handler.handle(msg(create.replace("RELID", "45000").replace("NSP", "public")));
        handler.handle(msg(create.replace("RELID", "45001").replace("NSP", "s2")));

        assertNotEquals(reg.table(45000L).location(), reg.table(45001L).location(),
                        "two tables, not one");
        assertTrue(java.nio.file.Files.isDirectory(warehouse.resolve("public.db").resolve("f")));
        assertTrue(java.nio.file.Files.isDirectory(warehouse.resolve("s2.db").resolve("f")));
        assertEquals(List.of("f", "f_hist"), catalog.listTables("s2"));
        assertTrue(catalog.listDatabases().containsAll(List.of("public", "s2")));

        // each carries its OWN applied-DDL identifier, where they previously shared one
        handler.handle(msg("""
            {"v":1,"op":"add_col","relid":45001,"col":{"id":3,"name":"only_s2","type":"INT"}}""",
            0x70000L));
        assertEquals(0x70000L, reg.appliedDdlLsn(45001L));
        assertNotEquals(reg.appliedDdlLsn(45000L), reg.appliedDdlLsn(45001L));
        assertTrue(reg.table(45001L).schema().fieldNames().contains("only_s2"));
        assertFalse(reg.table(45000L).schema().fieldNames().contains("only_s2"),
                    "the other schema's table must be untouched");
    }

    @Test @Order(71)
    void dropTableRemovesOnlyTheSchemaItNames() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"drop_table","relid":45001,"schema":"s2","table":"f"}""", 0x70100L));
        assertFalse(reg.tracked(45001L));
        assertThrows(Catalog.TableNotExistException.class,
            () -> catalog.getTable(Identifier.create("s2", "f")));
        assertDoesNotThrow(() -> catalog.getTable(Identifier.create("public", "f")),
                           "the same-named table in the other schema must survive");
        assertTrue(reg.tracked(45000L));
    }

}
