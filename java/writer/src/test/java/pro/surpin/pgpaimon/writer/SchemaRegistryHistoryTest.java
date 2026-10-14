// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.paimon.catalog.*;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * The history table's schema and its DDL fan-out against the real Paimon 1.4.1 (spec 2026-10-14
 * sections 2 and 3): created with its table, columns before_/after_, DDL applied to both tables
 * each under its own DDL LSN gate, dropped columns kept, drop_table leaving it, and a history
 * table of another registration refused.
 */
class SchemaRegistryHistoryTest {

    @TempDir static java.nio.file.Path wh;
    static Catalog catalog;
    static SchemaRegistry reg;
    static DdlMessageHandler ddl;
    static long lsn = 0x10000;

    @BeforeAll
    static void setUp() {
        Options o = new Options();
        o.set("warehouse", wh.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
        Map<String, String> env = new HashMap<>();
        env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db");
        env.put("PGPAIMON_WAREHOUSE", wh.toString());
        reg = new SchemaRegistry(catalog, WriterConfig.fromEnv(env));
        ddl = new DdlMessageHandler(reg, "paimon", TestAuth.TOKEN);
    }

    static long send(String json) {
        lsn += 0x10;
        ddl.handle(new PgEvent.Message(true, lsn, "paimon", TestAuth.authed(json)));
        return lsn;
    }

    static void replay(String json, long at) {
        ddl.handle(new PgEvent.Message(true, at, "paimon", TestAuth.authed(json)));
    }

    static String create(long relid, String table, String pk, String registration) {
        return """
            {"v":1,"op":"create","relid":%d,"schema":"public","table":"%s","pk":%s,
             "cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                     {"id":2,"name":"n","type":"INT"}],
             "options":{"bucket":"3","hist-table":"%s_hist","registration":"%s"}}"""
            .formatted(relid, table, pk, table, registration);
    }

    static FileStoreTable hist(String table) throws Exception {
        return (FileStoreTable) catalog.getTable(Identifier.create("public", table + "_hist"));
    }

    static List<String> fields(String table) throws Exception {
        return ((FileStoreTable) catalog.getTable(Identifier.create("public", table)))
            .rowType().getFieldNames();
    }

    static final List<String> META =
        List.of("_op", "_commit_lsn", "_lsn", "_op_seq", "_xid", "_ts");

    static List<String> with(List<String> head, String... tail) {
        List<String> out = new ArrayList<>(head);
        out.addAll(List.of(tail));
        return out;
    }

    @Test
    void createMakesTheHistoryTableBesideThePrimaryKeyTable() throws Exception {
        send(create(50001, "c_pk", "[\"id\"]", "r1"));
        FileStoreTable h = hist("c_pk");
        assertEquals(with(META, "before_id", "before_n", "after_id", "after_n"),
                     h.rowType().getFieldNames());
        assertEquals(List.of(), h.primaryKeys(), "append-only: no key, no merge");
        assertTrue(h.rowType().getFields().subList(6, 10).stream()
                       .allMatch(f -> f.type().isNullable()), "every image column nullable");
        assertFalse(h.rowType().getField("_ts").type().isNullable());
        assertEquals("TIMESTAMP(6) WITH LOCAL TIME ZONE NOT NULL",
                     h.rowType().getField("_ts").type().asSQLString());
        Map<String, String> o = h.options();
        assertEquals("3", o.get("bucket"));
        assertEquals("_lsn", o.get("bucket-key"));
        assertEquals("r1", o.get(SchemaRegistry.REGISTRATION_OPTION));
        assertEquals("public.c_pk", o.get(SchemaRegistry.HISTORY_OF_OPTION));
        assertNull(o.get("merge-engine"));
        assertEquals("public.c_pk_hist", h.fullName(),
                     "the name PGPAIMON_TEST_HALT_AFTER_COMMIT matches (CrashHook)");
        assertTrue(reg.hasHistory(50001));
        assertFalse(reg.keyless(50001));
        assertTrue(reg.table(HistoryKeys.histKey(50001)).location().toString()
                       .endsWith("public.db/c_pk_hist"), "the history key resolves to T_hist");
    }

    @Test
    void aKeylessTableIsAnAppendOnlyLog() throws Exception {
        send(create(50002, "c_log", "[]", "r2"));
        FileStoreTable t = reg.table(50002);
        assertEquals(with(List.of("id", "n"), META.toArray(String[]::new)),
                     t.rowType().getFieldNames());
        assertEquals(List.of(), t.primaryKeys());
        assertTrue(t.rowType().getField("id").type().isNullable(),
                   "INT NOT NULL in PostgreSQL, nullable in the log (a T row is all NULL)");
        assertEquals("_lsn", t.options().get("bucket-key"));
        assertNull(t.options().get("merge-engine"));
        assertTrue(reg.keyless(50002));
        assertTrue(reg.hasHistory(50002));
        assertEquals(with(META, "before_id", "before_n", "after_id", "after_n"),
                     fields("c_log_hist"));
    }

    @Test
    void ddlFansOutToBothTablesAndTheHistoryKeepsDroppedColumns() throws Exception {
        long rel = 50003;
        send(create(rel, "f_t", "[\"id\"]", "r3"));
        // add
        long add = send("""
            {"v":1,"op":"add_col","relid":%d,"col":{"id":3,"name":"x","type":"INT"}}"""
            .formatted(rel));
        assertEquals(List.of("id", "n", "_seq", "_op_seq", "x"), fields("f_t"));
        assertEquals(with(META, "before_id", "before_n", "after_id", "after_n", "before_x",
                          "after_x"), fields("f_t_hist"));
        // alter type (an allowed widening): both prefixed columns
        send("""
            {"v":1,"op":"alter_col","relid":%d,"id":3,"name":"x","type":"BIGINT"}"""
            .formatted(rel));
        assertEquals("BIGINT", hist("f_t").rowType().getField("before_x").type().asSQLString());
        assertEquals("BIGINT", hist("f_t").rowType().getField("after_x").type().asSQLString());
        // rename: both prefixed columns
        send("""
            {"v":1,"op":"rename_col","relid":%d,"id":3,"from":"x","to":"y"}""".formatted(rel));
        assertTrue(fields("f_t").contains("y"));
        assertTrue(fields("f_t_hist").containsAll(List.of("before_y", "after_y")));
        assertFalse(fields("f_t_hist").contains("before_x"));
        // drop: gone from the main table, KEPT in the history
        long drop = send("""
            {"v":1,"op":"drop_col","relid":%d,"name":"y"}""".formatted(rel));
        assertFalse(fields("f_t").contains("y"));
        assertTrue(fields("f_t_hist").containsAll(List.of("before_y", "after_y")),
                   "history is not rewritten: " + fields("f_t_hist"));
        assertEquals(drop, reg.appliedDdlLsn(HistoryKeys.histKey(rel)),
                     "a drop records its LSN on the history table");
        // re-adding the dropped name: the kept pair is renamed aside, the new pair starts empty
        long readd = send("""
            {"v":1,"op":"add_col","relid":%d,"col":{"id":4,"name":"y","type":"STRING"}}"""
            .formatted(rel));
        List<String> hf = fields("f_t_hist");
        String retired = "_retired_" + Long.toUnsignedString(readd);
        assertTrue(hf.containsAll(List.of("before_y" + retired, "after_y" + retired,
                                          "before_y", "after_y")), hf.toString());
        assertEquals("BIGINT", hist("f_t").rowType().getField("before_y" + retired).type()
            .asSQLString(), "the kept pair keeps its type and values");
        assertEquals("STRING", hist("f_t").rowType().getField("before_y").type().asSQLString());
        // every message replayed: no change to either table
        long mainVer = reg.table(rel).schema().id();
        long histVer = hist("f_t").schema().id();
        replay("""
            {"v":1,"op":"add_col","relid":%d,"col":{"id":3,"name":"x","type":"INT"}}"""
            .formatted(rel), add);
        replay("""
            {"v":1,"op":"drop_col","relid":%d,"name":"y"}""".formatted(rel), drop);
        replay("""
            {"v":1,"op":"add_col","relid":%d,"col":{"id":4,"name":"y","type":"STRING"}}"""
            .formatted(rel), readd);
        assertEquals(mainVer, reg.table(rel).schema().id());
        assertEquals(histVer, hist("f_t").schema().id());
    }

    /**
     * A crash between the main table's schema change and the history table's: each has its own
     * DDL LSN, so the replay applies the message to the one that missed it and nowhere else.
     */
    @Test
    void aDdlMessageAppliedToTheMainTableOnlyIsCompletedOnReplay() throws Exception {
        long rel = 50004;
        send(create(rel, "g_t", "[\"id\"]", "r4"));
        String json = """
            {"v":1,"op":"add_col","relid":%d,"col":{"id":3,"name":"z","type":"INT"}}"""
            .formatted(rel);
        long at = send(json);
        // undo it on the history table alone, as if the process died between the two
        long before = reg.appliedDdlLsn(HistoryKeys.histKey(rel));
        catalog.alterTable(Identifier.create("public", "g_t_hist"), List.of(
            SchemaChange.dropColumn("before_z"), SchemaChange.dropColumn("after_z"),
            SchemaChange.setOption(SchemaRegistry.DDL_LSN_OPTION, "" + (at - 1))), false);
        reg.forget(rel);
        send(create(rel, "g_t", "[\"id\"]", "r4"));     // a restart's re-registration
        long mainVer = reg.table(rel).schema().id();
        replay(json, at);
        assertEquals(mainVer, reg.table(rel).schema().id(), "main: a replay, no-op");
        assertTrue(fields("g_t_hist").containsAll(List.of("before_z", "after_z")),
                   "history: applied now");
        assertEquals(at, reg.appliedDdlLsn(HistoryKeys.histKey(rel)));
        assertTrue(before >= at, "it had been applied before the simulated crash");
    }

    @Test
    void dropTableLeavesTheHistoryTableAndAnotherRegistrationIsRefused() throws Exception {
        long rel = 50005;
        send(create(rel, "d_t", "[\"id\"]", "r5"));
        send("""
            {"v":1,"op":"drop_table","relid":%d,"schema":"public","table":"d_t"}"""
            .formatted(rel));
        assertFalse(reg.tracked(rel));
        assertFalse(reg.tracked(HistoryKeys.histKey(rel)));
        assertThrows(Catalog.TableNotExistException.class,
                     () -> catalog.getTable(Identifier.create("public", "d_t")));
        assertEquals("r5", hist("d_t").options().get(SchemaRegistry.REGISTRATION_OPTION),
                     "the history table stays as the audit trail");
        // a NEW table of the same name: refused, and nothing written to the old history
        long ver = hist("d_t").schema().id();
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> send(create(50006, "d_t", "[\"id\"]", "r6")));
        assertTrue(e.getMessage().contains("not the history table of this registration"),
                   e.getMessage());
        assertTrue(e.getMessage().contains("Rename or drop"), e.getMessage());
        assertEquals(ver, hist("d_t").schema().id());
        // the same registration (a restart) adopts it
        assertDoesNotThrow(() -> send(create(50005, "d_t", "[\"id\"]", "r5")));
    }

    @Test
    void aHistoryTableThisWriterDidNotMakeIsRefused() throws Exception {
        catalog.createDatabase("public", true);
        catalog.createTable(Identifier.create("public", "e_t_hist"),
            org.apache.paimon.schema.Schema.newBuilder()
                .column("x", org.apache.paimon.types.DataTypes.INT()).build(), false);
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> send(create(50007, "e_t", "[\"id\"]", "r7")));
        assertTrue(e.getMessage().contains("this writer did not create it"), e.getMessage());
    }
}
