// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * The history table's layout and its rotation on DDL against the real Paimon 1.4.1 (spec
 * 2026-10-14 sections 2, 3 and 5; Amendments A and B): created with its table, partitioned by
 * day, a new table per DDL with the old one archived as {@code <t>_hist_<lsn>}, every crash
 * point of a rotation converging on replay, drop_table leaving all of them, the bucket count of a
 * new table measured from the archive's partitions, and a history table of another registration
 * refused.
 */
class SchemaRegistryHistoryTest {

    @TempDir static Path wh;
    static Catalog catalog;
    static WriterConfig cfg;
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
        cfg = WriterConfig.fromEnv(env);
        reg = new SchemaRegistry(catalog, cfg, CrashHook.OFF);
        ddl = new DdlMessageHandler(reg, "paimon", TestAuth.TOKEN);
    }

    static long send(DdlMessageHandler h, String json) {
        lsn += 0x10;
        h.handle(new PgEvent.Message(true, lsn, "paimon", TestAuth.authed(json)));
        return lsn;
    }

    static long send(String json) {
        return send(ddl, json);
    }

    static void replay(DdlMessageHandler h, String json, long at) {
        h.handle(new PgEvent.Message(true, at, "paimon", TestAuth.authed(json)));
    }

    static String create(long relid, String table, String pk, String registration,
                         String histBuckets) {
        return """
            {"v":1,"op":"create","relid":%d,"schema":"public","table":"%s","pk":%s,
             "cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                     {"id":2,"name":"n","type":"INT"}],
             "options":{"bucket":"3","hist-table":"%s_hist","registration":"%s",
                        "hist-bucket":"%s","hist-bucket-user":"false"}}"""
            .formatted(relid, table, pk, table, registration, histBuckets);
    }

    static String create(long relid, String table, String pk, String registration) {
        return create(relid, table, pk, registration, "2");
    }

    static FileStoreTable get(String table) throws Exception {
        catalog.invalidateTable(Identifier.create("public", table));
        return (FileStoreTable) catalog.getTable(Identifier.create("public", table));
    }

    static List<String> fields(String table) throws Exception {
        return get(table).rowType().getFieldNames();
    }

    static List<String> archives(String table) throws Exception {
        List<String> out = new ArrayList<>();
        for (String n : catalog.listTables("public")) {
            if (HistoryKeys.archiveLsn(n, table + "_hist") >= 0) {
                out.add(n);
            }
        }
        Collections.sort(out);
        return out;
    }

    static String archive(String table, long at) {
        return table + "_hist_" + String.format("%016X", at);
    }

    static final List<String> META =
        List.of("_op", "_commit_lsn", "_lsn", "_op_seq", "_xid", "_ts", "_dt");

    static List<String> with(List<String> head, List<String> tail) {
        List<String> out = new ArrayList<>(head);
        out.addAll(tail);
        return out;
    }

    @Test
    void createMakesADailyPartitionedHistoryTableBesideThePrimaryKeyTable() throws Exception {
        send(create(50001, "c_pk", "[\"id\"]", "r1", "5"));
        FileStoreTable h = get("c_pk_hist");
        assertEquals(with(List.of("id", "n"), META), h.rowType().getFieldNames());
        assertEquals(List.of(), h.primaryKeys(), "append-only: no key, no merge");
        assertEquals(List.of("_dt"), h.partitionKeys(), "partitioned by day");
        assertTrue(h.rowType().getField("id").type().isNullable(),
                   "INT NOT NULL in PostgreSQL, nullable in the history (a T row is all NULL)");
        assertEquals("DATE NOT NULL", h.rowType().getField("_dt").type().asSQLString());
        assertEquals("TIMESTAMP(6) WITH LOCAL TIME ZONE NOT NULL",
                     h.rowType().getField("_ts").type().asSQLString());
        Map<String, String> o = h.options();
        assertEquals("5", o.get("bucket"), "the history's own bucket count per partition");
        assertEquals("_lsn", o.get("bucket-key"));
        assertEquals("r1", o.get(SchemaRegistry.REGISTRATION_OPTION));
        assertNull(o.get(SchemaRegistry.BASELINE_OPTION), "a first history has no baseline");
        assertEquals("public.c_pk_hist", h.fullName());
        assertEquals("3", get("c_pk").options().get("bucket"), "the main table keeps its own");
    }

    @Test
    void aKeylessTableIsAnUnpartitionedAppendOnlyLog() throws Exception {
        send(create(50002, "c_log", "[]", "r2"));
        FileStoreTable t = reg.table(50002);
        assertEquals(with(List.of("id", "n"), META.subList(0, 6)), t.rowType().getFieldNames());
        assertEquals(List.of(), t.primaryKeys());
        assertEquals(List.of(), t.partitionKeys());
        assertTrue(reg.keyless(50002));
        assertEquals(with(List.of("id", "n"), META), fields("c_log_hist"));
    }

    /** Every DDL rotates: archive with the old schema, new table with the new, main altered. */
    @Test
    void everyDdlRotatesTheHistoryAndAReplayChangesNothing() throws Exception {
        long rel = 50003;
        send(create(rel, "f_t", "[\"id\"]", "r3"));
        List<String> msgs = List.of(
            "{\"v\":1,\"op\":\"add_col\",\"relid\":%d,\"col\":{\"id\":3,\"name\":\"x\",\"type\":\"INT\"}}",
            "{\"v\":1,\"op\":\"alter_col\",\"relid\":%d,\"id\":3,\"name\":\"x\",\"type\":\"BIGINT\"}",
            "{\"v\":1,\"op\":\"rename_col\",\"relid\":%d,\"id\":3,\"from\":\"x\",\"to\":\"y\"}",
            "{\"v\":1,\"op\":\"drop_col\",\"relid\":%d,\"name\":\"y\"}");
        List<List<String>> schemas = List.of(
            List.of("id", "n"), List.of("id", "n", "x"), List.of("id", "n", "x"),
            List.of("id", "n", "y"), List.of("id", "n"));
        List<Long> at = new ArrayList<>();
        for (int i = 0; i < msgs.size(); i++) {
            at.add(send(msgs.get(i).formatted(rel)));
            String arch = archive("f_t", at.get(i));
            assertEquals(with(schemas.get(i), META), fields(arch), "archive " + i + " keeps its schema");
            assertEquals(with(schemas.get(i + 1), META), fields("f_t_hist"), "new history " + i);
            assertEquals(Long.toUnsignedString(at.get(i)),
                         get("f_t_hist").options().get(SchemaRegistry.BASELINE_OPTION));
            assertEquals(List.of("_dt"), get(arch).partitionKeys());
        }
        assertEquals("INT", get(archive("f_t", at.get(1))).rowType().getField("x").type()
            .asSQLString(), "the archive of alter_col keeps the old type");
        assertEquals("BIGINT", get(archive("f_t", at.get(2))).rowType().getField("x").type()
            .asSQLString(), "the next history has the new type");
        assertEquals(List.of("id", "n", "_seq", "_op_seq"), fields("f_t"),
                     "the main table altered as before");
        assertEquals(4, archives("f_t").size(), "one archive per DDL: " + archives("f_t"));
        // every message replayed: nothing changes anywhere
        long mainVer = reg.table(rel).schema().id();
        long histVer = get("f_t_hist").schema().id();
        for (int i = 0; i < msgs.size(); i++) {
            replay(ddl, msgs.get(i).formatted(rel), at.get(i));
        }
        assertEquals(mainVer, reg.table(rel).schema().id());
        assertEquals(histVer, get("f_t_hist").schema().id());
        assertEquals(4, archives("f_t").size());
        // routing by change LSN
        assertEquals(HistoryKeys.archiveKey(rel, at.get(0)), reg.historyKey(rel, at.get(0) - 1));
        assertEquals(HistoryKeys.archiveKey(rel, at.get(2)), reg.historyKey(rel, at.get(1) + 1));
        assertEquals(HistoryKeys.histKey(rel), reg.historyKey(rel, at.get(3) + 1));
    }

    /**
     * A crash after each step of a rotation, then a fresh process replaying the message:
     * converges to exactly one archive (holding the old rows once), one new history table and
     * the main table altered once.
     */
    @Test
    void aCrashAtEveryStepOfARotationConvergesOnReplay() throws Exception {
        long rel = 50100;
        for (String step : List.of("rotate-renamed", "rotate-created", "rotate-altered")) {
            rel++;
            String t = "k_" + step.substring(7);
            send(create(rel, t, "[\"id\"]", "r" + rel));
            writeRows(t + "_hist", 3, 1);
            SchemaRegistry crashing = new SchemaRegistry(catalog, cfg, CrashHook.throwingAt(step));
            DdlMessageHandler h = new DdlMessageHandler(crashing, "paimon", TestAuth.TOKEN);
            h.handle(new PgEvent.Message(true, 1, "paimon", TestAuth.authed(
                create(rel, t, "[\"id\"]", "r" + rel))));
            String add = "{\"v\":1,\"op\":\"add_col\",\"relid\":%d,\"col\":{\"id\":3,\"name\":\"x\",\"type\":\"INT\"}}"
                .formatted(rel);
            lsn += 0x10;
            long at = lsn;
            assertThrows(CrashHook.SimulatedCrash.class, () -> replay(h, add, at), step);
            // a fresh process: bootstrap, then the replayed message
            SchemaRegistry fresh = new SchemaRegistry(catalog, cfg, CrashHook.OFF);
            DdlMessageHandler fh = new DdlMessageHandler(fresh, "paimon", TestAuth.TOKEN);
            fh.handle(new PgEvent.Message(true, 1, "paimon", TestAuth.authed(
                create(rel, t, "[\"id\"]", "r" + rel))));
            long r0 = rel;
            assertDoesNotThrow(() -> new OffsetStore(fresh, cfg).highestCommittedLsn(
                List.of(r0, HistoryKeys.histKey(r0))), step + ": the startup's resume position");
            replay(fh, add, at);
            replay(fh, add, at);
            assertEquals(List.of(archive(t, at)), archives(t), step);
            assertEquals(3, countRows(archive(t, at)), step + ": the old rows, once, archived");
            assertEquals(0, countRows(t + "_hist"), step + ": the new history starts empty");
            assertEquals(with(List.of("id", "n", "x"), META), fields(t + "_hist"), step);
            assertEquals(List.of("id", "n", "_seq", "_op_seq", "x"), fields(t), step);
            assertEquals(at, fresh.appliedDdlLsn(rel), step);
        }
    }

    /**
     * A rename interrupted half-way, as only a non-atomic store (copy, then delete) can leave
     * it: the archive exists as a partial copy and the source is intact. The replay keeps the
     * source's data, drops the partial copy, and repeats the rename.
     */
    @Test
    void aHalfDoneRenameConvergesOnReplay() throws Exception {
        long rel = 50200;
        send(create(rel, "hd", "[\"id\"]", "r200"));
        writeRows("hd_hist", 4, 1);
        lsn += 0x10;
        long at = lsn;
        Path src = wh.resolve("public.db").resolve("hd_hist");
        Path dst = wh.resolve("public.db").resolve(archive("hd", at));
        copyTree(src.resolve("schema"), dst.resolve("schema"));   // a partial copy: schema only
        replay(ddl, "{\"v\":1,\"op\":\"add_col\",\"relid\":%d,\"col\":{\"id\":3,\"name\":\"x\",\"type\":\"INT\"}}"
            .formatted(rel), at);
        assertEquals(List.of(archive("hd", at)), archives("hd"));
        assertEquals(4, countRows(archive("hd", at)), "the source's rows, once");
        assertEquals(0, countRows("hd_hist"));
        assertEquals(with(List.of("id", "n", "x"), META), fields("hd_hist"));
    }

    /**
     * B2: a new history table at a rotation gets a bucket count measured from the archive's
     * daily partitions once three days exist; with fewer it keeps the registry's estimate; a
     * user's hist_buckets always wins. The count of an existing table never changes.
     */
    @Test
    void aRotationResizesTheNewHistoryFromMeasuredPartitions() throws Exception {
        String addX = "{\"v\":1,\"op\":\"add_col\",\"relid\":%d,\"col\":{\"id\":3,\"name\":\"x\",\"type\":\"INT\"}}";
        // three small days: measured, ceil(small / 1 GiB) = 1, replacing the estimate of 9
        send(create(50301, "bk3", "[\"id\"]", "r301", "9"));
        assertEquals("9", get("bk3_hist").options().get("bucket"));
        writeRows("bk3_hist", 5, 3);
        send(addX.formatted(50301));
        assertEquals("1", get("bk3_hist").options().get("bucket"), "measured from 3 days");
        assertEquals("9", get(archives("bk3").get(0)).options().get("bucket"),
                     "the archived table keeps its count");
        // two days: not enough, the estimate stays
        send(create(50302, "bk2", "[\"id\"]", "r302", "9"));
        writeRows("bk2_hist", 5, 2);
        send(addX.formatted(50302));
        assertEquals("9", get("bk2_hist").options().get("bucket"), "two days: estimate kept");
        // the user's count wins over any measurement
        send(create(50303, "bku", "[\"id\"]", "r303", "7")
            .replace("\"hist-bucket-user\":\"false\"", "\"hist-bucket-user\":\"true\""));
        writeRows("bku_hist", 5, 4);
        send(addX.formatted(50303));
        assertEquals("7", get("bku_hist").options().get("bucket"), "hist_buckets wins");
    }

    @Test
    void dropTableLeavesTheHistoryAndItsArchivesAndAnotherRegistrationIsRefused()
            throws Exception {
        long rel = 50005;
        send(create(rel, "d_t", "[\"id\"]", "r5"));
        send("{\"v\":1,\"op\":\"add_col\",\"relid\":%d,\"col\":{\"id\":3,\"name\":\"x\",\"type\":\"INT\"}}"
            .formatted(rel));
        send("{\"v\":1,\"op\":\"drop_table\",\"relid\":%d,\"schema\":\"public\",\"table\":\"d_t\"}"
            .formatted(rel));
        assertFalse(reg.tracked(rel));
        assertFalse(reg.tracked(HistoryKeys.histKey(rel)));
        assertThrows(Catalog.TableNotExistException.class,
                     () -> catalog.getTable(Identifier.create("public", "d_t")));
        assertEquals("r5", get("d_t_hist").options().get(SchemaRegistry.REGISTRATION_OPTION));
        assertEquals(1, archives("d_t").size(), "the archive stays too");
        long ver = get("d_t_hist").schema().id();
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> send(create(50006, "d_t", "[\"id\"]", "r6")));
        assertTrue(e.getMessage().contains("not the history table of this registration"),
                   e.getMessage());
        assertEquals(ver, get("d_t_hist").schema().id());
        assertDoesNotThrow(() -> send(create(rel, "d_t", "[\"id\"]", "r5")),
                           "the same registration (a restart) adopts it");
    }

    /**
     * Review of the history tables, I3: a table dropped after a DDL leaves T_hist and an archive.
     * The operator moves only T_hist aside and registers a new table of the same name. The
     * archive used to be taken for an interrupted rotation of the NEW table (T_hist never made,
     * the writer stopped on every restart with a misleading message); it is refused by name.
     */
    @Test
    void anArchiveOfAnEarlierRegistrationIsRefusedByName() throws Exception {
        long rel = 50020;
        send(create(rel, "q_arc", "[\"id\"]", "r20"));
        long at = send("{\"v\":1,\"op\":\"add_col\",\"relid\":%d,\"col\":{\"id\":3,\"name\":\"x\",\"type\":\"INT\"}}"
            .formatted(rel));
        send("{\"v\":1,\"op\":\"drop_table\",\"relid\":%d,\"schema\":\"public\",\"table\":\"q_arc\"}"
            .formatted(rel));
        catalog.renameTable(Identifier.create("public", "q_arc_hist"),
                            Identifier.create("public", "q_arc_hist_moved_aside"), false);
        assertEquals(List.of(archive("q_arc", at)), archives("q_arc"));
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> send(create(50021, "q_arc", "[\"id\"]", "r21")));
        assertTrue(e.getMessage().contains(archive("q_arc", at) + " (registration r20)"), e.getMessage());
        assertTrue(e.getMessage().contains("Rename or drop them"), e.getMessage());
        assertFalse(e.getMessage().contains("interrupted"), e.getMessage());
        assertThrows(Catalog.TableNotExistException.class,
            () -> catalog.getTable(Identifier.create("public", "q_arc_hist")), "nothing created");
        // moved aside too: the new registration starts clean
        catalog.renameTable(Identifier.create("public", archive("q_arc", at)),
                            Identifier.create("public", "q_arc_old_archive"), false);
        assertDoesNotThrow(() -> send(create(50021, "q_arc", "[\"id\"]", "r21")));
        assertEquals("r21", get("q_arc_hist").options().get(SchemaRegistry.REGISTRATION_OPTION));
    }

    /**
     * Review of the history tables, I4: remove_table() keeps the main table; the table gets a key
     * (or loses it) and is registered again. The writer used to warn and then stop on every
     * restart ("no metadata column '_seq'"); it refuses with the remedy, before writing anything,
     * in both directions.
     */
    @Test
    void aKeptMainTableOfTheOtherLayoutIsRefusedWithTheRemedy() throws Exception {
        send(create(50030, "lay_a", "[]", "r30"));
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> send(create(50031, "lay_a", "[\"id\"]", "r31")));
        assertTrue(e.getMessage().contains("exists as an operation log"), e.getMessage());
        assertTrue(e.getMessage().contains("Rename or drop"), e.getMessage());
        assertEquals(List.of(), get("lay_a").primaryKeys(), "untouched");
        assertFalse(reg.tracked(50031));

        send(create(50032, "lay_b", "[\"id\"]", "r32"));
        e = assertThrows(IllegalStateException.class,
            () -> send(create(50033, "lay_b", "[]", "r33")));
        assertTrue(e.getMessage().contains("exists as a primary-key table (key [id])"), e.getMessage());
        assertFalse(reg.tracked(50033));
    }

    /**
     * Review of the history tables, I5: retention set on T_hist (as the guide says) was dropped
     * at the next DDL, because the new table's options were built from scratch.
     */
    @Test
    void retentionSetOnTheHistoryTableSurvivesARotation() throws Exception {
        long rel = 50040;
        send(create(rel, "ret_t", "[\"id\"]", "r40"));
        catalog.alterTable(Identifier.create("public", "ret_t_hist"), List.of(
            org.apache.paimon.schema.SchemaChange.setOption("partition.expiration-time", "30 d"),
            org.apache.paimon.schema.SchemaChange.setOption("partition.timestamp-formatter",
                                                            "yyyy-MM-dd")), false);
        send("{\"v\":1,\"op\":\"add_col\",\"relid\":%d,\"col\":{\"id\":3,\"name\":\"e\",\"type\":\"INT\"}}"
            .formatted(rel));
        Map<String, String> o = get("ret_t_hist").options();
        assertEquals("30 d", o.get("partition.expiration-time"), o.toString());
        assertEquals("yyyy-MM-dd", o.get("partition.timestamp-formatter"), o.toString());
        assertEquals("r40", o.get(SchemaRegistry.REGISTRATION_OPTION), "the writer's own still win");
        assertNotEquals(get(archives("ret_t").get(0)).options().get(SchemaRegistry.BASELINE_OPTION),
                        o.get(SchemaRegistry.BASELINE_OPTION), "a new baseline, not the archive's");
        assertTrue(fields("ret_t_hist").contains("e"));
    }

    @Test
    void theWritersOwnLayoutOptionsAreNotCarried() {
        assertEquals(Map.of("partition.expiration-time", "30 d", "snapshot.num-retained.max", "5"),
            SchemaRegistry.carriedOptions(Map.of("partition.expiration-time", "30 d",
                "snapshot.num-retained.max", "5", "bucket", "7", "bucket-key", "_lsn",
                "partition.legacy-name", "false", "path", "/x", "pgpaimon.registration", "r",
                "pgpaimon.applied-ddl-lsn", "1")));
    }

    /** Review of the history tables, m2: a key-less log's added column goes before the metadata. */
    @Test
    void aColumnAddedToAnOperationLogGoesBeforeItsMetadata() throws Exception {
        long rel = 50050;
        send(create(rel, "log_add", "[]", "r50"));
        send("{\"v\":1,\"op\":\"add_col\",\"relid\":%d,\"col\":{\"id\":3,\"name\":\"c\",\"type\":\"INT\"}}"
            .formatted(rel));
        assertEquals(with(List.of("id", "n", "c"), LogRowBuilder.META), fields("log_add"));
        assertEquals(with(List.of("id", "n", "c"), META), fields("log_add_hist"));
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

    @Test
    void anArchiveNameThatIsATrackedTableIsRefused() throws Exception {
        long rel = 50008;
        send(create(rel, "g_t", "[\"id\"]", "r8"));
        lsn += 0x10;
        long at = lsn;
        send(create(50009, archive("g_t", at), "[\"id\"]", "r9"));   // made behind SQL's back
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> replay(ddl,
            "{\"v\":1,\"op\":\"add_col\",\"relid\":%d,\"col\":{\"id\":3,\"name\":\"x\",\"type\":\"INT\"}}"
                .formatted(rel), at));
        assertTrue(e.getMessage().contains("which is a replicated table's Paimon table"),
                   e.getMessage());
    }

    /** Writes {@code perDay} rows into each of {@code days} daily partitions of a history table. */
    static void writeRows(String hist, int perDay, int days) throws Exception {
        FileStoreTable t = get(hist);
        int n = t.rowType().getFieldCount();
        List<String> names = t.rowType().getFieldNames();
        try (var w = t.newWrite("seed"); var c = t.newCommit("seed")) {
            long l = 1;
            for (int d = 0; d < days; d++) {
                for (int i = 0; i < perDay; i++) {
                    GenericRow r = new GenericRow(n);
                    r.setField(names.indexOf("id"), (int) l);
                    r.setField(names.indexOf("_op"), BinaryString.fromString("+I"));
                    r.setField(names.indexOf("_commit_lsn"), l);
                    r.setField(names.indexOf("_lsn"), l);
                    r.setField(names.indexOf("_op_seq"), 0);
                    r.setField(names.indexOf("_xid"), l);
                    r.setField(names.indexOf("_ts"), Timestamp.fromMicros((20000L + d) * 86_400_000_000L));
                    r.setField(names.indexOf("_dt"), 20000 + d);
                    w.write(r);
                    l++;
                }
            }
            c.commit(1, w.prepareCommit(false, 1));
        }
    }

    static long countRows(String table) throws Exception {
        FileStoreTable t = get(table);
        var rb = t.newReadBuilder();
        long n = 0;
        try (var r = rb.newRead().createReader(rb.newScan().plan());
             var it = r.toCloseableIterator()) {
            while (it.hasNext()) {
                it.next();
                n++;
            }
        }
        return n;
    }

    static void copyTree(Path from, Path to) throws Exception {
        try (var walk = Files.walk(from)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                Path q = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) {
                    Files.createDirectories(q);
                } else {
                    Files.copy(p, q);
                }
            }
        }
    }
}
