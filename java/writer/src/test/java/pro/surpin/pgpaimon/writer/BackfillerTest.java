// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

class BackfillerTest {

    @Test
    void copyTextIsParsedWithTabsAndBackslashNForNull() {
        // COPY ... TO STDOUT text format: tab-separated, \N for NULL,
        // and \t \n \r \\ escaped inside values.
        assertEquals(java.util.Arrays.asList("1", null, "a\tb"),
                     Backfiller.parseCopyLine("1\t\\N\ta\\tb"));
        assertEquals(List.of("", "x"), Backfiller.parseCopyLine("\tx"));
        assertEquals(List.of("a\nb"), Backfiller.parseCopyLine("a\\nb"));
        assertEquals(List.of("a\\b"), Backfiller.parseCopyLine("a\\\\b"));
    }

    @Test
    void backspaceFormFeedAndVerticalTabComeFromTheServersOwnOutput() {
        // `COPY (select E'a\bb\fc\vd') TO STDOUT` prints: a\bb\fc\vd
        assertEquals(List.of("a\u0008b\u000cc\u000bd"),
                     Backfiller.parseCopyLine("a\\bb\\fc\\vd"));
    }

    @Test
    void carriageReturnIsUnescaped() {
        assertEquals(List.of("a\rb"), Backfiller.parseCopyLine("a\\rb"));
    }

    @Test
    void aNullMarkerIsNotTheTwoCharacterStringBackslashN() {
        assertNull(Backfiller.parseCopyLine("\\N").get(0));
        assertEquals("\\N", Backfiller.parseCopyLine("\\\\N").get(0));
    }

    @Test
    void emptyStringNullAndLiteralNAreThreeDifferentValues() {
        assertEquals(java.util.Arrays.asList("", null, "N", ""),
                     Backfiller.parseCopyLine("\t\\N\tN\t"));
        // an empty string is a field, not a missing one
        assertEquals(1, Backfiller.parseCopyLine("").size());
        assertEquals("", Backfiller.parseCopyLine("").get(0));
        assertEquals(java.util.Arrays.asList(null, null), Backfiller.parseCopyLine("\\N\t\\N"));
    }

    @Test
    void nullMarkerIsOnlyAWholeField() {
        // 'x' followed by backslash-N is not a NULL: it is the text x then an N escape, which
        // COPY itself never produces; it must not turn the field into NULL.
        assertEquals("x", Backfiller.parseCopyLine("x\\N").get(0));
    }

    @Test
    void escapesCombineWithinOneField() {
        assertEquals(List.of("a\\\tb\n\r", "z"),
                     Backfiller.parseCopyLine("a\\\\\\tb\\n\\r\tz"));
    }

    @Test
    void identifiersAreQuoted() {
        assertEquals("\"a\"\"b\"", Backfiller.quoteIdent("a\"b"));
        assertEquals("\"public\".\"T\"", Backfiller.quoteIdent("public") + "." + Backfiller.quoteIdent("T"));
    }

    @Test
    void commitIdentifiersAscendAndEndAtTheConsistentPoint() {
        // 3 chunks: the identifiers are cp-2, cp-1, cp; the truncate that precedes them is cp-3
        assertEquals(97L, Backfiller.truncateIdentifier(100L, 3));
        assertEquals(98L, Backfiller.chunkIdentifier(100L, 3, 0));
        assertEquals(100L, Backfiller.chunkIdentifier(100L, 3, 2));
        // no chunks at all: the truncate itself sits on the consistent point
        assertEquals(100L, Backfiller.truncateIdentifier(100L, 0));
        assertThrows(IllegalStateException.class, () -> Backfiller.truncateIdentifier(3L, 3));
    }

    @Test
    void startupDecisionNeverReconcilesWhatItCannot() {
        List<String> none = List.of();
        assertEquals(Main.BackfillAction.CREATE_SLOT_AND_BACKFILL,
                     Main.decideBackfill(false, 2, 2, none, "s"));
        assertEquals(Main.BackfillAction.CREATE_SLOT_AND_BACKFILL,
                     Main.decideBackfill(false, 0, 0, none, "s"),
                     "no tables yet: still make the slot");
        assertEquals(Main.BackfillAction.NONE, Main.decideBackfill(true, 0, 3, none, "s"));
        assertEquals(Main.BackfillAction.RECREATE_SLOT_AND_BACKFILL,
                     Main.decideBackfill(true, 3, 3, none, "s"));
        assertThrows(IllegalStateException.class,
                     () -> Main.decideBackfill(false, 1, 2, List.of("public.a"), "s"));
        assertThrows(IllegalStateException.class,
                     () -> Main.decideBackfill(true, 1, 2, none, "s"));
    }

    /**
     * The documented first start: one table registered while EMPTY, so backfill_due is false
     * although nothing has ever been written to Paimon. Nothing is stranded, so the slot is
     * created; keying this on backfill_due refused the start with "stream position is lost".
     */
    @Test
    void firstStartWithOnlyEmptyTablesRegisteredCreatesTheSlot() {
        assertEquals(Main.BackfillAction.CREATE_SLOT_AND_BACKFILL,
                     Main.decideBackfill(false, 0, 1, List.of(), "s"));
        assertEquals(Main.BackfillAction.CREATE_SLOT_AND_BACKFILL,
                     Main.decideBackfill(false, 1, 3, List.of(), "s"),
                     "one table with rows, two registered empty: still a first start");
    }

    /**
     * The whole of the startup classification, all eight combinations. The last two lines are the
     * ones that were wrong: a table registered while empty is not due and has never been written,
     * and with the slot ABSENT it must be LOADED, not skipped. Skipping it silently dropped every
     * row inserted between add_table() and the writer's first start -- on the documented order of
     * operations, with no warning and no recovery.
     */
    @Test
    void startupClassificationOfOneRegistryRow() {
        // due: load it, whatever else is true
        assertEquals(Main.RowRole.DUE,       Main.classify(true,  false, false));
        assertEquals(Main.RowRole.DUE,       Main.classify(true,  false, true));
        assertEquals(Main.RowRole.DUE,       Main.classify(true,  true,  false),
                     "re-bootstrap: due although Paimon holds our data, so it is reloaded");
        assertEquals(Main.RowRole.DUE,       Main.classify(true,  true,  true));
        // not due but written by us: its stream position was the slot
        assertEquals(Main.RowRole.STRANDED,  Main.classify(false, true,  false));
        assertEquals(Main.RowRole.STRANDED,  Main.classify(false, true,  true));
        // registered while empty, never written
        assertEquals(Main.RowRole.STREAMING, Main.classify(false, false, true),
                     "slot present: the create message came through it and DML follows");
        assertEquals(Main.RowRole.DUE,       Main.classify(false, false, false),
                     "slot absent: first start, so load it -- it may have gained rows since");
    }

    /**
     * The documented re-bootstrap (drop the slot, set backfill_due on every row) leaves the
     * warehouse full of data this writer committed, and must still be allowed: every table is
     * due, so none is stranded and the backfill truncates and reloads each one.
     */
    @Test
    void reBootstrapIsAllowedAlthoughPaimonHoldsOurData() {
        assertEquals(Main.BackfillAction.CREATE_SLOT_AND_BACKFILL,
                     Main.decideBackfill(false, 3, 3, List.of(), "s"));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "PGPAIMON_IT_JDBC_URL", matches = ".+")
    void backfillLoadsEveryRowAtTheConsistentPoint() throws Exception {
        String jdbc = System.getenv("PGPAIMON_IT_JDBC_URL");
        String wh   = System.getenv("PGPAIMON_IT_WAREHOUSE");

        try (Connection c = DriverManager.getConnection(jdbc);
             Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS bf_t");
            st.execute("DROP TABLE IF EXISTS bf_renamed");
            st.execute("CREATE TABLE bf_t (id int PRIMARY KEY, v text)");
            st.execute("INSERT INTO bf_t SELECT g, 'v' || g FROM generate_series(1, 1000) g");
            st.execute("SELECT paimon.add_table('bf_t'::regclass)");
            // the PostgreSQL name changes; the Paimon identity (registry + warehouse) must not
            st.execute("ALTER TABLE bf_t RENAME TO bf_renamed");
        }
        long relid = relidOf(jdbc, "bf_renamed");

        Map<String, String> env = new HashMap<>();
        env.put("PGPAIMON_JDBC_URL", jdbc);
        env.put("PGPAIMON_WAREHOUSE", wh);
        env.put("PGPAIMON_FLUSH_MAX_RECORDS", "300");     // 4 chunks
        WriterConfig cfg = WriterConfig.fromEnv(env);

        Options o = new Options();
        o.set("warehouse", wh);
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
        SchemaRegistry reg = new SchemaRegistry(catalog, cfg);
        reg.createTable(relid, "public", "bf_t", List.of("id"),
            List.of(new DdlMessage.Col(1, "id", "INT NOT NULL"),
                    new DdlMessage.Col(2, "v", "STRING")));

        PaimonSink sink = new PaimonSink(reg, cfg);
        try {
            sink.setRelation(relid, new RelationMeta(relid, "public", "bf_t",
                List.of("id", "v"), List.of(true, false)));
            Backfiller.BackfillPlan plan;
            try (Backfiller bf = new Backfiller(cfg, reg, sink)) {
                plan = bf.createSlotAndExportSnapshot();
                // a row committed after the slot exists must NOT be in the snapshot
                try (Connection c = DriverManager.getConnection(jdbc);
                     Statement st = c.createStatement()) {
                    st.execute("INSERT INTO bf_renamed VALUES (5000, 'late')");
                }
                bf.backfill(plan, relid, "public", "bf_t", List.of("id", "v"));
            }

            // Every record is +I and every _seq is the consistent point: a streaming
            // change always carries a higher LSN, so it wins with no coordination.
            List<RawRecord> raw = RawRead.read(wh, "bf_t");
            assertEquals(1000, raw.size());
            assertTrue(raw.stream().allMatch(r -> r.rowKind().equals("+I")),
                       "every backfilled record must be RowKind.INSERT");
            assertTrue(raw.stream().allMatch(r -> r.seq() == plan.consistentPoint()),
                       "every backfilled _seq must equal the consistent point");
            assertTrue(java.nio.file.Files.isDirectory(
                java.nio.file.Path.of(wh, "default.db", "bf_t")),
                "the Paimon table keeps its original name");
            assertFalse(java.nio.file.Files.exists(
                java.nio.file.Path.of(wh, "default.db", "bf_renamed")));
            assertEquals(plan.consistentPoint(),
                new OffsetStore(reg, cfg).highestCommittedLsn(List.of(relid)));
        } finally {
            sink.close();
            catalog.close();
            // a leaked logical slot retains WAL indefinitely and blocks the next run
            try (Connection c = DriverManager.getConnection(jdbc);
                 Statement st = c.createStatement()) {
                st.execute("SELECT pg_drop_replication_slot(slot_name)"
                    + " FROM pg_replication_slots WHERE slot_name = '" + cfg.slotName() + "'");
            }
        }

        try (Connection c = DriverManager.getConnection(jdbc);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT backfill_due FROM paimon.table_registry"
                 + " WHERE relid = 'bf_renamed'::regclass")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1), "mark_backfilled must have run");
        }
    }

    private static long relidOf(String jdbc, String table) throws Exception {
        try (Connection c = DriverManager.getConnection(jdbc);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT '" + table + "'::regclass::oid")) {
            rs.next();
            return rs.getLong(1);
        }
    }
    /**
     * A stale PGPAIMON_SKIP_LSN -- one left set after its skip already happened -- can never fire,
     * and must say so rather than sit armed and forgotten. It is never a refusal to start:
     * refusing would turn a leftover variable into an outage.
     */
    @Test
    void aStaleSkipLsnIsAnnouncedButNeverRefused() {
        assertTrue(mainWarnings(() -> Main.warnOnStaleSkipLsn(0x100L, 0x200L)).stream()
                       .anyMatch(m -> m.contains("can never fire")),
                   "below the resume position");
        assertTrue(mainWarnings(() -> Main.warnOnStaleSkipLsn(0x200L, 0x200L)).stream()
                       .anyMatch(m -> m.contains("can never fire")),
                   "exactly at the resume position");
        assertTrue(mainWarnings(() -> Main.warnOnStaleSkipLsn(0x300L, 0x200L)).stream()
                       .anyMatch(m -> m.contains("is armed")),
                   "ahead of the resume position: it can still fire");
        assertTrue(mainWarnings(() -> Main.warnOnStaleSkipLsn(0x100L, 0L)).stream()
                       .anyMatch(m -> m.contains("is armed")),
                   "resume 0 means the slot's own position, so nothing is known to be behind us");
        assertTrue(mainWarnings(() -> Main.warnOnStaleSkipLsn(0L, 0x200L)).isEmpty(),
                   "unset says nothing at all");
    }

    private static List<String> mainWarnings(Runnable r) {
        List<String> out = new ArrayList<>();
        java.util.logging.Logger log = java.util.logging.Logger.getLogger(Main.class.getName());
        java.util.logging.Handler h = new java.util.logging.Handler() {
            public void publish(java.util.logging.LogRecord x) { out.add(x.getMessage()); }
            public void flush() { }
            public void close() { }
        };
        log.addHandler(h);
        try { r.run(); } finally { log.removeHandler(h); }
        return out;
    }

}
