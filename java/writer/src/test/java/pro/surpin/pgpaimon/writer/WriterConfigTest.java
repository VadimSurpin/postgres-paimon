// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class WriterConfigTest {

    private static Map<String, String> minimal() {
        Map<String, String> e = new HashMap<>();
        e.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost:5432/testdb");
        e.put("PGPAIMON_USER", "postgres");
        e.put("PGPAIMON_PASSWORD", "secret");
        e.put("PGPAIMON_WAREHOUSE", "/tmp/wh");
        return e;
    }

    @Test
    void defaultsMatchTheSpec() {
        WriterConfig c = WriterConfig.fromEnv(minimal());
        assertEquals("paimon", c.slotName());
        assertEquals("paimon_pub", c.publicationName());
        assertEquals("paimon", c.messagePrefix());
        assertEquals("_seq", c.seqField());
        assertEquals("_op_seq", c.opSeqField());
        assertNull(c.xidField(), "_xid is optional and off by default");
        assertEquals("paimon-writer-paimon", c.commitUser());
    }

    /**
     * There is no Paimon database setting. The Paimon database of a table is its PostgreSQL
     * schema, so a configured one would have nothing to name, and PGPAIMON_DATABASE was removed
     * rather than left parsed and ignored. Setting it must therefore change nothing -- and must
     * not be mistaken for working.
     */
    @Test
    void thereIsNoPaimonDatabaseSetting() {
        Map<String, String> e = minimal();
        e.put("PGPAIMON_DATABASE", "whatever");
        assertDoesNotThrow(() -> WriterConfig.fromEnv(e));
        for (java.lang.reflect.RecordComponent rc : WriterConfig.class.getRecordComponents()) {
            assertNotEquals("database", rc.getName(),
                            "a database setting would have nothing to name");
        }
    }

    @Test
    void commitUserIsDerivedFromTheSlotAndOverridable() {
        Map<String, String> env = new HashMap<>(minimal());
        env.put("PGPAIMON_SLOT", "other_slot");
        assertEquals("paimon-writer-other_slot", WriterConfig.fromEnv(env).commitUser());
        env.put("PGPAIMON_COMMIT_USER", "explicit");
        assertEquals("explicit", WriterConfig.fromEnv(env).commitUser());
    }

    @Test
    void replicationUrlForcesSimpleQueryMode() {
        WriterConfig c = WriterConfig.fromEnv(minimal());
        assertTrue(c.replicationUrl().contains("replication=database"));
        assertTrue(c.replicationUrl().contains("preferQueryMode=simple"));
    }

    @Test
    void missingWarehouseIsRejectedWithAUsefulMessage() {
        Map<String, String> e = minimal();
        e.remove("PGPAIMON_WAREHOUSE");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> WriterConfig.fromEnv(e));
        assertTrue(ex.getMessage().contains("PGPAIMON_WAREHOUSE"));
    }

    @Test
    void s3CatalogOptionsArePassedThrough() {
        Map<String, String> e = minimal();
        e.put("PGPAIMON_WAREHOUSE", "s3://bucket/wh");
        e.put("PGPAIMON_S3_ENDPOINT", "http://minio:9000");
        e.put("PGPAIMON_S3_ACCESS_KEY", "ak");
        e.put("PGPAIMON_S3_SECRET_KEY", "sk");
        WriterConfig c = WriterConfig.fromEnv(e);
        assertEquals("http://minio:9000", c.catalogOptions().get("s3.endpoint"));
        assertEquals("ak", c.catalogOptions().get("s3.access-key"));
        assertEquals("sk", c.catalogOptions().get("s3.secret-key"));
        assertEquals("true", c.catalogOptions().get("s3.path.style.access"));
    }

    @Test
    void columnValueDistinguishesNullFromEmpty() {
        assertEquals(ColState.NULL, ColumnValue.ofNull().state());
        assertNull(ColumnValue.ofNull().text());
        assertEquals(ColState.TEXT, ColumnValue.of("").state());
        assertEquals("", ColumnValue.of("").text());
        assertEquals(ColState.UNCHANGED, ColumnValue.unchanged().state());
    }

    @Test
    void blankEnvValuesMeanUnset() {
        Map<String, String> e = minimal();
        e.put("PGPAIMON_XID_FIELD", "");
        e.put("PGPAIMON_SLOT", "  ");
        e.put("PGPAIMON_S3_ENDPOINT", "");
        WriterConfig c = WriterConfig.fromEnv(e);
        assertNull(c.xidField());
        assertEquals("paimon", c.slotName());
        assertFalse(c.catalogOptions().containsKey("s3.path.style.access"));
    }

    @Test
    void columnValueOfNullIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> ColumnValue.of(null));
    }

    @Test
    void oidAboveIntMaxIsHeldUnsignedAsLong() {
        // binary protocol route: 4 bytes through a real ByteBuffer
        ByteBuffer buf = ByteBuffer.allocate(4).putInt((int) 3_000_000_000L);
        buf.flip();
        long fromBinary = Integer.toUnsignedLong(buf.getInt());
        // DDL JSON route: decimal string
        long fromJson = Long.parseLong("3000000000");
        assertEquals(fromJson, fromBinary);
        assertEquals(3_000_000_000L, fromBinary);

        RelationMeta m = new RelationMeta(fromBinary, "public", "t", List.of("id"), List.of(true));
        PgEvent.Insert ins = new PgEvent.Insert(fromJson, new TupleData(List.of(ColumnValue.of("1"))));
        PgEvent.Truncate tr = new PgEvent.Truncate(List.of(fromBinary), false, false);
        PgEvent.TypeMsg ty = new PgEvent.TypeMsg(fromBinary, "public", "my_type");
        assertEquals(3_000_000_000L, m.relid());
        assertEquals(3_000_000_000L, ins.relid());
        assertEquals(3_000_000_000L, tr.relids().get(0));
        assertEquals(3_000_000_000L, ty.typeOid());
    }

    @Test
    void invalidColumnValueCombinationsThrow() {
        assertThrows(IllegalArgumentException.class, () -> new ColumnValue(ColState.TEXT, null));
        assertThrows(IllegalArgumentException.class, () -> new ColumnValue(ColState.NULL, ""));
        assertThrows(IllegalArgumentException.class, () -> new ColumnValue(ColState.UNCHANGED, "x"));
        assertThrows(IllegalArgumentException.class, () -> new ColumnValue(null, "x"));
        assertEquals(ColState.TEXT, new ColumnValue(ColState.TEXT, "").state());
    }

    @Test
    void malformedNumberNamesTheVariable() {
        Map<String, String> e = minimal();
        e.put("PGPAIMON_FLUSH_MAX_RECORDS", "lots");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> WriterConfig.fromEnv(e));
        assertTrue(ex.getMessage().contains("PGPAIMON_FLUSH_MAX_RECORDS"));
        assertTrue(ex.getMessage().contains("lots"));

        Map<String, String> e3 = minimal();
        e3.put("PGPAIMON_FLUSH_MAX_RECORDS", "99999999999");
        assertThrows(IllegalArgumentException.class, () -> WriterConfig.fromEnv(e3));
    }
    @Test
    void skipLsnIsUnsetByDefaultAndAcceptsBothSpellings() {
        assertEquals(0L, WriterConfig.fromEnv(minimal()).skipLsn(), "unset means no escape");

        Map<String, String> hex = minimal();
        hex.put("PGPAIMON_SKIP_LSN", "0/19A6F590");   // the form the POISON RECORD error prints
        assertEquals(0x19A6F590L, WriterConfig.fromEnv(hex).skipLsn());

        Map<String, String> dec = minimal();
        dec.put("PGPAIMON_SKIP_LSN", "430000016");
        assertEquals(430000016L, WriterConfig.fromEnv(dec).skipLsn());

        Map<String, String> high = minimal();
        high.put("PGPAIMON_SKIP_LSN", "1/9ABCDEF0");
        assertEquals(0x1_9ABCDEF0L, WriterConfig.fromEnv(high).skipLsn());
    }

    @Test
    void anUnusableSkipLsnIsRefusedAtStartup() {
        for (String bad : new String[] { "", " ", "0", "0/0", "nonsense", "1/", "/1",
                                         "0/ZZZZ", "1-2", "0x10" }) {
            Map<String, String> e = minimal();
            e.put("PGPAIMON_SKIP_LSN", bad);
            if (bad.isBlank()) {
                assertEquals(0L, WriterConfig.fromEnv(e).skipLsn(), "blank means unset");
                continue;
            }
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> WriterConfig.fromEnv(e), "must refuse '" + bad + "'");
            assertTrue(ex.getMessage().contains("PGPAIMON_SKIP_LSN"), ex.getMessage());
        }
    }

    @Test
    void theStreamingBatchLimitsHaveDefaultsAndAreSeparateFromTheBackfillChunk() {
        WriterConfig c = WriterConfig.fromEnv(minimal());
        assertEquals(10_000, c.batchMaxRows());
        assertEquals(16L * 1024 * 1024, c.batchMaxBytes());
        assertEquals(2_000, c.batchMaxIntervalMs());
        assertEquals(10_000, c.flushMaxRecords(), "the backfill chunk is a different setting");

        Map<String, String> e = minimal();
        e.put("PGPAIMON_FLUSH_MAX_RECORDS", "300");
        assertEquals(10_000, WriterConfig.fromEnv(e).batchMaxRows(),
            "PGPAIMON_FLUSH_MAX_RECORDS must not size the streaming batch");
        e = minimal();
        e.put("PGPAIMON_BATCH_MAX_ROWS", "1");
        assertEquals(10_000, WriterConfig.fromEnv(e).flushMaxRecords(),
            "PGPAIMON_BATCH_MAX_ROWS must not size the backfill chunk");
    }

    @Test
    void theBatchLimitsAreConfigurableAndRefuseNonsense() {
        Map<String, String> e = minimal();
        e.put("PGPAIMON_BATCH_MAX_ROWS", "1");
        e.put("PGPAIMON_BATCH_MAX_BYTES", "4096");
        e.put("PGPAIMON_BATCH_MAX_INTERVAL_MS", "0");
        WriterConfig c = WriterConfig.fromEnv(e);
        assertEquals(1, c.batchMaxRows());
        assertEquals(4096, c.batchMaxBytes());
        assertEquals(0, c.batchMaxIntervalMs());

        for (String[] bad : new String[][] {
                {"PGPAIMON_BATCH_MAX_ROWS", "0"}, {"PGPAIMON_BATCH_MAX_ROWS", "lots"},
                {"PGPAIMON_BATCH_MAX_BYTES", "0"}, {"PGPAIMON_BATCH_MAX_BYTES", "-5"},
                {"PGPAIMON_BATCH_MAX_INTERVAL_MS", "-1"}, {"PGPAIMON_BATCH_MAX_INTERVAL_MS", "1s"}}) {
            Map<String, String> b = minimal();
            b.put(bad[0], bad[1]);
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> WriterConfig.fromEnv(b), bad[0] + "=" + bad[1]);
            assertTrue(ex.getMessage().contains(bad[0]), ex.getMessage());
        }
    }

    /**
     * The extension publishes to paimon_pub and emits DDL under 'paimon' and nothing else, so a
     * writer configured otherwise would replicate nothing and say nothing.
     */
    @Test
    void thePublicationAndThePrefixCanOnlyBeTheExtensionsOwn() {
        Map<String, String> base = Map.of("PGPAIMON_JDBC_URL", "jdbc:postgresql://h/db",
                                          "PGPAIMON_WAREHOUSE", "/wh");
        Map<String, String> pub = new HashMap<>(base);
        pub.put("PGPAIMON_PUBLICATION", "other_pub");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> WriterConfig.fromEnv(pub));
        assertTrue(e.getMessage().contains("PGPAIMON_PUBLICATION must be 'paimon_pub'"), e.getMessage());
        Map<String, String> prefix = new HashMap<>(base);
        prefix.put("PGPAIMON_MESSAGE_PREFIX", "mine");
        assertThrows(IllegalArgumentException.class, () -> WriterConfig.fromEnv(prefix));
        Map<String, String> explicit = new HashMap<>(base);
        explicit.put("PGPAIMON_PUBLICATION", "paimon_pub");
        explicit.put("PGPAIMON_MESSAGE_PREFIX", "paimon");
        assertEquals("paimon_pub", WriterConfig.fromEnv(explicit).publicationName());
    }

    @Test
    void theHealthIntervalDefaultsToAMinuteAndZeroTurnsItOff() {
        Map<String, String> env = new HashMap<>(Map.of("PGPAIMON_JDBC_URL", "jdbc:postgresql://h/db",
                                                       "PGPAIMON_WAREHOUSE", "/wh"));
        assertEquals(60_000L, WriterConfig.fromEnv(env).healthIntervalMs());
        env.put("PGPAIMON_HEALTH_INTERVAL_MS", "0");
        assertEquals(0L, WriterConfig.fromEnv(env).healthIntervalMs());
        env.put("PGPAIMON_HEALTH_INTERVAL_MS", "-1");
        assertThrows(IllegalArgumentException.class, () -> WriterConfig.fromEnv(env));
    }
}
