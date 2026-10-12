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
        assertEquals("default", c.database());
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

}
