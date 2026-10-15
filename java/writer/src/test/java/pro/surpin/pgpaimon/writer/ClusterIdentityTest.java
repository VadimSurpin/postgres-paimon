// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The warehouse belongs to one cluster's timeline: another cluster, or this one with its WAL
 * behind the warehouse, is refused before anything is written; a failover and a re-bootstrap are
 * not. The identity is read with a plain query (fake JDBC here).
 */
class ClusterIdentityTest {

    static final ClusterIdentity.Identity NOW = new ClusterIdentity.Identity("7000", "1", 5_000);

    static ClusterIdentity.Stamp stamp(String sysid, String tl) {
        return new ClusterIdentity.Stamp("public.t", sysid, tl);
    }

    @Test
    void theIdentityIsReadFromTheServer() throws Exception {
        FakeJdbc db = new FakeJdbc(sql -> sql.contains("pg_control_system()")
            ? List.of("7461234567890123456|3|0/1A2B3C4D") : null);
        ClusterIdentity.Identity id = ClusterIdentity.read(db.connection());
        assertEquals("7461234567890123456", id.systemId());
        assertEquals("3", id.timeline());
        assertEquals(0x1A2B3C4DL, id.walLsn());
        assertEquals(id.systemId(), id.options().get(ClusterIdentity.SYSID_OPTION));
        assertEquals("3", id.options().get(ClusterIdentity.TIMELINE_OPTION));
    }

    @Test
    void theSameClusterOrNoStampAtAllIsAMatch() {
        assertEquals(ClusterIdentity.Verdict.MATCH, ClusterIdentity.decide(
            List.of(stamp("7000", "1"), stamp(null, null)), NOW, 4_000, true, false, null));
        assertEquals(ClusterIdentity.Verdict.MATCH,
            ClusterIdentity.decide(List.of(), NOW, 0, false, true, null));
    }

    @Test
    void anotherClusterIsRefusedWithBothRemedies() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> ClusterIdentity.decide(List.of(stamp("6999", "1")), NOW, 4_000, true, false,
                                         null));
        assertTrue(e.getMessage().contains("CLUSTER MISMATCH"), e.getMessage());
        assertTrue(e.getMessage().contains("6999"), e.getMessage());
        assertTrue(e.getMessage().contains("backfill_due = true"), e.getMessage());
        assertTrue(e.getMessage().contains("PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER=7000"), e.getMessage());
        // a wrong acknowledgement is no acknowledgement
        assertThrows(IllegalStateException.class, () -> ClusterIdentity.decide(
            List.of(stamp("6999", "1")), NOW, 4_000, true, false, "6999"));
    }

    @Test
    void anotherClusterIsTakenOverByAReBootstrapOrAnAcknowledgement() {
        assertEquals(ClusterIdentity.Verdict.ACCEPTED_OTHER_CLUSTER, ClusterIdentity.decide(
            List.of(stamp("6999", "1")), NOW, 4_000, false, true, null));
        assertEquals(ClusterIdentity.Verdict.ACCEPTED_OTHER_CLUSTER, ClusterIdentity.decide(
            List.of(stamp("6999", "1")), NOW, 4_000, true, false, "7000"));
    }

    /** A restore or PITR: the server's WAL is behind what the warehouse committed. Always refused. */
    @Test
    void aServerWhoseWalIsBehindTheWarehouseIsRefusedEvenForAReload() {
        for (boolean reload : new boolean[] {false, true}) {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ClusterIdentity.decide(List.of(stamp("7000", "1")), NOW, 6_000, true, reload,
                                             "7000"));
            assertTrue(e.getMessage().contains("WAL is behind the warehouse"), e.getMessage());
            assertTrue(e.getMessage().contains("NEW warehouse"), e.getMessage());
        }
    }

    @Test
    void aNewTimelineWithTheSlotIsAFailover() {
        assertEquals(ClusterIdentity.Verdict.NEW_TIMELINE, ClusterIdentity.decide(
            List.of(stamp("7000", "0")), NOW, 4_000, true, false, null));
    }

    @Test
    void theSlotIsCreatedForFailoverWhereTheServerHasIt() {
        assertEquals("CREATE_REPLICATION_SLOT paimon LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER)",
                     Backfiller.createSlotCommand("paimon", 18));
        assertEquals("CREATE_REPLICATION_SLOT paimon LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER)",
                     Backfiller.createSlotCommand("paimon", 17));
        assertEquals("CREATE_REPLICATION_SLOT paimon LOGICAL pgoutput (SNAPSHOT 'export')",
                     Backfiller.createSlotCommand("paimon", 16));
    }

    @Test
    void theAcknowledgementMustBeAnIdentifier() {
        java.util.Map<String, String> env = new java.util.HashMap<>(java.util.Map.of(
            "PGPAIMON_JDBC_URL", "jdbc:postgresql://h/db", "PGPAIMON_WAREHOUSE", "/tmp/wh",
            "PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER", "7461234567890123456"));
        assertEquals("7461234567890123456", WriterConfig.fromEnv(env).acceptSystemIdentifier());
        env.put("PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER", "yes");
        assertThrows(IllegalArgumentException.class, () -> WriterConfig.fromEnv(env));
    }
}
