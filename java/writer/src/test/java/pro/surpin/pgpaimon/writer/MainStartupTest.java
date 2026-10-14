// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** What {@link Main} does before the stream opens, without a server. */
class MainStartupTest {

    static WriterConfig config() {
        Map<String, String> e = new HashMap<>();
        e.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost:5432/testdb");
        e.put("PGPAIMON_USER", "writer");
        e.put("PGPAIMON_PASSWORD", "pw");
        e.put("PGPAIMON_WAREHOUSE", "/tmp/wh");
        return WriterConfig.fromEnv(e);
    }

    /**
     * The plain connection (health checks, registry, credential) never resolves a name through
     * a schema another role can write: a planted operator in public would otherwise run inside
     * the writer's unattended queries, usually as a superuser.
     */
    @Test
    void thePlainConnectionPinsSearchPath() {
        Properties p = Main.plainProperties(config());
        assertEquals("-c search_path=pg_catalog,pg_temp", p.getProperty("options"));
        assertEquals("writer", p.getProperty("user"));
    }

    /**
     * Startup refuses to run on any health problem, and does so BEFORE it reads the credential
     * or the registry. (Review finding I5, mutation M7: removing the startup check passed every
     * unit test.)
     */
    @Test
    void preflightRefusesAProblemBeforeReadingAnything() {
        FakeJdbc db = new FakeJdbc(sql -> sql.contains("paimon.health()")
            ? java.util.List.of("event trigger paimon_ddl_end is missing")
            : sql.contains("paimon.secret") ? java.util.List.of("tok") : null);
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> Main.preflight(db.connection()));
        assertTrue(e.getMessage().contains("at startup"), e.getMessage());
        assertTrue(e.getMessage().contains("paimon_ddl_end is missing"), e.getMessage());
        assertTrue(db.executed.stream().noneMatch(q -> q.contains("paimon.secret")),
            "the credential is not read while a guardrail is bypassed: " + db.executed);
    }

    @Test
    void preflightOfAHealthyInstallationReturnsTheCredential() throws Exception {
        FakeJdbc db = new FakeJdbc(sql -> sql.contains("paimon.secret")
            ? java.util.List.of("tok") : null);
        assertEquals("tok", Main.preflight(db.connection()));
        assertTrue(db.executed.indexOf("SELECT * FROM paimon.health()")
            < db.executed.indexOf("SELECT token FROM paimon.secret"), db.executed.toString());
    }

    /** An extension that is gone stops startup with that diagnosis, not a raw SQL error. */
    @Test
    void preflightWithoutTheExtensionSaysSo() {
        FakeJdbc db = new FakeJdbc(sql -> sql.contains("paimon.health()")
            ? new java.sql.SQLException("function paimon.health() does not exist", "42883") : null);
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> Main.preflight(db.connection()));
        assertTrue(e.getMessage().contains("is missing or not usable"), e.getMessage());
    }

    /** A missing grant names every grant the writer needs (review minor m5). */
    @Test
    void aPermissionErrorNamesEveryGrant() {
        RuntimeException e = Main.missingGrant(
            new java.sql.SQLException("permission denied for schema paimon", "42501"));
        for (String g : java.util.List.of("USAGE ON SCHEMA paimon", "paimon.secret",
                                          "paimon.retained_history", "UPDATE ON paimon.table_registry")) {
            assertTrue(e.getMessage().contains(g), g + " in " + e.getMessage());
        }
    }
}
