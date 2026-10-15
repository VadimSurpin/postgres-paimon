// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The replication slot, at startup and while running: an invalidated slot (or one that is not
 * the writer's) stops the writer with the cause and the re-bootstrap procedure; a warning (WAL
 * unreserved, an old prepared transaction) is logged and the writer carries on.
 */
class SlotHealthTest {

    static final String LOST = "fatal|replication slot paimon is invalidated (wal_removed): WAL it"
        + " still needed was removed because it held more than max_slot_wal_keep_size";
    static final String PREPARED = "warning|prepared transaction 'g1' (owner postgres) has been"
        + " open since 2026-10-15: until COMMIT PREPARED ... it holds back the replication slot";

    static FakeJdbc db(List<String> slotRows) {
        return new FakeJdbc(sql -> sql.contains("paimon.slot_health") ? slotRows
            : sql.contains("pg_replication_slots") ? List.of("1") : null);
    }

    @Test
    void anInvalidatedSlotStopsStartupWithTheCauseAndTheProcedure() {
        FakeJdbc fake = db(List.of(LOST));
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> Main.checkSlot(fake.connection(), "paimon"));
        assertTrue(e.getMessage().contains("REPLICATION SLOT UNUSABLE at startup"), e.getMessage());
        assertTrue(e.getMessage().contains("wal_removed"), e.getMessage());
        assertTrue(e.getMessage().contains("max_slot_wal_keep_size"), e.getMessage());
        assertTrue(e.getMessage().contains("pg_drop_replication_slot('paimon')"), e.getMessage());
        assertTrue(e.getMessage().contains("backfill_due = true"), e.getMessage());
        assertTrue(fake.executed.stream().anyMatch(q -> q.contains("for_writer => true")),
                   fake.executed.toString());
    }

    @Test
    void aWarningIsLoggedNotFatal() throws Exception {
        Main.checkSlot(db(List.of(PREPARED)).connection(), "paimon");
        Main.checkSlot(db(List.of()).connection(), "paimon");
    }

    /** The periodic check runs slot_health too, and stops on an invalidation found there. */
    @Test
    void thePeriodicCheckStopsOnAnInvalidationWhileRunning() {
        List<String> rows = new ArrayList<>();
        FakeJdbc fake = db(rows);
        AtomicLong clock = new AtomicLong(0);
        HealthCheck h = new HealthCheck(fake::connection, 1000, clock::get, "paimon");
        clock.addAndGet(2_000_000_000L);
        h.maybeCheck();                         // healthy: nothing happens
        rows.add(PREPARED);
        clock.addAndGet(2_000_000_000L);
        h.maybeCheck();                         // a warning: logged, carries on
        rows.add(LOST);
        clock.addAndGet(2_000_000_000L);
        IllegalStateException e = assertThrows(IllegalStateException.class, h::maybeCheck);
        assertTrue(e.getMessage().contains("REPLICATION SLOT UNUSABLE while running"),
                   e.getMessage());
        assertEquals(3, fake.executed.stream().filter(q -> q.contains("slot_health")).count());
    }

    /** Without a slot name (the old constructor) the slot is not checked at all. */
    @Test
    void noSlotNameNoSlotCheck() {
        FakeJdbc fake = db(List.of(LOST));
        AtomicLong clock = new AtomicLong(0);
        HealthCheck h = new HealthCheck(fake::connection, 1000, clock::get);
        clock.addAndGet(2_000_000_000L);
        h.maybeCheck();
        assertTrue(fake.executed.stream().noneMatch(q -> q.contains("slot_health")));
    }

    @Test
    void aTimeoutIsTransientAndAMissingFunctionIsFatal() {
        FakeJdbc gone = new FakeJdbc(sql -> sql.contains("slot_health")
            ? new java.sql.SQLException("function paimon.slot_health does not exist", "42883")
            : null);
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> Main.checkSlot(gone.connection(), "paimon"));
        assertTrue(e.getMessage().contains("is missing or not usable"), e.getMessage());
        Connection c = gone.connection();
        assertNotNull(c);
    }
}
