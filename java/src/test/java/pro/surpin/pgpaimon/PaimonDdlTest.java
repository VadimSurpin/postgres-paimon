// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.StreamTableScan;

import java.io.File;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import org.junit.jupiter.api.Test;
import java.util.*;

/**
 * Integration tests for the paimon_ddl.cpp ProcessUtility_hook.
 *
 * Each test corresponds to one DDL case intercepted by the hook:
 *
 *   D1  DROP TABLE         — pre-execute capture; table removed from PostgreSQL
 *   D2  TRUNCATE           — pre-execute capture; DML after truncate still works
 *   D3  ADD COLUMN         — post-execute capture; new column propagated to Paimon schema
 *   D4  DROP COLUMN        — pre-execute capture; column removed from Paimon schema
 *   D5  RENAME COLUMN      — pre-execute capture; new name reflected in Paimon schema
 *   D6  ALTER COLUMN TYPE  — post-execute capture; new type reflected in Paimon schema
 *   D7  key-less table     — add_table refuses it (the guardrail replaces ADD PRIMARY KEY)
 *   D8  DROP PRIMARY KEY   — blocked with ERROR before any execution
 *   D9  Non-paimon passthrough — regular heap tables are unaffected by the hook
 *
 * Schema evolution tests (D3–D6) verify that after the hook sends the DDL
 * message the bgworker updates the Paimon schema file (schema-1).  A reliable
 * signal is to insert a row after the ALTER and wait for the next Parquet file:
 * because the ring is FIFO, parquet written for that row means the preceding
 * DDL message was already processed.
 *
 * Usage:
 *   java -cp paimon-ddl-test.jar pro.surpin.pgpaimon.PaimonDdlTest [warehouse] [jdbc-url]
 */
public class PaimonDdlTest {

    /**
     * The PostgreSQL schema these suites create their tables in, and therefore the
     * Paimon database they land in: the writer maps one to the other, so the warehouse
     * layout is <warehouse>/<schema>.db/<table>.
     */
    static final String PG_SCHEMA = "public";

    static final String DEFAULT_WAREHOUSE = env("PGPAIMON_IT_WAREHOUSE", "/tmp/paimon-warehouse");
    static final String DEFAULT_JDBC =
        env("PGPAIMON_IT_JDBC_URL", "jdbc:postgresql://localhost:5434/testdb?user=ubuntu&password=");

    static String warehouse;
    static String jdbcUrl;
    static LocalFileIO fileIO = LocalFileIO.create();
    static int passed = 0, failed = 0;
    static final List<String> failures = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        System.exit(run(args) > 0 ? 1 : 0);
    }

    static int run(String[] args) throws Exception {
        passed = 0; failed = 0; failures.clear();
        warehouse = args.length > 0 ? args[0] : DEFAULT_WAREHOUSE;
        jdbcUrl   = args.length > 1 ? args[1] : DEFAULT_JDBC;

        System.out.println("╔════════════════════════════════════════════════════╗");
        System.out.println("║         paimon_heap DDL Hook Test Suite            ║");
        System.out.println("╚════════════════════════════════════════════════════╝");
        System.out.printf("  Warehouse : %s%n", warehouse);
        System.out.printf("  JDBC      : %s%n", jdbcUrl);
        System.out.println();

        testD1_dropTable();
        testD2_truncate();
        testD3_addColumn();
        testD4_dropColumn();
        testD5_renameColumn();
        testD6_alterColumnType();
        testD7_addPkBlocked();
        testD8_dropPkBlocked();
        testD9_nonPaimonPassthrough();

        System.out.println();
        System.out.println("═".repeat(52));
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        return failed;
    }

    /** Surefire entry point: the harness exports the stack's location. */
    @Test
    void acceptanceSuite() throws Exception {
        String jdbc = System.getenv("PGPAIMON_IT_JDBC_URL");
        String wh = System.getenv("PGPAIMON_IT_WAREHOUSE");
        if ("1".equals(System.getenv("PGPAIMON_HARNESS"))) {
            assertNotNull(jdbc, "harness did not export PGPAIMON_IT_JDBC_URL");
            assertNotNull(wh, "harness did not export PGPAIMON_IT_WAREHOUSE");
        }
        assumeTrue(jdbc != null && wh != null, "no live stack: run test/slot-acceptance/run_acceptance.sh");
        int failedChecks = run(new String[0]);
        assertEquals(0, failedChecks, "failed checks: " + failures);
    }

    // ── D1: DROP TABLE ───────────────────────────────────────────────────────
    // The hook captures the OID and name pre-execute (before the relation is
    // gone), sends PMSG_DDL_DROP_TBL, then lets standard_ProcessUtility run.
    // Verifications:
    //   • DROP TABLE completes without error
    //   • Table is gone from the PostgreSQL catalog
    //   • Re-creating the same table and inserting works normally
    static void testD1_dropTable() throws Exception {
        System.out.println("\n── D1: DROP TABLE ──────────────────────────────────");
        String tbl = "ddl_drop_table";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'a'),(2,'b')");
        }
        waitForParquet(tbl, 1);
        Thread.sleep(1000);

        boolean dropOk = true;
        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE " + tbl);
        } catch (Exception e) {
            dropOk = false;
            System.err.println("  DROP error: " + e.getMessage());
        }
        check("D1 drop table: DROP TABLE no error", dropOk);

        // Verify PG no longer has the table
        boolean tableGone = false;
        try (Connection conn = connect();
             Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT 1 FROM information_schema.tables " +
                 "WHERE table_name = '" + tbl + "' AND table_schema = 'public'")) {
            tableGone = !rs.next();
        }
        check("D1 drop table: table removed from PG catalog", tableGone);

        // The WRITER removes the Paimon table: DROP TABLE emits drop_table and the writer purges
        // the warehouse data. Wait for that and assert it, rather than deleting the directory by
        // hand. That hand-deletion was a leftover from main, where DROP TABLE left the data
        // behind; here it raced the writer's own catalog.dropTable, which can list schema-0 and
        // then find it gone, and the writer stopped fatally -- taking every later suite with it.
        File dir = new File(tableDir(tbl));
        long gone = System.currentTimeMillis() + 30_000;
        while (dir.exists() && System.currentTimeMillis() < gone) Thread.sleep(250);
        check("D1 drop table: the writer removed the Paimon table from the warehouse",
              !dir.exists());

        // Re-create the table and verify DML capture still works
        boolean recreateOk = true;
        try (Connection conn = connect()) {
            disableMeta(conn);
            createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'re-a'),(2,'re-b'),(3,'re-c')");
        } catch (Exception e) {
            recreateOk = false;
            System.err.println("  Re-create error: " + e.getMessage());
        }
        check("D1 drop table: re-create + INSERT no error", recreateOk);
        if (recreateOk) {
            PaimonRows rows = waitAndRead(tbl, 3);
            check("D1 drop table: 3 rows captured after re-create", rows.total == 3);
        }
    }

    // ── D2: TRUNCATE ──────────────────────────────────────────────────────────
    // The hook sends PMSG_DDL_TRUNCATE pre-execute (before rows are removed),
    // allowing the bgworker to record the truncate event.  DML after TRUNCATE
    // must continue to be captured normally.
    static void testD2_truncate() throws Exception {
        System.out.println("\n── D2: TRUNCATE ─────────────────────────────────────");
        String tbl = "ddl_truncate";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            createTracked(conn, tbl, "id INT PRIMARY KEY, v INT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20),(3,30),(4,40),(5,50)");
        }
        waitForParquet(tbl, 1);
        Thread.sleep(1000);
        long rowsBefore = readRows(tbl).total;
        check("D2 truncate: 5 rows captured before TRUNCATE", rowsBefore == 5);

        boolean truncateOk = true;
        try (Connection conn = connect()) {
            exec(conn, "TRUNCATE TABLE " + tbl);
        } catch (Exception e) {
            truncateOk = false;
            System.err.println("  TRUNCATE error: " + e.getMessage());
        }
        check("D2 truncate: TRUNCATE no error", truncateOk);

        if (truncateOk) {
            // DML after TRUNCATE must still be captured
            try (Connection conn = connect()) {
                exec(conn, "INSERT INTO " + tbl + " VALUES (6,60),(7,70),(8,80)");
            }
            // Wait for bgworker to flush the post-truncate rows
            long deadline = System.currentTimeMillis() + 120_000;
            while (System.currentTimeMillis() < deadline) {
                long total = readRows(tbl).total;
                if (total >= 3) break;
                Thread.sleep(500);
            }
            PaimonRows rows = readRows(tbl);
            check("D2 truncate: DML after TRUNCATE captured (>= 3 rows)",
                  rows.total >= 3);
            check("D2 truncate: post-truncate rows are inserts",
                  rows.ofKind(0) >= 3);
        }
    }

    // ── D3: ADD COLUMN ────────────────────────────────────────────────────────
    // The hook runs post-execute and sends PMSG_DDL_ADD_COL with the new
    // column's attnum, type, and name as read from the updated pg_attribute.
    // The bgworker adds the column to the Paimon schema (schema-1).
    static void testD3_addColumn() throws Exception {
        System.out.println("\n── D3: ALTER TABLE ADD COLUMN ───────────────────────");
        String tbl = "ddl_add_col";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            createTracked(conn, tbl, "id INT PRIMARY KEY, v INT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20)");
        }
        waitForFile(new File(tableDir(tbl) + "/schema/schema-0"));
        waitForParquet(tbl, 1);
        Thread.sleep(500);

        boolean alterOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " ADD COLUMN extra TEXT");
            // Insert using the new column — ensures bgworker sees DDL before this DML
            exec(conn, "INSERT INTO " + tbl + " VALUES (3,30,'hello')");
            exec(conn, "INSERT INTO " + tbl + " VALUES (4,40,'world')");
        } catch (Exception e) {
            alterOk = false;
            System.err.println("  ALTER/INSERT error: " + e.getMessage());
        }
        check("D3 add col: ALTER TABLE ADD COLUMN + INSERT no error", alterOk);

        if (alterOk) {
            // Wait for schema-1 (bgworker processed PMSG_DDL_ADD_COL)
            boolean schemaUpdated = waitForSchemaV(tbl, 1);
            check("D3 add col: schema-1 written by bgworker", schemaUpdated);

            // Verify the column was added to the Paimon schema
            TableSchema schema = readSchema(tbl);
            boolean extraPresent = schema.fields().stream()
                .anyMatch(f -> f.name().equals("extra"));
            check("D3 add col: 'extra' column present in Paimon schema", extraPresent);

            if (extraPresent) {
                // Verify the column has STRING type (TEXT → PTYPE_STRING → "STRING")
                schema.fields().stream()
                    .filter(f -> f.name().equals("extra"))
                    .findFirst()
                    .ifPresent(f -> check("D3 add col: 'extra' has STRING type",
                        f.type().toString().toUpperCase().contains("STRING") ||
                        f.type().toString().toUpperCase().contains("VARCHAR") ||
                        f.type().toString().toUpperCase().contains("CHAR")));
            }

            // Verify all 4 rows are captured
            PaimonRows rows = waitAndRead(tbl, 4);
            check("D3 add col: 4 rows captured after ADD COLUMN", rows.total == 4);
        }
    }

    // ── D4: DROP COLUMN ───────────────────────────────────────────────────────
    // The hook runs pre-execute and reads the column's attnum from pg_attribute
    // (while the column still exists) before sending PMSG_DDL_DROP_COL.
    // The bgworker removes the field from the Paimon schema (schema-1).
    static void testD4_dropColumn() throws Exception {
        System.out.println("\n── D4: ALTER TABLE DROP COLUMN ──────────────────────");
        String tbl = "ddl_drop_col";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            createTracked(conn, tbl, "id INT PRIMARY KEY, v INT, extra TEXT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10,'a'),(2,20,'b')");
        }
        waitForFile(new File(tableDir(tbl) + "/schema/schema-0"));
        waitForParquet(tbl, 1);
        Thread.sleep(500);

        // Verify 'extra' is in the initial schema
        TableSchema schemaBefore = readSchema(tbl);
        boolean extraBefore = schemaBefore.fields().stream()
            .anyMatch(f -> f.name().equals("extra"));
        check("D4 drop col: 'extra' present in schema before DROP", extraBefore);

        boolean alterOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " DROP COLUMN extra");
            exec(conn, "INSERT INTO " + tbl + " VALUES (3,30)");
            exec(conn, "INSERT INTO " + tbl + " VALUES (4,40)");
        } catch (Exception e) {
            alterOk = false;
            System.err.println("  ALTER/INSERT error: " + e.getMessage());
        }
        check("D4 drop col: ALTER TABLE DROP COLUMN + INSERT no error", alterOk);

        if (alterOk) {
            boolean schemaUpdated = waitForSchemaV(tbl, 1);
            check("D4 drop col: schema-1 written by bgworker", schemaUpdated);

            TableSchema schema = readSchema(tbl);
            boolean extraGone = schema.fields().stream()
                .noneMatch(f -> f.name().equals("extra"));
            check("D4 drop col: 'extra' removed from Paimon schema", extraGone);

            PaimonRows rows = waitAndRead(tbl, 4);
            check("D4 drop col: 4 rows captured after DROP COLUMN", rows.total == 4);
        }
    }

    // ── D5: RENAME COLUMN ─────────────────────────────────────────────────────
    // RENAME COLUMN arrives as a RenameStmt, not AlterTableStmt.  The hook
    // looks up the old attnum pre-execute (while the old name is still in
    // pg_attribute) and sends PMSG_DDL_RENAME with the new name.
    // The bgworker renames the field in the Paimon schema (schema-1).
    static void testD5_renameColumn() throws Exception {
        System.out.println("\n── D5: RENAME COLUMN ────────────────────────────────");
        String tbl = "ddl_rename_col";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            createTracked(conn, tbl, "id INT PRIMARY KEY, v_old INT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,100),(2,200)");
        }
        waitForFile(new File(tableDir(tbl) + "/schema/schema-0"));
        waitForParquet(tbl, 1);
        Thread.sleep(500);

        boolean alterOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " RENAME COLUMN v_old TO v_new");
            exec(conn, "INSERT INTO " + tbl + " VALUES (3,300)");
        } catch (Exception e) {
            alterOk = false;
            System.err.println("  RENAME/INSERT error: " + e.getMessage());
        }
        check("D5 rename col: RENAME COLUMN + INSERT no error", alterOk);

        if (alterOk) {
            boolean schemaUpdated = waitForSchemaV(tbl, 1);
            check("D5 rename col: schema-1 written by bgworker", schemaUpdated);

            TableSchema schema = readSchema(tbl);
            boolean newNamePresent = schema.fields().stream()
                .anyMatch(f -> f.name().equals("v_new"));
            boolean oldNameGone = schema.fields().stream()
                .noneMatch(f -> f.name().equals("v_old"));
            check("D5 rename col: 'v_new' present in Paimon schema", newNamePresent);
            check("D5 rename col: 'v_old' removed from Paimon schema", oldNameGone);

            PaimonRows rows = waitAndRead(tbl, 3);
            check("D5 rename col: 3 rows captured after RENAME", rows.total == 3);
        }
    }

    // ── D6: ALTER COLUMN TYPE ─────────────────────────────────────────────────
    // The hook runs post-execute and reads the new type OID from pg_attribute
    // (after the type has been changed by standard_ProcessUtility), then sends
    // PMSG_DDL_TYPE.  The bgworker updates the field type in the Paimon schema.
    static void testD6_alterColumnType() throws Exception {
        System.out.println("\n── D6: ALTER COLUMN TYPE ────────────────────────────");
        String tbl = "ddl_alter_type";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            createTracked(conn, tbl, "id INT PRIMARY KEY, v SMALLINT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20)");
        }
        waitForFile(new File(tableDir(tbl) + "/schema/schema-0"));
        waitForParquet(tbl, 1);
        Thread.sleep(500);

        // Verify initial type is SMALLINT
        TableSchema schemaBefore = readSchema(tbl);
        schemaBefore.fields().stream()
            .filter(f -> f.name().equals("v"))
            .findFirst()
            .ifPresent(f -> check("D6 alter type: 'v' initially SMALLINT",
                f.type().toString().toUpperCase().contains("SMALL")));

        boolean alterOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " ALTER COLUMN v TYPE INT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (3,30000)");
        } catch (Exception e) {
            alterOk = false;
            System.err.println("  ALTER/INSERT error: " + e.getMessage());
        }
        check("D6 alter type: ALTER COLUMN TYPE + INSERT no error", alterOk);

        if (alterOk) {
            boolean schemaUpdated = waitForSchemaV(tbl, 1);
            check("D6 alter type: schema-1 written by bgworker", schemaUpdated);

            TableSchema schema = readSchema(tbl);
            schema.fields().stream()
                .filter(f -> f.name().equals("v"))
                .findFirst()
                .ifPresent(f -> {
                    String typeStr = f.type().toString().toUpperCase();
                    // Must be INT (not SMALLINT or BIGINT)
                    boolean isInt = typeStr.contains("INT") && !typeStr.contains("SMALL")
                                    && !typeStr.contains("BIG");
                    check("D6 alter type: 'v' type updated to INT in Paimon schema", isInt);
                });

            PaimonRows rows = waitAndRead(tbl, 3);
            check("D6 alter type: 3 rows captured after ALTER COLUMN TYPE", rows.total == 3);
        }
    }

    // ── D7: a table without a primary key cannot be tracked ──────────────────
    // Main blocked ADD PRIMARY KEY on a paimon_heap table so that Paimon's key
    // semantics never changed mid-life.  Here a table can only be tracked if it
    // already has a key, and the key is frozen once tracked (D8).  The guardrail
    // therefore moves to the door: paimon.add_table() must refuse a key-less table,
    // and that refusal is the assertion.  The table must stay untracked and usable.
    static void testD7_addPkBlocked() throws Exception {
        System.out.println("\n── D7: key-less table refused by add_table ──────────");
        String tbl = "ddl_add_pk_blocked";
        cleanup(tbl);
        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT, v TEXT)");
        }

        boolean errorThrown = false;
        String errorMsg = "";
        try (Connection conn = connect()) {
            exec(conn, "SELECT paimon.add_table('" + tbl + "'::regclass)");
        } catch (SQLException e) {
            errorThrown = true;
            errorMsg = e.getMessage();
        }
        check("D7 no pk: add_table on a key-less table is rejected", errorThrown);
        check("D7 no pk: error message mentions paimon_slot",
              errorMsg.toLowerCase().contains("paimon_slot"));
        check("D7 no pk: error message says there is no primary key",
              errorMsg.toLowerCase().contains("no primary key"));

        long registered = -1;
        try (Connection conn = connect();
             Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT count(*) FROM paimon.table_registry WHERE relid = '" + tbl + "'::regclass")) {
            rs.next();
            registered = rs.getLong(1);
        }
        check("D7 no pk: refused table is not in the registry", registered == 0);

        // The table must still accept DML after the refused add_table
        boolean dmlOk = true;
        try (Connection conn = connect()) {
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'a'),(2,'b')");
        } catch (Exception e) {
            dmlOk = false;
            System.err.println("  Post-error DML failed: " + e.getMessage());
        }
        check("D7 no pk: DML still works after refused add_table", dmlOk);
    }

    // ── D8: DROP PRIMARY KEY blocked ──────────────────────────────────────────
    // AT_DropConstraint for the primary key is similarly blocked.  The hook
    // looks up the constraint type in pg_constraint before running the DDL.
    static void testD8_dropPkBlocked() throws Exception {
        System.out.println("\n── D8: DROP PRIMARY KEY blocked ─────────────────────");
        String tbl = "ddl_drop_pk_blocked";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            createTracked(conn, tbl, "id INT PRIMARY KEY, v TEXT");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'a')");
        }

        // Resolve the actual PK constraint name from pg_constraint
        String pkName = null;
        try (Connection conn = connect();
             Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT conname FROM pg_constraint " +
                 "WHERE conrelid = '" + tbl + "'::regclass AND contype = 'p'")) {
            if (rs.next()) pkName = rs.getString(1);
        }
        check("D8 drop pk: PK constraint found in pg_constraint", pkName != null);

        if (pkName != null) {
            boolean errorThrown = false;
            String errorMsg = "";
            try (Connection conn = connect()) {
                exec(conn, "ALTER TABLE " + tbl + " DROP CONSTRAINT " + pkName);
            } catch (SQLException e) {
                errorThrown = true;
                errorMsg = e.getMessage();
            }
            check("D8 drop pk: ALTER TABLE DROP CONSTRAINT (PK) is rejected", errorThrown);
            check("D8 drop pk: error message mentions paimon_slot",
                  errorMsg.toLowerCase().contains("paimon_slot"));

            // PK constraint must still be there
            String pkAfter = null;
            try (Connection conn = connect();
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                     "SELECT conname FROM pg_constraint " +
                     "WHERE conrelid = '" + tbl + "'::regclass AND contype = 'p'")) {
                if (rs.next()) pkAfter = rs.getString(1);
            }
            check("D8 drop pk: PK constraint still present after blocked DROP", pkAfter != null);

            // DML must still work
            boolean dmlOk = true;
            try (Connection conn = connect()) {
                exec(conn, "INSERT INTO " + tbl + " VALUES (2,'b')");
            } catch (Exception e) {
                dmlOk = false;
            }
            check("D8 drop pk: DML still works after blocked DROP", dmlOk);
        }
    }

    // ── D9: Non-paimon passthrough ────────────────────────────────────────────
    // The hook must only intercept paimon_heap tables; DDL on plain heap tables
    // must pass through untouched.  Specifically, ADD PRIMARY KEY on a heap
    // table must NOT be blocked.
    static void testD9_nonPaimonPassthrough() throws Exception {
        System.out.println("\n── D9: Non-paimon passthrough ───────────────────────");
        String tbl = "ddl_heap_passthrough";
        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            exec(conn, "CREATE TABLE " + tbl + " (id INT, v TEXT, extra INT)");
        }

        boolean addColOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " ADD COLUMN w BIGINT");
        } catch (Exception e) {
            addColOk = false;
            System.err.println("  Heap ADD COLUMN error: " + e.getMessage());
        }
        check("D9 passthrough: ADD COLUMN on heap table no error", addColOk);

        boolean dropColOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " DROP COLUMN extra");
        } catch (Exception e) {
            dropColOk = false;
            System.err.println("  Heap DROP COLUMN error: " + e.getMessage());
        }
        check("D9 passthrough: DROP COLUMN on heap table no error", dropColOk);

        boolean renameOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " RENAME COLUMN v TO renamed_v");
        } catch (Exception e) {
            renameOk = false;
            System.err.println("  Heap RENAME error: " + e.getMessage());
        }
        check("D9 passthrough: RENAME COLUMN on heap table no error", renameOk);

        // ADD PRIMARY KEY on a heap table must NOT be blocked by the hook
        boolean addPkOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " ADD PRIMARY KEY (id)");
        } catch (Exception e) {
            addPkOk = false;
            System.err.println("  Heap ADD PK error: " + e.getMessage());
        }
        check("D9 passthrough: ADD PRIMARY KEY on heap table not blocked", addPkOk);

        boolean truncateOk = true;
        try (Connection conn = connect()) {
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'r',42)");
            exec(conn, "TRUNCATE TABLE " + tbl);
        } catch (Exception e) {
            truncateOk = false;
            System.err.println("  Heap TRUNCATE error: " + e.getMessage());
        }
        check("D9 passthrough: TRUNCATE on heap table no error", truncateOk);

        // Drop constraint on heap table must not be blocked
        boolean dropPkOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " DROP CONSTRAINT " + tbl + "_pkey");
        } catch (Exception e) {
            dropPkOk = false;
            System.err.println("  Heap DROP PK error: " + e.getMessage());
        }
        check("D9 passthrough: DROP PRIMARY KEY on heap table not blocked", dropPkOk);

        try (Connection conn = connect()) {
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    static void disableMeta(Connection c) throws SQLException {
        noGuc();
    }

    static void cleanup(String tbl) {
        File dir = new File(tableDir(tbl));
        if (dir.exists()) deleteDir(dir);
    }

    static void deleteDir(File f) {
        if (f.isDirectory()) for (File c : Objects.requireNonNull(f.listFiles())) deleteDir(c);
        f.delete();
    }

    static void waitForFile(File f) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!f.exists() && System.currentTimeMillis() < deadline) Thread.sleep(500);
    }

    /**
     * Counts a table's parquet files across EVERY bucket directory. Main's C++ writer put all files
     * in bucket-0; with fixed bucketing Paimon hashes the primary key, so a table's files spread
     * over bucket-0, bucket-1, ... and a helper that looks only in bucket-0 waits forever for a
     * table whose rows hash elsewhere. How the files are found changes with the storage layout;
     * what the suites assert about their content does not.
     */
    static int countParquet(String tbl) {
        File[] buckets = new File(tableDir(tbl)).listFiles((d, n) -> n.startsWith("bucket-"));
        int total = 0;
        if (buckets != null) {
            for (File b : buckets) {
                File[] f = b.listFiles((d, n) -> n.endsWith(".parquet"));
                if (f != null) total += f.length;
            }
        }
        return total;
    }

    static void waitForParquet(String tbl, int minFiles) throws InterruptedException {
        File snapshotLatest = new File(tableDir(tbl) + "/snapshot/LATEST");
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            if (countParquet(tbl) >= minFiles && snapshotLatest.exists()) return;
            Thread.sleep(500);
        }
        throw new RuntimeException("Timed out waiting for snapshot in " + tbl);
    }

    /*
     * Wait for a specific Paimon schema version file (schema-N) to appear.
     * Returns true if found within the timeout, false on timeout.
     * The ring is FIFO: if a DML-triggered parquet is already written for rows
     * that came after the DDL message, the schema file may not have appeared yet
     * — give it a generous timeout.
     */
    static boolean waitForSchemaV(String tbl, long version) throws InterruptedException {
        File f = new File(tableDir(tbl) + "/schema/schema-" + version);
        long deadline = System.currentTimeMillis() + 30_000;
        while (!f.exists() && System.currentTimeMillis() < deadline) Thread.sleep(500);
        return f.exists();
    }

    static TableSchema readSchema(String tbl) throws Exception {
        Path tablePath = new Path(tableDir(tbl));
        return new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema for " + tbl));
    }

    static PaimonRows waitAndRead(String tbl, int minTotal) throws Exception {
        waitForParquet(tbl, 1);
        // The writer commits asynchronously: poll until the changelog is complete (the
        // assertions that follow are exact, so this only waits, it never relaxes them).
        long deadline = System.currentTimeMillis() + 60_000;
        PaimonRows rows = readRows(tbl);
        while (rows.total < minTotal && System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
            rows = readRows(tbl);
        }
        return rows;
    }

    static PaimonRows readRows(String tbl) throws Exception {
        PaimonRows result = new PaimonRows();
        try {
            Options o = new Options();
            o.set("warehouse", warehouse);
            try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
                /* Row kinds come from the $audit_log system table over a streaming scan: Paimon
                 * stores the kind as the reserved _VALUE_KIND, so a batch read (merged, or with
                 * a faked _row_kind column) cannot report it.  Column 0 is the kind as a string. */
                Table al = catalog.getTable(Identifier.create(PG_SCHEMA, tbl + "$audit_log"))
                    .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
                StreamTableScan scan = al.newReadBuilder().newStreamScan();
                scan.plan();                          // the first plan() is always empty: discard
                Long latest = ((FileStoreTable) catalog.getTable(Identifier.create(PG_SCHEMA, tbl)))
                    .snapshotManager().latestSnapshotId();
                List<Split> splits = new ArrayList<>();
                for (long i = 0; i <= (latest == null ? 0 : latest) + 1; i++) {
                    splits.addAll(scan.plan().splits());
                }
                try (RecordReader<InternalRow> reader = al.newReadBuilder().newRead().createReader(splits)) {
                    reader.forEachRemaining(r -> {
                        result.total++;
                        result.kindCounts.merge(kindOf(r.getString(0).toString()), 1L, Long::sum);
                    });
                }
            }
        } catch (Exception e) {
            System.err.println("  warn: row-kind read failed for " + tbl + ": " + e.getMessage());
        }
        return result;
    }

    /** $audit_log's rowkind string to the ordinal the assertions use: +I 0, -U 1, +U 2, -D 3. */
    static int kindOf(String k) {
        switch (k) {
            case "+I": return 0;
            case "-U": return 1;
            case "+U": return 2;
            case "-D": return 3;
            default: throw new IllegalStateException("unknown row kind " + k);
        }
    }

    static class PaimonRows {
        long total = 0;
        Map<Integer, Long> kindCounts = new HashMap<>();
        long ofKind(int k) { return kindCounts.getOrDefault(k, 0L); }
    }


    // ── Slot-architecture setup helpers (the only thing the port changes) ─────

    /** Creates a tracked table.  Replaces "USING paimon_heap" from the TAM architecture. */
    static void createTracked(Connection conn, String tbl, String columns) throws Exception {
        exec(conn, "DROP TABLE IF EXISTS " + tbl);
        exec(conn, "CREATE TABLE " + tbl + " (" + columns + ")");
        exec(conn, "SELECT paimon.add_table('" + tbl + "'::regclass)");
    }

    /** Paimon's filesystem catalog nests tables under <database>.db/. */
    /** The warehouse path of a table: <warehouse>/<postgres schema>.db/<table>. */
    static String tableDir(String schema, String tbl) {
        return warehouse + "/" + schema + ".db/" + tbl;
    }

    /** These suites create every table in PG_SCHEMA. */
    static String tableDir(String tbl) {
        return tableDir(PG_SCHEMA, tbl);
    }

    static boolean gucNoteLogged = false;

    /** paimon_heap.* GUCs no longer exist: metadata columns are writer-side configuration. */
    static void noGuc() {
        if (!gucNoteLogged) {
            gucNoteLogged = true;
            System.out.println("  note: paimon_heap.* GUCs are no-ops in the slot architecture");
        }
    }

    static String env(String name, String dflt) {
        String v = System.getenv(name);
        return v != null ? v : dflt;
    }

    static void check(String name, boolean condition) {
        if (condition) {
            System.out.printf("  PASS  %s%n", name);
            passed++;
        } else {
            System.out.printf("  FAIL  %s%n", name);
            failed++;
            failures.add(name);
        }
    }
}
