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

import java.io.File;
import java.sql.*;
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
 *   D7  ADD PRIMARY KEY    — blocked with ERROR before any execution
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

    static final String DEFAULT_WAREHOUSE = "/tmp/paimon-warehouse";
    static final String DEFAULT_JDBC =
        "jdbc:postgresql://localhost:5434/testdb?user=ubuntu&password=";

    static String warehouse;
    static String jdbcUrl;
    static LocalFileIO fileIO = LocalFileIO.create();
    static int passed = 0, failed = 0;

    public static void main(String[] args) throws Exception {
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
        System.exit(failed > 0 ? 1 : 0);
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
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v TEXT) USING paimon_heap");
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

        // Re-create the table and verify DML capture still works
        cleanup(tbl);
        boolean recreateOk = true;
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v TEXT) USING paimon_heap");
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
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v INT) USING paimon_heap");
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
            exec(conn, "CREATE TABLE " + tbl + " (id INT PRIMARY KEY, v INT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20)");
        }
        waitForFile(new File(warehouse + "/" + tbl + "/schema/schema-0"));
        waitForParquet(tbl, 1);
        Thread.sleep(500);

        boolean alterOk = true;
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " ADD COLUMN extra TEXT DEFAULT 'x'");
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
            exec(conn, "CREATE TABLE " + tbl +
                       " (id INT PRIMARY KEY, v INT, extra TEXT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10,'a'),(2,20,'b')");
        }
        waitForFile(new File(warehouse + "/" + tbl + "/schema/schema-0"));
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
            exec(conn, "CREATE TABLE " + tbl +
                       " (id INT PRIMARY KEY, v_old INT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,100),(2,200)");
        }
        waitForFile(new File(warehouse + "/" + tbl + "/schema/schema-0"));
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
            exec(conn, "CREATE TABLE " + tbl +
                       " (id INT PRIMARY KEY, v SMALLINT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,10),(2,20)");
        }
        waitForFile(new File(warehouse + "/" + tbl + "/schema/schema-0"));
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

    // ── D7: ADD PRIMARY KEY blocked ───────────────────────────────────────────
    // AT_AddConstraint with CONSTR_PRIMARY is rejected before execution so that
    // Paimon table ordering / primary-key semantics are never changed mid-life.
    // The hook ERRORs out; the table must remain fully functional afterwards.
    static void testD7_addPkBlocked() throws Exception {
        System.out.println("\n── D7: ADD PRIMARY KEY blocked ──────────────────────");
        String tbl = "ddl_add_pk_blocked";
        cleanup(tbl);
        try (Connection conn = connect()) {
            disableMeta(conn);
            exec(conn, "DROP TABLE IF EXISTS " + tbl);
            // Create WITHOUT a primary key so we can attempt to add one
            exec(conn, "CREATE TABLE " + tbl + " (id INT, v TEXT) USING paimon_heap");
            exec(conn, "INSERT INTO " + tbl + " VALUES (1,'a')");
        }

        boolean errorThrown = false;
        String errorMsg = "";
        try (Connection conn = connect()) {
            exec(conn, "ALTER TABLE " + tbl + " ADD PRIMARY KEY (id)");
        } catch (SQLException e) {
            errorThrown = true;
            errorMsg = e.getMessage();
        }
        check("D7 add pk: ALTER TABLE ADD PRIMARY KEY is rejected", errorThrown);
        check("D7 add pk: error message mentions paimon_heap",
              errorMsg.toLowerCase().contains("paimon_heap"));

        // Table must still accept DML after the failed ALTER
        boolean dmlOk = true;
        try (Connection conn = connect()) {
            exec(conn, "INSERT INTO " + tbl + " VALUES (2,'b')");
        } catch (Exception e) {
            dmlOk = false;
            System.err.println("  Post-error DML failed: " + e.getMessage());
        }
        check("D7 add pk: DML still works after blocked ALTER", dmlOk);
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
            exec(conn, "CREATE TABLE " + tbl +
                       " (id INT PRIMARY KEY, v TEXT) USING paimon_heap");
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
            check("D8 drop pk: error message mentions paimon_heap",
                  errorMsg.toLowerCase().contains("paimon_heap"));

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
        exec(c, "SET paimon_heap.meta_field_xid = ''");
        exec(c, "SET paimon_heap.meta_field_op  = ''");
        exec(c, "SET paimon_heap.meta_field_tid = ''");
    }

    static void cleanup(String tbl) {
        File dir = new File(warehouse + "/" + tbl);
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

    static void waitForParquet(String tbl, int minFiles) throws InterruptedException {
        File bucketDir      = new File(warehouse + "/" + tbl + "/bucket-0");
        File snapshotLatest = new File(warehouse + "/" + tbl + "/snapshot/LATEST");
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            File[] f = bucketDir.listFiles((d, n) -> n.endsWith(".parquet"));
            if (f != null && f.length >= minFiles && snapshotLatest.exists()) return;
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
        File f = new File(warehouse + "/" + tbl + "/schema/schema-" + version);
        long deadline = System.currentTimeMillis() + 30_000;
        while (!f.exists() && System.currentTimeMillis() < deadline) Thread.sleep(500);
        return f.exists();
    }

    static TableSchema readSchema(String tbl) throws Exception {
        Path tablePath = new Path(warehouse + "/" + tbl);
        return new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema for " + tbl));
    }

    static PaimonRows waitAndRead(String tbl, int minTotal) throws Exception {
        waitForParquet(tbl, 1);
        Thread.sleep(1500);
        return readRows(tbl);
    }

    static PaimonRows readRows(String tbl) throws Exception {
        Path tablePath = new Path(warehouse + "/" + tbl);

        TableSchema stored = new SchemaManager(fileIO, tablePath).latest()
            .orElseThrow(() -> new RuntimeException("No schema for " + tbl));
        Map<String, String> opts = new HashMap<>(stored.options());
        opts.put("bucket", "-1");
        opts.remove("merge-engine");
        opts.remove("changelog-producer");
        opts.remove("sequence.field");

        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(-1, "_row_kind", DataTypes.TINYINT().notNull()));
        fields.addAll(stored.fields());

        TableSchema schema = new TableSchema(
            stored.id(), fields, stored.highestFieldId(),
            stored.partitionKeys(), stored.primaryKeys(), opts, stored.comment());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, schema);

        ReadBuilder rb = table.newReadBuilder();
        List<Split> splits = rb.newScan().plan().splits();

        PaimonRows result = new PaimonRows();
        try (RecordReader<InternalRow> reader = rb.newRead().createReader(splits)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow r;
                while ((r = batch.next()) != null) {
                    result.total++;
                    if (!r.isNullAt(0)) result.kindCounts.merge((int) r.getByte(0), 1L, Long::sum);
                }
                batch.releaseBatch();
            }
        } catch (Exception e) {
            System.err.println("  warn: row read failed for " + tbl + ": " + e.getMessage());
        }
        return result;
    }

    static class PaimonRows {
        long total = 0;
        Map<Integer, Long> kindCounts = new HashMap<>();
        long ofKind(int k) { return kindCounts.getOrDefault(k, 0L); }
    }

    static void check(String name, boolean condition) {
        if (condition) {
            System.out.printf("  PASS  %s%n", name);
            passed++;
        } else {
            System.out.printf("  FAIL  %s%n", name);
            failed++;
        }
    }
}
