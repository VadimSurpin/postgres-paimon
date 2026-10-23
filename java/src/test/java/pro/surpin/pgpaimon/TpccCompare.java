// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * End-to-end verification that PostgreSQL and a Paimon warehouse hold the same TPC-C state after a
 * BenchBase run replicated through {@code paimon_slot} + {@code paimon-writer}.
 *
 * <p>This is the <em>verdict</em> of {@code test/tpcc/run_tpcc.sh} and of
 * {@code .github/workflows/tpcc.yml}: a non-zero exit means PostgreSQL and Paimon disagree.
 *
 * <h2>Why it compares what it compares</h2>
 *
 * <p><b>Full table contents, not row counts.</b> A row count agrees with PostgreSQL in exactly the
 * case this project kept finding and fixing: a merged table that looks right while the changelog
 * that produced it is wrong. Every column of every row is compared, so a wrong value, a lost
 * update or a stale before-image shows up.
 *
 * <p><b>Both sides canonicalised through the Paimon type.</b> The Paimon column type drives both
 * decoders, so {@code numeric(12,2)} is compared as a scale-2 decimal on both sides rather than as
 * two differently formatted strings. Nothing is compared as a double unless it is a double in both.
 *
 * <p><b>The table with no key.</b> {@code history} has no primary key in the stock TPC-C schema, so
 * it is replicated as an operation log (spec 2026-10-14): its Paimon table has no key and one row
 * per operation. With no key to compare by, {@link #compareLog} compares it as a multiset: the
 * rows PostgreSQL holds, each as many times as it holds it, against the state the log's history
 * rebuilds, and checks the log row for row against that history.
 *
 * <p><b>The history of every table.</b> Every replicated table {@code T} also has
 * {@code T_hist} (plus an archive {@code T_hist_<lsn>} per DDL, none in TPC-C), partitioned by
 * the UTC date {@code _dt}, one row per row image: {@code +I}, {@code -U} then {@code +U},
 * {@code -D}. {@link #checkHistory} asserts it holds exactly the changes PostgreSQL made: its
 * +I/-U/+U/-D counts equal the changelog's (TPC-C never moves a key), every -U is followed by its
 * +U, every image is complete, every row sits in its commit's UTC date, no change is in it twice,
 * every {@code _ts} and {@code _xid} lies inside the run (bounds {@code run_tpcc.sh} takes before
 * the writer starts and after it drains) and is one value per transaction, every {@code -U} and
 * {@code -D} carries exactly the row as the replay holds it (the before-image), and -- the
 * strongest form -- replaying it from empty, in commit order, rebuilds exactly PostgreSQL's final
 * table. TpccCompareSelfTest proves each of these fails on a tampered history.
 *
 * <p><b>The changelog, not only the merged state.</b> A merged read reports {@code INSERT} for
 * every surviving row — it physically cannot report a row kind — so the row kinds come from the
 * {@code $audit_log} system table over a streaming scan, exactly as
 * {@code java/writer/src/test/java/pro/surpin/pgpaimon/writer/RawRead.java} does (the first
 * {@code plan()} is always empty and is discarded). TPC-C is update-heavy, and real {@code -U}
 * before-images are the whole reason this architecture pairs {@code REPLICA IDENTITY FULL} with
 * {@code changelog-producer = input}: a writer that silently dropped them would still produce a
 * correct merged table. Three changelog invariants are asserted per table:
 *
 * <ul>
 *   <li>{@code -U} count equals {@code +U} count — every update carries its before-image;</li>
 *   <li>{@code +I} minus {@code -D} equals the surviving row count — TPC-C never moves a primary
 *       key and never re-inserts a deleted one, so the changelog must account for the whole
 *       population;</li>
 *   <li>{@code -U > 0} for every table TPC-C updates (and {@code -D > 0} for {@code new_order}
 *       once Delivery has run, which is detected from PostgreSQL's own state rather than
 *       assumed).</li>
 * </ul>
 *
 * <h2>Warehouse layout</h2>
 *
 * <p>One Paimon database per PostgreSQL schema: a table lives at
 * {@code <warehouse>/<schema>.db/<table>}. BenchBase creates TPC-C in {@code public}, so the
 * tables are read through {@code Identifier.create("public", "warehouse")} and friends. The schema
 * is a parameter, never hard-coded into a path.
 *
 * <h2>Usage</h2>
 *
 * <pre>
 *   java -cp java/target/paimon-reader-2.0.0.jar pro.surpin.pgpaimon.TpccCompare \
 *        [warehouse] [jdbc-url] [pg-schema]
 * </pre>
 *
 * Environment: {@code PGPAIMON_IT_WAREHOUSE}, {@code PGPAIMON_IT_JDBC_URL}, {@code TPCC_SCHEMA},
 * {@code TPCC_TABLES} (comma-separated override of the table list), {@code TPCC_MAX_DIFFS}
 * (differing rows reported per table, default 10), {@code TPCC_META_FIELDS} (comma-separated
 * writer metadata columns to expect in Paimon and exclude from the comparison, default
 * {@code _seq,_op_seq,_xid}), {@code TPCC_TS_MIN}/{@code TPCC_TS_MAX} (the run's commit-time
 * window, microseconds since the epoch) and {@code TPCC_XID_MIN}/{@code TPCC_XID_MAX} (its 32-bit
 * transaction ids); without the four the history checks fail rather than pass unchecked.
 */
public final class TpccCompare {

    /**
     * The eight TPC-C tables this architecture replicates.
     *
     * <p>{@code history}, which has no key, is compared separately: see {@link #LOG_TABLES}.
     */
    static final String[] DEFAULT_TABLES = {
        "warehouse", "district", "customer",
        "item", "stock", "oorder", "new_order", "order_line"
    };

    /**
     * TPC-C tables with no primary key, replicated as operation logs. {@code history} has two
     * foreign keys and nothing else; Payment inserts into it throughout the run. The stock
     * BenchBase schema is left exactly as it ships.
     */
    static final String[] LOG_TABLES = {"history"};

    /** The metadata columns of an operation log and of every history table, in their order. */
    static final List<String> LOG_META =
        List.of("_op", "_commit_lsn", "_lsn", "_op_seq", "_xid", "_ts");

    /** The metadata columns of every history table: the log's plus the partition date. */
    static final List<String> HIST_META =
        List.of("_op", "_commit_lsn", "_lsn", "_op_seq", "_xid", "_ts", "_dt");

    /**
     * Tables TPC-C always updates, so a run with any Payment or NewOrder transaction must leave
     * {@code -U} records on each. Payment updates warehouse, district and customer; NewOrder
     * updates district (d_next_o_id) and stock. Delivery's targets are handled separately,
     * because Delivery is 4% of the mix and a short window could in principle miss it.
     */
    static final Set<String> MUST_HAVE_UPDATES =
        new LinkedHashSet<>(Arrays.asList("warehouse", "district", "customer", "stock"));

    /** Columns the writer adds that have no PostgreSQL counterpart. */
    static final String[] DEFAULT_META_FIELDS = {"_seq", "_op_seq", "_xid"};

    /**
     * A fixed six-digit timestamp rendering. {@code LocalDateTime.toString()} drops trailing zero
     * groups, which is consistent across both sides but makes a diff harder to read; this does not.
     */
    static final DateTimeFormatter TS =
        DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");

    /** Field separator inside a canonicalised row. Not producible by any renderer below. */
    static final char SEP = '\u0001';

    /** The rendering of SQL NULL. Also not producible by any renderer below. */
    static final String NULL = "\u0002null";

    private TpccCompare() {}

    // ─────────────────────────────────────────────────────────────────────────

    /** One table's verdict, so the summary can be printed after every table has been read. */
    record TableResult(String table, long pgRows, long paimonRows,
                       long missing, long extra, long mismatched,
                       long insert, long updBefore, long updAfter, long delete,
                       List<String> failures, long[] hist) {
        TableResult(String table, long pgRows, long paimonRows, long missing, long extra,
                    long mismatched, long insert, long updBefore, long updAfter, long delete,
                    List<String> failures) {
            this(table, pgRows, paimonRows, missing, extra, mismatched, insert, updBefore,
                 updAfter, delete, failures, null);
        }
        boolean ok() { return failures.isEmpty(); }
    }

    public static void main(String[] args) throws Exception {
        // The reader jar is shaded without a ServicesResourceTransformer, so the driver's
        // META-INF/services/java.sql.Driver does not survive and DriverManager finds nothing.
        // Registering it by name is the fix that does not change what the jar means for anything
        // else that uses it.
        Class.forName("org.postgresql.Driver");

        String warehouse = args.length > 0 ? args[0]
            : env("PGPAIMON_IT_WAREHOUSE", "/tmp/paimon-warehouse");
        String jdbcUrl = args.length > 1 ? args[1]
            : env("PGPAIMON_IT_JDBC_URL", "jdbc:postgresql://localhost:5443/benchbase?user=postgres");
        String pgSchema = args.length > 2 ? args[2] : env("TPCC_SCHEMA", "public");

        String[] tables = env("TPCC_TABLES", "").isEmpty()
            ? DEFAULT_TABLES : env("TPCC_TABLES", "").split(",");
        String[] metaFields = env("TPCC_META_FIELDS", String.join(",", DEFAULT_META_FIELDS))
            .split(",");
        int maxDiffs = Integer.parseInt(env("TPCC_MAX_DIFFS", "10"));
        bounds = Bounds.fromEnv();

        System.out.println("TPC-C state comparison: PostgreSQL vs Paimon");
        System.out.println("  warehouse   : " + warehouse);
        System.out.println("  jdbc        : " + jdbcUrl.replaceAll("password=[^&]*", "password=***"));
        System.out.println("  pg schema   : " + pgSchema
            + "  (Paimon database; tables at <warehouse>/" + pgSchema + ".db/<table>)");
        System.out.println("  tables      : " + String.join(", ", tables));
        System.out.println("  meta fields : " + String.join(", ", metaFields) + " (excluded)");
        System.out.println("  run window  : " + (bounds == null ? "NOT GIVEN (fails the history checks)"
            : "_ts in [" + bounds.tsMin() + ", " + bounds.tsMax() + "] us, _xid in ["
              + bounds.xidMin() + ", " + bounds.xidMax() + "]"));
        System.out.println();

        List<TableResult> results = new ArrayList<>();
        Options o = new Options();
        o.set("warehouse", warehouse);

        try (Connection pg = DriverManager.getConnection(jdbcUrl);
             Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o))) {
            pg.setAutoCommit(false);          // required for a streaming (fetch-size) read
            boolean deliveryRan = deliveryRan(pg, pgSchema);
            System.out.println("Delivery transactions ran: " + deliveryRan
                + "  (orders beyond the loaded range carry a carrier id)");
            System.out.println();

            for (String table : tables) {
                System.out.println("── " + table + " ──");
                results.add(compareTable(pg, catalog, pgSchema, table.trim(),
                                         metaFields, maxDiffs, deliveryRan));
                System.out.println();
            }
            for (String table : LOG_TABLES) {
                System.out.println("── " + table + " (no primary key: an operation log) ──");
                results.add(compareLog(pg, catalog, pgSchema, table, maxDiffs));
                System.out.println();
            }
        }

        printSummary(results);

        boolean allOk = results.stream().allMatch(TableResult::ok);
        System.out.println();
        System.out.println(allOk
            ? "RESULT: PASS -- PostgreSQL and Paimon agree on every row of every table, "
              + "the changelog accounts for every change, and every history table holds "
              + "exactly the changes PostgreSQL made."
            : "RESULT: FAIL -- see the failures listed above.");
        System.exit(allOk ? 0 : 1);
    }

    // ── one table ───────────────────────────────────────────────────────────

    static TableResult compareTable(Connection pg, Catalog catalog, String pgSchema, String table,
                                    String[] metaFields, int maxDiffs, boolean deliveryRan)
            throws Exception {
        List<String> failures = new ArrayList<>();
        String qualified = quote(pgSchema) + "." + quote(table);

        List<String> pgCols = pgColumns(pg, qualified);
        List<String> pgPk = pgPrimaryKey(pg, qualified);
        if (pgPk.isEmpty()) {
            // Not a comparison failure but a setup failure: paimon_slot cannot track such a table,
            // so reaching here means the table was never registered.
            failures.add("PostgreSQL table has no primary key, so it cannot be tracked at all");
            System.out.println("  FAIL: no primary key in PostgreSQL");
            return new TableResult(table, -1, -1, 0, 0, 0, 0, 0, 0, 0, failures);
        }
        System.out.println("  pk          : " + String.join(", ", pgPk)
            + (pgPk.size() > 1 ? "   (composite, " + pgPk.size() + " columns)" : ""));

        Table paimon = catalog.getTable(Identifier.create(pgSchema, table));
        RowType rowType = paimon.rowType();

        // ── schema agreement, before any row is read ───────────────────────
        List<String> paimonNames = rowType.getFieldNames();
        List<String> expected = new ArrayList<>(pgCols);
        for (String m : metaFields) {
            if (paimonNames.contains(m.trim())) {
                expected.add(m.trim());
            }
        }
        if (!paimonNames.equals(expected)) {
            failures.add("Paimon columns " + paimonNames + " != PostgreSQL columns "
                + pgCols + " plus metadata");
            System.out.println("  FAIL: schema mismatch");
            System.out.println("        paimon    : " + paimonNames);
            System.out.println("        postgres  : " + pgCols);
        }
        List<String> paimonPk = paimon.primaryKeys();
        if (!paimonPk.equals(pgPk)) {
            failures.add("Paimon primary key " + paimonPk + " != PostgreSQL primary key " + pgPk);
            System.out.println("  FAIL: primary key mismatch: paimon " + paimonPk
                + " vs postgres " + pgPk);
        }

        // Positions and types of the PostgreSQL columns inside the Paimon row, by name.
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < paimonNames.size(); i++) {
            idx.put(paimonNames.get(i), i);
        }
        List<DataField> fields = rowType.getFields();
        int[] cols = new int[pgCols.size()];
        DataType[] types = new DataType[pgCols.size()];
        for (int i = 0; i < pgCols.size(); i++) {
            Integer p = idx.get(pgCols.get(i));
            if (p == null) {
                failures.add("PostgreSQL column " + pgCols.get(i) + " is absent from Paimon");
                System.out.println("  FAIL: column " + pgCols.get(i) + " missing from Paimon");
                return new TableResult(table, -1, -1, 0, 0, 0, 0, 0, 0, 0, failures);
            }
            cols[i] = p;
            types[i] = fields.get(p).type();
        }
        int[] pkPos = new int[pgPk.size()];
        for (int i = 0; i < pgPk.size(); i++) {
            pkPos[i] = pgCols.indexOf(pgPk.get(i));
        }

        // ── the merged Paimon state, keyed by primary key ───────────────────
        //
        // Held in memory: this is a correctness check at a small scale factor, not a benchmark.
        // At one warehouse the biggest table (order_line) is ~300k rows, a few hundred MB of
        // strings; run with -Xmx2g or more. The full row is kept, not a hash, so a difference can
        // be reported with both sides' values.
        Map<String, String> paimonRows = new HashMap<>();
        long duplicateKeys = 0;
        ReadBuilder rb = paimon.newReadBuilder();
        try (RecordReader<InternalRow> reader = rb.newRead().createReader(rb.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow r;
                while ((r = batch.next()) != null) {
                    String[] vals = new String[cols.length];
                    for (int i = 0; i < cols.length; i++) {
                        vals[i] = paimonValue(r, cols[i], types[i]);
                    }
                    String key = key(vals, pkPos);
                    if (paimonRows.put(key, String.join(String.valueOf(SEP), vals)) != null) {
                        duplicateKeys++;
                    }
                }
                batch.releaseBatch();
            }
        }
        long paimonCount = paimonRows.size();
        if (duplicateKeys > 0) {
            // A merged primary-key read must surface each key once. More than once would mean the
            // key is not actually the key, which would invalidate the whole comparison.
            failures.add(duplicateKeys + " duplicate primary key(s) in the merged Paimon read");
            System.out.println("  FAIL: " + duplicateKeys
                + " duplicate primary keys in the merged Paimon read");
        }

        // ── PostgreSQL, ordered by primary key so reported diffs are stable ─
        long pgCount = 0, missing = 0, mismatched = 0, reported = 0;
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < pgCols.size(); i++) {
            sql.append(i > 0 ? ", " : "").append(quote(pgCols.get(i)));
        }
        sql.append(" FROM ").append(qualified).append(" ORDER BY ");
        for (int i = 0; i < pgPk.size(); i++) {
            sql.append(i > 0 ? ", " : "").append(quote(pgPk.get(i)));
        }
        try (PreparedStatement st = pg.prepareStatement(sql.toString())) {
            st.setFetchSize(10_000);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    pgCount++;
                    String[] vals = new String[pgCols.size()];
                    for (int i = 0; i < pgCols.size(); i++) {
                        vals[i] = pgValue(rs, i + 1, types[i]);
                    }
                    String key = key(vals, pkPos);
                    String got = paimonRows.remove(key);
                    if (got == null) {
                        missing++;
                        if (reported < maxDiffs) {
                            reported++;
                            System.out.println("  DIFF missing from Paimon: " + show(key));
                            System.out.println("        postgres: " + render(pgCols, vals));
                        }
                        continue;
                    }
                    String[] other = got.split(String.valueOf(SEP), -1);
                    if (!Arrays.equals(vals, other)) {
                        mismatched++;
                        if (reported < maxDiffs) {
                            reported++;
                            System.out.println("  DIFF values differ, key " + show(key));
                            for (int i = 0; i < vals.length; i++) {
                                if (!vals[i].equals(other[i])) {
                                    System.out.println("        " + pgCols.get(i)
                                        + ": postgres=" + show(vals[i])
                                        + "  paimon=" + show(other[i]));
                                }
                            }
                        }
                    }
                }
            }
        }
        long extra = paimonRows.size();
        if (extra > 0) {
            // Deterministic order for the report: the leftover keys, sorted.
            Map<String, String> sorted = new TreeMap<>(paimonRows);
            long shown = 0;
            for (Map.Entry<String, String> e : sorted.entrySet()) {
                if (shown++ >= maxDiffs) {
                    break;
                }
                System.out.println("  DIFF present in Paimon, absent from PostgreSQL: "
                    + show(e.getKey()));
                System.out.println("        paimon: "
                    + render(pgCols, e.getValue().split(String.valueOf(SEP), -1)));
            }
        }

        System.out.printf("  rows        : postgres %d, paimon %d%n", pgCount, paimonCount);
        if (missing > 0) {
            failures.add(missing + " row(s) in PostgreSQL are absent from Paimon");
        }
        if (extra > 0) {
            failures.add(extra + " row(s) in Paimon are absent from PostgreSQL");
        }
        if (mismatched > 0) {
            failures.add(mismatched + " row(s) differ in at least one column");
        }

        // ── the changelog ──────────────────────────────────────────────────
        long[] kinds = auditLogKinds(catalog, pgSchema, table);
        long ins = kinds[0], ubf = kinds[1], uaf = kinds[2], del = kinds[3];
        System.out.printf("  changelog   : +I %d, -U %d, +U %d, -D %d  (over %d snapshot(s))%n",
                          ins, ubf, uaf, del, kinds[4]);

        if (ubf != uaf) {
            failures.add("changelog has " + ubf + " -U but " + uaf
                + " +U records: an update lost its before-image");
            System.out.println("  FAIL: -U (" + ubf + ") != +U (" + uaf + ")");
        }
        if (ins - del != pgCount) {
            // No TPC-C transaction moves a primary key or re-inserts a deleted one, so inserts
            // minus deletes is exactly the surviving population. A mismatch means the changelog
            // does not account for the merged table it produced.
            failures.add("changelog +I - -D = " + (ins - del) + " but PostgreSQL holds "
                + pgCount + " row(s)");
            System.out.println("  FAIL: +I - -D = " + (ins - del) + " != " + pgCount + " rows");
        }
        if (MUST_HAVE_UPDATES.contains(table) && ubf == 0) {
            failures.add("TPC-C updates " + table + " on every Payment or NewOrder, "
                + "but the changelog holds no -U record");
            System.out.println("  FAIL: no -U record for a table TPC-C always updates");
        }
        if (deliveryRan && "oorder".equals(table) && ubf == 0) {
            failures.add("Delivery ran and sets oorder.o_carrier_id, "
                + "but the changelog holds no -U record for oorder");
            System.out.println("  FAIL: Delivery ran but oorder has no -U record");
        }
        if (deliveryRan && "order_line".equals(table) && ubf == 0) {
            failures.add("Delivery ran and sets order_line.ol_delivery_d, "
                + "but the changelog holds no -U record for order_line");
            System.out.println("  FAIL: Delivery ran but order_line has no -U record");
        }
        if (deliveryRan && "new_order".equals(table) && del == 0) {
            failures.add("Delivery ran and deletes new_order rows, "
                + "but the changelog holds no -D record for new_order");
            System.out.println("  FAIL: Delivery ran but new_order has no -D record");
        }

        // ── the history table ───────────────────────────────────────────────
        long[] hist = checkHistory(pg, catalog, pgSchema, table, pgCols, types, pkPos,
                                   notNullColumns(pg, qualified), maxDiffs, failures);
        if (hist != null) {
            // TPC-C never moves a key, so the history's row kinds are the changelog's, one for one.
            if (hist[H_PI] != ins || hist[H_MU] != ubf || hist[H_PU] != uaf || hist[H_MD] != del) {
                failures.add("history has +I " + hist[H_PI] + ", -U " + hist[H_MU] + ", +U "
                    + hist[H_PU] + ", -D " + hist[H_MD] + " but the changelog has +I " + ins
                    + ", -U " + ubf + ", +U " + uaf + ", -D " + del);
                System.out.println("  FAIL: history counts do not match the changelog");
            }
        }

        System.out.println(failures.isEmpty() ? "  OK" : "  FAILED: " + failures.size() + " check(s)");
        return new TableResult(table, pgCount, paimonCount, missing, extra, mismatched,
                               ins, ubf, uaf, del, failures, hist);
    }

    // ── history tables ──────────────────────────────────────────────────────

    /** One history row: its order, its operation, its row image canonicalised (null for T). */
    record HistRow(long commitLsn, int opSeq, long lsn, long xid, String op, String[] image,
                   long tsMicros, int dt) {}

    /** Counts returned by {@link #checkHistory}, by index. */
    static final int H_PI = 0, H_MU = 1, H_PU = 2, H_MD = 3, H_T = 4, H_R = 5, H_ROWS = 6,
                     H_TABLES = 7;

    /**
     * Reads {@code <table>_hist} and every archive {@code <table>_hist_<lsn>} a DDL left (in name
     * order, which is chronological), and asserts they hold exactly the changes PostgreSQL made.
     * Returns the counts {+I, -U, +U, -D, T, R, rows, tables}, or null when nothing can be read.
     *
     * <ul>
     *   <li>each table's columns are the table's columns, then the seven metadata columns, it has
     *       no key and is partitioned by {@code _dt};</li>
     *   <li>no (commit LSN, op_seq) occurs twice: a change in it twice is a replay that leaked;</li>
     *   <li>every row but a T carries its image (every NOT NULL column set), and every -U is
     *       followed at once by its +U with the same commit LSN, change LSN and xid;</li>
     *   <li>{@code _dt} is the UTC date of {@code _ts} on every row;</li>
     *   <li>replayed from empty in commit order -- +I and R add, -U removes, +U adds, -D removes,
     *       T empties -- it rebuilds exactly PostgreSQL's table: by key for a table with one, as a
     *       multiset of whole rows for one without. Every PostgreSQL row is compared.</li>
     * </ul>
     */
    static long[] checkHistory(Connection pg, Catalog catalog, String pgSchema, String table,
                               List<String> pgCols, DataType[] types, int[] pkPos,
                               Set<String> notNull, int maxDiffs, List<String> failures)
            throws Exception {
        String histName = table + "_hist";
        List<String> generations = new ArrayList<>();
        for (String n : catalog.listTables(pgSchema)) {
            if (n.matches(java.util.regex.Pattern.quote(histName + "_") + "[0-9A-F]{16}")) {
                generations.add(n);
            }
        }
        java.util.Collections.sort(generations);
        generations.add(histName);
        List<HistRow> rows = new ArrayList<>();
        try {
            for (String g : generations) {
                if (!readHistory(catalog, pgSchema, g, pgCols, types, failures, rows)) {
                    return null;
                }
            }
        } catch (Catalog.TableNotExistException e) {
            failures.add("has no history table " + histName);
            System.out.println("  FAIL: no history table " + histName);
            return null;
        }
        // PostgreSQL's rows, canonicalised as the history's images are.
        List<String[]> pgRows = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < pgCols.size(); i++) {
            sql.append(i > 0 ? ", " : "").append(quote(pgCols.get(i)));
        }
        sql.append(" FROM ").append(quote(pgSchema)).append('.').append(quote(table));
        try (PreparedStatement st = pg.prepareStatement(sql.toString())) {
            st.setFetchSize(10_000);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    String[] vals = new String[pgCols.size()];
                    for (int i = 0; i < vals.length; i++) {
                        vals[i] = pgValue(rs, i + 1, types[i]);
                    }
                    pgRows.add(vals);
                }
            }
        }
        long[] counts = verifyHistory(rows, pgRows, pgCols, pkPos, notNull, bounds, maxDiffs,
                                      failures);
        counts[H_TABLES] = generations.size();
        return counts;
    }

    /**
     * The window the history's metadata must fall in: the commit times ({@code _ts}, microseconds
     * since the Unix epoch) and transaction ids ({@code _xid}, 32-bit as pgoutput sends them) of
     * the run, taken by {@code run_tpcc.sh} before the writer starts and after it has drained
     * ({@code TPCC_TS_MIN}, {@code TPCC_TS_MAX}, {@code TPCC_XID_MIN}, {@code TPCC_XID_MAX}).
     * Without them a history whose every {@code _ts} was a day late, or whose every {@code _xid}
     * was 0, passed (review of the history tables, finding I1).
     */
    record Bounds(long tsMin, long tsMax, long xidMin, long xidMax) {
        static Bounds fromEnv() {
            String a = env("TPCC_TS_MIN", ""), b = env("TPCC_TS_MAX", ""),
                   c = env("TPCC_XID_MIN", ""), d = env("TPCC_XID_MAX", "");
            if (a.isEmpty() || b.isEmpty() || c.isEmpty() || d.isEmpty()) {
                return null;
            }
            return new Bounds(Long.parseLong(a.trim()), Long.parseLong(b.trim()),
                              Long.parseLong(c.trim()) & 0xFFFFFFFFL,
                              Long.parseLong(d.trim()) & 0xFFFFFFFFL);
        }

        boolean tsOk(long micros) {
            return micros >= tsMin && micros <= tsMax;
        }

        /** 32-bit transaction ids, allowing for one wraparound between the two ends. */
        boolean xidOk(long xid) {
            long x = xid & 0xFFFFFFFFL;
            if (x != xid || x < 3) {
                return false;      // not a 32-bit xid, or a special one (0 invalid, 1-2 frozen)
            }
            return xidMin <= xidMax ? x >= xidMin && x <= xidMax : x >= xidMin || x <= xidMax;
        }
    }

    /** Set by {@link #main} from the environment; null when not given (which fails the run). */
    static Bounds bounds;

    /**
     * The checks of {@link #checkHistory} on rows already read: pure, so that the comparator can
     * be tested against tampered histories without a warehouse (TpccCompareSelfTest).
     */
    static long[] verifyHistory(List<HistRow> unsorted, List<String[]> pgRows, List<String> pgCols,
                                int[] pkPos, Set<String> notNull, Bounds window, int maxDiffs,
                                List<String> failures) {
        List<HistRow> rows = new ArrayList<>(unsorted);
        rows.sort((x, y) -> x.commitLsn() != y.commitLsn()
            ? Long.compare(x.commitLsn(), y.commitLsn()) : Integer.compare(x.opSeq(), y.opSeq()));
        long[] counts = new long[8];
        Set<String> seen = new java.util.HashSet<>();
        long dupes = 0, badImages = 0, unpaired = 0, badDates = 0, badTs = 0, badXid = 0,
             splitTxn = 0;
        Map<Long, long[]> perCommit = new HashMap<>();     // commit LSN -> {xid, ts}
        for (int i = 0; i < rows.size(); i++) {
            HistRow r = rows.get(i);
            switch (r.op()) {
                case "+I" -> counts[H_PI]++;
                case "-U" -> counts[H_MU]++;
                case "+U" -> counts[H_PU]++;
                case "-D" -> counts[H_MD]++;
                case "T" -> counts[H_T]++;
                case "R" -> counts[H_R]++;
                default -> badImages++;
            }
            if (!seen.add(r.commitLsn() + "/" + r.opSeq())) {
                dupes++;
            }
            boolean ok = "T".equals(r.op()) ? r.image() == null : image(r.image(), pgCols, notNull);
            if (!ok && badImages++ < maxDiffs) {
                System.out.println("  DIFF history " + r.op() + " at " + r.commitLsn() + "/"
                    + r.opSeq() + " has the wrong image: "
                    + (r.image() == null ? "none" : render(pgCols, r.image())));
            }
            if ("-U".equals(r.op())) {
                HistRow next = i + 1 < rows.size() ? rows.get(i + 1) : null;
                if (next == null || !"+U".equals(next.op()) || next.commitLsn() != r.commitLsn()
                    || next.lsn() != r.lsn() || next.xid() != r.xid()) {
                    unpaired++;
                }
            } else if ("+U".equals(r.op())) {
                HistRow prev = i > 0 ? rows.get(i - 1) : null;
                if (prev == null || !"-U".equals(prev.op()) || prev.lsn() != r.lsn()) {
                    unpaired++;
                }
            }
            if (r.dt() != Math.floorDiv(r.tsMicros(), 86_400_000_000L)) {
                badDates++;
            }
            // _ts and _xid: inside the run's window, and one value per transaction.
            if (window != null && !window.tsOk(r.tsMicros()) && badTs++ < maxDiffs) {
                System.out.println("  DIFF history " + r.op() + " at " + r.commitLsn() + "/"
                    + r.opSeq() + " has _ts " + r.tsMicros() + " outside the run ["
                    + window.tsMin() + ", " + window.tsMax() + "]");
            }
            // An R row (initial load) is made by no transaction: its _xid is 0 by design.
            boolean xidOk = "R".equals(r.op()) ? r.xid() == 0 : window == null || window.xidOk(r.xid());
            if (window != null && !xidOk && badXid++ < maxDiffs) {
                System.out.println("  DIFF history " + r.op() + " at " + r.commitLsn() + "/"
                    + r.opSeq() + " has _xid " + r.xid() + " outside the run ["
                    + window.xidMin() + ", " + window.xidMax() + "]");
            }
            long[] first = perCommit.putIfAbsent(r.commitLsn(), new long[] {r.xid(), r.tsMicros()});
            if (first != null && (first[0] != r.xid() || first[1] != r.tsMicros())) {
                splitTxn++;
            }
        }
        counts[H_ROWS] = rows.size();
        System.out.printf("  history     : +I %d, -U %d, +U %d, -D %d, T %d, R %d  (%d rows)%n",
                          counts[H_PI], counts[H_MU], counts[H_PU], counts[H_MD], counts[H_T],
                          counts[H_R], counts[H_ROWS]);
        if (dupes > 0) {
            failures.add("history holds " + dupes + " row(s) twice (same commit LSN and op_seq)");
            System.out.println("  FAIL: " + dupes + " duplicated history rows");
        }
        if (badImages > 0) {
            failures.add(badImages + " history row(s) lack the image they must carry");
            System.out.println("  FAIL: " + badImages + " history rows with wrong images");
        }
        if (unpaired > 0) {
            failures.add(unpaired + " -U/+U row(s) not paired with their other half");
            System.out.println("  FAIL: " + unpaired + " unpaired -U/+U rows");
        }
        if (badDates > 0) {
            failures.add(badDates + " history row(s) whose _dt is not the UTC date of _ts");
            System.out.println("  FAIL: " + badDates + " rows in the wrong _dt partition");
        }
        if (window == null) {
            failures.add("no run window given (TPCC_TS_MIN, TPCC_TS_MAX, TPCC_XID_MIN,"
                + " TPCC_XID_MAX), so _ts and _xid were not checked");
            System.out.println("  FAIL: _ts and _xid not checked: no run window in the environment");
        }
        if (badTs > 0) {
            failures.add(badTs + " history row(s) whose _ts is outside the run's commit times");
            System.out.println("  FAIL: " + badTs + " rows with a _ts outside the run");
        }
        if (badXid > 0) {
            failures.add(badXid + " history row(s) whose _xid is outside the run's transactions");
            System.out.println("  FAIL: " + badXid + " rows with an _xid outside the run");
        }
        if (splitTxn > 0) {
            failures.add(splitTxn + " history row(s) whose _xid or _ts differs from another row of"
                + " the same commit");
            System.out.println("  FAIL: " + splitTxn + " rows disagree with their commit's _xid/_ts");
        }

        // Replay from empty, in commit order, and compare with PostgreSQL row for row. A -U or -D
        // must carry exactly the image the replay holds for its key: the before-image is the
        // reason this layout exists, and a writer that put the after-image there passed when only
        // the key was checked (review finding I1).
        Map<String, Long> multiset = new HashMap<>();
        Map<String, String> byKey = new HashMap<>();
        long replayErrors = 0, wrongBefore = 0;
        for (HistRow r : rows) {
            if ("T".equals(r.op())) {
                multiset.clear();
                byKey.clear();
                continue;
            }
            String img = String.join(String.valueOf(SEP), r.image());
            boolean adds = "+I".equals(r.op()) || "+U".equals(r.op()) || "R".equals(r.op());
            if (pkPos != null) {
                String k = key(r.image(), pkPos);
                if (adds) {
                    if (byKey.put(k, img) != null) {
                        replayErrors++;    // an insert of a key the history already had
                    }
                } else {
                    String held = byKey.remove(k);
                    if (held == null) {
                        replayErrors++;    // an update or delete of a key the history never had
                    } else if (!held.equals(img) && wrongBefore++ < maxDiffs) {
                        System.out.println("  DIFF history " + r.op() + " at " + r.commitLsn()
                            + "/" + r.opSeq() + " carries " + render(pgCols, r.image())
                            + " but the row was " + render(pgCols, held.split(String.valueOf(SEP), -1)));
                    }
                }
            } else if (adds) {
                multiset.merge(img, 1L, Long::sum);
            } else if (multiset.merge(img, -1L, Long::sum) < 0) {
                replayErrors++;            // a change to a row the history did not hold
            }
        }
        if (replayErrors > 0) {
            failures.add("replaying the history hit " + replayErrors + " change(s) to rows it did"
                + " not hold, or inserts of keys it already held");
            System.out.println("  FAIL: " + replayErrors + " inconsistent history changes");
        }
        if (wrongBefore > 0) {
            failures.add(wrongBefore + " -U/-D row(s) whose before-image is not the row as it was");
            System.out.println("  FAIL: " + wrongBefore + " wrong before-images");
        }
        long missing = 0, differ = 0, reported = 0;
        for (String[] vals : pgRows) {
            String row = String.join(String.valueOf(SEP), vals);
            if (pkPos != null) {
                String got = byKey.remove(key(vals, pkPos));
                if (got == null) {
                    missing++;
                } else if (!got.equals(row)) {
                    differ++;
                    if (reported++ < maxDiffs) {
                        System.out.println("  DIFF history replay differs at key "
                            + show(key(vals, pkPos)) + ": history "
                            + render(pgCols, got.split(String.valueOf(SEP), -1))
                            + "; postgres " + render(pgCols, vals));
                    }
                }
            } else {
                Long n = multiset.get(row);
                if (n == null || n <= 0) {
                    missing++;
                    if (reported++ < maxDiffs) {
                        System.out.println("  DIFF a PostgreSQL row the history does not"
                            + " produce (or produces fewer times): " + render(pgCols, vals));
                    }
                } else {
                    multiset.put(row, n - 1);
                }
            }
        }
        long extra = pkPos != null ? byKey.size()
            : multiset.values().stream().filter(n -> n > 0).mapToLong(Long::longValue).sum();
        System.out.printf("  history replay vs postgres: missing %d, extra %d, differ %d%n",
                          missing, extra, differ);
        if (missing + extra + differ > 0) {
            failures.add("replaying the history does not rebuild PostgreSQL's table: missing "
                + missing + ", extra " + extra + ", differing " + differ);
            System.out.println("  FAIL: the history does not rebuild the table");
        }
        return counts;
    }

    /** An image is present and has every NOT NULL column set. */
    static boolean image(String[] img, List<String> cols, Set<String> notNull) {
        if (img == null) {
            return false;
        }
        for (int i = 0; i < cols.size(); i++) {
            if (notNull.contains(cols.get(i)) && NULL.equals(img[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Appends every row of one history table (current or archived) to {@code out}, its image
     * mapped by column name onto PostgreSQL's columns. A T row's image is null. Returns false (and
     * records the failure) when the table's shape is wrong.
     */
    static boolean readHistory(Catalog catalog, String pgSchema, String name, List<String> pgCols,
                               DataType[] types, List<String> failures, List<HistRow> out)
            throws Exception {
        Table h = catalog.getTable(Identifier.create(pgSchema, name));
        List<String> names = h.rowType().getFieldNames();
        List<String> meta = names.subList(Math.max(0, names.size() - HIST_META.size()), names.size());
        if (!meta.equals(HIST_META)) {
            failures.add(name + " columns " + names + " do not end with " + HIST_META);
            System.out.println("  FAIL: history table schema " + names);
            return false;
        }
        if (!h.primaryKeys().isEmpty()) {
            failures.add(name + " has a primary key " + h.primaryKeys());
        }
        if (!h.partitionKeys().equals(List.of("_dt"))) {
            failures.add(name + " is not partitioned by _dt: " + h.partitionKeys());
        }
        int[] pos = new int[pgCols.size()];
        for (int i = 0; i < pos.length; i++) {
            pos[i] = names.indexOf(pgCols.get(i));
        }
        int op = names.indexOf("_op"), cl = names.indexOf("_commit_lsn"), l = names.indexOf("_lsn"),
            os = names.indexOf("_op_seq"), x = names.indexOf("_xid"), ts = names.indexOf("_ts"),
            dt = names.indexOf("_dt");
        ReadBuilder rb = h.newReadBuilder();
        try (RecordReader<InternalRow> reader = rb.newRead().createReader(rb.newScan().plan())) {
            reader.forEachRemaining(r -> {
                String code = r.getString(op).toString();
                String[] img = null;
                if (!"T".equals(code)) {
                    img = new String[pos.length];
                    for (int i = 0; i < pos.length; i++) {
                        img[i] = pos[i] < 0 ? NULL : paimonValue(r, pos[i], types[i]);
                    }
                }
                out.add(new HistRow(r.getLong(cl), r.getInt(os), r.getLong(l), r.getLong(x), code,
                                    img, r.getTimestamp(ts, 6).toMicros(), r.getInt(dt)));
            });
        }
        return true;
    }

    static Set<String> notNullColumns(Connection pg, String qualified) throws Exception {
        Set<String> out = new java.util.HashSet<>();
        try (PreparedStatement st = pg.prepareStatement("SELECT attname FROM pg_attribute"
                 + " WHERE attrelid = ?::regclass AND attnum > 0 AND NOT attisdropped AND attnotnull")) {
            st.setString(1, qualified);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) out.add(rs.getString(1));
            }
        }
        return out;
    }

    // ── a table with no key: an operation log ───────────────────────────────

    /**
     * {@code history}: no primary key in PostgreSQL, so in Paimon an append-only operation log.
     * Asserted: the log's columns are PostgreSQL's then the six metadata columns and it has no key;
     * every log row is the matching history row's after image (the before image for a D, NULLs
     * for a T), one for one with the same (commit LSN, op_seq); and {@link #checkHistory} shows
     * the history rebuilds PostgreSQL's rows as a multiset. That the workload really wrote to it
     * is asserted too, or the comparison would prove nothing.
     */
    static TableResult compareLog(Connection pg, Catalog catalog, String pgSchema, String table,
                                  int maxDiffs) throws Exception {
        List<String> failures = new ArrayList<>();
        String qualified = quote(pgSchema) + "." + quote(table);
        List<String> pgCols = pgColumns(pg, qualified);
        if (!pgPrimaryKey(pg, qualified).isEmpty()) {
            failures.add("has a primary key: the stock TPC-C schema has been altered");
        }
        Table log = catalog.getTable(Identifier.create(pgSchema, table));
        List<String> want = new ArrayList<>(pgCols);
        want.addAll(LOG_META);
        if (!log.rowType().getFieldNames().equals(want)) {
            failures.add("log columns " + log.rowType().getFieldNames() + " != " + want);
            System.out.println("  FAIL: log schema " + log.rowType().getFieldNames());
            return new TableResult(table, -1, -1, 0, 0, 0, 0, 0, 0, 0, failures);
        }
        if (!log.primaryKeys().isEmpty()) {
            failures.add("the log has a primary key " + log.primaryKeys());
        }
        DataType[] types = new DataType[pgCols.size()];
        for (int i = 0; i < types.length; i++) {
            types[i] = log.rowType().getFields().get(i).type();
        }
        long pgCount;
        try (Statement st = pg.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM " + qualified)) {
            rs.next();
            pgCount = rs.getLong(1);
        }
        System.out.println("  rows        : postgres " + pgCount);
        if (pgCount == 0) {
            failures.add("holds no rows, so the workload never wrote to it and the comparison"
                + " proves nothing");
        }
        Map<String, String> logRows = new HashMap<>();
        long logCount = 0, logDupes = 0;
        int base = pgCols.size();
        ReadBuilder rb = log.newReadBuilder();
        try (RecordReader<InternalRow> reader = rb.newRead().createReader(rb.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow r;
                while ((r = batch.next()) != null) {
                    logCount++;
                    String[] vals = new String[pgCols.size()];
                    for (int i = 0; i < vals.length; i++) {
                        vals[i] = paimonValue(r, i, types[i]);
                    }
                    String k = r.getLong(base + 1) + "/" + r.getInt(base + 3);
                    // _xid and _ts too: the log row and its history rows are one change.
                    if (logRows.put(k, r.getString(base).toString() + SEP
                            + String.join(String.valueOf(SEP), vals) + SEP + r.getLong(base + 4)
                            + SEP + r.getTimestamp(base + 5, 6).toMicros()) != null) {
                        logDupes++;
                    }
                }
                batch.releaseBatch();
            }
        }
        System.out.println("  log rows    : " + logCount);
        if (logDupes > 0) {
            failures.add("the log holds " + logDupes + " change(s) twice");
        }
        long[] hist = checkHistory(pg, catalog, pgSchema, table, pgCols, types, null,
                                   notNullColumns(pg, qualified), maxDiffs, failures);
        if (hist != null) {
            // The log, row for row against the history: a log row takes its change's first
            // (commit LSN, op_seq); I matches a +I, U a -U followed by the +U whose values the log
            // row carries, D a -D, T a T, R an R.
            List<HistRow> hrows = new ArrayList<>();
            readHistory(catalog, pgSchema, table + "_hist", pgCols, types, new ArrayList<>(), hrows);
            hrows.sort((x, y) -> x.commitLsn() != y.commitLsn()
                ? Long.compare(x.commitLsn(), y.commitLsn()) : Integer.compare(x.opSeq(), y.opSeq()));
            long logMismatch = 0;
            for (int i = 0; i < hrows.size(); i++) {
                HistRow h = hrows.get(i);
                String logOp;
                String[] vals;
                switch (h.op()) {
                    case "+I" -> { logOp = "I"; vals = h.image(); }
                    case "-U" -> {
                        logOp = "U";
                        vals = i + 1 < hrows.size() ? hrows.get(i + 1).image() : null;
                        i++;                 // its +U is part of the same log row
                    }
                    case "-D" -> { logOp = "D"; vals = h.image(); }
                    case "T" -> { logOp = "T"; vals = null; }
                    default -> { logOp = h.op(); vals = h.image(); }
                }
                if (vals == null) {
                    vals = new String[pgCols.size()];
                    Arrays.fill(vals, NULL);
                }
                String expect = logOp + SEP + String.join(String.valueOf(SEP), vals) + SEP
                    + h.xid() + SEP + h.tsMicros();
                String got = logRows.remove(h.commitLsn() + "/" + h.opSeq());
                if (!expect.equals(got) && logMismatch++ < maxDiffs) {
                    System.out.println("  DIFF log row " + h.commitLsn() + "/" + h.opSeq()
                        + ": log " + (got == null ? "absent" : show(got))
                        + "; history " + show(expect));
                }
            }
            logMismatch += logRows.size();
            System.out.println("  log vs history: " + logMismatch + " row(s) disagree");
            if (logMismatch > 0) {
                failures.add(logMismatch + " log row(s) disagree with the history, or have no"
                    + " history row");
            }
            if (hist[H_PI] + hist[H_R] == 0) {
                failures.add("the history of " + table + " holds no insert");
            }
        }
        System.out.println(failures.isEmpty() ? "  OK" : "  FAILED: " + failures.size() + " check(s)");
        return new TableResult(table, pgCount, logCount, 0, 0, 0,
                               hist == null ? 0 : hist[H_PI], 0, 0, 0, failures, hist);
    }


    static boolean scalarBoolean(Connection pg, String sql, String... args) throws Exception {
        try (PreparedStatement st = pg.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                st.setString(i + 1, args[i]);
            }
            try (ResultSet rs = st.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    // ── changelog ───────────────────────────────────────────────────────────

    /**
     * Row-kind counts {@code {+I, -U, +U, -D}} from {@code <table>$audit_log} over a streaming
     * scan. A merged batch read reports {@code INSERT} for every surviving row, so it can never
     * answer this; and the kind is stored as Paimon's reserved {@code _VALUE_KIND}, which only
     * {@code $audit_log} exposes (as its first column, a string).
     *
     * <p>Each {@code plan()} after the first carries one snapshot's changelog, and the first is
     * always empty. The splits are drained in batches rather than accumulated, which keeps memory
     * flat over a run of any length.
     *
     * <p><b>This is the cost driver of the whole comparison, and it is linear in the number of
     * replicated transactions.</b> The writer commits once per PostgreSQL transaction — by design,
     * since a batch is one committed transaction or nothing — so a TPC-C run leaves one snapshot
     * per transaction, and a changelog scan has to plan once per snapshot. The snapshot count is
     * printed next to the row kinds so the cost is visible; raising the workload's rate or
     * duration raises it proportionally.
     */
    static long[] auditLogKinds(Catalog catalog, String pgSchema, String table) throws Exception {
        long[] counts = new long[5];          // +I, -U, +U, -D, snapshots scanned
        Table al = catalog.getTable(Identifier.create(pgSchema, table + "$audit_log"))
            .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
        ReadBuilder rb = al.newReadBuilder();
        StreamTableScan scan = rb.newStreamScan();
        scan.plan();                                   // the first plan() is always empty: discard
        Long latest = ((FileStoreTable) catalog.getTable(Identifier.create(pgSchema, table)))
            .snapshotManager().latestSnapshotId();
        long snapshots = latest == null ? 0 : latest;
        List<Split> pending = new ArrayList<>();
        for (long i = 0; i <= snapshots + 1; i++) {
            pending.addAll(scan.plan().splits());
            if (pending.size() >= 512) {
                drain(rb, pending, counts);
                pending.clear();
            }
        }
        if (!pending.isEmpty()) {
            drain(rb, pending, counts);
        }
        counts[4] = snapshots;
        return counts;
    }

    static void drain(ReadBuilder rb, List<Split> splits, long[] counts) throws Exception {
        try (RecordReader<InternalRow> reader = rb.newRead().createReader(splits)) {
            reader.forEachRemaining(r -> counts[kindIndex(r.getString(0).toString())]++);
        }
    }

    /** {@code $audit_log}'s row-kind string to an index: +I 0, -U 1, +U 2, -D 3. */
    static int kindIndex(String k) {
        switch (k) {
            case "+I": return 0;
            case "-U": return 1;
            case "+U": return 2;
            case "-D": return 3;
            default: throw new IllegalStateException("unknown row kind " + k);
        }
    }

    // ── PostgreSQL catalogue ────────────────────────────────────────────────

    static List<String> pgColumns(Connection pg, String qualified) throws Exception {
        List<String> out = new ArrayList<>();
        String sql = "SELECT a.attname FROM pg_attribute a"
            + " WHERE a.attrelid = ?::regclass AND a.attnum > 0 AND NOT a.attisdropped"
            + " ORDER BY a.attnum";
        try (PreparedStatement st = pg.prepareStatement(sql)) {
            st.setString(1, qualified);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getString(1));
                }
            }
        }
        return out;
    }

    /** The primary key in key order, which is also the order the extension registers. */
    static List<String> pgPrimaryKey(Connection pg, String qualified) throws Exception {
        List<String> out = new ArrayList<>();
        String sql = "SELECT a.attname FROM pg_constraint c"
            + " CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)"
            + " JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum"
            + " WHERE c.conrelid = ?::regclass AND c.contype = 'p' ORDER BY k.ord";
        try (PreparedStatement st = pg.prepareStatement(sql)) {
            st.setString(1, qualified);
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    out.add(rs.getString(1));
                }
            }
        }
        return out;
    }

    /**
     * Whether any Delivery transaction ran, read off PostgreSQL rather than assumed.
     *
     * <p>BenchBase's loader assigns a carrier id only to orders below {@code 2101}; every order
     * created by a NewOrder transaction starts with {@code o_carrier_id IS NULL} and gets one only
     * when Delivery picks it up. So a delivered order beyond the loaded range is proof Delivery
     * ran, and its absence is proof it did not — which is what lets the oorder / order_line /
     * new_order changelog assertions be strict when they apply and silent when they cannot.
     */
    static boolean deliveryRan(Connection pg, String pgSchema) {
        String sql = "SELECT EXISTS (SELECT 1 FROM " + quote(pgSchema) + ".oorder"
            + " WHERE o_id >= 2101 AND o_carrier_id IS NOT NULL)";
        try (Statement st = pg.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() && rs.getBoolean(1);
        } catch (Exception e) {
            System.out.println("  note: could not determine whether Delivery ran: " + e.getMessage());
            return false;
        }
    }

    // ── canonical values: one renderer per side, both driven by the Paimon type ──

    static String paimonValue(InternalRow r, int i, DataType t) {
        if (r.isNullAt(i)) {
            return NULL;
        }
        switch (t.getTypeRoot()) {
            case BOOLEAN:  return Boolean.toString(r.getBoolean(i));
            case TINYINT:  return Byte.toString(r.getByte(i));
            case SMALLINT: return Short.toString(r.getShort(i));
            case INTEGER:  return Integer.toString(r.getInt(i));
            case BIGINT:   return Long.toString(r.getLong(i));
            case FLOAT:    return Float.toString(r.getFloat(i));
            case DOUBLE:   return Double.toString(r.getDouble(i));
            case DECIMAL: {
                DecimalType d = (DecimalType) t;
                return decimal(r.getDecimal(i, d.getPrecision(), d.getScale()).toBigDecimal(),
                               d.getScale());
            }
            case CHAR:
            case VARCHAR:  return r.getString(i).toString();
            case BINARY:
            case VARBINARY: return hex(r.getBinary(i));
            case DATE:     return LocalDate.ofEpochDay(r.getInt(i)).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TS.format(r.getTimestamp(i, ((TimestampType) t).getPrecision())
                                  .toLocalDateTime());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return r.getTimestamp(i, ((LocalZonedTimestampType) t).getPrecision())
                        .toInstant().toString();
            default:
                throw new IllegalStateException("no canonical rendering for Paimon type " + t);
        }
    }

    static String pgValue(ResultSet rs, int col, DataType t) throws Exception {
        switch (t.getTypeRoot()) {
            case BOOLEAN: {
                boolean v = rs.getBoolean(col);
                return rs.wasNull() ? NULL : Boolean.toString(v);
            }
            case TINYINT: {
                byte v = rs.getByte(col);
                return rs.wasNull() ? NULL : Byte.toString(v);
            }
            case SMALLINT: {
                short v = rs.getShort(col);
                return rs.wasNull() ? NULL : Short.toString(v);
            }
            case INTEGER: {
                int v = rs.getInt(col);
                return rs.wasNull() ? NULL : Integer.toString(v);
            }
            case BIGINT: {
                long v = rs.getLong(col);
                return rs.wasNull() ? NULL : Long.toString(v);
            }
            case FLOAT: {
                float v = rs.getFloat(col);
                return rs.wasNull() ? NULL : Float.toString(v);
            }
            case DOUBLE: {
                double v = rs.getDouble(col);
                return rs.wasNull() ? NULL : Double.toString(v);
            }
            case DECIMAL: {
                BigDecimal v = rs.getBigDecimal(col);
                return v == null ? NULL : decimal(v, ((DecimalType) t).getScale());
            }
            case CHAR:
            case VARCHAR: {
                String v = rs.getString(col);
                return v == null ? NULL : v;
            }
            case BINARY:
            case VARBINARY: {
                byte[] v = rs.getBytes(col);
                return v == null ? NULL : hex(v);
            }
            case DATE: {
                LocalDate v = rs.getObject(col, LocalDate.class);
                return v == null ? NULL : v.toString();
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE: {
                LocalDateTime v = rs.getObject(col, LocalDateTime.class);
                return v == null ? NULL : TS.format(v);
            }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                OffsetDateTime v = rs.getObject(col, OffsetDateTime.class);
                return v == null ? NULL : Instant.from(v).toString();
            }
            default:
                throw new IllegalStateException("no canonical rendering for Paimon type " + t);
        }
    }

    /**
     * A decimal at the column's declared scale. {@code numeric(p,s)} always stores scale {@code s}
     * and so does Paimon's {@code DECIMAL(p,s)}, so this is a no-op that makes a scale drift on
     * either side visible instead of letting it pass as a formatting difference.
     */
    static String decimal(BigDecimal v, int scale) {
        return v.setScale(scale, RoundingMode.UNNECESSARY).toPlainString();
    }

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }

    // ── reporting ───────────────────────────────────────────────────────────

    static String key(String[] vals, int[] pkPos) {
        StringBuilder sb = new StringBuilder();
        for (int p : pkPos) {
            sb.append(vals[p]).append(SEP);
        }
        return sb.toString();
    }

    static String render(List<String> names, String[] vals) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < vals.length && i < names.size(); i++) {
            sb.append(i > 0 ? ", " : "").append(names.get(i)).append('=').append(show(vals[i]));
        }
        return sb.toString();
    }

    /** Printable form: the separator becomes a slash and NULL becomes the word. */
    static String show(String s) {
        String out = s.endsWith(String.valueOf(SEP)) ? s.substring(0, s.length() - 1) : s;
        return out.replace(String.valueOf(SEP), " / ").replace(NULL, "NULL");
    }

    static void printSummary(List<TableResult> results) {
        System.out.println("Summary");
        System.out.printf("%-12s %10s %10s %8s %8s %8s %9s %8s %8s %8s %9s %8s %8s  %s%n",
            "table", "pg rows", "paimon", "missing", "extra", "differ",
            "+I", "-U", "+U", "-D", "hist +I", "hist +U", "hist -D", "verdict");
        System.out.println("-".repeat(146));
        for (TableResult r : results) {
            long[] h = r.hist() == null ? new long[8] : r.hist();
            System.out.printf("%-12s %10d %10d %8d %8d %8d %9d %8d %8d %8d %9d %8d %8d  %s%n",
                r.table(), r.pgRows(), r.paimonRows(), r.missing(), r.extra(), r.mismatched(),
                r.insert(), r.updBefore(), r.updAfter(), r.delete(), h[H_PI], h[H_PU], h[H_MD],
                r.ok() ? "ok" : "FAIL");
        }
        System.out.println("-".repeat(146));
        Map<String, List<String>> failed = new LinkedHashMap<>();
        for (TableResult r : results) {
            if (!r.ok()) {
                failed.put(r.table(), r.failures());
            }
        }
        if (!failed.isEmpty()) {
            System.out.println();
            System.out.println("Failures");
            failed.forEach((t, fs) -> fs.forEach(f -> System.out.println("  " + t + ": " + f)));
        }
    }

    static String quote(String ident) {
        return '"' + ident.replace("\"", "\"\"") + '"';
    }

    static String env(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isEmpty() ? def : v;
    }
}
