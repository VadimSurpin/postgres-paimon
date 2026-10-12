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

import java.io.File;
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
 * <p><b>The table that must not be there.</b> {@code history} has no primary key in the stock
 * TPC-C schema, so {@code paimon_slot} cannot replicate it and the harness never registers it. That
 * exclusion is asserted, not assumed: {@link #checkExcluded} proves the table is untracked,
 * unpublished, absent from the Paimon catalog and absent from the warehouse directory, while still
 * accumulating rows from Payment — so an unregistered table in a busy workload is shown to stay
 * genuinely untouched.
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
 *   java -cp java/target/paimon-reader-1.0.jar pro.surpin.pgpaimon.TpccCompare \
 *        [warehouse] [jdbc-url] [pg-schema]
 * </pre>
 *
 * Environment: {@code PGPAIMON_IT_WAREHOUSE}, {@code PGPAIMON_IT_JDBC_URL}, {@code TPCC_SCHEMA},
 * {@code TPCC_TABLES} (comma-separated override of the table list), {@code TPCC_MAX_DIFFS}
 * (differing rows reported per table, default 10), {@code TPCC_META_FIELDS} (comma-separated
 * writer metadata columns to expect in Paimon and exclude from the comparison, default
 * {@code _seq,_op_seq,_xid}).
 */
public final class TpccCompare {

    /**
     * The eight TPC-C tables this architecture replicates.
     *
     * <p>{@code history} is deliberately absent, and its absence is asserted rather than assumed:
     * see {@link #EXCLUDED_TABLES}.
     */
    static final String[] DEFAULT_TABLES = {
        "warehouse", "district", "customer",
        "item", "stock", "oorder", "new_order", "order_line"
    };

    /**
     * TPC-C tables that cannot be replicated, and are therefore checked to be absent.
     *
     * <p>{@code history} has <b>no primary key</b> in the stock TPC-C schema — two foreign keys and
     * nothing else — and {@code paimon_slot} requires one on every replicated table. That is not a
     * shortfall to work around: the write target is a Paimon primary-key table, so without a key
     * there is nothing to merge on, and {@code add_table()} refuses the registration by design. The
     * stock BenchBase schema is therefore left exactly as it ships, and {@code history} stays out
     * of the publication and out of the warehouse.
     *
     * <p>Asserting the absence makes the exclusion a tested property. It is also a live test rather
     * than a formality: TPC-C's Payment transaction inserts into {@code history} throughout the
     * run, so this proves that a table never passed to {@code add_table} stays genuinely untouched
     * under a busy workload — the same guarantee the acceptance suite's T17 checks in miniature,
     * here at TPC-C scale.
     */
    static final String[] EXCLUDED_TABLES = {"history"};

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
                       List<String> failures) {
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

        System.out.println("TPC-C state comparison: PostgreSQL vs Paimon");
        System.out.println("  warehouse   : " + warehouse);
        System.out.println("  jdbc        : " + jdbcUrl.replaceAll("password=[^&]*", "password=***"));
        System.out.println("  pg schema   : " + pgSchema
            + "  (Paimon database; tables at <warehouse>/" + pgSchema + ".db/<table>)");
        System.out.println("  tables      : " + String.join(", ", tables));
        System.out.println("  meta fields : " + String.join(", ", metaFields) + " (excluded)");
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
            for (String table : EXCLUDED_TABLES) {
                System.out.println("── " + table + " (must NOT be replicated) ──");
                results.add(checkExcluded(pg, catalog, warehouse, pgSchema, table));
                System.out.println();
            }
        }

        printSummary(results);

        boolean allOk = results.stream().allMatch(TableResult::ok);
        System.out.println();
        System.out.println(allOk
            ? "RESULT: PASS -- PostgreSQL and Paimon agree on every row of every table, "
              + "and the changelog accounts for every change."
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

        System.out.println(failures.isEmpty() ? "  OK" : "  FAILED: " + failures.size() + " check(s)");
        return new TableResult(table, pgCount, paimonCount, missing, extra, mismatched,
                               ins, ubf, uaf, del, failures);
    }

    // ── the table that must not be there ────────────────────────────────────

    /**
     * Asserts that an unreplicable table reached neither the publication nor the warehouse.
     *
     * <p>Seven things are checked, and each one would fail for a different reason, so a failure
     * says which assumption broke:
     *
     * <ol>
     *   <li>the table exists in PostgreSQL — otherwise every check below is vacuous;</li>
     *   <li>it still has no primary key, so the documented reason for excluding it holds and
     *       nobody has quietly added one;</li>
     *   <li>it holds rows, so the workload really did write to it and the absence checks are
     *       live rather than trivially true;</li>
     *   <li>{@code paimon.table_registry} has no row for it;</li>
     *   <li>{@code paimon_pub} does not publish it, which is what keeps its changes out of the
     *       slot's stream in the first place;</li>
     *   <li>the Paimon catalog does not know the table;</li>
     *   <li>no directory for it exists under the warehouse's database directory.</li>
     * </ol>
     */
    static TableResult checkExcluded(Connection pg, Catalog catalog, String warehouse,
                                     String pgSchema, String table) throws Exception {
        List<String> failures = new ArrayList<>();
        String qualified = quote(pgSchema) + "." + quote(table);

        boolean exists = scalarBoolean(pg,
            "SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
            + " WHERE n.nspname = ? AND c.relname = ? AND c.relkind = 'r')", pgSchema, table);
        if (!exists) {
            failures.add("table does not exist in PostgreSQL, so nothing here was actually checked");
            System.out.println("  FAIL: " + qualified + " does not exist in PostgreSQL");
            return new TableResult(table, -1, -1, 0, 0, 0, 0, 0, 0, 0, failures);
        }

        List<String> pk = pgPrimaryKey(pg, qualified);
        if (!pk.isEmpty()) {
            failures.add("has a primary key " + pk + ", so the reason it is excluded no longer "
                + "holds: either replicate it or update the exclusion");
            System.out.println("  FAIL: unexpectedly has a primary key: " + pk);
        } else {
            System.out.println("  pk          : none  (which is why it cannot be replicated)");
        }

        long rows = 0;
        try (Statement st = pg.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM " + qualified)) {
            rows = rs.next() ? rs.getLong(1) : 0;
        }
        System.out.println("  rows         : postgres " + rows + "  (written by Payment "
            + "throughout the run, so the absence checks below are live)");
        if (rows == 0) {
            failures.add("holds no rows, so the workload never wrote to it and its absence from "
                + "Paimon proves nothing");
            System.out.println("  FAIL: no rows, so this check is vacuous");
        }

        boolean tracked = scalarBoolean(pg,
            "SELECT EXISTS (SELECT 1 FROM paimon.table_registry"
            + " WHERE schema_name = ? AND table_name = ?)", pgSchema, table);
        System.out.println("  registry     : " + (tracked ? "TRACKED" : "not tracked"));
        if (tracked) {
            failures.add("is registered in paimon.table_registry, but it must not be");
            System.out.println("  FAIL: present in paimon.table_registry");
        }

        boolean published = scalarBoolean(pg,
            "SELECT EXISTS (SELECT 1 FROM pg_publication_tables"
            + " WHERE pubname = 'paimon_pub' AND schemaname = ? AND tablename = ?)",
            pgSchema, table);
        System.out.println("  publication  : " + (published ? "PUBLISHED" : "not in paimon_pub"));
        if (published) {
            failures.add("is published by paimon_pub, so its changes travel the slot's stream");
            System.out.println("  FAIL: published by paimon_pub");
        }

        boolean inCatalog;
        try {
            catalog.getTable(Identifier.create(pgSchema, table));
            inCatalog = true;
        } catch (Catalog.TableNotExistException e) {
            inCatalog = false;
        }
        System.out.println("  paimon table : " + (inCatalog ? "EXISTS" : "absent from the catalog"));
        if (inCatalog) {
            failures.add("exists in the Paimon catalog as " + pgSchema + "." + table);
            System.out.println("  FAIL: the Paimon catalog knows " + pgSchema + "." + table);
        }

        // Only meaningful for a local warehouse; a remote URI is left to the catalog check above.
        if (!warehouse.contains("://") || warehouse.startsWith("file://")) {
            File dir = new File(warehouse.replaceFirst("^file://", ""),
                                pgSchema + ".db/" + table);
            System.out.println("  warehouse dir: " + dir
                + (dir.exists() ? "  EXISTS" : "  does not exist"));
            if (dir.exists()) {
                failures.add("has a directory in the warehouse at " + dir);
                System.out.println("  FAIL: the warehouse holds a directory for it");
            }
        }

        System.out.println(failures.isEmpty()
            ? "  OK -- never registered, never published, never written"
            : "  FAILED: " + failures.size() + " check(s)");
        return new TableResult(table, rows, 0, 0, 0, 0, 0, 0, 0, 0, failures);
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
        System.out.printf("%-12s %10s %10s %8s %8s %8s %9s %8s %8s %8s  %s%n",
            "table", "pg rows", "paimon", "missing", "extra", "differ",
            "+I", "-U", "+U", "-D", "verdict");
        System.out.println("-".repeat(118));
        for (TableResult r : results) {
            System.out.printf("%-12s %10d %10d %8d %8d %8d %9d %8d %8d %8d  %s%n",
                r.table(), r.pgRows(), r.paimonRows(), r.missing(), r.extra(), r.mismatched(),
                r.insert(), r.updBefore(), r.updAfter(), r.delete(),
                r.ok() ? "ok" : "FAIL");
        }
        System.out.println("-".repeat(118));
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
