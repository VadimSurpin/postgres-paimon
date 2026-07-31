// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Paimon-Spark compaction job for paimon_heap CDC tables.
 *
 * For each table in the warehouse:
 *   1. Sets changelog-producer=input so that Paimon uses the row kind values
 *      already present in the data to produce standard Paimon changelog files
 *      (written to the table's changelog/ directory, not a separate _history table).
 *   2. Calls the paimon.sys.compact procedure, which merges small files and
 *      applies the table's merge engine (deduplicate/etc.) to produce a
 *      compacted snapshot.
 *
 * Usage:
 *   java -jar paimon-compaction.jar <warehouse> [table1,table2,...] [--dry-run]
 *
 * Or via spark-submit (Spark provided externally):
 *   spark-submit --class pro.surpin.pgpaimon.PaimonCompactionJob paimon-compaction.jar \
 *       <warehouse> [table1,table2,...] [--dry-run]
 */
public class PaimonCompactionJob {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: PaimonCompactionJob <warehouse> [table1,table2,...] [--dry-run]");
            System.exit(1);
        }

        String warehouse = args[0];
        List<String> tables = new ArrayList<>();
        boolean dryRun = false;

        for (int i = 1; i < args.length; i++) {
            if ("--dry-run".equals(args[i])) dryRun = true;
            else tables.addAll(Arrays.asList(args[i].split(",")));
        }

        SparkSession spark = SparkSession.builder()
            .appName("PaimonCompaction")
            .master("local[*]")
            .config("spark.sql.catalog.paimon",
                    "org.apache.paimon.spark.SparkCatalog")
            .config("spark.sql.catalog.paimon.warehouse", warehouse)
            .config("spark.sql.extensions",
                    "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        try {
            if (tables.isEmpty()) {
                tables = discoverTables(warehouse);
                System.out.println("Auto-discovered tables: " + tables);
            }
            if (tables.isEmpty()) {
                System.out.println("No tables found in " + warehouse);
                return;
            }

            int failed = 0;
            for (String table : tables) {
                System.out.printf("%n── %s%s ──%n", table, dryRun ? " [DRY RUN]" : "");
                if (dryRun) continue;
                try {
                    // changelog-producer=input: Paimon uses the row-kind values in
                    // incoming writes to produce standard changelog files under the
                    // table's changelog/ directory instead of a separate history table.
                    spark.sql(String.format(
                        "ALTER TABLE paimon.default.`%s` SET TBLPROPERTIES " +
                        "('changelog-producer' = 'input')", table));

                    spark.sql(String.format(
                        "CALL paimon.sys.compact(`table` => 'default.%s')", table));

                    System.out.println("  Compacted: " + table);
                } catch (Exception e) {
                    System.err.printf("  FAILED: %s — %s%n", table, e.getMessage());
                    e.printStackTrace(System.err);
                    failed++;
                }
            }
            if (failed > 0) System.exit(1);
        } finally {
            spark.stop();
        }
    }

    static List<String> discoverTables(String warehouse) {
        File[] dirs = new File(warehouse).listFiles(
            f -> f.isDirectory() && new File(f, "schema").isDirectory());
        if (dirs == null) return Collections.emptyList();
        return Arrays.stream(dirs)
            .map(File::getName)
            .sorted()
            .collect(Collectors.toList());
    }
}
