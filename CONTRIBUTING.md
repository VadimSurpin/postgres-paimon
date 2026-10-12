# Contributing

## Prerequisites

* PostgreSQL 18 with the server development files (`postgresql-server-dev-18` on Debian and Ubuntu),
  which provide PGXS and `pg_regress` for the extension's tests.
* JDK 17 and Maven 3.9 or later.
* `make`. There is no C or C++ compiler requirement: the extension is pure SQL.

No Docker is needed for anything in this repository.

## Building

```bash
# The writer: a Spring Boot executable jar, built together with its unit tests.
mvn -pl writer -am -q package           # -> writer/target/paimon-writer.jar

# The Java acceptance suites and the TPC-C comparison tool.
mvn -pl java -am -q package
```

## Running the tests

Run all three layers before sending a change; they cover different failures.

```bash
# 1. Writer unit tests.
mvn -pl writer -am test

# 2. The SQL extension. install-local stages it under sql/paimon_slot/_install so that no
#    system directory is written; the test cluster must set extension_control_path to it, and
#    the settings in sql/paimon_slot/test/logical.conf (wal_level = logical,
#    max_prepared_transactions > 0: section (16) prepares a transaction).
make -C sql/paimon_slot install-local
make -C sql/paimon_slot installcheck

# 3. End to end. Builds a throwaway PostgreSQL 18 cluster on port 5442, runs the writer and
#    every acceptance suite. Needs the writer jar from step 1.
./test/slot-acceptance/run_acceptance.sh
```

`PaimonRobustTest` (what an administrator or developer can do to the database: section 8 of the
guide) runs last, in a Maven invocation of its own (`LAST_SUITES` in the script): it invalidates the
slot and re-bootstraps, which reloads every table of the database. A new suite that changes state
for everything after it belongs there too.

`run_acceptance.sh` with no argument also runs the writer module's live ITs (`BackfillerTest`'s
initial-load tests, skipped by plain `mvn test`) against a database of its own, and fails if any is
skipped. It takes suite names to run a subset. It fails if a named suite matches no tests
or if every test in it is skipped, so a mistyped name cannot pass silently. The S3 test is skipped
unless `E2E_MINIO_BIN` points at a MinIO binary; the run reports that plainly instead of counting
the skip as a pass.

The TPC-C comparison is `./test/tpcc/run_tpcc.sh` locally, or the `tpcc` workflow on GitHub Actions
(manual dispatch). See [docs/logical-slot-architecture.md](docs/logical-slot-architecture.md),
section 7, for what each layer proves.

## Code style

* Java: standard conventions; the writer uses Spring Boot. Keep invariants in comments next to the code
  that upholds them; several rules here (confirm the LSN only after the commit, never split a
  transaction, replay must be a no-op) are easy to break and hard to notice.
* SQL: match the surrounding style. Guardrails raise with a message, a hint and a pointer to the
  design spec.
* No new dependencies without a discussion in an issue first. The writer jar is already large.

## Writing tests

* **Assert what the system does, not that it did not crash.** This codebase has repeatedly had
  checks that passed whether or not the behaviour was correct. Prefer comparing real output (row
  contents, the changelog from the `$audit_log` system table, bytes captured from PostgreSQL) over
  counts, and confirm a new test fails when the behaviour it guards is broken.
* **Never read row kinds from a merged Paimon read**; it reports `INSERT` for every surviving row.
  Use `$audit_log` over a streaming scan and discard the always-empty first `plan()`. `RawRead` does
  this.
* **Register a table while it is empty.** `add_table` decides whether a backfill is due from whether
  the table has rows, and a non-empty table cannot be added once the slot exists.
* A harness that reports success for work it did not do is worse than none. If you add a suite,
  add it to `ALL_SUITES` in `run_acceptance.sh`.

## Submitting changes

1. Open an issue describing the change before starting significant work.
2. Keep commits focused; one logical change per commit.
3. Update the guide in `docs/` if you add or change a setting, a guardrail, a limitation or a
   build step; a new way for a database user to break replication (or a new refusal of one) is a
   row in section 8, "Rules for developers and administrators", with its marker and its test. State the reason for a limitation, not only the rule: a rule without its reason gets
   "fixed" later.
4. All source files must carry the `SPDX-License-Identifier: Apache-2.0` header.

## Java formatting

Both Maven modules use Spotless with the Eclipse Google Java Style profile in
`config/java-google-style.xml`. The project overrides declaration formatting:
each parameter and the closing `)` occupy separate lines for constructors,
methods, and records. From the repository root:

```bash
mvn spotless:apply   # Format Java sources and tests
mvn spotless:check   # Check formatting without modifying files
```

Formatting is checked during `validate`, so `mvn test`, `mvn package`, and CI also
check it. Run `mvn spotless:apply` before committing Java changes. `.editorconfig`
sets two-space Java indentation; Spotless is the shared formatter for line wrapping
and import ordering.
