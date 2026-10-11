# Local acceptance harness (no Docker)

    ./test/slot-acceptance/run_acceptance.sh [SuiteName ...]

Prerequisite: `cd java/writer && mvn -q package` (builds `java/writer/target/paimon-writer.jar`).

It starts, in order: a PostgreSQL 18 cluster on `127.0.0.1:5442` (TCP only, no unix socket),
the `paimon_slot` extension loaded from the worktree (`make install-local` +
`extension_control_path`, never `sudo make install`), database `paimon_acc` created from
`setup.sql` (tables + `paimon.add_table`), then the writer jar. Only when the replication slot
is active does it run `mvn test` in `java/pom.xml`, always with an explicit `-Dtest=` list.
The script's exit status is Maven's. Teardown runs on any exit.

**What a bare run covers.** With no argument it names every suite — the `ALL_SUITES` list in the
script — and runs all eight:

    SlotHarnessSmokeIT PaimonSchemaIT PaimonFeatureTest PaimonDdlTest
    PaimonAdvancedTest PaimonTypeTest PaimonSlotKeyMoveTest PaimonOffloadTest

That is **186 checks** (plus T18 not run without MinIO). It prints the list it is about to run.
`PaimonE2ETest` is deliberately not in it: it is not ported to this architecture, and naming it
fails the run by design. The selection is never left to Surefire's default includes — doing so
picked up only the two `*IT` suites, 24 of the 186 checks, and still printed `ALL SUITES PASSED`.

With names, it runs exactly those plus the smoke suite, which streams an insert, update and delete
and waits for them in Paimon: `-Dtest=SlotHarnessSmokeIT,Name1,...`.

**Each named suite must actually run.** After Maven, the harness requires a Surefire report with
`tests > 0` for every name on the list, and fails the run otherwise — so a mistyped name, a class
with no `@Test`, a suite dropped from `ALL_SUITES`, or a suite whose every test was skipped is a
failure, not a silent pass. (Surefire itself exits 0 when one `-Dtest` name matches nothing as long
as another matches, and `-DfailIfNoSpecifiedTests` does not change that; hence the check.) Because
a bare run names the full list, that check now covers the whole suite as well.

`add_table` runs before the writer because a table cannot be added once the slot exists.
Each run starts from a fresh database, slot and warehouse; only `pgdata` is reused.

Exported to the suites: `PGPAIMON_IT_JDBC_URL`, `PGPAIMON_IT_WAREHOUSE`, `E2E_JDBC_URL`
(all point at the live stack), and `PGPAIMON_SLOT=paimon_it` (BackfillerTest creates and drops the slot named by that variable;
unset, it would drop the live writer's `paimon` slot) and `PGPAIMON_HARNESS=1`.

Suite tables must be declared in `setup.sql`: `add_table` must precede the slot.

It does not touch the pg_regress cluster on 5441.

## Left behind

Everything is under `test/slot-acceptance/run/` (git-ignored): `pgdata/`, `pg.log`,
`writer.log`, `writer.pid`, `warehouse/`. Postgres and the writer are stopped; the directory is
kept for inspection. Remove it with `rm -rf test/slot-acceptance/run`. The replication slot
lives inside `pgdata`, so stopping the cluster stops it holding WAL and deleting the directory
removes it.

`READY_TIMEOUT` (seconds, default 60) bounds both readiness gates.

## Remote warehouses (PaimonOffloadTest: T18 S3, T19 HDFS)

There is no offload step in this architecture (on main a worker wrote Parquet locally, uploaded
it and deleted the local copy). The warehouse itself is `s3://bucket/wh` or `hdfs://host:port/wh`
and the writer's Paimon FileIO writes straight to it. The suite proves the data is still
readable from there.

Mode: the generated writer launcher (`run/start_writer.sh`, exported to suites as
`PGPAIMON_WRITER_LAUNCHER`) honours `PGPAIMON_WAREHOUSE_OVERRIDE`; any `PGPAIMON_S3_ENDPOINT`,
`PGPAIMON_S3_ACCESS_KEY`, `PGPAIMON_S3_SECRET_KEY` in its environment reach the writer. Unset,
the writer uses the local warehouse and every other suite is unaffected. `PaimonOffloadTest`
uses it: it stops the harness writer, runs `PGPAIMON_WAREHOUSE_OVERRIDE=<uri> bash $LAUNCHER`,
runs its three phases (INSERT 20, UPDATE 5, DELETE 3, row kinds read from `$audit_log`),
waits for the slot to catch up, and restores the default writer.

    ./test/slot-acceptance/run_acceptance.sh PaimonOffloadTest

- **T19 (HDFS)** runs in the test JVM on a `MiniDFSCluster`. No setup, no extra install.
- **T18 (S3)** runs only when `E2E_MINIO_BIN` names an executable MinIO server
  (`E2E_MINIO_BIN=/path/to/minio`; port `E2E_MINIO_PORT`, default 9100). Otherwise it prints
  `T18 SKIPPED (...)`, the results line reads `N passed, 0 failed, 1 skipped`, and the harness
  verdict reads `ALL SUITES PASSED -- except: T18 (S3) NOT RUN ...`. A skip is never a pass.
  The writer jar bundles `paimon-s3`. S3 is present but UNVERIFIED in this environment (no MinIO):
  verify with `E2E_MINIO_BIN=/path/to/minio ./test/slot-acceptance/run_acceptance.sh PaimonOffloadTest`.
  Surefire reports T18 and T19 as two tests (`s3Warehouse`, `hdfsWarehouse`); a skipped T18 shows in
  its Skipped column, and a suite whose tests were all skipped fails the harness.
