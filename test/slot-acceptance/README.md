# Local acceptance harness (no Docker)

    ./test/slot-acceptance/run_acceptance.sh [SuiteName ...]

Prerequisite: `cd java/writer && mvn -q package` (builds `java/writer/target/paimon-writer.jar`).

It starts, in order: a PostgreSQL 18 cluster on `127.0.0.1:5442` (TCP only, no unix socket),
the `paimon_slot` extension loaded from the worktree (`make install-local` +
`extension_control_path`, never `sudo make install`), database `paimon_acc` created from
`setup.sql` (tables + `paimon.add_table`), then the writer jar. Only when the replication slot
is active does it run `mvn test` in `java/pom.xml` (no argument: every `*IT` suite; with
names: `-Dtest=SlotHarnessSmokeIT,Name1,...`; the smoke suite, which streams an insert, update and
delete and waits for them in Paimon, always runs). The script's exit status is Maven's. Teardown runs on any exit.

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
