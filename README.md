# postgres-paimon

Replicate PostgreSQL tables into [Apache Paimon](https://paimon.apache.org/) using **logical
replication**. A small SQL extension registers the tables you want replicated and carries their DDL;
an external Java process reads a replication slot and writes to a Paimon warehouse on a local path,
HDFS or S3.

```
PostgreSQL 18                                   paimon-writer (JVM)                 Paimon warehouse
┌──────────────────────────────┐   pgoutput    ┌───────────────────────────┐       ┌──────────────────┐
│ tables  ──WAL──▶ slot "paimon"├──────────────▶│ decode ▶ resolve ▶ batch  ├──────▶│ <schema>.db/     │
│ paimon_slot extension: DDL   │  (proto v2)   │ ▶ commit to Paimon        │       │   <table>/       │
│ emitted as logical messages  │               │   confirm LSN after commit│       │   bucket-N/ ...  │
└──────────────────────────────┘               └───────────────────────────┘       └──────────────────┘
```

Nothing is loaded into the PostgreSQL server process: `paimon_slot` is pure SQL (no `.so`, no
`shared_preload_libraries`), and the writer is an ordinary client that can be stopped, upgraded and
restarted independently of the database.

## What you get

* **Effectively-once delivery.** The slot's confirmed position advances only after a Paimon commit
  has returned, and a replayed transaction is filtered by its commit identifier, so a crash neither
  loses nor duplicates data. Verified with `kill -9` mid-load, between a commit and its confirmation,
  and with a batch buffered.
* **A faithful changelog.** Tracked tables use `REPLICA IDENTITY FULL` and Paimon's
  `changelog-producer = input`, so every `UPDATE` produces a real `-U` / `+U` pair, including for
  rows whose large (TOASTed) columns were not touched.
* **An operation history of every table.** Beside each replicated table `T` the writer keeps
  `T_hist`, an append-only Paimon table partitioned by day, with one row per row image -- `+I`, an
  update's `-U` and `+U`, `-D` -- carrying the table's columns plus operation, commit and change LSN,
  transaction id and commit time, in commit order and never collapsed, even inside one transaction
  or one commit batch. Every DDL archives it as `T_hist_<lsn>` and starts a new one with the new
  schema. It survives `DROP TABLE`.
* **Tables without a primary key** are replicated as append-only operation logs: one row per
  insert, update, delete or truncate, with the same metadata.
* **DDL follows the data:** `ADD`/`DROP`/`RENAME COLUMN`, `ALTER COLUMN TYPE`, `DROP TABLE` and
  `TRUNCATE` are propagated. Changes the writer could not apply faithfully are refused up front by
  guardrails instead of failing later.
* **One Paimon database per PostgreSQL schema**, so `public.orders` and `archive.orders` are separate
  Paimon tables at `<warehouse>/public.db/orders` and `<warehouse>/archive.db/orders`.
* **Fixed bucketing** with a size-based default that you can override per table, and **mini-batch
  commits** (several transactions per Paimon commit, bounded by rows, bytes and time) so a busy
  table does not produce a snapshot per transaction.
* **Verified against TPC-C.** A GitHub Actions workflow (`tpcc.yml`, manual dispatch) runs BenchBase
  through the pipeline and compares every replicated row of PostgreSQL against Paimon column by
  column, and checks that replaying each history table rebuilds PostgreSQL's table exactly.

## Quick start

Requirements: PostgreSQL 18 with `wal_level = logical`, a JDK 17 and Maven.

```bash
# 1. Build the writer (a shaded jar; runs its unit tests).
cd java/writer && mvn -q package                 # -> java/writer/target/paimon-writer.jar

# 2. Install the extension (standard PGXS) and enable it in your database.
make -C sql/paimon_slot install                  # or install-local, see the guide
psql mydb -c "CREATE EXTENSION paimon_slot"

# 3. Register a table (with a primary key it is merged; without one it is an operation log).
#    Do this BEFORE starting the writer, or while it is still empty.
psql mydb -c "CREATE TABLE orders (id int PRIMARY KEY, customer text, amount numeric(12,2))"
psql mydb -c "SELECT paimon.add_table('orders'::regclass)"

# 4. Start the writer. It is configured entirely from the environment.
PGPAIMON_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/mydb \
PGPAIMON_USER=postgres PGPAIMON_PASSWORD=... \
PGPAIMON_WAREHOUSE=/var/lib/paimon/warehouse \
java -jar java/writer/target/paimon-writer.jar
```

The full setup, every setting, the operational hazards (an abandoned slot retains WAL and fills the
disk), the limitations and why each exists, troubleshooting and the test procedure are in
**[docs/logical-slot-architecture.md](docs/logical-slot-architecture.md)**. Read its section 3 before
running this anywhere that matters, and give its section 8, **"Rules for developers and
administrators"**, to everyone who changes the database: one table of everything not to do, each
marked REJECTED, STOPS WRITER, REINIT / HISTORY LOSS or SILENT, with the recovery and the test that
proves it.

## Limitations worth knowing up front

Each is deliberate and explained, with its reason, in section 5 of the guide:

* a table **without a primary key** is replicated as an operation log, not as a merged table, and a
  key cannot be added to a replicated table later; a primary key must not be `DEFERRABLE`;
* the history tables (`<table>_hist`) roughly **double write volume** and **grow without bound**,
  in rows and in archived tables (one per DDL): retention is yours to run (drop old `_dt`
  partitions, and drop old archives by hand: nothing written to an archive again, so Paimon never
  expires it); a dropped table's history and its archives are kept and their names stay taken until
  you move them aside and call `paimon.forget_history()`; a table re-added after
  `paimon.remove_table()` must keep its key (or you drop the kept Paimon table and call
  `paimon.forget_kept_table()`); the seven history field names (`_op`, `_commit_lsn`, `_lsn`,
  `_op_seq`, `_xid`, `_ts`, `_dt`) are reserved column names;
* `numeric` columns need an explicit precision of at most 38;
* any role that can read `paimon.secret`, **including members of `pg_read_all_data`**, can forge a
  DDL message the writer applies (`paimon.secret_readers()` lists them), and a DDL message without
  the credential stops the writer rather than being ignored; do not change the secret while the
  slot holds unconsumed DDL;
* `ADD COLUMN ... DEFAULT` and `ALTER COLUMN ... SET NOT NULL` are refused;
* a **non-empty** table cannot be added once the writer's slot exists;
* PostgreSQL `infinity` / `-infinity` are stored as the finite extremes of the Paimon type, which is
  order-preserving but lossy at the very end of the range;
* a few things are **not detected** at all (guide, section 8): consuming or advancing the writer's
  slot with another client after a crash of the writer (after a clean stop it is refused), a
  changed `PGPAIMON_COMMIT_USER`, a new partition nobody registered, a file-level snapshot restore
  started once the WAL has passed the warehouse; sequences and large objects are not replicated;
* one transaction is bounded by `PGPAIMON_TXN_MAX_BYTES` (a quarter of the heap by default): a
  larger one stops the writer, losing nothing, until it is given more memory;
* S3 support is wired in but **has not been exercised** in this repository's own environment: the
  test is skipped unless `E2E_MINIO_BIN` points at a MinIO binary.

## Repository layout

```
sql/paimon_slot/        the SQL extension: registry, type mapping, add_table, event-trigger
                        guardrails and DDL messages, with a pg_regress suite
java/writer/            paimon-writer: decoder, TOAST resolver, schema registry, batcher, sink,
                        slot consumer, backfill, with unit tests
java/                   acceptance suites (JDBC) and the TPC-C comparison tool
test/slot-acceptance/   one command that builds a throwaway PostgreSQL cluster, installs the
                        extension, runs the writer and executes every acceptance suite
test/tpcc/              BenchBase TPC-C driven through the same pipeline
.github/workflows/      java.yml (build the Java project), codeql.yml, tpcc.yml
docs/                   the architecture and operations guide; design spec and plan under
                        docs/superpowers/
```

## Testing

```bash
cd java/writer && mvn test                                         # writer unit tests
make -C sql/paimon_slot install-local && make -C sql/paimon_slot installcheck   # pg_regress
./test/slot-acceptance/run_acceptance.sh                           # all acceptance suites
./test/tpcc/run_tpcc.sh                                            # TPC-C, then compare
```

`run_acceptance.sh` needs no Docker: it starts its own PostgreSQL 18 cluster on port 5442. See
[test/slot-acceptance/README.md](test/slot-acceptance/README.md).

## License

Apache License 2.0. See [LICENSE](LICENSE). Every source file carries an
`SPDX-License-Identifier: Apache-2.0` header.
