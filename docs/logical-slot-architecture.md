# Logical-slot architecture (`paimon_slot` + `paimon-writer`)

Operations guide and limitations list for the logical-replication architecture on the
`feat/logical-slot-writer` branch. The design and its measured justification are in
[`docs/superpowers/specs/2026-10-09-logical-slot-writer-design.md`](superpowers/specs/2026-10-09-logical-slot-writer-design.md).
Where this page and the spec disagree, the spec was amended during implementation and is current;
tell us about the disagreement.

Every command and error string below was run against a real PostgreSQL 18 server and the built
writer jar while this page was written. Error strings are quoted exactly.

## 1. What this is

Two architectures live on this branch. They are alternatives, not layers; you use one.

| | `extension/` (the one in the rest of the README) | `sql/paimon_slot/` + `java/writer/` (this page) |
|---|---|---|
| Capture | custom table access method, `USING paimon_heap` | logical replication slot, ordinary heap tables |
| Write path | background worker inside PostgreSQL, shared-memory ring buffer | separate JVM process (`paimon-writer`) using Paimon's own Java API |
| In-database code | C++ shared library, `shared_preload_libraries` | pure SQL extension, no shared library |
| DDL | `ProcessUtility` hook | event triggers emitting logical WAL messages |
| Overload behaviour | ring buffer silently drops batches | slot retains WAL; disk fills instead (section 3) |

The C++ `extension/` is untouched and stays on the branch so the two write paths can be compared on
one workload. Nothing in the README sections on `paimon_heap` (GUCs, `USING paimon_heap`, staging
directory, S3/HDFS offload GUCs) applies to the logical-slot architecture.

Data flow: user DML hits an ordinary heap table. The publication `paimon_pub` feeds replication
slot `paimon` (pgoutput, text mode). The writer decodes it, batches complete transactions, and
commits them to Paimon with the transaction's commit LSN as the commit identifier. Only after a
Paimon commit succeeds does the writer confirm that LSN to PostgreSQL, so a crash replays rather
than loses (and replays are idempotent).

## 2. Setup

Requirements: PostgreSQL 18 (the writer connects with `assumeMinServerVersion=18`), `wal_level = logical`,
a JDK and Maven to build the writer.

```bash
# 1. postgresql.conf, then restart. Size max_replication_slots / max_wal_senders for at least one slot.
wal_level = logical
max_slot_wal_keep_size = 20GB        # read section 3 before choosing a value

# 2. Build the writer (shaded jar, about 199 MB; runs its 181 unit tests).
cd java/writer && mvn -q package     # -> java/writer/target/paimon-writer.jar
```

Install the extension. For a system install, run `make install` in `sql/paimon_slot` (standard PGXS; it copies the control file and SQL script into the server's `extension/` directory and needs write access to it). Without
touching the system share directory, stage it and point PostgreSQL at the staging directory, which
is what the test harness does:

```bash
make -C sql/paimon_slot install-local          # copies the control and SQL files into _install/extension/
# postgresql.conf:
extension_control_path = '/path/to/postgres-paimon/sql/paimon_slot/_install:$system'
```

```sql
CREATE EXTENSION paimon_slot;     -- superuser; creates schema "paimon" and publication "paimon_pub"
CREATE TABLE orders (id int PRIMARY KEY, customer text, amount numeric(12,2));
SELECT paimon.add_table('orders'::regclass);
```

Start the writer. It is configured entirely from the environment:

```bash
PGPAIMON_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/mydb \
PGPAIMON_USER=postgres PGPAIMON_PASSWORD=... \
PGPAIMON_WAREHOUSE=/var/lib/paimon/warehouse \
java -jar java/writer/target/paimon-writer.jar
```

| Variable | Default | Meaning |
|---|---|---|
| `PGPAIMON_JDBC_URL` | required | database to replicate (one writer per database) |
| `PGPAIMON_WAREHOUSE` | required | Paimon warehouse: a local path, `hdfs://host:port/wh`, or `s3://bucket/wh` |
| `PGPAIMON_USER`, `PGPAIMON_PASSWORD` | `postgres`, empty | needs the REPLICATION attribute (or superuser) |
| `PGPAIMON_DATABASE` | `default` | Paimon database name the tables are created in |
| `PGPAIMON_SLOT`, `PGPAIMON_PUBLICATION` | `paimon`, `paimon_pub` | |
| `PGPAIMON_COMMIT_USER` | `paimon-writer-<slot>` | **must stay stable across restarts**: replay deduplication is scoped to it |
| `PGPAIMON_SEQ_FIELD`, `PGPAIMON_OP_SEQ_FIELD` | `_seq`, `_op_seq` | mandatory metadata columns (sequencing) |
| `PGPAIMON_XID_FIELD` | unset (**off**) | set to e.g. `_xid` to add the transaction id column |
| `PGPAIMON_FLUSH_MAX_RECORDS`, `PGPAIMON_FLUSH_MAX_INTERVAL_MS` | `10000`, `1000` | batching |
| `PGPAIMON_S3_ENDPOINT`, `PGPAIMON_S3_ACCESS_KEY`, `PGPAIMON_S3_SECRET_KEY` | unset | S3 (path-style access is switched on when an endpoint is given) |

`_xid` is off by default because it is optional (spec section 6). The acceptance harness sets
`PGPAIMON_XID_FIELD=_xid` because test T4 asserts the column exists; do not remove that.

### Order of operations, and why it matters

The writer creates the replication slot itself, on its first start, together with a consistent
snapshot it uses to copy the existing rows of every tracked table. Nothing else ever creates the
slot. Two orders work:

* **Register tables, then start the writer** (the normal case). The first start backfills every
  registered table from an exported snapshot and then streams from the snapshot's LSN: no gap, no
  duplicates, no table lock.
* **Start the writer first, then register tables, which must be empty** (section 5, first item).

Registering a table that already holds rows once the slot exists is the one thing that does not
work, and the reason is not arbitrary; it is explained there.

### Remote warehouses (there is no "offload")

On the TAM architecture a worker wrote Parquet locally, uploaded it, and deleted the local copy.
Here the warehouse URI scheme picks Paimon's `FileIO` and the writer writes straight to it. There
are no `s3_*` / `hdfs_*` settings in PostgreSQL.

* **HDFS**: set `PGPAIMON_WAREHOUSE=hdfs://host:port/path`. Verified end to end (T19, against an
  in-process MiniDFSCluster).
* **S3**: set `PGPAIMON_WAREHOUSE=s3://bucket/wh` plus the three `PGPAIMON_S3_*` variables.
  **S3 is present but unverified in this environment.** `paimon-s3` is bundled in the writer jar
  and its `FileIO` registration survives shading (pointing a catalog at `s3://nobucket/wh` yields a
  connection refusal, not "no FileIO for scheme s3"), but there is no MinIO binary here, so the S3
  acceptance test (T18) has never executed. It reports itself as skipped, never as passed. To run it:

  ```bash
  E2E_MINIO_BIN=/path/to/minio ./test/slot-acceptance/run_acceptance.sh PaimonOffloadTest
  ```

## 3. Operational hazards

### The abandoned slot (read this before production)

A replication slot makes PostgreSQL keep every WAL segment the writer has not yet confirmed. **If
the writer stops and nobody notices, `pg_wal` grows without bound until the disk is full and the
database stops.** This is a new way to take a production database down that the TAM architecture did
not have. (The TAM architecture had the opposite failure: its ring buffer silently dropped data
under overload. Here data is no longer lost; disk is the failure mode.) The same applies to a
writer that is running but cannot keep up.

Monitor it:

```sql
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS behind,
       pg_size_pretty(safe_wal_size) AS safe_wal_size
FROM pg_replication_slots WHERE slot_name = 'paimon';
```

Alert on `active = false` for more than a few minutes, on `behind` growing, and on `wal_status`
leaving `reserved`. (`safe_wal_size` is NULL while `max_slot_wal_keep_size` is -1, the default, and
also after invalidation; NULL does not mean healthy.)

Typical progression, as observed: `active=t, reserved` while healthy, then `active=f` when the
writer is down, then `wal_status` moves from `reserved` (through `unreserved`) to **`lost`** once the
retained WAL exceeds `max_slot_wal_keep_size`.

**Set `max_slot_wal_keep_size`.** It is the only thing standing between a dead writer and a full
disk. Choose it to be comfortably more than the WAL you generate during the longest outage you are
willing to ride out, and less than the free space on the WAL volume. The price: when the limit is
hit PostgreSQL invalidates the slot, and **an invalidated slot cannot be repaired, only
re-bootstrapped**.

**A missing or invalidated slot is fatal to the writer, by design.** On start it exits with, for example:

```
SEVERE: fatal: org.postgresql.util.PSQLException: ERROR: can no longer access replication slot "paimon"
  Detail: This replication slot has been invalidated due to "wal_removed".
```

It does not retry. Nothing but the backfill path ever creates a slot, so retrying could never
resolve the condition; a retry loop would only hide it from whoever is watching the process.

### Re-bootstrapping

This is the single recovery procedure for an invalidated slot, for a replication-identity
accident (section 6), and for adding a non-empty table. It reloads every tracked table from scratch.

1. Stop the writer.
2. In the replicated database:

   ```sql
   SELECT pg_drop_replication_slot('paimon');
   UPDATE paimon.table_registry SET backfill_due = true;
   ```
3. Start the writer. It creates a new slot and snapshot, truncates and reloads every table, then
   streams. (Run as written, this was verified end to end, including after a slot had gone `lost`.)

Changes made between the loss of the slot and the re-bootstrap are not replayed as history; the
reload captures the table's state at the new snapshot. Paimon-side history (the changelog) restarts
from the reload.

A **DDL** statement in that window is a different matter: its message is lost with the slot, so the
Paimon schema would be left permanently behind the registry, and the writer would discard the new
column's value from every row with nothing but a warning. The startup bootstrap therefore
reconciles: a `create` for a table Paimon already has adds the columns the registry declares and
Paimon lacks, one WARNING per column —

```
WARNING: create for existing table Identifier{database='default', object='d4'}: column 'extra' STRING was missing from its Paimon schema and has been added (a DDL message for it never reached this writer; rows written before now hold NULL there)
```

Only that one direction is reconciled. A column present in both with a different type, a different
primary key, or a column Paimon has that the registry does not, stays a warning and nothing more:
neither side can be shown to be right, and guessing would be worse than saying so.

The writer prints this same recipe when it refuses to start (section 5). One caveat: the reload
copies each table using the name recorded at registration, so if a tracked table has been renamed
since (section 5), rename it back first or the reload fails with `relation "public.<name>" does not exist`.

### Other operational notes

* The writer is one process per slot. Multi-writer and HA are non-goals.
* The shaded jar is about 199 MB (Paimon, Hadoop client, `paimon-s3`).
* The writer accumulates a whole transaction before flushing it and never writes a partial one
  (pgoutput `streaming` is off on purpose), so very large transactions need heap.
* Stopping the writer with SIGTERM is safe at any time. A hard kill mid-batch is also safe: work
  that was not committed to Paimon is replayed from the last confirmed LSN, and replay is idempotent
  because the commit identifier is the transaction's commit LSN under a stable `commitUser`.

## 4. `REPLICA IDENTITY FULL`

`paimon.add_table()` runs `ALTER TABLE ... REPLICA IDENTITY FULL` on the table and a guardrail keeps
it that way.

**Why.** With the default identity an UPDATE logs only the old key. The writer needs the complete
old row for two reasons: to emit a faithful `-U` (before-image) with `changelog-producer = input`,
and to resolve TOASTed columns: an UPDATE that does not touch a large column sends the placeholder
`u` ("unchanged TOAST") for it in the new tuple, and the old tuple (flattened under `FULL`)
carries the real value, which is by definition also the new value. The writer substitutes it. This
is exact at the event's own LSN, with no lookups and no cache.

**What it costs.**

* Every UPDATE and DELETE on the table writes the whole old row into WAL. WAL volume rises, and
  every other logical consumer of the table sees the old row too.
* For a table with a large `jsonb` (or text) column that is updated frequently, the *flattened*
  value of that column is written to WAL on every otherwise unrelated update. That is the
  pathological case: a one-byte update to a status column drags the full document into WAL each
  time. Measure before tracking such a table.
* `add_table()` takes a brief `ACCESS EXCLUSIVE` lock for the catalog change (main's
  `SET ACCESS METHOD` took one too).

**The escape exists in the design but not in v1.** `add_table(..., replica_identity => 'default')`
(batched lookups on the writer for TOAST) and `changelog_producer => 'lookup'` (Paimon rebuilds
before-images itself) are specified in spec section 5, and the registry records the arguments, but
both raise `... is not implemented in v1` for anything other than `'full'` / `'input'`. Without
the first, there is no way to track a table whose old-row WAL you cannot afford.

## 5. Limitations, and why each exists

Each of these is deliberate. Several look like regressions against the TAM architecture until the
reason is known. Do not "fix" one without reading its reason.

### A non-empty table cannot be added once the writer's slot exists; an empty one can

`add_table()` computes `backfill_due` from whether the table has rows, under the
`SHARE ROW EXCLUSIVE` lock it already holds. That lock conflicts with `ROW EXCLUSIVE`, so no
concurrent write can commit between the emptiness check and the registry row: the check is not racy,
which is why the lock must stay where it is.

* An **empty** table registers fine against a live writer. Backfill exists to copy rows committed
  before streaming began; an empty table has none, and the `create` message travels through the slot,
  so the running writer tracks the table at once.
* A **non-empty** table is refused, **at the writer's next start, not at `add_table()`**:
  `add_table()` succeeds and the running writer carries on. On restart the writer exits with

  ```
  slot 'paimon' exists and 1 of 2 tracked table(s) are awaiting their initial backfill: a table added while the writer's slot exists cannot be loaded consistently. To reload everything: stop the writer, then run SELECT pg_drop_replication_slot('paimon'); UPDATE paimon.table_registry SET backfill_due = true; and start the writer again (it creates the slot, truncates and reloads every table)
  ```

**Why.** A correct backfill reads a snapshot taken at a known LSN and must then suppress that
table's streamed changes from before that LSN. The slot's stream is shared by every tracked table
and is already being consumed, so the writer cannot position one table independently. The
alternative to refusing is a Paimon table that silently begins mid-history, which is worse than a
loud refusal.

**Recourse.** Stop the writer, drop and recreate the slot, let the first start backfill everything
together: the re-bootstrap procedure in section 3. (A crash after some tables were marked backfilled but
before others leaves the same kind of state and needs the same treatment.)

### A deferrable primary key is refused

`add_table()` (and any later DDL on a tracked table) rejects a `DEFERRABLE` primary key:

```
ERROR:  paimon_slot: the primary key of doc_def must not be DEFERRABLE
HINT:  a deferred key permits transient duplicate keys that a Paimon primary-key table cannot represent; see design spec section 7
```

**Why.** A deferred key lets one transaction pass through states in which two rows share a key,
for example `UPDATE t SET id = id + 1` over contiguous keys. Each row decomposes to `-D(old key)`
plus `+I(new key)`, and ordered by LSN the delete of key 2 follows the insert that filled key 2, so
the delete wins and only the last key survives. A Paimon primary-key table cannot represent two
rows with one key at any LSN ordering, so this cannot be fixed on the writer side. On a
non-deferrable key PostgreSQL itself rejects the ascending cascade with a unique violation (so it
never reaches WAL), and a descending cascade vacates each key before filling it and converges
correctly. The refusal is what makes that safe. (Test T20.) The writer's own obligation is the
other half: it never reorders, collapses or deduplicates within a batch.

### `ADD COLUMN ... DEFAULT` is rejected

```
ERROR:  paimon_slot: ADD COLUMN with a DEFAULT is not supported (public.doc_t.d)
HINT:  add the column without a default, then UPDATE it
```

**This looks like a capability the TAM architecture had. It was not.** Main appeared to accept
`ADD COLUMN ... DEFAULT` but silently left every pre-existing row NULL in Paimon for that column
(`gaps.md` item 2 on main). The PostgreSQL table says `d = 1`, the Paimon table says NULL, and
nothing signals the divergence. Refusing is strictly more honest than the behaviour it replaces.

**Why it is not simply implemented.** With `merge-engine = deduplicate` there is no cheap way to
set one column on existing records: writing a column means rewriting whole rows, because nothing
short of the partial-update engine can write a column in isolation, and partial-update was rejected
for correctness reasons (it treats an incoming NULL as "no change" for every column, so
`UPDATE t SET note = NULL` would be dropped). A full table refill would be correct but is O(table)
for a DDL that PostgreSQL performs in O(1) via fast defaults; it was not imposed silently.
Detection is on `pg_attribute.atthasdef` of the new column at `ddl_command_end`, more reliable than
main's parse-tree walk.

**Workaround** (the one main already needed): `ALTER TABLE t ADD COLUMN d int;` then
`UPDATE t SET d = 1;`. The update streams as ordinary DML and the Paimon rows are correct.

Also rejected, for related reasons: `ADD COLUMN ... NOT NULL` without a default
(`paimon_slot: ADD COLUMN ... NOT NULL without a default is not supported (public.t.n)`; a new
column cannot be NOT NULL against existing Paimon files) and any change to the primary key
(`paimon_slot: cannot change the primary key of public.t`, hint: the Paimon table key is fixed at
`add_table()` time). These are the same as main.

### A primary-key column's type cannot change, and no column can gain NOT NULL

```
ERROR:  paimon_slot: cannot change the type of primary-key column public.key_t.id (INT -> BIGINT)
HINT:  Paimon cannot alter the type of a primary-key field; the key is fixed at add_table() time, so re-register the table (paimon.remove_table, drop the Paimon table, paimon.add_table) to change it
```

```
ERROR:  paimon_slot: SET NOT NULL on a replicated column is not supported (public.key_t.n)
HINT:  rows already written to Paimon may hold NULL there and Paimon never rewrites them, so the column must stay nullable; re-register the table to change it
```

Both are refusals in PostgreSQL because Paimon refuses them in the writer, and by then PostgreSQL
has already committed: `UnsupportedOperationException: Cannot update primary key` and
`Cannot update column type from nullable to non nullable` (measured, Paimon 1.4.1). A transaction
that is in the slot and can never be applied stops replication for the whole database on every
restart (section 3, "A record the writer can never apply"), so the only safe place to stop these is
before they reach WAL. `ALTER COLUMN <pk> TYPE bigint` previously passed every guardrail, because
the primary-key guardrail compares column *names* and the names do not change.

For the NOT NULL case Paimon offers the table option
`alter-column-null-to-not-null.disabled = false`. **It is deliberately not used.** Paimon rewrites
only the declared schema, never the data files, so every row already written — the initial load, and
every `-U` before-image in the changelog — may legitimately hold NULL in that column, and a schema
that declares NOT NULL over them is a silent lie to every reader. PostgreSQL's own `SET NOT NULL`
validates the live table, not that history. (The option is also per table and read from the stored
options, so enabling it would not even cover tables created by an earlier run.)

What still works: `ALTER COLUMN ... TYPE` on any non-key column, and `DROP NOT NULL` (Paimon accepts
that direction; verified). To change a key's type, re-register the table.

### `REPLICA IDENTITY` must stay FULL on a tracked table

```
ERROR:  paimon_slot: replica identity of a replicated table must stay FULL
HINT:  the writer needs the complete old row; see design spec section 5
```

The guardrail checks `pg_class.relreplident` after the statement, so every spelling is caught. The
reason is in section 4. If the identity is lowered anyway (for instance by disabling the event
trigger), the WAL written in that window has no old row and the writer stops with the `ToastResolver`
error in section 6. Restoring `FULL` afterwards does **not** unwedge it: the offending WAL record is
already in the slot and will be replayed on every restart. Re-bootstrap (section 3). The guardrail
exists because the alternative is worse: without an error, a lost before-image would silently corrupt
the changelog.

### Paimon's reserved field names are refused as column names

`_VALUE_KIND`, `_ROW_ID`, `rowkind`, `_LEVEL`, `_SEQUENCE_NUMBER` (compared case-insensitively):

```
ERROR:  paimon_slot: column name "rowkind" is reserved by Paimon and cannot be replicated
HINT:  choose a different column name
```

Paimon 1.4.1 rejects any schema containing one, and the writer would fail at table creation. The
check runs in `add_table()` and on `ADD COLUMN` / column rename. Rename the column in PostgreSQL.

### `ALTER TABLE ... RENAME TO` deliberately does nothing

The Paimon table keeps the name it was registered under (tracking is keyed on the table's OID, so
replication continues uninterrupted). A rename emits no message and changes neither the registry nor
the warehouse.

**Why.** The Paimon table's identity is fixed at registration. Following the rename would orphan the
data already written under the old name, or require moving a table in the warehouse and re-pointing
every reader. Consequence to be aware of: the PostgreSQL name and the Paimon name diverge, and a
re-bootstrap copies by the registered name, so rename the table back first (section 3).

### Metadata columns differ from the TAM architecture

* `meta_field_tid` has **no counterpart**. The physical `(block << 16) | offset` of a tuple is not
  carried by logical decoding, and no physical tuple identifier survives the reorder buffer. It is
  replaced, not emulated.
* `_tid` is replaced by **`_seq`**: the per-change LSN (read from the `XLogData` header), which is
  monotone for the life of the cluster (main's `xid << 32 | op` is not monotone across XID wraparound).
  It is the first `sequence.field`.
* `_op_seq` (the ordinal per emitted record) is now **mandatory**: it orders a `-U` before the `+U`
  from the same change, which share a `_seq`.
* `_xid` is optional and off by default (`PGPAIMON_XID_FIELD`).
* The `paimon_heap.meta_field_*` GUCs do not exist.

### Other differences from the TAM architecture

* `wal_level = logical` is mandatory.
* Unlogged and temporary tables produce no logical changes and cannot be tracked:
  `ERROR:  cannot add relation "u" to publication` (`This operation is not supported for unlogged tables.`).
* Only ordinary tables are accepted. A partitioned table is refused outright
  (`ERROR:  paimon_slot: p is not an ordinary table (relkind p)`); the design calls for
  `publish_via_partition_root` to be set deliberately before partitioned tables are supported, and v1
  does not do that. Track a partitioned table's leaf tables individually, or not at all.
* A primary key is **required** (main permits PK-less append-only tables). A PK-less table can
  stream with `REPLICA IDENTITY FULL`, but then a row is identified only by its full old value, which
  is not a key, and `deduplicate` has nothing to deduplicate on. Deferred to a follow-up spec.
* `merge-engine` is fixed at `deduplicate`; `changelog-producer = input`.
* `VACUUM`, `VACUUM FREEZE` and `CHECKPOINT` emit no logical changes, so they produce no Paimon records.
* `remove_table()` stops tracking and never deletes Paimon data. `DROP TABLE` of a tracked table
  *does* drop the Paimon table (this fixes main's `gaps.md` item 3), and dropping a column drops it
  from the Paimon schema too (item 1).
* Binary-mode pgoutput decoding is not implemented; text mode is exact for the supported types.
* There is no integrated "offload" step (section 2). `PaimonE2ETest` (PostgreSQL to Paimon to Spark
  compaction) is **deliberately not ported** to this architecture. It is not in the spec's acceptance
  list, and naming it to the harness fails the run by design ("matched no tests"). That is a
  deliberate exclusion, not a known bug.

## 6. Troubleshooting

The exact text of the errors a user can actually hit.

**`add_table` on a table without a primary key**

```
ERROR:  paimon_slot: doc_nopk has no primary key
HINT:  a primary key is required; see design spec section 4
```

Add a primary key first. See section 5 for why PK-less tables are out of scope.

**`ADD COLUMN ... DEFAULT`**

```
ERROR:  paimon_slot: ADD COLUMN with a DEFAULT is not supported (public.doc_t.d)
HINT:  add the column without a default, then UPDATE it
```

Workaround: add the column without a default, then `UPDATE` it (section 5 explains why this is a
refusal and not a missing feature).

**`REPLICA IDENTITY FULL` (raised by the writer, in `ToastResolver`)**

The writer logs this as `SEVERE: fatal:` and exits, on the first UPDATE or DELETE it decodes for a
table whose WAL carries no usable old row:

```
table public.doc_t2 sent no old tuple for an UPDATE/DELETE: its replica identity is not FULL (its replica identity is not FULL: check pg_class.relreplident, which must be 'f'; without it the writer cannot produce a faithful before-image; the table must be at REPLICA IDENTITY FULL)
```

(`sent no old tuple` becomes `sent a key-only old tuple` when the identity is the default and the
key changed.) First `SELECT relreplident FROM pg_class WHERE oid = 'doc_t2'::regclass;` (expect
`f`). Set it back with `ALTER TABLE doc_t2 REPLICA IDENTITY FULL`. Because the bad WAL record is
already in the slot, the writer will fail again on restart; recover with the re-bootstrap procedure
in section 3. Seeing this error means the guardrail was bypassed: the usual cause is an event trigger
disabled around a maintenance window.

**Writer refuses to start: tables awaiting backfill** and **slot invalidated** are both in sections 3
and 5, with their exact text.

## 7. Testing and verification

```bash
cd java/writer && mvn -q package                 # 181 unit tests, builds the shaded jar
( cd sql/paimon_slot && make install-local && make installcheck )   # pg_regress against a running cluster on 5441
./test/slot-acceptance/run_acceptance.sh [Suite ...]
```

The harness (`test/slot-acceptance/`) needs no Docker. It builds its own PostgreSQL 18 cluster on
port 5442 (leaving a cluster on 5441 alone), installs the extension from the worktree, runs the writer
jar locally, runs the JDBC suites and exits with their status. With no arguments it runs every
`*IT` suite; name the others (`PaimonFeatureTest`, `PaimonDdlTest`, `PaimonAdvancedTest`,
`PaimonTypeTest`, `PaimonSlotKeyMoveTest`, `PaimonOffloadTest`, `PaimonSchemaIT`). A suite that
matches no tests fails the harness; that is how `PaimonE2ETest` fails.

Status of the remote warehouses: **T19 (HDFS) passes; T18 (S3) is not run** here, because it needs
a MinIO server (`E2E_MINIO_BIN`). The harness says so in its verdict (`ALL SUITES PASSED -- except:
T18 (S3) NOT RUN`); a skip is never counted as a pass.
