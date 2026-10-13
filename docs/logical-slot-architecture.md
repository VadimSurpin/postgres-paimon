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
commits them to Paimon in **batches** of several whole transactions, using the commit LSN of the
batch's last transaction as the commit identifier. Only after a Paimon commit succeeds does the
writer confirm that transaction's end LSN to PostgreSQL, so a crash replays rather than loses (and
replays are idempotent, see "Commit batching").

## 2. Setup

Requirements: PostgreSQL 18 (the writer connects with `assumeMinServerVersion=18`), `wal_level = logical`,
a JDK and Maven to build the writer.

```bash
# 1. postgresql.conf, then restart. Size max_replication_slots / max_wal_senders for at least one slot.
wal_level = logical
max_slot_wal_keep_size = 20GB        # read section 3 before choosing a value

# 2. Build the writer (shaded jar, about 199 MB; runs its 279 unit tests).
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
| `PGPAIMON_SLOT`, `PGPAIMON_PUBLICATION` | `paimon`, `paimon_pub` | |
| `PGPAIMON_COMMIT_USER` | `paimon-writer-<slot>` | **must stay stable across restarts**: replay deduplication is scoped to it |
| `PGPAIMON_SEQ_FIELD`, `PGPAIMON_OP_SEQ_FIELD` | `_seq`, `_op_seq` | mandatory metadata columns (sequencing) |
| `PGPAIMON_XID_FIELD` | unset (**off**) | set to e.g. `_xid` to add the transaction id column |
| `PGPAIMON_BATCH_MAX_ROWS` | `10000` | **streaming** commit batch: change records (an UPDATE is two) buffered before a commit. See "Commit batching" |
| `PGPAIMON_BATCH_MAX_BYTES` | `16777216` (16 MiB) | streaming commit batch: estimated size of the buffered text payload |
| `PGPAIMON_BATCH_MAX_INTERVAL_MS` | `2000` | streaming commit batch: longest a transaction waits for its commit, counted from the batch's **first** transaction. Bounds replication latency |
| `PGPAIMON_FLUSH_MAX_RECORDS` | `10000` | rows per COPY chunk during the **initial load** only (each chunk is a commit of its own). **Not** the streaming batch size: that is `PGPAIMON_BATCH_MAX_ROWS`. The two do not interact |
| `PGPAIMON_SKIP_LSN` | unset | emergency escape for a single poison record: the change LSN of the ONE record to drop. Read section 3, "A record the writer can never apply", before setting it |
| `PGPAIMON_S3_ENDPOINT`, `PGPAIMON_S3_ACCESS_KEY`, `PGPAIMON_S3_SECRET_KEY` | unset | S3 (path-style access is switched on when an endpoint is given) |

`_xid` is off by default because it is optional (spec section 6). The acceptance harness sets
`PGPAIMON_XID_FIELD=_xid` because test T4 asserts the column exists; do not remove that.

### Commit batching

A PostgreSQL transaction is **not** one Paimon commit. The writer accumulates completed
transactions and commits them together, because a Paimon commit costs a snapshot, a manifest and
at least one file per touched bucket however small the change: committing every transaction left
the ten-row TPC-C `district` table with 1,664 snapshots and 12,391 files (see the measured
before/after below). The batch is flushed when **any one** of three limits is reached:

| Limit | Default | Counted |
|---|---|---|
| `PGPAIMON_BATCH_MAX_ROWS` | 10,000 | change records buffered (an UPDATE is a `-U` and a `+U`: two) |
| `PGPAIMON_BATCH_MAX_BYTES` | 16 MiB | an estimate of the buffered text payload: the character count of every non-NULL value plus 48 bytes per record |
| `PGPAIMON_BATCH_MAX_INTERVAL_MS` | 2,000 | milliseconds since the batch's **first** transaction was added; later traffic does not postpone it |

**The trade-off.** Larger limits mean fewer snapshots and files (cheaper read planning, a smaller
`manifest/` and `snapshot/`, less compaction) and a longer wait before a committed PostgreSQL
change is visible in Paimon: replication latency is bounded by `PGPAIMON_BATCH_MAX_INTERVAL_MS`
plus the time to commit (plus up to ten seconds before the server learns the position was
confirmed, which only affects WAL retention, not visibility). Smaller limits mean the opposite.
At the default, a table that is written continuously gets at most one snapshot per two seconds
however many transactions arrive, and a row is visible within about two seconds.

**Operator tuning.**
* Need rows visible sooner: lower `PGPAIMON_BATCH_MAX_INTERVAL_MS` (it can be `0`, which commits as
  soon as a transaction completes). The snapshot count rises with it.
* Seeing too many small files or snapshots: raise the interval, not the row limit. On a steady
  stream the interval is what closes batches; the row and byte limits are the memory ceiling for
  bursts.
* Memory: the buffer holds at most the row and byte limits **plus the one transaction that crossed
  them**, and the transaction being assembled. A limit is checked between whole transactions and
  never splits one, so a single huge transaction is committed whole regardless of the limits.
* `PGPAIMON_BATCH_MAX_ROWS=1` reproduces the previous behaviour exactly, one Paimon commit per
  PostgreSQL transaction. It is the regression switch and the A/B baseline.
* Each limit has a minimum: rows and bytes at least 1, the interval at least 0. Anything else
  refuses to start.

**What the commit looks like.** The commit identifier is the commit LSN of the **last**
transaction in the batch, and the position confirmed to PostgreSQL is that transaction's end LSN,
sent only after the commit has returned. Transactions that are buffered but not yet committed are
never confirmed. Empty transactions (DDL only, or touching only untracked tables) with nothing
buffered are confirmed at once with no snapshot; behind buffered data they queue with it.

**A batch never spans a schema change or an overwrite.** Records are built against the table's
schema as it is when the batch is flushed, so a batch holding records from before and after an
`ALTER TABLE` would have inconsistent shapes under one commit. So the writer flushes the buffer
**before** it applies a DDL message, **before** it applies a `TRUNCATE` (the overwrite keeps
today's order: after every earlier append, before every later one) and before it accepts a
changed relation description. The remainder of a truncating transaction starts a new buffer.

**Why the grouping is not deterministic, and what the writer does about it.** A size limit is a
function of the stream: replayed, it regroups identically. The time limit is not. Suppose a batch
of T1..T10 commits under identifier L10 and the process dies before the server learns the position
was confirmed. After the restart the same transactions are replayed, but the timer fires at a
different moment and produces T1..T12 under L12. Paimon's `filterAndCommit` discards a commit whose
identifier is **at or below the highest one this commit user has committed** (measured on 1.4.1:
with 10 committed, 7, 10 and 11 after 12 are discarded; 12 after 10 is committed). A regrouping
that ends lower is therefore filtered, but L12 is higher than L10, so it commits, and T1..T10 are
in the table twice. The merged table survives, since `deduplicate` on `(_seq, _op_seq)` collapses
identical records, but `changelog-producer = input` writes **every** input record, so the changelog
would carry the duplicates and effectively-once would silently become at-least-once exactly where
`-U == +U` and `+I - -D == row count` are asserted.

The rule that prevents it: **when a batch is assembled, a transaction's records for a table are
dropped if the transaction's commit LSN is at or below that table's highest committed identifier.**
It is per table because the writer commits table by table: a transaction touching A and B, where A
is already committed past it and B is not (a crash between the two commits), applies to B alone.
The identifier is read from Paimon once per table, kept current in memory after every commit and
truncate, and re-read after any DDL message. Backfill and truncate identifiers count too, since
they are snapshots of the same commit user. A test pins it against the real Paimon: without the
rule, the T1..T10 then T1..T12 replay leaves 22 inserts in the changelog for 12 rows.

**Failure behaviour is unchanged.** An exception from the commit is fatal and nothing after it is
confirmed. On connection loss the buffer is discarded (complete but unconfirmed transactions
replay from the last confirmed position). On a clean stop (SIGTERM) the buffer is committed and
confirmed before the final status update. There is no timer thread: the single consumer thread
checks the interval on every event and on an idle callback roughly every 10 ms, which keeps the
ordering and exception rules above checkable.

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

### Warehouse layout: one Paimon database per PostgreSQL schema

A replicated table lands at

```
<warehouse>/<postgres schema>.db/<table>
```

so `public.orders` is `<warehouse>/public.db/orders` and `billing.orders` is
`<warehouse>/billing.db/orders`. The Paimon database is the PostgreSQL schema; the writer creates
it on demand. This is why two same-named tables in different schemas can both be replicated — with
the schema dropped from the identity they resolved to one Paimon table and interleaved into it,
two tables' rows merging on a shared primary key.

**There is no Paimon database setting.** The schema names the database, so there is nothing left
for one to name; `PGPAIMON_DATABASE` was removed rather than left parsed and ignored.

**Two consequences worth knowing.**

* **This differs from the TAM architecture**, which puts every table in `default.db`. A reader
  pointed at a warehouse from the other architecture needs a different path.
* **A warehouse written by an earlier build of this branch is not readable by this one**, because
  its tables are under `default.db`. Nothing is released, so there is no migration: start from a
  fresh warehouse, or move each `default.db/<table>` directory to `<schema>.db/<table>` yourself
  and fix the catalog's database directories. Re-bootstrapping into a new warehouse is the
  supported route. **The same applies to bucketing** (next section): a table created by an earlier
  build has no `bucket` option and stays in dynamic mode, so it keeps its key index. Only tables
  created from now on are fixed-bucket, and re-bootstrapping is again the remedy.

### Bucketing: fixed, with a count you can set

Every replicated table is created with a fixed `bucket` count, so Paimon hashes the primary key to
choose the bucket. The alternative, leaving `bucket` unset, puts the table in Paimon's *dynamic*
bucket mode, which maintains a **key index** to remember where each key went. A TPC-C run billed
that index at **1.2 GB of a 1.8 GB warehouse — 63%** — with `order_line` holding 1.1 GB on disk for
88 MB of data. Fixed bucketing removes it outright.

**Setting the count.** `paimon.add_table()` resolves it once, at registration:

```sql
SELECT paimon.add_table('orders'::regclass, buckets => 8);   -- explicit, any positive integer
SELECT paimon.add_table('orders'::regclass);                 -- guessed; see below
```

The resolved value is recorded in `paimon.table_registry.buckets` and carried to the writer, so a
restart recreates the table with the same count.

**The guess, and why it is mostly its floor.** With no `buckets` argument the count is
`clamp(4, ceil(pg_total_relation_size / 1 GiB), 64)`. Be clear-eyed about what that gives you: the
ordinary registration path **requires an empty table**, where the size is 16 kB and tells you
nothing, so you get 4. The floor is there so a new table is not a single-bucket hotspot; the
ceiling because a single-threaded writer gains nothing past it but open files per commit. The size
only says anything when you re-register a populated table after `remove_table()`. **If you know the
table will be large, pass `buckets => N`.** As a rough guide, aim for a few hundred MB to a GB of
data per bucket.

**Changing it afterwards** is possible — `bucket` is an ordinary table option, alterable from a
compute engine — but **Paimon does not redistribute existing data**, so the old rows stay in the
buckets they are in and only new writes follow the new count. Treat a change as needing a rewrite
of the table (re-bootstrap), not as a free knob. A wrong guess is therefore recoverable, not
permanent.

An emptied database directory is left in place when its last table is dropped. That is harmless —
Paimon re-uses it, and `createDatabase` is idempotent — and removing it would be presumptuous while
the PostgreSQL schema still exists.

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
reconciles, **when it finds no slot**: it adds the columns the registry declares and Paimon lacks,
one WARNING per column —

```
WARNING: create for existing table Identifier{database='default', object='d4'}: column 'extra' STRING was missing from its Paimon schema and has been added (the slot is absent, so its DDL message is lost, not pending; rows written before now hold NULL there)
```

**Only when the slot is absent**, because that is the only way a message can have been lost. With
the slot still there the same difference means the opposite — at startup the registry is always
ahead of the stream by everything unconsumed, so the column is *pending*, not lost, and the
writer says so and leaves it to the message:

```
WARNING: create for existing table Identifier{database='default', object='r'} differs from its Paimon schema: column 'memo' is missing from Paimon; its DDL message is still in the slot and will apply it
```

(Reconciling there would fabricate the column and the pending `rename_col` would then collide with
it, which is what an ordinary `RENAME COLUMN` performed while the writer was stopped used to do.)

Only that one direction is reconciled, and only then. A column present in both with a different
type, a different primary key, or a column Paimon has that the registry does not, stays a warning
and nothing more: neither side can be shown to be right, and guessing would be worse than saying
so.

A lost **rename** therefore leaves the pre-rename column behind, beside the correctly reloaded new
one, and that deserves more than a shrug: **it reads NULL for every row and every `SELECT *`
downstream will return it.** It gets its own warning at every start —

```
WARNING: table Identifier{database='default', object='o'} has a column 'note' that paimon.table_registry does not: it is NOT maintained, reads NULL for every row, and any SELECT * will return it. ... Check it holds nothing you need, then remove it from a compute engine: ALTER TABLE `default`.`o` DROP COLUMN `note`
```

The writer does not drop it, because the registry not naming a column is not proof that no reader
wants it — a column could predate registration, or be maintained by something else. Check it holds
nothing you need and run the `ALTER TABLE` the warning prints from Spark or Flink.

The writer prints this same recipe when it refuses to start (section 5). A table renamed since
registration needs nothing extra: the reload resolves the source table by its OID, so it copies the
table under its current name while the Paimon table keeps the name it was registered under.

### A record the writer can never apply (a poison record)

**This is an unsolved gap, not a bug with a workaround.** One ordinary INSERT can permanently stop
replication for the whole database:

```sql
CREATE TABLE v (id int PRIMARY KEY, amt numeric(10,2));
SELECT paimon.add_table('v'::regclass);
INSERT INTO v VALUES (2, 'NaN');
```

`numeric 'NaN'` has no Paimon representation. The writer refuses it rather than inventing a value
(a silently wrong number is worse), which is right, but the refusal is fatal and the
failure is permanent:

```
SEVERE: fatal: java.lang.IllegalStateException: POISON RECORD in public.v (INSERT at _seq 0/14CD98C8), transaction committing at 0/14CD9958: column 'amt': cannot parse DECIMAL(10, 2) from "NaN": numeric NaN and Infinity have no Paimon representation. THIS IS NOT TRANSIENT and will not clear by itself: the change is already in replication slot 'paimon', the slot advances only after a successful Paimon commit, so every restart replays this same record and fails identically. Deleting or correcting the row in PostgreSQL does NOT help -- the offending WAL record stays in the slot. ...
```

**`DELETE FROM v WHERE id = 2` does not help** (verified: the restart after the delete exits 1 with
the same message). The slot only advances past a transaction after that transaction's Paimon commit
has returned, so the record is replayed on every start, for ever, and no later DML can remove it
from the slot.

The same applies to any value PostgreSQL stores happily that Paimon cannot represent. Two classes
of value reach it:

* **`numeric` NaN**, and a `numeric` whose digits do not fit the target `DECIMAL(p, s)`.
* **A `timestamp` above `294247-01-10 04:00:54.775807`.** That is the largest instant Paimon can
  write: `Timestamp.toMicros()` is `multiplyExact(millisecond, 1000) + nanoOfMillisecond / 1000`
  and Parquet's writer calls it for every row, so one microsecond later the arithmetic either
  throws or silently wraps. PostgreSQL's own maximum, `294276-12-31 23:59:59.999999`, is about
  thirty years higher, so **a range of perfectly legal PostgreSQL timestamps cannot be stored**
  and is refused here. Dates are not affected: PostgreSQL's whole date range fits Paimon's int,
  with about 6700 years to spare. `infinity`, `-infinity`, BC years and years above 9999 are all
  handled and never refused — but the maximum is a real bound, not a formality.

**No guardrail can prevent this one.** `add_table()` refuses column *types* it cannot map, and
refuses a `numeric` without explicit precision or above 38 digits, but a `numeric(10,2)` column is
perfectly mappable and can still be given the value `NaN`. The check can only happen on the write
path, by which time PostgreSQL has committed and the record is in the slot.

#### Recovery: skip the one record, `PGPAIMON_SKIP_LSN`

The error itself tells you the two recoveries and offers the cheap one first. It drops **one
record** — not the transaction, which commits normally around it — and it logs the record's
complete raw values so you can put the row back by hand. The loss is recoverable, not merely
survivable.

1. **Read the error.** It names the table, the column, the value, the change LSN and the commit
   LSN, and prints the variable to set with the LSN already in it:

   ```
   ... POISON RECORD in public.v (INSERT at _seq 0/1C79B718), in the commit batch ending at 0/1C79B830: column 'amt': cannot parse DECIMAL(10, 2) from "NaN" ... set PGPAIMON_SKIP_LSN=0/1C79B718 and restart ...
   ```
2. **Inspect the row in PostgreSQL** before you discard anything: `SELECT * FROM v WHERE id = 2;`.
   Correct the value if you can. That does not unblock the slot — the old record is still in it —
   but it stops the next write of that row poisoning the slot again.
3. **Set the variable to that exact LSN and restart.** The writer announces it is armed, drops that
   one record, and streams on:

   ```bash
   PGPAIMON_SKIP_LSN=0/1C79B718 java -jar java/writer/target/paimon-writer.jar
   ```
4. **Keep the audit line.** At WARNING, once the surrounding transaction has committed:

   ```
   WARNING: SKIPPED POISON RECORD (PGPAIMON_SKIP_LSN=0/1C79B718): public.v (relation 28531) INSERT at _seq 0/1C79B718, xid 5490: column 'amt': cannot parse DECIMAL(10, 2) from "NaN": numeric NaN and Infinity have no Paimon representation; the record's complete raw values, in relation column order [id, amt, note], were ["2", "NaN", "poison"], in the transaction committed at 0/1C79B830. The rest of that transaction WAS applied. ...
   ```

   Reinsert the row from those values with the offending column corrected. Nothing will replay it.
5. **Unset it** and restart. Leaving it set is not dangerous — the startup line then reads
   `is at or below the resume position ... so it can never fire` — but a forgotten escape hatch is
   still a forgotten escape hatch.

**What keeps this from being an "ignore errors" switch**, which is the only reason it is safe to
offer at all:

* **One LSN, never a range.** A range would let a single mistyped digit discard arbitrary committed
  data, silently.
* **One record, not the transaction.** Everything else committed alongside it is applied. (An
  UPDATE decomposes to two records sharing one change LSN, so the change to that one *row* may be
  two records; each is logged.)
* **Only an unrepresentable value.** It fires solely for a value PostgreSQL holds and Paimon has no
  representation for. A commit failure, a schema error, an arity mismatch, or a value that simply
  does not *parse* as its Paimon type — a drifted column, or `bytea_output = 'escape'` — at the very
  same LSN all stay fatal. Those are systematic faults that would mis-handle every row of a column,
  and skipping one would hide the cause; the writer says so instead ("a systematic mismatch").
* **The raw values are logged in full**, which is what makes the row recoverable.
* It never applies to the initial load, which does not go through this path.

Verified end to end: a three-row transaction with the poison record in the middle, skipped; the
other two rows present, the record gone, streaming continued. A *wrong* skip LSN still exits 1 on
the same record, and a stale one is announced rather than silently armed.

**The remaining recourse** is the re-bootstrap procedure above, for anything the skip does not
cover. It works, at the price it always costs: every tracked table is reloaded from a fresh snapshot
and Paimon-side changelog history restarts.

**What is still missing.** There is no quarantine: nothing records the skipped record durably for a
reader to find (the log line is the whole audit trail), and nothing automates the decision — an
operator must read the error and act. A dead-letter table is the next step and is not in v1. So
keep reading spec section 8 with the correction it now carries: a failed Paimon commit is replayed
on retry, but a replay that can never succeed stops the writer until somebody intervenes. Watch for
`SEVERE: fatal:` and alert on the writer being down (section 3, first item).

### Other operational notes

* Each replicated Paimon table carries a `pgpaimon.applied-ddl-lsn` option: the WAL LSN of the
  last DDL message applied to it, in decimal (the writer logs it both as decimal and as
  `X/XXXXXXXX`). It is the writer's DDL commit identifier — the counterpart of the commit
  identifier that makes a replayed *data* transaction a no-op — and it is written in the same
  atomic schema change as the column alteration itself. Do not edit or remove it: a lower value
  replays applied DDL, and a higher one skips DDL that was never applied.
  It also covers `create` and `drop_table`, not only the column operations: a Paimon table is
  identified by name, so a PostgreSQL table dropped and recreated under the same name resolves to
  the same Paimon table, and a `drop_table` whose LSN is at or below what the table records is
  refused as stale rather than purging a table it was never about.
  **It carries no cluster or timeline identity**, which matters in exactly one situation: point a
  warehouse at a *different* PostgreSQL server, or at one restored by PITR or re-`initdb`'d, and
  its LSNs start again from a lower point, so DDL below the recorded value is skipped as a replay
  it is not. Nothing detects this. A warehouse belongs to one cluster's timeline; re-bootstrap into
  a fresh warehouse rather than reusing one across a restore.
* The writer is one process per slot. Multi-writer and HA are non-goals.
* The shaded jar is about 199 MB (Paimon, Hadoop client, `paimon-s3`).
* The writer accumulates a whole transaction before flushing it and never writes a partial one
  (pgoutput `streaming` is off on purpose), so very large transactions need heap. Completed
  transactions then wait in the commit batch (see "Commit batching") up to its row, byte and
  interval limits.
* Stopping the writer with SIGTERM is safe at any time, and commits and confirms whatever is
  buffered on the way out. A hard kill is also safe, mid-buffer or between a commit and its
  confirm: work that was not committed to Paimon is replayed from the last confirmed LSN, and
  replay is idempotent because a transaction already committed to a table is dropped at assembly
  (see "Commit batching") and the commit user is stable.

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
every reader. Consequence to be aware of: the PostgreSQL name and the Paimon name diverge. A
re-bootstrap handles it — the initial load resolves the source table by its OID, not by the
registered name — so no renaming back is needed.

### `infinity` is stored as the extreme value, which is lossy

`timestamp`/`timestamptz`/`date` `infinity` and `-infinity` have no Paimon counterpart. They are
**encoded as the extreme values**, not refused and not nulled:

| PostgreSQL | stored in Paimon |
|---|---|
| `date 'infinity'` | `5874897-12-31` (PostgreSQL's own maximum date) |
| `date '-infinity'` | `4714-11-24 BC` (PostgreSQL's own minimum date) |
| `timestamp 'infinity'` | `294247-01-10 04:00:54.775807` (the largest instant Paimon can write) |
| `timestamp '-infinity'` | `4714-11-24 00:00:00 BC` (PostgreSQL's own minimum) |

**Why encode rather than refuse.** `infinity` almost always means an open-ended interval —
`valid_to = 'infinity'` — and a query like `valid_to > now()` has to keep matching after
replication. An order-preserving sentinel does that; a NULL or a refusal does not. The ordering
holds over everything a Paimon table can contain, because every value above the positive target is
refused rather than written and nothing below `4714-11-24 BC` exists in PostgreSQL at all.

**What it costs, and this is the part to know.** The encoding is **not lossless**. A row that
genuinely holds one of those four values becomes indistinguishable from `infinity` once it is in
Paimon, where PostgreSQL keeps them distinct. If you store real dates at PostgreSQL's extremes —
unusual, but it happens in generated or sentinel data — do not also rely on `infinity`, or add a
companion boolean column. Nothing detects the collision.

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
cd java/writer && mvn -q package                 # 279 unit tests, builds the shaded jar
( cd sql/paimon_slot && make install-local && make installcheck )   # pg_regress against a running cluster on 5441
./test/slot-acceptance/run_acceptance.sh [Suite ...]
```

The harness (`test/slot-acceptance/`) needs no Docker. It builds its own PostgreSQL 18 cluster on
port 5442 (leaving a cluster on 5441 alone), installs the extension from the worktree, runs the writer
jar locally, runs the JDBC suites and exits with their status. With no arguments it runs **every**
suite — `SlotHarnessSmokeIT`, `PaimonSchemaIT`, `PaimonFeatureTest`, `PaimonDdlTest`,
`PaimonAdvancedTest`, `PaimonTypeTest`, `PaimonSlotKeyMoveTest`, `PaimonOffloadTest`, named
explicitly in the script's `ALL_SUITES` and printed at the start of the run — which is **238
checks**. Name a subset to run just those (plus the smoke suite). A named suite that matches no
tests, or whose every test is skipped, fails the harness; that is how `PaimonE2ETest` fails.

Status of the remote warehouses: **T19 (HDFS) passes; T18 (S3) is not run** here, because it needs
a MinIO server (`E2E_MINIO_BIN`). The harness says so in its verdict (`ALL SUITES PASSED -- except:
T18 (S3) NOT RUN`); a skip is never counted as a pass.

### TPC-C end to end: PostgreSQL and Paimon hold the same state

A separate harness runs a real BenchBase TPC-C workload through the slot and then compares the two
sides row by row. It is the only test here that exercises the streaming write path under a bulk
load and an update-heavy OLTP mix at once.

```bash
./test/tpcc/run_tpcc.sh        # defaults: 1 warehouse, 60s execute at up to 25 tps
```

It needs a JDK 21 as well as the project's 17, because BenchBase targets 21; set `JAVA21_HOME` if
it is not under `/usr/lib/jvm`. Everything else is configurable from the environment (see the
header of the script). Runtime state goes to `/tmp/pgpaimon-tpcc`, never into the worktree, and the
cluster is its own, on port 5443. In CI it is `.github/workflows/tpcc.yml`, **`workflow_dispatch`
only**.

**The order of the phases is the whole design.** BenchBase can create, load and execute
separately, and it has to here:

1. `--create=true --load=false --execute=false` — the stock TPC-C schema, empty.
2. `test/tpcc/setup.sql` — `CREATE EXTENSION`, then `paimon.add_table()` on each of the eight
   replicable tables **while they are still empty and before the slot exists**. The Paimon tables
   come from the `create` messages this emits; nothing in the harness writes the warehouse.
3. the writer starts, creates the slot, and backfills eight empty tables (nothing to copy).
4. `--load=true` — about 540k rows for one warehouse, **through the slot**, not through the
   backfill. That is deliberate: it is the only bulk-insert test of the streaming path.
5. `--execute=true` — the stock mix (NewOrder 45, Payment 43, OrderStatus 4, Delivery 4,
   StockLevel 4), which is where the updates and the `new_order` deletes come from.

Then the harness waits until `confirmed_flush_lsn` has passed the WAL position reached when the
workload stopped, stops the writer with SIGTERM, and runs the comparison.

**BenchBase and its TPC-C schema are used exactly as they ship.** No surrogate key, no added
column, no DDL edit, no patch: the workload stays recognisably TPC-C.

**`history` is not replicated, and that is the primary-key requirement working as specified.**
TPC-C's `history` table has no primary key — two foreign keys and nothing else — and `paimon_slot`
requires one on every replicated table, because the write target is a Paimon primary-key table and
without a key there is nothing to merge on. `add_table()` refuses such a table on purpose
(`paimon_slot: history has no primary key`), so eight of the nine tables are registered and
`history` reaches neither the publication nor the warehouse.

This is a genuine, visible consequence of the design, not an oversight and not a workaround, so
**do not "fix" it by giving `history` a key.** The harness treats the exclusion as a property to
test rather than an assumption, in two places:

* `setup.sql` asserts `history` is unregistered, unpublished and still key-less, and then calls
  `paimon.add_table('history')` and requires it to fail with the primary-key error — so the refusal
  is exercised, not merely relied upon;
* the comparison asserts `history` is untracked, absent from `paimon_pub`, absent from the Paimon
  catalog and absent from `<warehouse>/public.db/`, **while holding rows**. Payment inserts into
  `history` throughout the run, so this is a live demonstration that a table never passed to
  `add_table` stays genuinely untouched under a busy workload — the guarantee acceptance test T17
  checks in miniature, here at TPC-C scale.

What the exclusion costs is one table's breadth, not a class of behaviour: `history` is insert-only
in TPC-C, and the eight replicated tables exercise inserts, composite-key updates and deletes far
harder. Their keys are one to four columns wide (`order_line` is four), so composite keys are well
covered.

**What the comparison asserts** (`java/src/test/java/pro/surpin/pgpaimon/TpccCompare.java`, run as
`java -cp java/target/paimon-reader-1.0.jar pro.surpin.pgpaimon.TpccCompare <warehouse> <jdbc-url>
[schema]`):

* the Paimon column list is exactly PostgreSQL's plus the writer's metadata columns, and the Paimon
  primary key is PostgreSQL's primary key;
* **every column of every row** of the eight replicated tables, not row counts. A row count agrees with PostgreSQL in exactly the
  case this branch kept finding: a merged table that looks right while the changelog is wrong. Both
  sides are canonicalised through the *Paimon* column type, so `numeric(12,2)` is compared as a
  scale-2 decimal rather than as two string renderings; PostgreSQL is read ordered by primary key
  so a reported difference is the same one on every run. The first ten differing rows of each table
  are printed with both sides' values.
* **the changelog, from `$audit_log` over a streaming scan** (a merged read reports `INSERT` for
  every surviving row and physically cannot answer this): `-U` count equals `+U` count, so no
  update lost its before-image; `+I` minus `-D` equals the surviving row count, since no TPC-C
  transaction moves a primary key; and `-U > 0` on every table TPC-C updates. Whether Delivery ran
  is read off PostgreSQL — the loader gives a carrier id only to orders below 2101, so a delivered
  order above that is proof — and the `oorder` / `order_line` / `new_order` assertions are strict
  only when it did.

A difference fails the harness and the CI job. The job uploads the writer log, the comparison
output and a warehouse listing whatever the outcome.

**What sets the runtime is the transaction rate and the commit batch.** The writer commits a batch
of whole transactions (see "Commit batching"), so a run leaves roughly one Paimon snapshot per
batch per touched table; the drain and the `$audit_log` scan (one `plan()` per snapshot) are linear
in the snapshot count. The comparison prints the snapshot count next to each table's row kinds so
the cost is visible. The defaults, 60s at 25 tps, are about 1500 transactions: enough for every
update and delete path TPC-C has, including roughly sixty Delivery transactions. Raising
`TPCC_RATE` or `TPCC_TIME` costs time, not rigour; the load is ~540k rows either way.

**Measured: one Paimon commit per transaction against the default commit batch.** The same local
run (1 warehouse, 60s at 25 tps, fixed bucketing) twice: with `PGPAIMON_BATCH_MAX_ROWS=1`, which
is the previous behaviour exactly, and with the defaults (10,000 rows, 16 MiB, 2,000 ms). Both
passed the comparison with 0 missing, 0 extra, 0 differing and `-U == +U` on every update-driven
table.

| | one commit per transaction | default batch |
|---|---|---|
| snapshots, all tables | 9,778 | 344 |
| files, all tables | 85,027 | 3,823 |
| warehouse size | 1,000 MB | 166 MB |
| `district` (10 rows): snapshots / files | 1,673 / 12,443 | 38 / 430 |
| `order_line`: snapshots / files | 2,117 / 22,311 | 79 / 946 |
| whole run, `run_tpcc.sh` | 759 s | 186 s |
| the comparison (PostgreSQL scan plus every changelog) | 84 s | 16 s |

The two runs are different random workloads, so row counts differ by about 0.2%; the snapshot and
file counts are what the batch controls. The remaining snapshots are the interval at work: about
one per table every two seconds while the table is being written.

Two consequences for an operator:

* **Nothing is reclaimed for the first hour.** Paimon's defaults are `snapshot.num-retained.min = 10`,
  `num-retained.max = Integer.MAX_VALUE`, `snapshot.time-retained = 1h`. A short run never reaches
  expiry and a sustained one accumulates an hour's worth first, so `snapshot.time-retained` and
  `snapshot.num-retained.max` want deliberate values on a write-heavy table.
* **Read planning scales with the snapshot count, not the data.** A streaming changelog scan must
  `plan()` once per snapshot, which is why the batch exists: before it, reading the changelogs of
  9,467 snapshots took 120 s.
