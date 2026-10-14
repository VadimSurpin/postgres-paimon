# Operation history tables and key-less operation logs

Amends docs/superpowers/specs/2026-10-09-logical-slot-writer-design.md. Requested by the project owner:

> Treat tables with no PK and no user-provided natural key as an operation log: append every insert, update and
> delete with op type and lsn, seqno, timestamp and xid fields -- the log itself saves only after values. For all
> tables, with and without PK, the changelog should store before/after pairs for each row; do not collapse
> operations that occur in the same transaction or batch; save this log to a table named [table name] + `_hist`.

**Current state.** Sections 1-5 below are the CURRENT design. They were rewritten to fold in Amendment A
(one row per row image; the history table rotates on DDL) and Amendment B (daily partitions; bucket count sized per
day), whose original texts are kept verbatim at the end of this document as the record of what was asked.

## 1. Two new behaviours

**H. History table (every tracked table).** For each tracked table `T`, the writer maintains a second Paimon table
`T_hist` in the same Paimon database (= the same PostgreSQL schema). It is an *append-only* table: no primary key,
no merge engine, no deduplication, partitioned by day. Every row image of every row-level change PostgreSQL logged
becomes exactly one row, in commit order: an INSERT one row (`+I`), an UPDATE two consecutive rows (`-U` with the old
values, then `+U` with the new), a DELETE one row (`-D`, the old values). Two changes to the same row in one
transaction, or in one mini-batch, are separate rows. Nothing is collapsed. Every DDL the writer applies to `T`
archives the current `T_hist` as `T_hist_<lsn>` and starts a new, empty `T_hist` with the new schema (rule 3).

**K. Key-less operation log.** A tracked table with no primary key is accepted (the "PK required" guardrail is
relaxed). Its main Paimon table `T` is itself an append-only operation log carrying the *after* values (for a delete,
the deleted row's values) plus the operation metadata below, one row per operation, unpartitioned. `T_hist` is still
produced for it, in the layout of section 2. There is no `-U`/`-D` semantics in `T`: a reader reconstructs state,
if it wants it, from the log or from `T_hist`. The key-less `T` evolves in place on DDL; only `T_hist` rotates.

## 2. Row layout

`T_hist` columns, in this order: the table's own columns (same names, same Paimon types, all nullable), then

| column | type | meaning |
|---|---|---|
| `_op` | STRING NOT NULL | `+I`, `-U`, `+U`, `-D` (exactly Paimon's `RowKind.shortString()`), `T` truncate, `R` initial-load row |
| `_commit_lsn` | BIGINT NOT NULL | LSN of the commit record of the transaction (also the Paimon commit identifier) |
| `_lsn` | BIGINT NOT NULL | LSN of the change itself (the existing `_seq`); the two rows of an update share it |
| `_op_seq` | INT NOT NULL | ordinal of the row image inside its transaction; `-U` before its `+U` |
| `_xid` | BIGINT NOT NULL | PostgreSQL transaction id |
| `_ts` | TIMESTAMP WITH LOCAL TIME ZONE NOT NULL | commit timestamp of the transaction (from pgoutput) |
| `_dt` | DATE NOT NULL | the UTC calendar date of `_ts`; the partition key |

| PostgreSQL change | rows appended | `_op` | user columns hold |
|---|---|---|---|
| INSERT | 1 | `+I` | the new row |
| UPDATE | 2, consecutive | `-U` then `+U` | the old row, then the new row |
| DELETE | 1 | `-D` | the deleted (old) row |
| TRUNCATE | 1 | `T` | all NULL |
| initial load | 1 per row | `R` | the loaded row |

The two rows of an update share `_commit_lsn`, `_lsn`, `_xid`, `_ts` and `_dt` and have increasing `_op_seq`. An update
that changes nothing still yields its pair. Because user columns are not prefixed, the seven metadata names
(`_op`, `_commit_lsn`, `_lsn`, `_op_seq`, `_xid`, `_ts`, `_dt`) are reserved column names on every replicated table.

Key-less main table `T`: the user columns (all nullable), then `_op` (`I`, `U`, `D`, `T`, `R`), `_commit_lsn`,
`_lsn`, `_op_seq`, `_xid`, `_ts`; no `_dt`, not partitioned. For `D` the user columns hold the deleted row; for `T`,
NULLs. Primary-key tables' main tables are unchanged.

## 3. Rules and decisions (assumptions the owner can overturn)

1. `T_hist` exists for every tracked table by default; there is no per-table opt-out in v1 (nothing asked for one).
   Cost: roughly doubles Paimon writes and storage, and the history grows without bound -- retention is the
   operator's job (dropping old `_dt` partitions, or Paimon's `partition.expiration-time`, which the writer never
   sets); the docs must say so.
2. Exactly-once: each `T_hist` (and each archived `T_hist_<lsn>`) is a separate Paimon table with its own commit
   identifier stream. The existing per-table rules apply to it unchanged (identifier = last transaction commit LSN;
   rule 4 drops a transaction's records for a table when its commit LSN is <= that table's highest committed
   identifier). Identifiers are per table, never per partition: a batch, or one transaction, that spans several
   `_dt` partitions is ONE Paimon commit. The resume position is the MIN over the current tables (every main table
   and every current `T_hist`); archived tables never constrain it. Main and history commits are NOT atomic with each
   other; a crash between them is repaired by replay, which each table filters independently.
3. DDL: `create` creates `T` and `T_hist`. Every DDL message applied to a tracked table -- `add_col`, `drop_col`,
   `rename_col`, `alter_col` -- rotates the history table: (a) the current `T_hist` is renamed `T_hist_<lsn>`, the
   DDL message's LSN as 16 upper-case zero-padded hex digits (sortable, name-safe); (b) a new, empty `T_hist` is
   created with the new schema, recording the DDL LSN as its baseline; (c) `T` is altered as before. Each step is
   replay-safe (a crash between any two converges on replay). A history row goes to the history table of the schema
   it was decoded under: rows of changes before the DDL (lower change LSN) go to the archive, later ones to the new
   table, so a transaction straddling the DDL writes to both. `drop_table` drops `T` only and LEAVES `T_hist` and
   every `T_hist_<lsn>` as the retained audit trail; `remove_table` likewise. A later `add_table` of a new table of
   the same name must refuse with a hint (rename or drop them by hand) rather than append different data.
4. Name collisions: `add_table` refuses a table `T` if a tracked table is named `T_hist` or `T_hist_<16 hex>`, if `T`
   itself has such a name for a tracked table, or if `T_hist` already exists in Paimon and does not belong to `T`
   (the writer re-checks what SQL cannot see). Hist tables are registered in the registry (`hist_table`).
5. TRUNCATE: for a PK table the main table is overwritten empty as today and `T_hist` gets one `T` row. For a
   key-less log table the log is NOT erased (it is an audit log); a `T` row is appended to both.
6. Backfill (initial load) writes `R` rows to `T_hist` and to a key-less `T`; PK-table `T` is loaded as today.
7. Guardrails that change: "no primary key" is no longer refused, but a key-less table must have
   REPLICA IDENTITY FULL (add_table sets it, as for PK tables) so UPDATE/DELETE are legal and the old image is
   complete. Everything else (UNLOGGED, numeric, names, type-change rules, health check) applies to key-less tables
   as it does to PK tables. The seven metadata names are reserved (section 2).
8. Partitions and buckets: `T_hist` is partitioned by `_dt` and bucketed per partition, so its bucket count is sized
   from ONE day's expected history volume: `buckets = clamp(1, ceil(bytes_per_day / target), 64)` (constants and
   estimate: section 5). `paimon.add_table(..., hist_buckets => N)` overrides it and always wins. At a rotation the
   new `T_hist` is sized from the measured daily volume of the archived partitions when at least three exist. The
   bucket count of an existing table is never altered in place. The key-less main log and the PK main tables are
   not partitioned.
9. Mini-batching applies unchanged: a batch may contain many transactions; `T_hist` rows keep per-record metadata
   (`_commit_lsn`, `_lsn`, `_op_seq`), so batching never loses ordering information. A batch still never spans DDL
   or truncate.
10. TPC-C: BenchBase's `history` table (no PK) becomes a replicated key-less log; `history_hist` appears too.
   `TpccCompare` must compare, for every table, that `T_hist` contains exactly the changes PostgreSQL made: per-table
   counts of `+I`/`-U`/`+U`/`-D` equal the changelog counts already checked, every `-U` is followed by its `+U`, and
   the `history` log has exactly the PostgreSQL row set (multiset equality, since it has no key).
11. Not in scope: a view that exposes "current state" of a key-less table; retention/compaction of `_hist`;
   cleaning up archived `T_hist_<lsn>` tables.

## 4. Open risks to state in the docs

* Doubled write volume; unbounded growth (of rows, and of archived tables: one per DDL per table).
* `T_hist` and `T` can transiently disagree after a crash until replay finishes.
* Wide tables: an update writes the whole row twice.
* Paimon's `renameTable` on a filesystem catalog is a directory rename: atomic on local and HDFS, NOT on S3-like
  stores (copy + delete).

## 5. Implementation decisions (2026-10-14)

* **Bucket key.** Append tables (`T_hist`, archives, key-less `T`) use a fixed bucket count with
  `bucket-key = _lsn`. Measured on Paimon 1.4.1, 60 commits of 20 rows: `bucket = -1` (unaware) got no in-writer
  compaction and held 60 live files; fixed buckets compacted to 16. A user column in `bucket-key` is **not**
  acceptable: Paimon then refuses `DROP COLUMN` of it ("Field names [...] should contains all bucket keys").
* **Bucket count of `T_hist`.** `target = 1 GiB` of history per bucket per daily partition, minimum 1, maximum 64.
  Measured on 1.4.1 (120 commits of 200 rows, partitioned by `_dt`): live files after in-writer compaction were
  about 4 per bucket per partition (1 bucket: 4 per day; 4 buckets: 16; 16 buckets: 64), so files grow as
  buckets x days; a year of daily partitions at the old floor of 4 buckets is ~5,800 live files per table against
  ~1,460 at one bucket, and the writer is single-threaded, so more buckets buy no parallelism. The add_table estimate:
  history rows per day = `(n_tup_ins + 2 n_tup_upd + n_tup_del) / days since pg_stat_database.stats_reset`, times the
  average row width `pg_relation_size / reltuples`; when the counters are zero, the statistics are less than one day
  old or never reset, or the table has no rows to measure width on, it falls back to `paimon.default_buckets()`, the
  main table's size heuristic. At a rotation the writer measures the archived history's partitions (data file
  bytes per `_dt`, over the most recent seven days present across the archives) and applies the same formula when at
  least three days exist; otherwise it keeps the registry's value. A user `hist_buckets` always wins.
* **`_dt`.** The UTC date of the transaction's commit timestamp, computed in the writer from pgoutput's value
  (microseconds since 2000-01-01 UTC), never from any session or JVM time zone. `R` rows: `_ts` is PostgreSQL's
  `now()` inside the initial-load snapshot transaction (the server's clock at the load's start, the same for every
  row of a table's load), and `_dt` its UTC date.
* **Rule 4, collision with a Paimon table SQL cannot see.** `add_table()` cannot read the warehouse. It refuses the
  collisions it can know (tracked names, and history tables retained by `DROP TABLE`/`remove_table()`, recorded in
  `paimon.retained_history`, including their archives by name pattern; `paimon.forget_history()` releases a name). A
  `T_hist` that exists in Paimon without this registration's id (`pgpaimon.registration`) is refused by the writer,
  which stops with the remedy; renaming that table and restarting resolves it. The writer refuses to archive onto a
  name that is a tracked table.
* **Rotation, replay and routing.** The new `T_hist` records the DDL LSN as its applied-DDL LSN and as
  `pgpaimon.history-baseline-lsn`. A history row whose change LSN is above the current table's baseline goes to the
  current table; otherwise to the archive `T_hist_<L>` with the smallest rotation LSN `L` above it. Archives are
  discovered from the catalog by name at startup. Within one flush the archives are committed before the current
  tables, so a crash can never leave an archive behind tables that are ahead of it (archives do not constrain the
  resume position). A half-done rename (both names present) keeps the source when it is still readable and drops
  the partial target, otherwise keeps the target.
* **Metadata of `R` rows:** `_commit_lsn = _lsn =` the load's consistent point, `_xid = 0`. A history that already
  holds streamed rows is not overwritten by a reload.
* **`_op_seq`** of a history row is the `_op_seq` of the changelog record of the same image (for a PK table); for a
  key-less table the log row takes the first `_op_seq` of its change and the history rows one each.
* Key-less `T`: user columns are all nullable (a `T` row is NULL everywhere).
* Column counts and names: a 3,206-column append table with 62-character names, and a 300-character field name,
  were created, written and read on 1.4.1; no guard is needed beyond the existing ones.

---

# Amendment A (2026-10-14, owner request) -- row-per-image history and DDL rotation

> In the history table keep two rows for before and after values with exactly +U and -U markers. In case of DDL
> rename the old history table to [table_name]_hist_[lsn] and start a new history table with [table name]_hist name.

This AMENDS sections 1-3 above; where they disagree, this amendment wins, and the implementer must rewrite those
sections so the document no longer contradicts itself.

## A1. Layout: one row per row image, Paimon row-kind markers

`T_hist` has NO `before_*` / `after_*` columns any more. Its columns are the table's own columns (same names, same
Paimon types, all nullable) followed by the metadata columns (`_op`, `_commit_lsn`, `_lsn`, `_op_seq`, `_xid`, `_ts`,
exactly as in section 2). One PostgreSQL row change becomes:

| PostgreSQL change | rows appended | `_op` | values |
|---|---|---|---|
| INSERT | 1 | `+I` | the new row |
| UPDATE | 2, consecutive | `-U` then `+U` | the old row, then the new row |
| DELETE | 1 | `-D` | the deleted (old) row |
| TRUNCATE | 1 | `T` | all NULL |
| initial load | 1 per row | `R` | the loaded row |

The markers are exactly Paimon's `RowKind.shortString()` strings (`+I`, `-U`, `+U`, `-D`); `T` and `R` are unchanged.
The two rows of an update share `_commit_lsn`, `_lsn` and `_xid` and have distinct, increasing `_op_seq` (`-U` first).
Nothing is collapsed: an update that changes nothing still yields its `-U`/`+U` pair.
A user column whose name equals a metadata column (`_op`, `_commit_lsn`, `_lsn`, `_op_seq`, `_xid`, `_ts`) is now a
possible collision (there is no prefix any more): the existing reserved-name guardrail must cover all six, in
`add_table`, `ADD COLUMN` and `RENAME COLUMN`. The key-less main log table `T` keeps its layout (section 1.K).

## A2. DDL rotates the history table

For every DDL message the writer applies to a tracked table -- `add_col`, `drop_col`, `rename_col`, `alter_col` (not
`create`, not `drop_table`) -- the history table is rotated, atomically with respect to replay:

1. the current `T_hist` is renamed to `T_hist_<lsn>`, where `<lsn>` is the DDL message's LSN (the same value as the
   DDL commit identifier `pgpaimon.applied-ddl-lsn`), rendered as 16 upper-case zero-padded hex digits so the
   archived names sort chronologically (implementer may choose another sortable, name-safe rendering if hex
   proves unsuitable; document it);
2. a new, empty `T_hist` is created with the table's NEW schema;
3. the main table `T` is altered as today.

Consequences the implementation and tests must honour:

* Every row in `T_hist_<lsn>` was written under exactly one schema; `T_hist` always matches the current schema.
  Archived tables are never written again.
* Replay safety: a crash between any two of the steps above, followed by a replay of the DDL message, must converge
  to the same final state (rename done / not done, create done / not done, main altered / not). The new `T_hist`
  records the DDL LSN as its baseline (like the main table's `pgpaimon.applied-ddl-lsn`) so that transactions that
  committed BEFORE the DDL and are replayed after a crash are NOT written into the new table; they belong to the
  archive.
* A transaction that straddles the DDL (`BEGIN; INSERT; ALTER TABLE ... ADD COLUMN; INSERT; COMMIT`): the first
  insert was decoded under the old schema and belongs in the archived table, the second in the new `T_hist`. Records
  already carry the `RelationMeta` they were decoded under (commit 1d5d302); route each history row by its schema
  generation. The same transaction therefore commits to both tables, each with its own commit identifier rules.
* Resume position = MIN over all tables is computed over the CURRENT tables; archived tables never constrain it.
* Names: `T_hist_<lsn>` must not collide with a tracked table. The `add_table` / `ADD COLUMN`-free collision
  guardrail extends to the pattern `<tracked>_hist` and `<tracked>_hist_<hex>`; the writer re-checks.
* `drop_table` / `remove_table` leave `T_hist` and every `T_hist_<lsn>` (retained audit trail; section 3.3 applies to
  all of them, including the re-add refusal).
* The key-less main log `T` still evolves in place (only `T_hist` rotates).
* Cost to document: every DDL creates one more Paimon table per tracked table touched; the number of archived tables
  is unbounded, and nothing cleans them up. `paimon.health()` / docs should tell operators how to list them.
* Paimon's `renameTable` on the filesystem catalog is a directory rename: atomic on HDFS/local, NOT atomic on
  S3-like stores (copy + delete). State this; the implementer must verify what 1.4.1 does and make the replay logic
  robust to a half-done rename.

Folded into sections 1-5 above (2026-10-14).


# Amendment B (2026-10-14, owner request) -- date partitions for history tables

> Lets add partitioning to history tables. Do date partitions and calculate required bucket count for history tables
> with respect to date partitions

Folded into sections 1-5 above; the coordinator's text follows as given.

B1. Every T_hist (current and rotated) is partitioned by day. New metadata column `_dt` DATE NOT NULL = the UTC
calendar date of `_ts` (the PostgreSQL COMMIT timestamp of the transaction, never the writer's clock, so a replay of a
transaction writes the same partition). Partition keys = [`_dt`]. `_dt` becomes the 7th reserved metadata column name
(reserved-name guardrail in add_table/ADD COLUMN/RENAME COLUMN). Rows with `_op` = R (initial load) take `_dt` from
the load's timestamp (state precisely which and test it); `T` rows take their transaction's commit date. A mini-batch
or one transaction may span several partitions; commit identifiers stay per table, not per partition. A batch
crossing midnight must write both partitions in one Paimon commit.

B2. Bucket count is per partition for a fixed-bucket Paimon table, so the count is sized from the expected volume of
ONE day, not of the whole table: buckets = clamp(1, ceil(estimated_bytes_per_day / target_bytes_per_bucket_per_partition),
64), with an explicit, documented target (propose one from measurements: many small buckets x many daily partitions is
the small-file explosion we already measured in the batching work; fewer, fuller buckets are better) and a documented
minimum. Estimation: at add_table, from pg_stat_user_tables counters (n_tup_ins + 2*n_tup_upd + n_tup_del, i.e.
history rows, divided by the days since the statistics reset / since the database stats reset time) times the
table's average row width (pg_relation_size/reltuples; history rows carry the full row each), falling back to the
existing table-size heuristic when counters are zero or stats are fresh; user-tunable per table:
`paimon.add_table(..., hist_buckets => N)` (positive int; recorded in the registry). At DDL rotation (Amendment A) the
NEW T_hist's bucket count is recomputed from the MEASURED average daily volume of the table's recent partitions in the
archive when at least 3 daily partitions exist (else the add_table estimate or the user's hist_buckets, which always
wins). Record the formula, constants, and the evidence behind the constants in the architecture guide. Bucket count of
an existing partitioned table is never altered in place.

B3. Retention is the operator's job but is now cheap: document `partition.expiration-time` / dropping old `_dt`
partitions as the way to cap growth, and state that nothing in the writer sets it. Do not add retention behaviour.

B4. Not in scope: partitioning the key-less main log T (it stays unpartitioned; say so in the docs) and partitioning
the main PK tables.
