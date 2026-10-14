# Operation history tables and key-less operation logs

Amends docs/superpowers/specs/2026-10-09-logical-slot-writer-design.md. Requested by the project owner:

> Treat tables with no PK and no user-provided natural key as an operation log: append every insert, update and
> delete with op type and lsn, seqno, timestamp and xid fields -- the log itself saves only after values. For all
> tables, with and without PK, the changelog should store before/after pairs for each row; do not collapse
> operations that occur in the same transaction or batch; save this log to a table named [table name] + `_hist`.

## 1. Two new behaviours

**H. History table (every tracked table).** For each tracked table `T`, the writer maintains a second Paimon table
`T_hist` in the same Paimon database (= the same PostgreSQL schema). It is an *append-only* table: no primary key,
no merge engine, no deduplication. Every row-level change PostgreSQL logged becomes exactly one row, in commit order.
Two changes to the same row in one transaction, or in one mini-batch, are two rows. Nothing is collapsed.

**K. Key-less operation log.** A tracked table with no primary key is accepted (the "PK required" guardrail is
relaxed). Its main Paimon table `T` is itself an append-only operation log carrying the *after* values (for a delete,
the deleted row's values) plus the operation metadata below. `T_hist` is still produced for it, with before/after
pairs. There is no `-U`/`-D` semantics in `T`: a reader reconstructs state, if it wants it, from the log.

## 2. Row layout

`T_hist` columns, in this order:

| column | type | meaning |
|---|---|---|
| `_op` | STRING NOT NULL | `I` insert, `U` update, `D` delete, `T` truncate, `R` initial-load (backfill) row |
| `_commit_lsn` | BIGINT NOT NULL | LSN of the commit record of the transaction (also the Paimon commit identifier) |
| `_lsn` | BIGINT NOT NULL | LSN of the change itself (the existing `_seq`) |
| `_op_seq` | INT NOT NULL | ordinal of the record inside its transaction (the existing `_op_seq`) |
| `_xid` | BIGINT NOT NULL | PostgreSQL transaction id |
| `_ts` | TIMESTAMP WITH LOCAL TIME ZONE NOT NULL | commit timestamp of the transaction (from pgoutput) |
| `before_<col>` ... | the column's Paimon type, nullable | old row; all NULL for `I`, `R`, `T` |
| `after_<col>` ... | the column's Paimon type, nullable | new row; for `D` all NULL in `T_hist` (the old row is in `before_*`) |

Because every user column is prefixed, a user column can never collide with a metadata column.
(Key-less main table `T`: the user columns unprefixed, then `_op`, `_commit_lsn`, `_lsn`, `_op_seq`, `_xid`, `_ts`.
For `D` the unprefixed columns hold the deleted row. For `T`, a row of NULLs with `_op = 'T'`.) Primary-key tables'
main tables are unchanged.

## 3. Rules and decisions (assumptions the owner can overturn)

1. `T_hist` exists for every tracked table by default; there is no per-table opt-out in v1 (nothing asked for one).
   Cost: roughly doubles Paimon writes and storage, and the history grows without bound -- retention is the
   operator's job; the docs must say so.
2. Exactly-once: `T_hist` is a separate Paimon table with its own commit identifier stream. The existing per-table
   rules apply to it unchanged (identifier = last transaction commit LSN; "rule 5" drops a transaction's records for
   a table when its commit LSN is <= that table's highest committed identifier; resume position = MIN over all
   tables, now including every `_hist` and every key-less main table). Main and history commits are NOT atomic with
   each other; a crash between them is repaired by replay, which each table filters independently.
3. DDL fan-out: `create` creates `T` and `T_hist`. `add_col` adds the column to `T` and `before_x`/`after_x` to
   `T_hist`. `drop_col` drops from `T` but KEEPS the `before_x`/`after_x` columns (history is not rewritten; they are
   NULL for later rows). `rename_col` renames in `T` and in `T_hist` (both prefixed names). `alter_col` type change
   applies to both prefixed columns, under the same representability rules as the main table. `drop_table` drops `T`
   only and LEAVES `T_hist` as the retained audit trail, which the documentation must state; `remove_table` likewise. A later `add_table` of a new table of the same name finds
   an existing `T_hist` and must refuse with a hint (rename or drop it by hand) rather than append different
   data into it.
4. Name collisions: `add_table` refuses a table `T` if a tracked table is named `T_hist`, or if `T_hist` already
   exists in Paimon and does not belong to `T`. Hist tables are registered in the registry (a `hist_table` column or
   a derived name, implementer's choice) so the health check can verify them.
5. TRUNCATE: for a PK table the main table is overwritten empty as today and `T_hist` gets one `T` row. For a
   key-less log table the log is NOT erased (it is an audit log); a `T` row is appended to both.
6. Backfill (initial load) writes `R` rows to `T_hist` and to a key-less `T`; PK-table `T` is loaded as today.
7. Guardrails that change: "no primary key" is no longer refused, but a key-less table must have
   REPLICA IDENTITY FULL (add_table sets it, as for PK tables) so UPDATE/DELETE are legal and `before_*` is complete.
   Everything else (UNLOGGED, numeric, names, type-change rules, health check) applies to key-less tables as it does
   to PK tables. Paimon bucket count for append tables: the same size-based default; append tables use fixed
   buckets without a bucket key (`bucket-key` = all columns is acceptable) -- implementer to pick what Paimon 1.4.1
   supports for an unaware-bucket or fixed-bucket append table, and state it.
8. Mini-batching applies unchanged: a batch may contain many transactions; `T_hist` rows keep per-record metadata
   (`_commit_lsn`, `_lsn`, `_op_seq`), so batching never loses ordering information. A batch still never spans DDL
   or truncate.
9. TPC-C: BenchBase's `history` table (no PK) becomes a replicated key-less log; `history_hist` appears too.
   `TpccCompare` must compare, for every table, that `T_hist` contains exactly the changes PostgreSQL made: per-table
   counts of I/U/D equal the changelog counts already checked, every `U` row has `before_*` and `after_*`, and the
   `history` log has exactly the PostgreSQL row set (multiset equality, since it has no key).
10. Not in scope: a view that exposes "current state" of a key-less table; retention/compaction of `_hist`.

## 4. Open risks to state in the docs

* Doubled write volume; unbounded growth.
* `T_hist` and `T` can transiently disagree after a crash until replay finishes.
* Wide tables: `before_*` + `after_*` triples the column count of `T_hist`.
* Column-count and name-length limits for `before_`/`after_` prefixed names must be guarded (the 63-byte PostgreSQL
  identifier limit does not bind Paimon, but the combined Paimon field name rules do).

## 5. Implementation decisions (2026-10-14)

* **Bucketing (rule 7).** Append tables (`T_hist`, key-less `T`) use the registry's fixed bucket
  count with `bucket-key = _lsn`. Measured on Paimon 1.4.1, 60 commits of 20 rows: `bucket = -1`
  (unaware) got no in-writer compaction and held 60 live files; fixed buckets compacted to 16. A
  user column in `bucket-key` -- the "all columns" option above -- is **not** acceptable: Paimon
  then refuses `DROP COLUMN` of it ("Field names [...] should contains all bucket keys"), a DDL
  message the writer could never apply.
* **Rule 4, collision with a Paimon table SQL cannot see.** `add_table()` cannot read the
  warehouse. It refuses the collisions it can know (tracked names, and history tables retained by
  `DROP TABLE`/`remove_table()`, recorded in `paimon.retained_history`; `paimon.forget_history()`
  releases a name). A `T_hist` that exists in Paimon without this registration's id
  (`pgpaimon.registration`, from `table_registry.registration`) is refused by the writer, which
  stops with the remedy; renaming that table and restarting resolves it.
* **Rule 3, re-adding a dropped column's name.** The kept `before_x`/`after_x` pair is renamed to
  `before_x_retired_<lsn>`/`after_x_retired_<lsn>` in the same schema change that adds the new pair.
* **Metadata of `R` rows:** `_commit_lsn = _lsn =` the load's consistent point, `_xid = 0`, `_ts` the
  time the load began. A history that already holds streamed rows is not overwritten by a reload.
* **`_op_seq`** of a history row is the `_op_seq` of the change's first changelog record (for an
  UPDATE, its `-U`), so it increases within a transaction but is not contiguous.
* Key-less `T`: user columns are all nullable (a `T` row is NULL everywhere).
* Column counts and names: a 3,206-column append table with 62-character names, and a 300-character
  field name, were created, written and read on 1.4.1; no guard is needed beyond the existing ones.

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
