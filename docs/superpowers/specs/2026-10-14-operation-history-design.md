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
