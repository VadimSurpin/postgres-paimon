# Design: postgres-paimon on logical replication slots

**Date:** 2026-10-09
**Branch:** `feat/logical-slot-writer`
**Status:** approved design, pending implementation plan

---

## 1. Intent

Deliver the same observable behaviour as the `paimon_heap` extension on `main`, with a
different architecture:

- **Capture** via a logical replication slot instead of a custom Table Access Method plus
  shared-memory ring buffer.
- **Writing** from a process beside PostgreSQL instead of a `BackgroundWorker` inside it.
- **DDL** carried in-band as logical WAL messages emitted by event triggers.

### Success criteria

The branch works when the existing JDBC acceptance suite passes against the new
architecture. That suite is architecture-agnostic — it drives SQL into PostgreSQL and
asserts what a Paimon reader sees:

- `PaimonFeatureTest` T1–T8 — basic CRUD capture, metadata fields, configurable field
  names, schema structure, text columns, multi-row insert
- `PaimonAdvancedTest` T9–T17 — checkpoint, vacuum, vacuum freeze, ALTER variants, crash
  recovery, default-TAM passthrough
- `PaimonOffloadTest` T18–T19 — S3 and HDFS offload
- `PaimonDdlTest` D1–D9 — DDL propagation and the blocked-PK cases
- `PaimonSchemaIT`, `PaimonTypeTest` — schema evolution and type fidelity

Three suites change meaningfully and are expected to differ (see §9).

### Non-goals

- Replacing `main`. The C++ extension stays as-is; this is a parallel architecture.
- Multi-instance or HA writer. Single writer process per warehouse.
- Binary-mode pgoutput decoding in v1 (see §6).

### Constraints taken as given

- `wal_level = logical` is acceptable.
- The writer initially runs on the same host as PostgreSQL.
- `gaps.md` items 1 and 3 are in scope to fix properly. Item 2 is carried over: column
  defaults stay blocked, for the reason given in §4.

---

## 2. Why this architecture

Established in analysis before this design (recorded here so the plan does not relitigate it):

**What the slot buys.** Durability becomes a property of WAL rather than of a custom
rmgr — `extension/paimon_wal.cpp`, its recovery journal and its replay path all delete.
Ring-buffer drop under load (`paimon_heap.dropped_msgs`) is replaced by backpressure as
disk. LSN is a monotone, restart-safe sequence number, unlike main's
`(xid << 32) | op_counter` in `extension/paimon_heap.cpp:370`, which is not monotone across
XID wraparound. Initial backfill becomes possible at all — main's
`ALTER TABLE ... SET ACCESS METHOD paimon_heap` rewrites through the inherited heapam
`relation_copy_for_cluster` and emits nothing. No Arrow, Parquet, aws-sdk-cpp, libhdfs3 or
krb5 in the postmaster address space. No TAM signature churn across major versions
(`extension/paimon_heap.cpp:540-632` already carries three variants). Installable on managed
PostgreSQL.

**What it costs.** Tracked tables must be switched to `REPLICA IDENTITY FULL`, which adds
the old row to the WAL of every UPDATE and DELETE — that is what makes an unchanged TOASTed
column recoverable (§5). `merge-engine` is constrained to `deduplicate` (§5). Physical tuple
identifiers are not obtainable, so `meta_field_tid` has no counterpart (§6). An abandoned
slot is a new operational hazard (§8).

**What is preserved.** DDL interception with the ability to abort the user's DDL — the one
capability an external consumer structurally cannot have — survives as event triggers,
because PostgreSQL DDL is transactional (§4).

---

## 3. Architecture

```
┌─ PostgreSQL 18 (official image, wal_level=logical) ─────────┐
│                                                              │
│  user DML ──► heap (unmodified, no TAM)                      │
│                                                              │
│  paimon_slot  (SQL-only extension, no shared library)        │
│    ddl_command_end  ─► guardrails: RAISE aborts the DDL      │
│                     ─► pg_logical_emit_message('paimon', …)  │
│    sql_drop         ─► drop_table message                    │
│    paimon.add_table() / paimon.remove_table()                │
│                                                              │
│  publication paimon_pub ──► replication slot 'paimon'        │
└───────────────────────────────┬──────────────────────────────┘
                                │ pgoutput over PGReplicationStream
┌───────────────────────────────▼──────────────────────────────┐
│  paimon-writer  (JVM, separate process / container)          │
│                                                              │
│   SlotConsumer → PgOutputDecoder → TxnBatcher → PaimonSink   │
│                         │                        ↑           │
│                         └─► DdlMessageHandler ─► SchemaRegistry
│                                                              │
│   Backfiller (exported snapshot, one-shot per table)         │
│   OffsetStore (last committed LSN, read from Paimon)         │
└──────────────────────────────┬───────────────────────────────┘
                               │ org.apache.paimon Catalog
                        warehouse: local / S3 / HDFS
```

The writer is the only component that touches Arrow, Parquet, S3 or Hadoop.
`docker/demo/postgres/Dockerfile`, which currently builds PostgreSQL against aws-sdk-cpp,
libhdfs3 and Arrow, is replaced by the official `postgres:18` image plus `CREATE EXTENSION`.

---

## 4. In-database extension: `paimon_slot`

Pure SQL. No C, no `shared_preload_libraries`, so it installs on RDS, Aurora and Cloud SQL.

### Registry

```sql
CREATE TABLE paimon.table_registry (
    relid        oid PRIMARY KEY,
    schema_name  text    NOT NULL,
    table_name   text    NOT NULL,
    pk_cols      text[]  NOT NULL,
    field_ids    jsonb   NOT NULL,   -- attname -> attnum, the stable Paimon field_id
    backfill_due boolean NOT NULL DEFAULT true,
    replica_identity text NOT NULL DEFAULT 'full',   -- 'full' | 'default', §5 escape
    changelog_producer text NOT NULL DEFAULT 'input',-- 'input' | 'lookup',  §5 escape
    registered_at timestamptz NOT NULL DEFAULT now()
);
```

The registry is the extension's memory of last-known schema, which the guardrails diff
against. Publication membership is the source of truth for *which* tables are tracked;
the registry records *what they looked like*.

### API

- `paimon.add_table(regclass, replica_identity => 'full', changelog_producer => 'input')`
  — sets `REPLICA IDENTITY FULL` unless the escape says otherwise, adds the table to
  `paimon_pub`, inserts the registry row, emits a `create` message carrying the Paimon
  table options. Raises if the table has no primary key. The two escape arguments are
  accepted and recorded in v1 but only their default values are implemented (§5).
- `paimon.remove_table(regclass)` — drops from publication, deletes the registry row.
  Does not delete Paimon data.

### Guardrails: `ddl_command_end`, not `ddl_command_start`

Event triggers expose no parse tree at `ddl_command_start` — only `TG_TAG` and
`current_query()` — so blocking there would require parsing SQL text. At
`ddl_command_end`, `pg_event_trigger_ddl_commands()` gives structured object identities
and the catalog is readable in its new state, so the trigger can diff against
`paimon.table_registry`.

Because `ALTER TABLE` is transactional, `RAISE` at `ddl_command_end` rolls the whole
statement back. The user-visible outcome is identical to pre-execution blocking: error
returned, schema unchanged. The only cost is that a table rewrite may be performed and
then discarded.

Blocked, matching main:

- PK add, drop or change — detected by diffing the current PK column set against
  `pk_cols`. Replaces main's `ProcessUtility` checks at `extension/paimon_ddl.cpp:197`
  and `:237`.
- `ADD COLUMN ... NOT NULL` without a default — new column with `attnotnull` and no
  default. Replaces `extension/paimon_ddl.cpp:102`.
- `ADD COLUMN ... DEFAULT` — any new column carrying a default, matching main's rejection
  (commits `abb792b`, `7529932`). Detected from `pg_attribute.atthasdef` on the new column
  at `ddl_command_end`, which is more reliable than main's parse-tree walk over
  `ColumnDef->constraints` for `CONSTR_DEFAULT`.

`merge-engine = deduplicate` offers no cheap way to populate one column on existing
records: that requires rewriting whole rows, because nothing short of a partial-update
engine can write a column in isolation, and §5 records why partial-update was rejected.
A full table refill through the `Backfiller` would work and is O(table) for a DDL that
PostgreSQL performs in O(1) with its fast-default mechanism. Rather than impose that
silently, the guardrail stays and `gaps.md` item 2 is carried over rather than fixed. The
documented workaround is the one main already requires: add the column without a default,
then `UPDATE` it, which streams as ordinary DML.

### Primary key is required

`paimon.add_table()` raises if the table has no primary key. This is a deliberate scope
reduction from main, which permits `pk_count = 0` (`extension/paimon_client.cpp:105`) and
produces an append-only Paimon table.

PostgreSQL refuses `UPDATE` and `DELETE` on a published table that has no replica
identity. `REPLICA IDENTITY FULL` lifts that restriction, so a PK-less table *can* stream
updates — but the stream then identifies a row only by its complete old value, which is
not a key: two identical rows are indistinguishable, and `merge-engine = deduplicate` has
nothing to deduplicate on. Every §5 write rule that references the old key loses meaning.
Supporting PK-less tables means a second write path onto an append table with
`(commit_lsn, op_index)` identity; it is deferred to a follow-up spec.

### Resolution of `gaps.md`

| Gap | Resolution |
|---|---|
| 1. DROP COLUMN not propagated | `SchemaChange.dropColumn` via the official Paimon API |
| 2. Defaults not backfilled on ADD COLUMN | **not fixed** — defaults stay blocked, per above |
| 3. DROP TABLE leaves warehouse data | `Catalog.dropTable(identifier, true)` |

---

## 5. Schema mapping and Paimon table options

### Type mapping

Lifted from `extension/type_map_pg.h` so the two architectures agree on `numeric`
precision/scale encoding, timestamp units, and UUID-as-string. Field IDs are `attnum`,
preserving main's convention from `extension/paimon_event.h` — the thing a
Relation-message-only consumer cannot recover, and the reason DDL goes over logical
messages rather than being inferred.

### The TOAST problem

`logicalrep_write_tuple()` (`src/backend/replication/logical/proto.c:46`) substitutes
`LOGICALREP_COLUMN_UNCHANGED` (`u`) for any attribute still held externally at
serialisation time:

```c
if (att->attlen == -1 && VARATT_IS_EXTERNAL_ONDISK(DatumGetPointer(values[i])))
{
    pq_sendbyte(out, LOGICALREP_COLUMN_UNCHANGED);
    continue;
}
```

`ReorderBufferToastReplace` reassembles a TOAST chain only from chunks written in the same
transaction, and only into `newtuple`. For the **new** tuple the gap is therefore exactly
and only *an UPDATE that does not touch a TOASTed column*. Everything else arrives complete
and byte-exact: INSERT always, COPY and multi-insert through `clear_toast_afterwards`
(`decode.c:1222-1228`), and any UPDATE that does rewrite the column. Truncation is
impossible by assertion — `Assert(data_done == VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer))`
— so a partial value is never silently delivered.

The **old** tuple behaves differently, and the design turns on this. `ExtractReplicaIdentity`
(`src/backend/access/heap/heapam.c`) force-inlines external values *before* the record
reaches WAL, in both of its branches:

```c
if (replident == REPLICA_IDENTITY_FULL)
{
    /* When logging the entire old tuple, it very well could contain
     * toasted columns. If so, force them to be inlined. */
    if (HeapTupleHasExternal(tp))
        tp = toast_flatten_tuple(tp, desc);
    return tp;
}
...
if (HeapTupleHasExternal(key_tuple))        /* key-only path */
    key_tuple = toast_flatten_tuple(oldtup, desc);
```

Because the old tuple is already flattened when it is written, the `EXTERNAL_ONDISK` test
in `logicalrep_write_tuple` never fires for it. Under `REPLICA IDENTITY FULL` the old tuple
carries **every column in full, TOASTed ones included**.

### Chosen resolution: `REPLICA IDENTITY FULL`, resolved on the write path

An UPDATE that leaves a TOASTed column untouched sends `u` for that column in the new
tuple, and the flattened old tuple carries the column's full value. The column was not
modified, so the old value *is* the new value; the writer substitutes it.

Exact, at the event's own LSN, with no cache, no lookup query, and no merge-engine
constraint. The writer stays stateless.

Per-table options:

```
merge-engine          = deduplicate
changelog-producer    = input
sequence.field        = _seq,_op_seq
ignore-update-before  = false
```

`sequence.field` is a list — `CoreOptions.sequenceField()` returns `List<String>` — and it
needs to be, because a `-U` and the `+U` it precedes originate from one change and would
otherwise carry the same sequence value. §7 defines the two fields. `ignore-update-before`
is set explicitly rather than left to its default: `-U` records are required by the
acceptance suite, and `TableWriteImpl` carries a `rowKindFilter` that this option drives.

No `bucket` option is written, matching `extension/paimon_schema.cpp:219-223` exactly. For a
primary-key table that means Paimon's **dynamic bucket mode**, in which `TableWrite.write(row)`
throws:

```
Can't extract bucket from row in dynamic bucket mode, you should use
'TableWrite.write(InternalRow row, int bucket)' method.
```

The writer therefore always calls `write(row, 0)`. Every file lands under `bucket-0/`, which is
where `extension/paimon_table_writer.cpp:155` puts it on main, so the layout the acceptance
suite inspects is unchanged. A single-threaded writer gains nothing from bucket-level
parallelism; if throughput ever demands it, moving to fixed buckets is a schema change and a
new decision, not an implementation detail.

`paimon.add_table()` issues `ALTER TABLE ... REPLICA IDENTITY FULL`. This takes a brief
`ACCESS EXCLUSIVE` lock for the catalog update, and thereafter adds the size of the old row
to the WAL written by every UPDATE and DELETE on that table. For a row carrying a large
`jsonb` column that is updated often, the flattened value is written to WAL on each
otherwise-unrelated update — see the second escape below for that case.

Write rules, per decoded message:

| pgoutput message | Emitted to Paimon |
|---|---|
| `I` Insert | `+I(new)` |
| `U` Update, key unchanged | `-U(old)`, `+U(new)`; each `u` column filled from `old` |
| `U` Update, key changed | `-D(old)`, `+I(new)`; each `u` column filled from `old` |
| `D` Delete | `-D(old)` |
| `T` Truncate | empty overwrite commit, matching main's `PaimonTableWriter::truncate()` (`extension/paimon_table_writer.cpp:302`) |

The presence of an old tuple is not evidence that the key changed. `heap_update` logs the
old key when `bms_overlap(modified_attrs, id_attrs) || id_has_external`
(`heapam.c:4007-4010`), so a row with a TOASTed key column produces one on every UPDATE,
and under `REPLICA IDENTITY FULL` the `'O'` old tuple is present unconditionally. The
writer compares old and new key values; it never infers a key move from presence alone.

### Two documented escapes

Both are per-table arguments to `paimon.add_table()`, neither is a default, and neither is
implemented in v1. They are recorded now so the table options do not have to be retrofitted
onto existing warehouses later.

**`changelog_producer => 'lookup'`.** Paimon reconstructs before-images from its own prior
state instead of taking them from Postgres. Verified present in `paimon-api` 1.4.1:
`CoreOptions$ChangelogProducer` is `{none, input, lookup, full-compaction}`, and
`changelog-producer.row-deduplicate` is documented as *"Whether to generate -U, +U changelog
for the same record. This configuration is only valid for the changelog-producer is lookup
or full-compaction."* `LookupChangelogMergeFunctionWrapper` carries `reusedBefore`,
`reusedAfter`, `setChangelog(before, after)` and a `valueEqualiser`. Use it when a
downstream consumer needs before-images consistent with *Paimon's* own history across
replays rather than with Postgres's. Costs: `changelog-producer.lookup-wait` defaults on, so
commit waits for lookup compaction — and commit is also the LSN-advance path, hence the
slot-retention path; and the lookup state is RocksDB-backed on the writer's local disk
(`org/apache/paimon/lookup/rocksdb/`), which makes the writer stateful. If enabled,
`changelog-producer.row-deduplicate-ignore-fields` must list the metadata columns of §6:
they change on every event and would otherwise defeat row deduplication entirely.

**`replica_identity => 'default'`.** For tables where logging the old row on every UPDATE is
unaffordable. `add_table()` then mutates nothing and takes no lock, at the price of moving
the TOAST work to the writer: `u` columns are resolved by a batched
`SELECT <cols> FROM t WHERE pk = ANY($1)` once per flush. That reads Postgres's *current*
value, so the result is eventually exact rather than exact at the event's LSN — a later
change emits its own higher-LSN event, and the table converges. Before-images are
unavailable under this escape, so it pairs with `changelog-producer = lookup` for any
consumer that needs them.

### What Paimon cannot do for us

Paimon's `lookup` producer *reports* a before-image; it does not merge. If the writer emits
`+U` with a NULL where a `u` column belongs, `deduplicate` stores the NULL and `lookup`
faithfully emits `-U(old)` / `+U(NULL)`. Filling `u` columns is unavoidably a write-path
responsibility.

Merging would require `merge-engine = partial-update`, which is rejected here.
`PartialUpdateMergeFunction` treats an incoming NULL as "no change" for *every* column, so
`UPDATE t SET note = NULL` on a plain `varchar` would be silently dropped. Sequence groups
recover that, but only by reserving a sentinel sequence value that no real sequence may
take, and they still cannot express `-U` or handle a primary-key value change.

`merge-engine` and `changelog-producer` are closed enums — `CoreOptions$MergeEngine` is
`{deduplicate, partial-update, aggregation, first-row}` — and `MergeFunctionFactory` is a
bare `@FunctionalInterface extends Serializable` that is **not** registered in the
`org.apache.paimon.factories.Factory` SPI, so custom merge semantics would mean forking
Paimon. The one supported extension door is a per-column aggregator: that SPI does carry
`FieldSumAggFactory`, `FieldLastNonNullValueAggFactory` and twenty-odd siblings, usable with
`merge-engine = aggregation`. Recorded as an escape hatch; not used.

---

## 6. Writer modules

`java/writer/` as an **independent Maven project**, not a module of `java/pom.xml`.
That pom is `<packaging>jar</packaging>` with no `<modules>`, and converting it to a
reactor parent would disturb the five shade profiles that build the existing test JARs.
The writer declares its own coordinates and depends on `paimon-bundle` and PgJDBC at the
same versions. One responsibility per class; each testable without the others.

| Module | Responsibility | Depends on |
|---|---|---|
| `SlotConsumer` | `PGReplicationStream` lifecycle, keepalives, status updates, reconnect | PgJDBC |
| `PgOutputDecoder` | protocol bytes → `ChangeEvent`; Relation cache; `n`/`t`/`u` column states | — |
| `DdlMessageHandler` | `Message` JSON → list of `SchemaChange` | `SchemaRegistry` |
| `SchemaRegistry` | PG relid → Paimon `Identifier`; applies evolution; owns field-id ↔ column mapping | Paimon `Catalog` |
| `ToastResolver` | fills `u` columns in the new tuple from the old tuple; decomposes key changes into `-D` + `+I` | — |
| `TxnBatcher` | groups by transaction, preserves LSN order, flushes on transaction boundary or size | — |
| `PaimonSink` | `TableWriteImpl` writes, commit with identifier | Paimon |
| `Backfiller` | exported-snapshot COPY → `+I` rows | PgJDBC, `PaimonSink` |
| `OffsetStore` | last committed LSN, read back from Paimon snapshots | Paimon |

### Slot connection

```
CREATE_REPLICATION_SLOT paimon LOGICAL pgoutput EXPORT_SNAPSHOT
START_REPLICATION SLOT paimon LOGICAL <lsn>
    (proto_version '2', publication_names 'paimon_pub',
     messages 'true', binary 'false', streaming 'off')
```

Decoded message types: `B` Begin, `C` Commit, `R` Relation, `I` Insert, `U` Update,
`D` Delete, `T` Truncate, `M` Message, `O` Origin, `Y` Type.

### Metadata fields

Main exposes three optional metadata columns via GUC
(`extension/paimon_heap.cpp:110-127`). Their availability changes here:

| Main GUC | New architecture | Source |
|---|---|---|
| `meta_field_xid` | supported, optional | `xid` from the pgoutput `Begin` message |
| `meta_field_op` | supported, **now mandatory** | ordinal within the transaction, assigned by `TxnBatcher`; second `sequence.field` |
| `meta_field_tid` | **not supported** | physical `(block << 16) \| offset` is not carried by logical decoding |
| `_seq` | mandatory, as in main | per-change LSN; first `sequence.field` |

`_seq` keeps its name and its role from main but changes its derivation. Main computes
`((uint64_t)xid << 32) | op` (`extension/paimon_heap.cpp:370`), which is not monotone across
XID wraparound; the per-change LSN is monotone for the lifetime of the cluster.

`meta_field_op` becomes mandatory because it is the tiebreaker that orders a `-U` before its
`+U` (§7). Its default name stays `_op_seq`, so T4 (`test4_metadataFields`) continues to
pass for `_xid` and `_op_seq`; only its `_tid` assertion must change. T5
(`test5_configurableFieldNames`) and T6 (`test6_schemaStructure`) assert the *presence* of
`_seq` and set the metadata GUCs to `''`, and both still pass — an additional mandatory
column breaks no assertion there.

`meta_field_tid` is replaced rather than emulated: no physical tuple identifier survives the
reorder buffer. This is a genuine, documented behaviour difference, not a defect.

### Text mode in v1

`binary 'false'`. A binary decoder needs a per-type parser for every PostgreSQL binary
output format and is the most error-prone part of a pgoutput decoder. Text output is
round-trip exact for the types in `type_map_pg.h`: `extra_float_digits = 3` is the default
since PG 12, `numeric` is exact as text, `bytea` is hex. Binary mode becomes an
optimisation behind a flag once the acceptance suite is green.

### Backfill

```
CREATE_REPLICATION_SLOT paimon LOGICAL pgoutput EXPORT_SNAPSHOT
    -> snapshot_name, consistent_point
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY
SET TRANSACTION SNAPSHOT '<snapshot_name>'
COPY <table> TO STDOUT        -> Paimon +I, _seq = consistent_point
COMMIT
START_REPLICATION SLOT paimon LOGICAL <consistent_point>
```

No gap, no duplicates, no lock on the table. Tables with `backfill_due = true` in the
registry are loaded before streaming begins.

---

## 7. Delivery guarantees

### Sequencing

`sequence.field = _seq,_op_seq`, compared in that order:

- `_seq` — the **per-change LSN**, read from the `XLogData` header via
  `PGReplicationStream.getLastReceiveLSN()`, not the transaction's commit LSN. Using the
  commit LSN would give every change in a transaction the same value and make tie
  resolution inside `deduplicate` load-bearing.
- `_op_seq` — an ordinal incremented once per **emitted record**, not per change. A `-U`
  and the `+U` that follows it come from one change and therefore share `_seq`; `_op_seq`
  separates them, and a stored `-U` winning that comparison would be read back as a
  retraction and silently drop the row.

The pair is lexicographically monotone over the whole stream: `_seq` is non-decreasing
because LSNs are, and `_op_seq` increases strictly within a transaction. This mirrors
main, whose `next_seq()` already uses a per-operation counter in its low 32 bits
(`extension/paimon_heap.cpp:370`); the difference is that the high bits are an LSN rather
than an XID.

### Intra-transaction key collisions

`TxnBatcher` does **not** collapse per key. Every change is written with its own `_seq`, so
Paimon orders them without help, and intermediate row versions are preserved rather than
discarded — matching main, which buffers every slot (`extension/paimon_heap.cpp:380`).

One case makes the rule concrete:

```sql
UPDATE t SET id = id + 1;        -- rewrites every key, in an unspecified row order
```

Each row decomposes to `-D(old_key)` + `+I(new_key)`, and those pairs interleave across
keys: the `+I` for key 3 may arrive before the `-D` that vacates it. Because every event
carries its own LSN and Paimon applies events in LSN order, the final state is correct for
any interleaving — the highest-LSN event for a key wins. The obligation this places on the
writer is narrow and absolute: **never reorder, collapse or deduplicate within a batch**,
because the batch's correctness rests entirely on LSN order surviving intact. T20 (§9) is
the named acceptance test.

### Complete transactions only

The pgoutput `streaming` option stays off. `proto_version '2'` can stream in-progress
transactions (`Stream Start` / `Stream Stop` / `Stream Commit` / `Stream Abort`), which
would deliver changes that may still abort, and the guarantee below requires that a batch
never contains a transaction that has not committed.

### Effectively-once

1. `TxnBatcher` accumulates complete transactions only — never a partial one.
2. `PaimonSink` commits with `commitIdentifier = commit_lsn` under a fixed `commitUser`,
   through `TableCommitImpl.filterAndCommit(Map<Long, List<CommitMessage>>)`.
3. Only after the Paimon commit returns does `SlotConsumer` advance the flushed and
   applied LSN.
4. On restart, `OffsetStore` reads the highest committed identifier for that `commitUser`
   from the Paimon snapshots and resumes the slot from it. `filterAndCommit` discards
   commits whose identifier that `commitUser` has already used, so replayed transactions
   are idempotent.

Two details carry the whole guarantee, and both were established by measurement rather than
from the API's shape (§12, spike 3):

- **`commit(identifier, messages)` does not deduplicate.** Replaying an already-committed
  identifier through it produces a second snapshot carrying duplicate data *and* duplicate
  changelog entries. Only `filterAndCommit` consults the committed-identifier set. The
  distinction is invisible until a crash recovery replays a transaction, which is precisely
  when it matters, so the plain `commit` overload must not appear in the writer.
- **`commitUser` must be stable across process restarts.** Deduplication is scoped to the
  user, so a per-process UUID — the obvious default for a Paimon writer — would make every
  restart a fresh identity whose replay nothing filters. The writer derives it from the
  replication slot name, which is already unique per writer (§4) and already durable.

Commit identity remains the transaction's commit LSN even though sequencing uses
per-change LSNs: the commit is the unit of atomicity, the per-change LSN the unit of
ordering. The two never need to agree.

Confirming the LSN only after the Paimon commit returns is what makes crash recovery a
property of the code rather than best-effort, and is why `extension/paimon_wal.cpp` has no
counterpart here.

---

## 8. Failure modes

| Condition | Behaviour |
|---|---|
| Writer down | Slot retains WAL, `pg_wal` grows, no data loss. Replaces main's silent ring drop. |
| Writer cannot keep up | Same backpressure; monitor `pg_replication_slots.safe_wal_size`. |
| Slot abandoned | `max_slot_wal_keep_size` caps growth, slot is invalidated, writer must re-bootstrap. New operational hazard; must be documented in the README. |
| Paimon commit fails | LSN not advanced; transaction replayed on retry. |
| Writer crash mid-batch | Uncommitted work discarded, replayed from last committed LSN. |
| DDL arrives for an unregistered table | Message ignored with a warning. |

### New limitations versus main

- Unlogged and temporary tables produce no logical changes and cannot be tracked.
- Partitioned tables require `publish_via_partition_root` set deliberately.
- `wal_level = logical` is mandatory.
- `merge-engine` is fixed at `deduplicate` for tracked tables.
- Tracked tables are switched to `REPLICA IDENTITY FULL`, which adds the old row to the
  WAL written by every UPDATE and DELETE and is visible to any other logical consumer of
  the same table. The §5 escape exists for tables that cannot afford it.
- `paimon.add_table()` briefly takes an `ACCESS EXCLUSIVE` lock to set the replica
  identity. Main's `SET ACCESS METHOD` took one too, so this is not a regression.

---

## 9. Testing

### Acceptance: the existing suite

`PaimonFeatureTest`, `PaimonAdvancedTest`, `PaimonOffloadTest`, `PaimonDdlTest`,
`PaimonSchemaIT` and `PaimonTypeTest` port over with changed **setup only**:
`CREATE EXTENSION paimon_slot` plus `paimon.add_table()` plus a running writer, in place of
`shared_preload_libraries` and `USING paimon_heap`. Assertions are kept.

Three suites change meaningfully:

| Test | Change |
|---|---|
| T4 (metadata fields) | `_tid` assertion replaced; `_xid` and `_op_seq` assertions kept (§6) |
| T10, T11 (vacuum, vacuum freeze) | Become assertions that nothing spurious appears — VACUUM emits no logical changes |
| T16 (crash recovery) | Strengthened: kill the writer mid-load, assert zero loss, which main cannot promise |
| D7, D8 (blocked PK) | Same error asserted, now raised from an event trigger rather than `ProcessUtility` |

T17 (`test17_defaultTam`) loses its meaning — there is no TAM — and becomes a check that an
unregistered table reaches neither the publication nor the warehouse.

### New tests

- `PgOutputDecoder` against recorded byte fixtures, including `u` columns, multi-column
  keys, and `Message` interleaved with DML.
- `SchemaRegistry` evolution unit tests for each DDL op.
- A direct Paimon test that `deduplicate` with `sequence.field = _seq,_op_seq` applies
  events in that order irrespective of the order they are written within one commit, and
  that a `-U` written alongside its `+U` never survives the merge.
- `ToastResolver` unit tests: `u` filled from the old tuple; key change decomposed into
  `-D` + `+I`; an old tuple present with an unchanged key treated as unchanged.
- `TxnBatcher` preserves LSN order and never splits a transaction.
- **T20** (new, named): `UPDATE t SET id = id + 1` over contiguous integer keys. One
  transaction rewrites every key, producing `-D` / `+I` pairs whose key ranges overlap and
  whose order is unspecified. Asserts the final Paimon state equals the final PostgreSQL
  state. This is the test that any writer-side collapsing or reordering would fail.
- A pg_regress suite for `paimon_slot`, replacing `extension/test/sql/paimon_heap.sql`.

### Harness

`docker/slot-demo/docker-compose.yml`: official `postgres:18` with `wal_level=logical`,
MinIO, and a writer container. No custom PostgreSQL build, which removes most of the
project's CI cost.

---

## 10. Repository layout

```
sql/paimon_slot/
  paimon_slot.control
  paimon_slot--1.0.sql
java/
  pom.xml                      unchanged
  writer/                      new, independent Maven project
    pom.xml
docker/slot-demo/
  docker-compose.yml
  writer/Dockerfile
docs/superpowers/specs/
  2026-10-09-logical-slot-writer-design.md
extension/                     untouched, excluded from branch CI
java/src/test/.../Paimon*Test.java    setup adapted, assertions kept
```

`extension/` stays in place rather than being deleted: it keeps the branch diff reviewable
and allows benchmarking the two write paths against the same workload later.

---

## 11. Implementation phases

The work is sequenced so each phase ends at a runnable state with a subset of the
acceptance suite passing, rather than integrating everything at the end.

| Phase | Deliverable | Green tests |
|---|---|---|
| 0 | The five spikes in §12 | — |
| 1 | `paimon_slot` registry + `add_table`, `SlotConsumer`, `PgOutputDecoder`, `PaimonSink`, `TxnBatcher` | T1–T3, T6–T8, `PaimonTypeTest` |
| 2 | Metadata fields, `ToastResolver`, key-change decomposition | T4 (adapted per §6), T5, T20 |
| 3 | DDL event triggers, messages, `DdlMessageHandler`, `SchemaRegistry` | D1–D9, `PaimonSchemaIT` |
| 4 | `Backfiller`, `OffsetStore`, effectively-once commit | T16, backfill tests |
| 5 | S3 and HDFS warehouse config, docker harness | T18, T19 |
| 6 | Vacuum/checkpoint no-op assertions, README, limitations | T9–T15, T17 |

Phase 0 gates the rest: a failure in spike 1 sends §5 back to the user before any writer
code is written, because the whole TOAST resolution rests on it.

## 12. Spikes to run before implementation

Each can invalidate part of the design, and each is cheap.

1. **The `u` placeholder and old-tuple flattening** — on a table with a TOASTed column
   under `REPLICA IDENTITY FULL`, use `pg_recvlogical` to confirm all three halves of §5:
   that an UPDATE which does not touch the column sends `u` for it in the new tuple, that
   the old tuple carries that column's full value, and that a short inline `text` value is
   not reported as `u`. **Load-bearing for §5** — the entire TOAST resolution rests on the
   second of the three. On failure, fall back to the `replica_identity => 'default'` escape
   with batched lookups and return to the user.
2. **PgJDBC + pgoutput + `messages 'true'`** — confirm `Message` records reach the stream
   through `PGReplicationStream`. Load-bearing for §4 and §6.
3. **Commit-identifier dedup** — confirm replaying an already-committed identifier is a
   no-op. Load-bearing for §7.
4. **Per-change LSN exposure** — confirm `getLastReceiveLSN()` returns the LSN of the
   change just read rather than a batch-level value, since `sequence.field` depends on it
   being per change. Load-bearing for §7.
5. **`-U` survival** — write `-U` and `+U` for one key with `ignore-update-before = false`
   and `sequence.field = _seq,_op_seq`; confirm both appear in a pre-merge read and that the
   merge keeps the `+U`. Load-bearing for §5 and §7, and for T2, E2E T2 and the offload tests.

### How a pre-merge read works here, and why it is not what main does

Main's acceptance suite reads row kinds through `PaimonFeatureTest.readRows`, which rebuilds
the stored schema with a `_row_kind` `TINYINT` field prepended at field id `-1`. That works on
main only because main writes its own Parquet files and physically emits that column —
`extension/paimon_parquet.cpp:50` prepends it and says so. Nothing in Paimon produces it.

Writing through Paimon's own `TableWrite` stores the row kind as `_VALUE_KIND`, so the
technique cannot be carried over, and it fails in two ways that are easy to misread:

- Declaring `_row_kind` yields `Required column is missing in data file. Col: [_row_kind]`,
  and declaring `_VALUE_KIND` is rejected outright — it is reserved, alongside `_ROW_ID`,
  `rowkind`, `_LEVEL` and `_SEQUENCE_NUMBER`. No replicated column may map to these five
  names, which §4's guardrails must reject.
- Stripping `merge-engine`, `changelog-producer` and `sequence.field` from the options does
  **not** bypass merging. With `primaryKeys` still set the table remains a
  `PrimaryKeyFileStoreTable`, and removing `merge-engine` merely restores its default, which
  is `deduplicate` — the same engine. `readRows` also swallows read failures in a
  `catch (Exception)` that only warns, so a broken read of this kind reports zero rows rather
  than failing, and an assertion on presence alone passes vacuously.

The supported route is the `$audit_log` system table over a streaming scan, which reads the
changelog that `changelog-producer = input` writes:

```java
Table al = cat.getTable(Identifier.create(db, table + "$audit_log"))
              .copy(Map.of("scan.mode", "from-snapshot", "scan.snapshot-id", "1"));
StreamTableScan scan = al.newReadBuilder().newStreamScan();
scan.plan();                                  // first plan() is always empty — discard it
List<Split> splits = scan.plan().splits();    // second plan() carries the changelog
```

Discarding the first `plan()` is required, not defensive. A merged batch read is the wrong
instrument for a different reason: it reports `rowKind = INSERT` for every surviving row, even
where the latest input for that key was `+U`, so row kinds must never be asserted on one.
