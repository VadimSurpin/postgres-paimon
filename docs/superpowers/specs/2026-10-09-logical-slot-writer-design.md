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
- `gaps.md` items 1–3 are in scope to fix properly, not to carry over.

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

**What it costs.** Unchanged-TOAST values are unavailable at any replica identity setting
(§5). Intra-transaction intermediate row versions are collapsed (§7). `merge-engine` is
constrained to `partial-update` (§5). An abandoned slot is a new operational hazard (§8).

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
    registered_at timestamptz NOT NULL DEFAULT now()
);
```

The registry is the extension's memory of last-known schema, which the guardrails diff
against. Publication membership is the source of truth for *which* tables are tracked;
the registry records *what they looked like*.

### API

- `paimon.add_table(regclass)` — sets `REPLICA IDENTITY FULL`, adds the table to
  `paimon_pub`, inserts the registry row, emits a `create` message.
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

**No longer blocked: `ADD COLUMN ... DEFAULT`.** Main rejects column defaults outright
(commits `abb792b`, `7529932`) because `gaps.md` item 2 has no cheap fix there. Under the
merge engine chosen in §5 it does: the handler emits `add_col` followed by a `refill_col`
job, and the writer replays `(pk, newcol, newcol_seq = LSN)` tuples that populate exactly
one column on existing records and touch nothing else.

### Primary key is required

`paimon.add_table()` raises if the table has no primary key. This is a deliberate scope
reduction from main, which permits `pk_count = 0` (`extension/paimon_client.cpp:105`) and
produces an append-only Paimon table. A PK-less table cannot use `partial-update`, because
that merge engine needs a key to merge on, so the §5 TOAST resolution would not apply and
`u` columns would be unrecoverable. Supporting PK-less tables means supporting a second
TOAST strategy; it is deferred to a follow-up spec.

### Resolution of `gaps.md`

| Gap | Resolution |
|---|---|
| 1. DROP COLUMN not propagated | `SchemaChange.dropColumn` via the official Paimon API |
| 2. Defaults not backfilled on ADD COLUMN | `refill_col` job, per above |
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

`logicalrep_write_tuple()` in core emits `LOGICALREP_COLUMN_UNCHANGED` (`u`) for any
attribute that is `VARATT_IS_EXTERNAL_ONDISK`, for the old tuple as well as the new one.
`ReorderBufferToastReplace` can only reassemble TOAST chains written in the same
transaction. Therefore a TOASTed column that an UPDATE did not touch is unavailable at
**any** replica identity setting. `REPLICA IDENTITY FULL` buys before-images; it does not
buy TOAST values. Main gets them free via `slot_getallattrs()`
(`extension/paimon_heap.cpp:394`).

### Chosen resolution: partial-update with sequence groups

`merge-engine = partial-update` retains the previous value for any omitted column. But
`PartialUpdateMergeFunction` treats an incoming NULL as "no change" for *every* column,
not only toastable ones, which would silently drop `UPDATE t SET note = NULL` on a plain
`varchar`. Sequence groups recover it: a group is overwritten only when its sequence field
advances, and NULLs do overwrite within a group.

Per-table options:

```
merge-engine                            = partial-update
partial-update.remove-record-on-delete  = true
changelog-producer                      = lookup
fields.main_seq.sequence-group          = <all non-toastable columns>
fields.<col>_seq.sequence-group         = <col>      -- one per toastable column
```

Write rules:

| Column state on UPDATE | Value sent | Group sequence sent | Effect |
|---|---|---|---|
| unchanged (`u`) | NULL | `0` (sentinel) | group not advanced, prior value retained |
| changed | new value | `commit_lsn` | group advanced, overwrites |
| set to NULL explicitly | NULL | `commit_lsn` | group advanced, NULL overwrites |

`0` is reserved as the "do not advance" sentinel, so **no real sequence value may ever be
0**. Backfill therefore stamps group sequences with the slot's `consistent_point` LSN, not
0 (§6) — otherwise a stored 0 compared against an incoming 0 could advance the group and
overwrite a live value with NULL.

"Toastable" is decided from the PG type: varlena types (`text`, `varchar`, `bytea`,
`numeric`, `jsonb`, …) get their own sequence group; fixed-width types cannot be TOASTed
and share `main_seq`. The `*_seq` columns exist only in the Paimon schema, not in
PostgreSQL.

**This section depends on spike 1 (§12).** If NULL does not overwrite within a sequence
group, or the comparison is strictly `>` rather than `>=`, fall back to writer-side
resolution — a bounded per-PK cache plus lookup by PK on miss — and return to the user
before switching, since that changes the user's explicit choice.

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
| `TxnBatcher` | groups by transaction, collapses per-PK, flushes on size or time | — |
| `PaimonSink` | `TableWriteImpl` writes, commit with identifier | Paimon |
| `Backfiller` | exported-snapshot COPY → `+I` rows | PgJDBC, `PaimonSink` |
| `OffsetStore` | last committed LSN, read back from Paimon snapshots | Paimon |

### Slot connection

```
CREATE_REPLICATION_SLOT paimon LOGICAL pgoutput EXPORT_SNAPSHOT
START_REPLICATION SLOT paimon LOGICAL <lsn>
    (proto_version '2', publication_names 'paimon_pub',
     messages 'true', binary 'false')
```

Decoded message types: `B` Begin, `C` Commit, `R` Relation, `I` Insert, `U` Update,
`D` Delete, `T` Truncate, `M` Message, `O` Origin, `Y` Type.

### Metadata fields

Main exposes three optional metadata columns via GUC
(`extension/paimon_heap.cpp:110-127`). Their availability changes here:

| Main GUC | New architecture | Source |
|---|---|---|
| `meta_field_xid` | supported | `xid` from the pgoutput `Begin` message |
| `meta_field_op` | supported | per-row counter within the transaction, assigned by `TxnBatcher` |
| `meta_field_tid` | **not supported** | physical `(block << 16) \| offset` is not carried by logical decoding |
| — | `meta_field_lsn` (new) | transaction `commit_lsn`, strictly more useful than a TID |

`meta_field_tid` is replaced rather than emulated. T4 (`test4_metadataFields`) and T5
(`test5_configurableFieldNames`) assert on the TID field and must be updated to assert on
`lsn` instead. This is a genuine, documented behaviour difference, not a defect.

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
COPY <table> TO STDOUT        -> Paimon +I, all group seqs = consistent_point
COMMIT
START_REPLICATION SLOT paimon LOGICAL <consistent_point>
```

No gap, no duplicates, no lock on the table. Tables with `backfill_due = true` in the
registry are loaded before streaming begins.

---

## 7. Delivery guarantees

Effectively-once, using Paimon's own commit-identifier dedup rather than a bespoke
mechanism:

1. `TxnBatcher` accumulates complete transactions only — never a partial one.
2. `PaimonSink` commits with `commitIdentifier = commit_lsn` under a fixed `commitUser`.
3. Only after the Paimon commit returns does `SlotConsumer` advance the flushed and
   applied LSN.
4. On restart, `OffsetStore` reads the highest committed identifier for that `commitUser`
   from the Paimon snapshots and resumes the slot from it. Paimon discards commits whose
   identifier it has already seen, so replayed transactions are idempotent.

Confirming the LSN only after the Paimon commit is what makes crash recovery a property of
the code rather than best-effort, and is why `extension/paimon_wal.cpp` has no counterpart
here.

### Ordering

The sequence value for every group is the transaction's `commit_lsn`. `TxnBatcher`
collapses multiple changes to the same primary key within one transaction to their final
state, which removes the question of how Paimon compares equal sequence values.

Consequence: intra-transaction intermediate row versions do not appear in the changelog.
Those versions were never visible to any other transaction. T2 (`test2_updateCapture`) and
`testCrudCdcAndCompaction` must be checked for reliance on them; both are expected to use
autocommit and be unaffected.

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
- `merge-engine` is fixed at `partial-update` for tracked tables.
- Intra-transaction intermediate row versions are collapsed.

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
| T10, T11 (vacuum, vacuum freeze) | Become assertions that nothing spurious appears — VACUUM emits no logical changes |
| T16 (crash recovery) | Strengthened: kill the writer mid-load, assert zero loss, which main cannot promise |
| D7, D8 (blocked PK) | Same error asserted, now raised from an event trigger rather than `ProcessUtility` |

T17 (`test17_defaultTam`) loses its meaning — there is no TAM — and becomes a check that an
unregistered table reaches neither the publication nor the warehouse.

### New tests

- `PgOutputDecoder` against recorded byte fixtures, including `u` columns, multi-column
  keys, and `Message` interleaved with DML.
- `SchemaRegistry` evolution unit tests for each DDL op.
- A direct Paimon test of sequence-group semantics (also spike 1).
- `TxnBatcher` per-PK collapse.
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
| 0 | The four spikes in §12 | — |
| 1 | `paimon_slot` registry + `add_table`, `SlotConsumer`, `PgOutputDecoder`, `PaimonSink`, `TxnBatcher` | T1–T3, T6–T8, `PaimonTypeTest` |
| 2 | Metadata fields, sequence groups, TOAST handling | T4, T5 (adapted per §6) |
| 3 | DDL event triggers, messages, `DdlMessageHandler`, `SchemaRegistry` | D1–D9, `PaimonSchemaIT` |
| 4 | `Backfiller`, `OffsetStore`, effectively-once commit | T16, backfill tests |
| 5 | S3 and HDFS warehouse config, docker harness | T18, T19 |
| 6 | Vacuum/checkpoint no-op assertions, README, limitations | T9–T15, T17 |

Phase 0 gates the rest: a failure in spike 1 sends §5 back to the user before any writer
code is written.

## 12. Spikes to run before implementation

Each can invalidate part of the design, and each is cheap.

1. **Paimon sequence-group semantics** — does NULL overwrite within a group, and is the
   comparison `>` or `>=`? Direct test against paimon-bundle 1.4.1. **Load-bearing for §5.**
   On failure, fall back to writer-side cache-and-lookup and return to the user.
2. **The `u` placeholder** — confirm via `pg_recvlogical` that an unchanged TOASTed column
   arrives as `u` under `REPLICA IDENTITY FULL`, and that a short inline `text` value does
   not. Load-bearing for §5.
3. **PgJDBC + pgoutput + `messages 'true'`** — confirm `Message` records reach the stream
   through `PGReplicationStream`. Load-bearing for §4 and §6.
4. **Commit-identifier dedup** — confirm replaying an already-committed identifier is a
   no-op. Load-bearing for §7.
