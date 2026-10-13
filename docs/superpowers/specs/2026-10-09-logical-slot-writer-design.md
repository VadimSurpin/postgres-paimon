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

The `bucket` option **is** written, so every table is in Paimon's **fixed (static) bucket
mode** and the writer calls `TableWrite.write(row)`, letting Paimon hash the primary key to
choose the bucket.

**Amendment (replacing ruling R1).** This originally wrote no `bucket` option, matching
`extension/paimon_schema.cpp:219-223` for layout parity with main. That put every primary-key
table in **dynamic bucket mode**, where `write(row)` throws —

```
Can't extract bucket from row in dynamic bucket mode, you should use
'TableWrite.write(InternalRow row, int bucket)' method.
```

— so the writer had to call `write(row, 0)` and every file landed under `bucket-0/`, as on main.
The reasoning was that a single-threaded writer gains nothing from bucket-level parallelism, which
is true and was not the cost. The cost is that dynamic bucket mode maintains a **key index**, to
remember which bucket each key went to. A measured TPC-C run billed it: `order_line` held 1.1 GB
on disk for 88 MB of data, and `index/` directories were 1.2 GB of a 1.8 GB warehouse — 63% of it.
Fixed bucketing removes the index entirely (measured: `index=420000` of 951387 bytes under
dynamic, absent under `bucket=4`) and makes `write(row)` work, so the forced-bucket-0 special
case disappears with it.

**The count** comes from `paimon.add_table()`, which resolves it once at registration and records
it in `paimon.table_registry.buckets`, whence the `create` message carries it:

* `buckets => N` names it explicitly, for any positive `N`;
* otherwise `paimon.default_buckets()` guesses from the only signal available,
  `clamp(4, ceil(pg_total_relation_size / 1 GiB), 64)`.

**That guess is mostly its floor, and knowingly.** The ordinary registration path requires an
*empty* table, so the size is 16 kB and says nothing; on the TPC-C schema at scale factor 1 even
the *loaded* tables are 39 MB at most, so the divisor never engages there either. The floor of 4
exists so that every new table is not a single-bucket hotspot; the ceiling of 64 because a
single-threaded writer gains nothing beyond it but open files per commit. The size only speaks when
a populated table is re-registered after `remove_table()`. `buckets => N` is the control that
matters.

**Tunable later, at a price.** `SchemaChange.setOption("bucket", "8")` is accepted on an existing
table, but Paimon does not redistribute existing data, so changing the count costs a rewrite of
the table rather than being free — and leaving it wrong is not permanent.

`paimon.add_table()` issues `ALTER TABLE ... REPLICA IDENTITY FULL`. This takes a brief
`ACCESS EXCLUSIVE` lock for the catalog update, and thereafter adds the size of the old row
to the WAL written by every UPDATE and DELETE on that table. For a row carrying a large
`jsonb` column that is updated often, the flattened value is written to WAL on each
otherwise-unrelated update — see the second escape below for that case.

**The key columns come from the registry, never from pgoutput's `Relation` flags.** Under
`REPLICA IDENTITY FULL` pgoutput marks **every** column as a key column — verified against a real
server, where `public.t (id int PRIMARY KEY, n int, body text)` arrives as three columns all with
`flags=1`. A writer that trusted those flags would compare every column when deciding whether the
key moved, so **any** update would look like a key change and be decomposed into `-D` + `+I`,
producing no `-U` at all. That defeats the reason this design chose `REPLICA IDENTITY FULL` with
`changelog-producer = input` over Paimon's `lookup` producer, and it is invisible in the merged
table: the row ends up correct and only the changelog is wrong. `paimon.table_registry.pk_cols` is
the authority, and the writer rebuilds each `Relation`'s key flags from it.

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
| `TxnBatcher` | groups by transaction, preserves LSN order, flushes on the transaction boundary **only** | — |
| `MiniBatcher` | accumulates completed transactions and commits them together on a row, byte or interval limit; drops transactions a table already holds (§7) | — |
| `PaimonSink` | `TableWriteImpl` writes, commit with identifier | Paimon |
| `Backfiller` | exported-snapshot COPY → `+I` rows | PgJDBC, `PaimonSink` |
| `OffsetStore` | last committed LSN, read back from Paimon snapshots | Paimon |

**Amendment (final whole-branch review):** "or size" above was wrong *as a trigger inside
`TxnBatcher`*. A transaction is one committed unit or nothing (section 7, "Complete transactions
only"), so splitting one on a record count would break atomicity and let the slot advance past
records that were never written. `PGPAIMON_FLUSH_MAX_RECORDS` is therefore the chunk size of the
**initial load** only, where each chunk is a commit of its own.

**Amendment (mini-batch commits):** the limits do exist now, one layer up, and they never split a
transaction: `MiniBatcher` accumulates *whole, completed* transactions and commits them together
on `PGPAIMON_BATCH_MAX_ROWS`, `PGPAIMON_BATCH_MAX_BYTES` or `PGPAIMON_BATCH_MAX_INTERVAL_MS`
(§7, "Effectively-once"). The streaming batch limits and the backfill's
`PGPAIMON_FLUSH_MAX_RECORDS` are different settings for different paths and do not interact.

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

Each row decomposes to `-D(old_key)` + `+I(new_key)`. The claim previously made here — that
LSN order makes the final state correct for any interleaving — is **false**, and T20 was written
to prove it. With rows `1..200` processed in ascending order the stream is
`-D(1), +I(2), -D(2), +I(3), …`, so for key 2 the `-D` carries a *higher* LSN than the `+I` that
filled it, the delete wins the merge, and only key 201 survives. The two events concern the same
key but different rows, and a Paimon primary-key table cannot hold both.

What rescues this is that **PostgreSQL rejects the statement outright** on a non-deferrable
primary key:

```
ERROR:  duplicate key value violates unique constraint "km_nd_pkey"
DETAIL:  Key (id)=(2) already exists.
```

Uniqueness is checked per row, so an ascending cascade cannot commit and the problematic
interleaving never reaches the WAL. It commits only when the key is
`DEFERRABLE INITIALLY DEFERRED`, and then the transaction genuinely passes through states in which
two rows share a key — states a Paimon primary-key table has no way to represent, at any LSN
ordering.

So `paimon.add_table()` **refuses a table whose primary key is deferrable**, which is what makes
the rule below sound rather than hopeful. A descending cascade (`id = id - 1`) commits normally,
vacates each key before filling it, and converges correctly; that is the case T20 asserts.

The obligation on the writer is unchanged and absolute: **never reorder, collapse or deduplicate
within a batch**, because correctness rests on LSN order surviving intact. Collapsing per key would
not have saved the deferred case either — `BEGIN; INSERT(5); DELETE(5); COMMIT;` must leave key 5
absent, so a rule of "any `+I` beats a `-D`" is wrong in the opposite direction. T20 (§9) is the
named acceptance test.

### Complete transactions only

The pgoutput `streaming` option stays off. `proto_version '2'` can stream in-progress
transactions (`Stream Start` / `Stream Stop` / `Stream Commit` / `Stream Abort`), which
would deliver changes that may still abort, and the guarantee below requires that a batch
never contains a transaction that has not committed.

### Effectively-once

*Amended for mini-batch commits.* The commit unit is a **batch** of whole transactions, not one
transaction.

1. `TxnBatcher` accumulates complete transactions only — never a partial one — and hands each to
   `MiniBatcher` as it completes.
2. `MiniBatcher` buffers completed transactions and flushes when any of three limits is reached:
   change records buffered (`PGPAIMON_BATCH_MAX_ROWS`, default 10,000), estimated text payload
   (`PGPAIMON_BATCH_MAX_BYTES`, default 16 MiB: the character count of every non-NULL value plus
   48 bytes per record), or time since the buffer's **first** transaction
   (`PGPAIMON_BATCH_MAX_INTERVAL_MS`, default 2,000). A limit is checked only between whole
   transactions, so none can split one. The time limit is checked on every event and from an idle
   callback on the single consumer thread; there is no timer thread.
3. A batch never spans a schema change or an overwrite. Records are built against the schema
   current at flush time, so the buffer is flushed **before** a DDL message is applied, **before**
   a truncate is applied (under its own transaction's commit LSN; the rest of that transaction
   starts a new buffer) and before a changed relation description is accepted.
4. At assembly, a transaction's records for a table are **dropped if its commit LSN is at or below
   that table's highest committed identifier** (per table; read from Paimon once and kept current
   in memory, re-read after any DDL). See "Why a timeout needs rule 4" below.
5. `PaimonSink` commits the batch with `commitIdentifier` = the commit LSN of the **last**
   transaction in it, under a fixed `commitUser`, through
   `TableCommitImpl.filterAndCommit(Map<Long, List<CommitMessage>>)`.
6. Only after the Paimon commit returns does `SlotConsumer` advance the flushed and applied LSN,
   and it advances it to the **last** transaction's end LSN. Buffered, uncommitted transactions are
   never confirmed.
7. On restart, `OffsetStore` reads the highest committed identifier for that `commitUser` from the
   Paimon snapshots and resumes the slot from it (the minimum across tables). `filterAndCommit`
   discards commits whose identifier is at or below one that `commitUser` has already used, and
   rule 4 discards the overlapping case it cannot, so replayed transactions are idempotent.

Failure handling around the buffer: an exception from the commit is fatal and nothing after it is
confirmed; on connection loss the buffer is discarded (the transactions are complete but
unconfirmed and replay); on a clean stop the buffer is committed and confirmed before the final
status update. Memory is bounded by the row and byte limits plus the one transaction that crossed
them. `PGPAIMON_BATCH_MAX_ROWS=1` reproduces one commit per transaction exactly.

Three details carry the whole guarantee, established by measurement rather than from the API's
shape (§12, spike 3, and the mini-batch experiment):

- **`commit(identifier, messages)` does not deduplicate.** Replaying an already-committed
  identifier through it produces a second snapshot carrying duplicate data *and* duplicate
  changelog entries. Only `filterAndCommit` consults the committed-identifier set. The
  distinction is invisible until a crash recovery replays a transaction, which is precisely
  when it matters, so the plain `commit` overload must not appear in the writer.
- **`filterAndCommit` discards a commit whose identifier is at or below the latest one the commit
  user has committed, not merely an exact match.** Measured on Paimon 1.4.1, one table, one user:
  10 committed; then 7 is discarded, 12 is committed, 12 again is discarded, 11 (below the latest
  12) is discarded, 10 again is discarded; a different user's 3 is committed. So a replay that is
  regrouped to end *lower* is already filtered, and only a regrouping that ends *higher* and
  overlaps is dangerous.
- **`commitUser` must be stable across process restarts.** Deduplication is scoped to the
  user, so a per-process UUID — the obvious default for a Paimon writer — would make every
  restart a fresh identity whose replay nothing filters. The writer derives it from the
  replication slot name, which is already unique per writer (§4) and already durable.

#### Why a timeout needs rule 4

A size limit is a deterministic function of the stream, so a replay regroups identically. A time
limit is not: after a crash the same transactions can be grouped differently. Suppose T1..T10
committed under L10 and the process died before the server learned of it. On replay the timer
fires later and produces T1..T12 under L12. L12 > L10, so `filterAndCommit` commits it, and
T1..T10 are in the table twice. The merged table survives (`deduplicate` over `sequence.field =
_seq,_op_seq`, both per-change, collapses identical records), but `changelog-producer = input`
writes **every** input record, so the changelog would carry the duplicates and effectively-once
would silently become at-least-once exactly where the exactness assertions live (`-U == +U`,
`+I − -D == row count` in T2 and the TPC-C comparison).

Rule 4 closes it, and it has the same shape as `filterAndCommit` and the DDL LSN gate, applied per
transaction instead of per commit. It must be **per table**: the sink commits table by table, so a
transaction touching A and B where A is already committed past it and B is not (the partly
committed case the minimum-across-tables resume rule exists for) applies to B alone. Backfill
identifiers (`cp - chunks + 1 .. cp`) and truncate identifiers count as committed identifiers.

Commit identity is the **last transaction's** commit LSN even though sequencing uses per-change
LSNs: the commit is the unit of atomicity, the per-change LSN the unit of ordering. The two never
need to agree, and the several transactions in a batch share one identifier but keep distinct
`_seq` values, so ordering is unaffected.

Confirming the LSN only after the Paimon commit returns is what makes crash recovery a
property of the code rather than best-effort, and is why `extension/paimon_wal.cpp` has no
counterpart here. pgjdbc advances its own flushed position on a keepalive, gated on
`explicitlyFlushedLSN >= startOfLastMessageLSN`; with a batch sitting in the buffer across idle
keepalives that gate was re-verified live (T16c) rather than assumed.

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
| Writer restarts | `SchemaRegistry` is rebuilt from `paimon.table_registry` before the slot is consumed. See below. |

### What "replayed on retry" does not cover

**Amendment (final whole-branch review).** The row "Paimon commit fails → LSN not advanced;
transaction replayed on retry" reads as though every write-path failure were transient. It is not,
and the table says nothing about the case that matters:

> a **non-retryable** failure in the write path, plus a slot that advances only after a successful
> commit, equals a **permanent stop**.

The replay is faithful, so a replay that can never succeed repeats for ever. Three independent
defects landed in that blind spot: non-idempotent DDL replay, two ALTERs Paimon refuses after
PostgreSQL has committed them, and a value Paimon cannot represent.

The first two are closed, and closed in the only place they can be — by giving the operation a
commit identifier so a replay is skipped, or by refusing it before it reaches WAL (`paimon_slot`'s
guardrails on a key-column type change and on nullable → NOT NULL). That is the general rule this
section should be read with: **anything the write path cannot retry must be impossible to
enqueue.**

On the first of those: *"every DDL op is a no-op when already applied"* was claimed here and was
**false**. Per-op state inspection makes a single repeated op safe and cannot make a *sequence*
safe — `ADD COLUMN num int` then `ALTER COLUMN num TYPE bigint` leaves a replayed `add_col` looking
at a BIGINT column, indistinguishable from a genuine conflict, and `ADD COLUMN x` then
`RENAME x TO y` leaves a replayed `add_col` re-creating a spurious `x` for the replayed rename to
collide with. What closes it is the mechanism data already had: each DDL message carries its own
WAL LSN, the LSN of the last applied one is recorded in the Paimon table's schema options in the
same `alterTable` call as the column change, and a message at or below it is skipped. The claim
that holds is the invariant, not the per-op one: **replaying any suffix of an already-applied DDL
sequence converges to the same schema and never fails.**

The third cannot be closed that way. `INSERT INTO v VALUES (2, 'NaN')` into a `numeric` column is
accepted by every guardrail — the column *type* maps, only that one *value* does not — and Paimon
has no representation for it. Reject-never-clamp is still right; what was never considered is that
the rejection is unbounded, so one ordinary INSERT stops replication for the whole database,
permanently, and still does after the row is deleted, because the WAL record is in the slot. The
same holds for a `numeric` beyond `DECIMAL(38, 18)`.

**Amendment.** An earlier version of this paragraph said "no temporal value is refused any more",
on the strength of `infinity` being encoded as an extreme finite value rather than rejected. That
was wrong twice over. Paimon's `Timestamp.toMicros()` is
`multiplyExact(millisecond, 1000) + nanoOfMillisecond / 1000` — the multiply is checked and the add
is not — and Parquet's writer calls it per row, so the largest instant the writer can store is
`294247-01-10 04:00:54.775807`, about thirty years *below* PostgreSQL's own maximum. A range of
legal PostgreSQL timestamps therefore cannot be stored at all, and the first encoding target was
itself inside the overflow window, so it failed in `prepareCommit` where neither poison arm catches
it — an undiagnosed permanent stop, strictly worse than the diagnosed one it replaced. The bound is
now computed, checked in `TextDecoder`, and surfaced as `UnrepresentableValueException`, which
makes an out-of-range timestamp both diagnosable and skippable. Dates are unaffected: PostgreSQL's
whole date range fits Paimon's int.

The lesson generalises past this one value: **a round-trip through an in-memory object is not
evidence that a value can be stored.** The mapping was verified by `fromLocalDateTime` followed by
`toLocalDateTime`, which never calls the arithmetic the writer depends on. Nothing in the unit or
acceptance suites wrote a decoded `infinity` to disk, so nothing noticed. The test that now covers
it writes a row to a Paimon file and reads it back.

**The failure is made diagnosable, and given one narrow escape.** The error names the table, the
column, the value, the change LSN and its commit LSN, and states that the record is in the slot so
deleting the row will not help.

`PGPAIMON_SKIP_LSN` then lets an operator who has read that error and inspected the row advance
past **exactly one** record. It is set to the offending record's change LSN; the rest of that
record's transaction commits normally, so the loss is one row rather than everything committed
alongside it; and the record's complete raw text values are logged at WARNING with the table, the
column and both LSNs, so the row can be reinserted by hand. The loss is recoverable, not merely
survivable.

The constraints are the design, because the obvious failure mode of such a thing is to decay into
an "ignore errors" switch:

- one LSN and never a range, so a mistyped digit cannot discard arbitrary data;
- one record, not the transaction;
- only a `TextDecoder.UnrepresentableValueException` — a dedicated type thrown solely for a value
  PostgreSQL holds and Paimon has no representation for. A commit failure, a schema error, an arity
  mismatch, or a value that does not *parse* as its Paimon type at the very same LSN all stay
  fatal. That last distinction matters: a parse failure is systematic (a drifted column type, a
  session setting) and would mis-handle every row of the column, so skipping one row of it would
  hide the cause;
- the raw values logged in full, which is what makes it an audit trail rather than a hole;
- a configured LSN at or below the resume position is announced as unable to fire, so a stale
  setting is visible — but never refused, since refusing would turn a leftover variable into an
  outage.

**What v1 still does not have is a quarantine.** Nothing records the skipped record durably for a
reader to find — the log line is the whole audit trail — and nothing automates the decision. A
dead-letter table is a design decision in its own right and is left to a follow-up spec. The hazard
and the exact operator sequence are written up in `docs/logical-slot-architecture.md`, "A record the
writer can never apply".

### Registry bootstrap on startup

`SchemaRegistry` learns a table's identity from a `create` DDL message, and `add_table()` emits
that message exactly once. Nothing re-emits it. So a restarted writer that consumed the slot
immediately would know about no table at all, would classify every replicated row as belonging to
an untracked relation, and would discard it under the rule in the row above — silently, and for
every tracked table, from the first restart onward.

The writer therefore queries `paimon.table_registry` over its JDBC connection at startup, before
it opens the replication stream, and reconstructs the equivalent of a `create` for each row. The
registry table is the only place the `relid` → Paimon identity mapping is recorded: the Paimon
catalog knows which tables exist but not which PostgreSQL OID each one belongs to, so the mapping
cannot be recovered from the warehouse alone. This is also why `add_table()` writes the registry
row inside the same transaction as the `create` message — the two must not diverge.

Bootstrapping from the registry rather than from a replayed message stream also means the writer's
startup cost is one query, independent of how much WAL the slot is holding.

### New limitations versus main

- Unlogged and temporary tables produce no logical changes and cannot be tracked.
- Partitioned tables are **refused outright** in v1. `publish_via_partition_root` would be the route to supporting them, and is not taken here.
- `wal_level = logical` is mandatory.
- `merge-engine` is fixed at `deduplicate` for tracked tables.
- Tracked tables are switched to `REPLICA IDENTITY FULL`, which adds the old row to the
  WAL written by every UPDATE and DELETE and is visible to any other logical consumer of
  the same table. The §5 escape exists for tables that cannot afford it.
- `paimon.add_table()` briefly takes an `ACCESS EXCLUSIVE` lock to set the replica
  identity. Main's `SET ACCESS METHOD` took one too, so this is not a regression.
- **A *non-empty* table cannot be added once the writer's slot exists**, and the refusal comes
  from the **writer at its next start**, not from `add_table()` — which succeeds, leaving
  `backfill_due` set. `add_table()` sets
  `backfill_due` from whether the table actually has rows, under the `SHARE ROW EXCLUSIVE` lock it
  already holds — which conflicts with `ROW EXCLUSIVE`, so no concurrent write can commit between
  the check and the registry row. An **empty** table therefore registers fine against a live
  writer: backfill exists to copy rows committed before streaming began, an empty table has none,
  and the `create` message travels through the slot so the running writer tracks it immediately.
  A **non-empty** table still cannot be added, and a restart refuses to start rather than
  proceeding: a correct backfill would have to read a snapshot at a known LSN and then suppress
  streamed changes for that table from before that point, and the slot's stream is shared by every
  tracked table. Refusing loudly is deliberate — the alternative is a table whose Paimon copy
  silently begins mid-history. The recourse is to stop the writer, drop and recreate the slot, and
  let the first start backfill every registered table together.
- **Each PostgreSQL schema becomes its own Paimon database**, so a table lands at
  `<warehouse>/<schema>.db/<table>` rather than in one configured database. Main puts every table
  in `default.db`, so this is a deliberate layout difference from it, and a warehouse written by
  an earlier build of this branch is NOT readable by this one. It is what lets `public.f` and
  `s2.f` both be replicated: with the schema dropped from the identity they resolved to one Paimon
  table and interleaved into it, two tables' rows merging on a shared primary key. There is
  consequently no Paimon database setting; `PGPAIMON_DATABASE` was removed rather than left
  naming nothing.
- A crash after some tables are marked backfilled but before others leaves a state that needs
  manual re-registration, for the same reason.

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
