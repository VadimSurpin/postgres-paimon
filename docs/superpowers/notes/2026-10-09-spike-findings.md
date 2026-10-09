# Spike findings — logical-slot writer architecture

Date: 2026-10-09
Branch: `feat/logical-slot-writer`
Task: Task 1 of the logical-slot-writer plan (spec §12 spikes). Throwaway code only; this
note is the deliverable.

## Environment

| Item | Value |
| --- | --- |
| PostgreSQL | `PostgreSQL 18.6 (Ubuntu 18.6-0ubuntu0.26.04.1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 15.2.0-16ubuntu1) 15.2.0, 64-bit` |
| `wal_level` | `logical` |
| Server binaries | `/usr/lib/postgresql/18/bin` (not on the default `PATH`) |
| JDK | `openjdk version "17.0.20.1" 2026-08-18` / `OpenJDK 64-Bit Server VM (build 17.0.20.1+1-1-26.04-Ubuntu, mixed mode, sharing)` |
| Maven | `Apache Maven 3.9.12`, Java 17.0.20.1, `/usr/share/maven` |
| Paimon | `org.apache.paimon:paimon-bundle:1.4.1` (from `java/pom.xml`) |
| PgJDBC | `org.postgresql:postgresql:42.7.3` (from `java/pom.xml`) |

**These spikes ran against a throwaway local PostgreSQL 18 cluster, not a container:
this machine has neither Docker nor podman available.** The cluster lives in
`.superpowers/sdd/2026-10-09-logical-slot-writer/spike/pgdata` on port 5440, created with
`initdb --auth=trust` and `wal_level=logical`, and all clients connect over TCP to
`127.0.0.1:5440`.

An unrelated PostgreSQL 19beta1 tree exists at `/home/vadim/cloud-db/distro/pg` on this
machine. It was **not** used — the spec pins PostgreSQL 18 and mixing the two would
invalidate these findings.

### Environment deviations from the brief

1. **`unix_socket_directories` must be overridden.** Ubuntu's PostgreSQL 18 build defaults
   it to `/var/run/postgresql`, which this user cannot write, so `pg_ctl start` dies with
   `FATAL: could not create lock file "/var/run/postgresql/.s.PGSQL.5440.lock": Permission
   denied` while `pg_ctl` itself only prints `could not start server`. The cluster config
   now carries `unix_socket_directories = ''`. Every client in the brief already connects
   over TCP (`-h 127.0.0.1`, `jdbc:postgresql://127.0.0.1:5440/spike`), so nothing else
   changed. Do not point it at a directory under a long path instead — a socket path over
   107 bytes fails with `Unix-domain socket path "..." is too long`.
2. **No `xxd` on this machine** (only `od`). `spike1.hex` was produced by a throwaway
   Python script that emits byte-identical `xxd` default format. The raw stream is also
   kept verbatim as `$SPIKE/spike1.bin`, which is the better fixture source for Task 6.
3. **`pg_recvlogical --no-loop` does not exit at end-of-WAL.** It keeps the stream open
   waiting for more WAL, so the brief's pipeline never terminates on its own. The decode
   was run with `-f <file>` and the process stopped once the file stopped growing; the
   whole 256408-byte stream had in fact been written within the first second. A future
   task that scripts `pg_recvlogical` should pass `-E <endpos>` to get a deterministic
   exit.
4. **The brief's `repeat('x', 100000)` does not produce an externally TOASTed value** — it
   compresses to 1156 bytes and stays inline, which would have made spike 1 test nothing.
   The brief anticipates this; its documented fallback (`string_agg(md5(g::text), '')` over
   `generate_series(1,4000)`) was used, giving `pg_column_size = 128000`, 65 rows in
   `pg_toast.pg_toast_16395`, and a 180224-byte toast relation. Spike 1 below is the
   re-run on the incompressible value. **Any later task that needs a genuinely TOASTed
   test value must use the `md5` form, not `repeat()`.**

---

## Spike 1 (GATE) — `u` placeholder and old-tuple flattening under `REPLICA IDENTITY FULL`

**Assumption under test.** With `REPLICA IDENTITY FULL`, an `UPDATE` that does not touch a
TOASTed column produces a pgoutput `U` message whose **old** tuple carries that column's
value *in full* (because `ExtractReplicaIdentity` calls `toast_flatten_tuple` at WAL-write
time), while the **new** tuple carries the single byte `'u'` (0x75) for the same column.
The entire write path of this project is designed around that asymmetry (spec §5).

### Command

```bash
psql -p 5440 -h 127.0.0.1 -U postgres -d spike <<'SQL'
CREATE TABLE t (id int PRIMARY KEY, n int, body text);
ALTER TABLE t REPLICA IDENTITY FULL;
CREATE PUBLICATION p FOR TABLE t;
SELECT pg_create_logical_replication_slot('s', 'pgoutput');
INSERT INTO t VALUES (1, 1, (SELECT string_agg(md5(g::text), '') FROM generate_series(1,4000) g));
INSERT INTO t VALUES (2, 2, 'short');
UPDATE t SET n = 99 WHERE id = 1;    -- does NOT touch body
UPDATE t SET n = 99 WHERE id = 2;    -- does NOT touch body, body is inline
SQL
pg_recvlogical -p 5440 -h 127.0.0.1 -U postgres -d spike -S s --start \
  -o proto_version=2 -o publication_names=p -o messages=true -o binary=false \
  -f "$SPIKE/spike1.bin" --no-loop
```

### TOAST precondition

```
 id | pg_column_size | length
----+----------------+--------
  1 |         128000 | 128000
  2 |              6 |      5

      reltoastrelid      | pg_total_relation_size
-------------------------+------------------------
 pg_toast.pg_toast_16395 |                 180224

SELECT count(*) FROM pg_toast.pg_toast_16395  ->  65
```

The `id = 1` value is genuinely stored out of line: 65 toast chunks, `pg_column_size`
equal to `length` (so uncompressed, external). This is the precondition spike 1 needs.

### Decoded stream

Decoded with a throwaway pgoutput parser over `$SPIKE/spike1.bin` (256408 bytes, all of
which parsed cleanly — no trailing slack):

```
0x00000000 B  final_lsn=0/1BD7D00 xid=765
0x00000016 R  relid=16395 public.t replident='f' cols=['id', 'n', 'body']
0x0000004d I  relid=16395
    N tuple: ncols=3  offset=0x53..0x1f466
      col0 kind='t' len=1 value=b'1'
      col1 kind='t' len=1 value=b'1'
      col2 kind='t' len=128000 value=b'c4ca4238a0b923820dcc509a...f3112fb9a584fbd9edfc6c90'
0x0001f467 C  commit_lsn=0/1BD7D00 end_lsn=0/1BD7D30
0x0001f482 B  final_lsn=0/1BD7DB8 xid=766
0x0001f498 I  relid=16395
    N tuple: ncols=3  offset=0x1f49e..0x1f4b6
      col0 kind='t' len=1 value=b'2'
      col1 kind='t' len=1 value=b'2'
      col2 kind='t' len=5 value=b'short'
0x0001f4b7 C  commit_lsn=0/1BD7DB8 end_lsn=0/1BD7DE8
0x0001f4d2 B  final_lsn=0/1BF7418 xid=767
0x0001f4e8 U  relid=16395
    O tuple: ncols=3  offset=0x1f4ee..0x3e901
      col0 kind='t' len=1 value=b'1'
      col1 kind='t' len=1 value=b'1'
      col2 kind='t' len=128000 value=b'c4ca4238a0b923820dcc509a...f3112fb9a584fbd9edfc6c90'
    N tuple: ncols=3  offset=0x3e902..0x3e912
      col0 kind='t' len=1 value=b'1'
      col1 kind='t' len=2 value=b'99'
      col2 kind='u'  <no value>
0x0003e913 C  commit_lsn=0/1BF7418 end_lsn=0/1BF7448
0x0003e92e B  final_lsn=0/1BF74B0 xid=768
0x0003e944 U  relid=16395
    O tuple: ncols=3  offset=0x3e94a..0x3e962
      col0 kind='t' len=1 value=b'2'
      col1 kind='t' len=1 value=b'2'
      col2 kind='t' len=5 value=b'short'
    N tuple: ncols=3  offset=0x3e963..0x3e97c
      col0 kind='t' len=1 value=b'2'
      col1 kind='t' len=2 value=b'99'
      col2 kind='t' len=5 value=b'short'
0x0003e97d C  commit_lsn=0/1BF74B0 end_lsn=0/1BF74E0
--- parsed 256408 of 256408 bytes ---
```

Both predictions hold exactly:

- `id = 1`: old tuple `col2 kind='t' len=128000` (the **full** flattened value); new tuple
  `col2 kind='u'`.
- `id = 2`: `'short'` appears **in full in both tuples** — a short inline value is never
  reported as `u`.
- The `R` message reports `replident='f'`, confirming `REPLICA IDENTITY FULL` was in force.

### Verdict: **CONFIRMED**

Spec §5's resolution strategy holds: the writer can always recover an unchanged-TOAST
column from the old tuple of the same `U` message, with no catalog lookup and no extra
round trip to PostgreSQL.

### Byte fixtures for Task 6

Source of truth: `$SPIKE/spike1.bin` (raw stream) and `$SPIKE/spike1.hex` (xxd-format
dump of the same bytes), where
`$SPIKE = .superpowers/sdd/2026-10-09-logical-slot-writer/spike`. Both are git-ignored and
deliberately **not** deleted — Task 6 reads them.

**Read this before using the dump as a fixture:** `pg_recvlogical` appends a `0x0a`
newline after **every** message it writes to the output file. The file is therefore *not*
a clean concatenation of pgoutput messages; there are 13 messages and 13 injected
newlines. A fixture built by slicing `spike1.bin` must drop that separator byte, and a
parser written against the real `PGReplicationStream` must **not** expect it (PgJDBC hands
over one message per `ByteBuffer`, with no separator — see spike 2).

**How to read the hex excerpts below.** They are `xxd`-style in *format* (8-hex-digit
address, byte pairs, ASCII gutter) but **not** `xxd` output, and two things differ from a
plain `xxd spike1.hex`:

1. **The addresses are offsets of the message within the file, so they are not
   16-byte-aligned rows.** Each excerpt starts at the first byte of the message, not at a
   multiple of 16, so its rows are offset relative to the aligned rows that `xxd` or the
   `spike1.hex` dump produce for the same bytes. Cross-checking a value against
   `spike1.hex` means looking at the byte at that *offset*, not at the identically
   numbered line.
2. **The offsets are into `spike1.bin`, which contains the injected `0x0a` separators**
   described above. They are therefore not offsets into a clean pgoutput message stream;
   subtract one separator byte per preceding message to convert.

Neither point affects the byte *values* quoted, only how to locate them. The field tables
below are the authoritative form for building a fixture.

#### `U` message for `id = 1` — the TOAST case

Message span `0x1f4e8..0x3e912`, **128042 bytes**. Header plus the old tuple through the
first 32 bytes of the value:

```
0001f4e8: 5500 0040 0b4f 0003 7400 0000 0131 7400  U..@.O..t....1t.
0001f4f8: 0000 0131 7400 01f4 0063 3463 6134 3233  ...1t....c4ca423
0001f508: 3861 3062 3932 3338 3230 6463 6335 3039  8a0b923820dcc509
0001f518: 6136 6637 3538 3439 62                   a6f75849b
```

```
-- [ 127936 bytes of the old tuple's body value ELIDED ] --
```

Last 32 bytes of the value, then the `N` new tuple **in full**:

```
0003e8e1: 3162 6436 3963 3764 6633 3131 3266 6239  1bd69c7df3112fb9
0003e8f1: 6135 3834 6662 6439 6564 6663 3663 3930  a584fbd9edfc6c90
0003e901: 4e00 0374 0000 0001 3174 0000 0002 3939  N..t....1t....99
0003e911: 75                                       u
```

Field-by-field, for a hand-built fixture:

| Bytes | Meaning |
| --- | --- |
| `55` | `'U'`, Update |
| `00 00 40 0b` | relid 16395 |
| `4f` | `'O'`, old tuple follows (RI FULL, so not `'K'`) |
| `00 03` | 3 columns |
| `74 00 00 00 01 31` | col0 `'t'` len 1 `"1"` |
| `74 00 00 00 01 31` | col1 `'t'` len 1 `"1"` |
| `74 00 01 f4 00` + 128000 bytes | col2 `'t'` len 0x0001f400 = 128000, full value |
| `4e` | `'N'`, new tuple follows |
| `00 03` | 3 columns |
| `74 00 00 00 01 31` | col0 `'t'` len 1 `"1"` |
| `74 00 00 00 02 39 39` | col1 `'t'` len 2 `"99"` |
| `75` | col2 `'u'` — **unchanged TOAST, no length, no value** |

The value itself is `string_agg(md5(g::text), '')` over `generate_series(1,4000)`: 4000
concatenated lowercase MD5 hex digests, starting `c4ca4238a0b923820dcc509a6f75849b`
(= `md5('1')`) and ending `...a584fbd9edfc6c90`. It is reproducible exactly, so a fixture
need not store the middle verbatim — regenerate it from the same expression.

#### `U` message for `id = 2` — the inline case, complete

Message span `0x3e944..0x3e97c`, 56 bytes, nothing elided:

```
0003e944: 5500 0040 0b4f 0003 7400 0000 0132 7400  U..@.O..t....2t.
0003e954: 0000 0132 7400 0000 0573 686f 7274 4e00  ...2t....shortN.
0003e964: 0374 0000 0001 3274 0000 0002 3939 7400  .t....2t....99t.
0003e974: 0000 0573 686f 7274                      ...short
```

Note the contrast with `id = 1`: col2 of the `N` tuple is `74 00 00 00 05 73 68 6f 72 74`
(`'t'`, len 5, `"short"`), **not** `75`.

---

## Classpath (step 3)

```bash
mvn -f /home/vadim/postgres-paimon/java/pom.xml \
    dependency:build-classpath -Dmdep.outputFile="$SPIKE/cp.txt" -DincludeScope=runtime
```

`BUILD SUCCESS`, 185 classpath entries, including both jars the spikes need:

```
/home/vadim/.m2/repository/org/apache/paimon/paimon-bundle/1.4.1/paimon-bundle-1.4.1.jar
/home/vadim/.m2/repository/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
```

First Maven run on this machine; it downloaded roughly 145 MB and took about 9 minutes.
Note for later tasks: `paimon-bundle` **shades** Parquet into
`org.apache.paimon.shade.org.apache.parquet`, so there is no unshaded Parquet reader on
this classpath — a test cannot open a Paimon data file with a plain Parquet API.

---

## Spike 2 — pgoutput `Message` records reach PgJDBC

**Assumption under test.** A `pg_logical_emit_message` record surfaces to a
`PGReplicationStream` consumer as a pgoutput `M` message, so the extension can ship DDL
out of band through the same slot.

### Command

```bash
psql -p 5440 -h 127.0.0.1 -U postgres -d spike \
  -c "SELECT pg_create_logical_replication_slot('s2','pgoutput')"
javac -cp "$SPIKE_CP" -d "$SPIKE/jdbc" "$SPIKE/jdbc/Spike2.java"
( sleep 3; psql -p 5440 -h 127.0.0.1 -U postgres -d spike \
    -c "SELECT pg_logical_emit_message(true, 'paimon', '{\"v\":1,\"op\":\"ping\"}')" \
    -c "INSERT INTO t VALUES (3,3,'z')" \
    -c "INSERT INTO t VALUES (10,10,'a'),(11,11,'b'),(12,12,'c')" ) &
java -cp "$SPIKE_CP:$SPIKE/jdbc" Spike2 | tee "$SPIKE/spike2.txt"
```

The connection sets no `password` property; the cluster is `--auth=trust`.

### Output (complete)

```
kind=B lsn=LSN{0/1C52B60} remaining=21
kind=M lsn=LSN{0/1C52B60} remaining=40
kind=C lsn=LSN{0/1C52B90} remaining=26
kind=B lsn=LSN{0/1C52B90} remaining=21
kind=R lsn=LSN{0/0} remaining=54
kind=I lsn=LSN{0/1C52B90} remaining=26
kind=C lsn=LSN{0/1C52DC0} remaining=26
kind=B lsn=LSN{0/1C52DC0} remaining=21
kind=I lsn=LSN{0/1C52DC0} remaining=28
kind=I lsn=LSN{0/1C52E48} remaining=28
kind=I lsn=LSN{0/1C52ED0} remaining=28
kind=C lsn=LSN{0/1C52F88} remaining=26
```

`kind=M` is present, with a 40-byte payload, delivered inside its own `B`/`C` pair
(because `pg_logical_emit_message(true, …)` is transactional). PgJDBC 42.7.3 against
PostgreSQL 18.6 needed no special handling: `proto_version=2`, `messages=true`,
`streaming=off`.

Also worth recording for the writer implementation: **PgJDBC hands over exactly one
pgoutput message per `ByteBuffer`**, with no separator byte — unlike `pg_recvlogical -f`,
which injects a `0x0a` after each message (see spike 1).

### Verdict: **CONFIRMED**

---

## Spike 4 — per-change LSN

**Assumption under test.** Every change within one transaction carries its own, distinct,
increasing LSN, so `_seq` can order changes inside a transaction (spec §7).

### Command

Read from the spike 2 output, which already contains the three-row `INSERT`:

```bash
grep -n 'kind=' "$SPIKE/spike2.txt"
```

### Evidence

The three `kind=I` lines from `INSERT INTO t VALUES (10,…),(11,…),(12,…)`:

```
kind=I lsn=LSN{0/1C52DC0} remaining=28
kind=I lsn=LSN{0/1C52E48} remaining=28
kind=I lsn=LSN{0/1C52ED0} remaining=28
```

Three **distinct, strictly increasing** values — `0/1C52DC0` < `0/1C52E48` <
`0/1C52ED0`, 0x88 = 136 bytes apart. `_seq` alone therefore orders changes within a
transaction.

`lsn=` does differ between the `kind=I` and `kind=M` lines: the `M` message is at
`0/1C52B60` while the inserts are at `0/1C52B90` and `0/1C52DC0`–`0/1C52ED0`.

### Verdict: **CONFIRMED**

### Two further properties, confirmed by a second dedicated run

The single run above showed one `R` message and would have supported the two claims below
only anecdotally. Because both are load-bearing for Tasks 10 and 12, they were re-tested
on a **separate slot (`s3`) and publication (`p2`) over three tables (`t2`, `t3`, `t4`),
across five explicit transactions**, so that several `R` messages and several
transaction-opening changes of differing kinds were observed. Source:
`$SPIKE/jdbc/Spike4b.java` (identical to `Spike2.java` apart from the slot, the
publication and the loop count); output `$SPIKE/spike4b.txt`.

```bash
psql -p 5440 -h 127.0.0.1 -U postgres -d spike <<'SQL'
BEGIN; INSERT INTO t2 VALUES (1,'a'); INSERT INTO t2 VALUES (2,'b'); COMMIT;
BEGIN; INSERT INTO t3 VALUES (1,'c'); INSERT INTO t2 VALUES (3,'d'); COMMIT;
BEGIN; UPDATE t2 SET v='z' WHERE id=1; UPDATE t2 SET v='y' WHERE id=2; COMMIT;
BEGIN; INSERT INTO t4 VALUES (1,'e'),(2,'f'),(3,'g'); COMMIT;
BEGIN; DELETE FROM t2 WHERE id=3; INSERT INTO t3 VALUES (2,'h'); COMMIT;
SQL
```

Complete output:

```
kind=B lsn=LSN{0/1C93020} remaining=21
kind=R lsn=LSN{0/0} remaining=41
kind=I lsn=LSN{0/1C93020} remaining=20
kind=I lsn=LSN{0/1C93100} remaining=20
kind=C lsn=LSN{0/1C931B0} remaining=26
kind=B lsn=LSN{0/1C931B0} remaining=21
kind=R lsn=LSN{0/0} remaining=41
kind=I lsn=LSN{0/1C931B0} remaining=20
kind=I lsn=LSN{0/1C93290} remaining=20
kind=C lsn=LSN{0/1C93340} remaining=26
kind=B lsn=LSN{0/1C93340} remaining=21
kind=U lsn=LSN{0/1C93340} remaining=35
kind=U lsn=LSN{0/1C93398} remaining=35
kind=C lsn=LSN{0/1C93420} remaining=26
kind=B lsn=LSN{0/1C93420} remaining=21
kind=R lsn=LSN{0/0} remaining=41
kind=I lsn=LSN{0/1C93420} remaining=20
kind=I lsn=LSN{0/1C93500} remaining=20
kind=I lsn=LSN{0/1C93580} remaining=20
kind=C lsn=LSN{0/1C93630} remaining=26
kind=B lsn=LSN{0/1C93630} remaining=21
kind=D lsn=LSN{0/1C93630} remaining=20
kind=I lsn=LSN{0/1C93678} remaining=20
kind=C lsn=LSN{0/1C93728} remaining=26
```

**1. A `Relation` (`R`) message reports `LSN{0/0}`.** All **three** `R` messages in this
run did, one per table. `getLastReceiveLSN()` is therefore not monotonic across all
message kinds; the writer must not feed an `R` message's LSN into `_seq`, nor use it to
advance the flush position. (Confirmed for `R` across 3 observations plus the 1 in spike 2
— 4 in total. Not tested for message kinds this project does not enable, such as the
streaming `S`/`E` family or `O`/`T`.)

**2. The first change of a transaction shares its LSN with that transaction's `B`
message.** True in **all five** transactions, and for three different first-change kinds:

| txn | `B` LSN | first change | its LSN | equal? |
| --- | --- | --- | --- | --- |
| 1 | `0/1C93020` | `I` | `0/1C93020` | yes |
| 2 | `0/1C931B0` | `I` | `0/1C931B0` | yes |
| 3 | `0/1C93340` | `U` | `0/1C93340` | yes |
| 4 | `0/1C93420` | `I` | `0/1C93420` | yes |
| 5 | `0/1C93630` | `D` | `0/1C93630` | yes |

So an LSN is unique *per change* but is **not** unique across *message kinds* — do not use
it as a global key without the kind. The same run also shows each `C`'s LSN equal to the
*next* `B`'s LSN (`0/1C931B0`, `0/1C93340`, `0/1C93420`, `0/1C93630`), i.e. four more
instances of the same "LSN shared across kinds" effect.

Within every transaction the change LSNs remain strictly increasing, so spike 4's primary
conclusion is unaffected by either property.

---

## Spike 3 — commit-identifier dedup on replay

**Assumption under test.** Committing the same `commitIdentifier` twice under one
`commitUser` is a no-op the second time, so a writer that crashes after a Paimon commit
but before advancing the slot can safely replay (spec §13 / Task 13).

### Command

```bash
javac -cp "$SPIKE_CP" -d "$SPIKE/jdbc" "$SPIKE/jdbc/Spike3.java"
java -cp "$SPIKE_CP:$SPIKE/jdbc" Spike3 "$SPIKE/wh"
```

### Output — the brief's version, using `commit(identifier, messages)`

```
attempt 0 snapshot=1
attempt 1 snapshot=2
```

**The second commit was not a no-op.** `TableCommitImpl.commit(long, List<CommitMessage>)`
does no filtering at all: it produced snapshot 2, and the table directory ended up with
duplicate data *and* duplicate changelog files:

```
bucket-0/data-3223a8fc-a6f2-4fa7-9fc2-5e6d1e9e4563-1.parquet
bucket-0/changelog-3223a8fc-a6f2-4fa7-9fc2-5e6d1e9e4563-0.parquet
bucket-0/data-dfd2eb54-1d70-4049-b85b-d1d200e90df3-1.parquet
bucket-0/changelog-dfd2eb54-1d70-4049-b85b-d1d200e90df3-0.parquet
```

Because `merge-engine = deduplicate` collapses the duplicate primary key, a *batch* read
still returns one row — but the duplicate **changelog** file means a streaming consumer
would see the change twice. Replay is therefore **not** idempotent through this entry
point.

### The entry point that does dedup: `filterAndCommit`

`Spike3b.java` (extra spike, written because the brief's version refuted the assumption)
exercises the alternative:

```java
int filterAndCommit(java.util.Map<java.lang.Long, java.util.List<CommitMessage>>)
```

```
after plain commit():        snapshot=1 rows=1 [(1,a,100,0)]
filterAndCommit() returned:  0  (number of identifiers actually committed)
after filterAndCommit():     snapshot=1 rows=1 [(1,a,100,0)]
filterAndCommit() new id:    1
after new identifier:        snapshot=2 rows=2 [(1,a,100,0), (2,b,101,0)]
filterAndCommit() other user:1
after other commitUser:      snapshot=3 rows=2 [(1,a,100,0), (2,b,101,0)]
```

- Replaying identifier 42 under the same `commitUser` through `filterAndCommit` returns
  **0** and creates **no new snapshot** — a true no-op.
- A fresh identifier (43) returns 1 and commits normally, so the filter is not simply
  swallowing everything.
- The same identifier 42 under a **different** `commitUser` returns 1 and **does** commit
  (snapshot 3). Dedup state is therefore per-`commitUser`.

#### How the replay was performed — this matters for reading the result

The `filterAndCommit` attempt did **not** reuse the original `CommitMessage` objects, and
did **not** reuse the original committer. Each attempt in `Spike3b` opens a **fresh
`TableWriteImpl`** and a **fresh `TableCommitImpl`**, re-writes the same row, and calls
`prepareCommit(false, 42L)` again, so the `CommitMessage` list is a **newly produced one
describing newly written data files**. That is deliberate: it is the shape a real crash
recovery takes, where the process has restarted and nothing in memory survived. The dedup
therefore cannot be relying on object identity or on a cached committable — it is keyed on
`(commitUser, commitIdentifier)` alone.

#### What the snapshots record

Reading the snapshot metadata directly shows what the filter has to work from — each
snapshot stores the `commitUser` and `commitIdentifier` that produced it:

```
=== snapshot-1 ===            === snapshot-2 ===            === snapshot-3 ===
  id = 1                        id = 2                        id = 3
  commitUser = 'spike-user'     commitUser = 'spike-user'     commitUser = 'other-user'
  commitIdentifier = 42         commitIdentifier = 43         commitIdentifier = 42
  commitKind = 'APPEND'         commitKind = 'APPEND'         commitKind = 'APPEND'
```

This is consistent with dedup being decided from the committed snapshots' recorded
`(commitUser, commitIdentifier)` pairs — identifier 42 already present for `spike-user`
was filtered, while identifier 42 for `other-user` was not. I did not read Paimon's
implementation to confirm *how* it searches those snapshots (how far back it scans, or
what it does once snapshots are expired), so treat the mechanism as observed behaviour
rather than a documented contract. **A later task that expires snapshots aggressively
should re-verify that replay is still deduplicated.**

#### `Spike3b.java` source

```java
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.*;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.*;
import java.util.*;

/** Spike 3b: does commit-identifier dedup happen, and through which entry point? */
public class Spike3b {
  static FileStoreTable t;

  public static void main(String[] a) throws Exception {
    Options o = new Options();
    o.set("warehouse", a[0]);
    Catalog cat = CatalogFactory.createCatalog(CatalogContext.create(o));
    cat.createDatabase("db", true);
    Schema s = Schema.newBuilder()
        .column("id", DataTypes.INT().notNull())
        .column("v", DataTypes.STRING())
        .column("_seq", DataTypes.BIGINT().notNull())
        .column("_op_seq", DataTypes.INT().notNull())
        .primaryKey("id")
        .option("merge-engine", "deduplicate")
        .option("changelog-producer", "input")
        .option("sequence.field", "_seq,_op_seq")
        .option("ignore-update-before", "false")
        .build();
    Identifier id = Identifier.create("db", "t");
    cat.createTable(id, s, true);
    t = (FileStoreTable) cat.getTable(id);

    /* Attempt 0: plain commit(identifier, messages). */
    try (TableWriteImpl<?> w = t.newWrite("spike-user");
         TableCommitImpl c = t.newCommit("spike-user")) {
      w.write(GenericRow.of(1, BinaryString.fromString("a"), 100L, 0), 0);
      c.commit(42L, w.prepareCommit(false, 42L));
    }
    System.out.println("after plain commit():        snapshot=" + snap() + " rows=" + rows());

    /* Attempt 1: SAME identifier, SAME commitUser, via filterAndCommit().
       Fresh writer, fresh committer, freshly produced CommitMessages. */
    try (TableWriteImpl<?> w = t.newWrite("spike-user");
         TableCommitImpl c = t.newCommit("spike-user")) {
      w.write(GenericRow.of(1, BinaryString.fromString("a"), 100L, 0), 0);
      Map<Long, List<CommitMessage>> m = new HashMap<>();
      m.put(42L, w.prepareCommit(false, 42L));
      int committed = c.filterAndCommit(m);
      System.out.println("filterAndCommit() returned:  " + committed
          + "  (number of identifiers actually committed)");
    }
    System.out.println("after filterAndCommit():     snapshot=" + snap() + " rows=" + rows());

    /* Attempt 2: a NEW identifier via filterAndCommit() must still go through. */
    try (TableWriteImpl<?> w = t.newWrite("spike-user");
         TableCommitImpl c = t.newCommit("spike-user")) {
      w.write(GenericRow.of(2, BinaryString.fromString("b"), 101L, 0), 0);
      Map<Long, List<CommitMessage>> m = new HashMap<>();
      m.put(43L, w.prepareCommit(false, 43L));
      int committed = c.filterAndCommit(m);
      System.out.println("filterAndCommit() new id:    " + committed);
    }
    System.out.println("after new identifier:        snapshot=" + snap() + " rows=" + rows());

    /* Attempt 3: same identifier 42 but a DIFFERENT commitUser is a different lineage. */
    try (TableWriteImpl<?> w = t.newWrite("other-user");
         TableCommitImpl c = t.newCommit("other-user")) {
      w.write(GenericRow.of(1, BinaryString.fromString("a"), 100L, 0), 0);
      Map<Long, List<CommitMessage>> m = new HashMap<>();
      m.put(42L, w.prepareCommit(false, 42L));
      int committed = c.filterAndCommit(m);
      System.out.println("filterAndCommit() other user:" + committed);
    }
    System.out.println("after other commitUser:      snapshot=" + snap() + " rows=" + rows());
  }

  static Long snap() { return t.snapshotManager().latestSnapshotId(); }

  static String rows() throws Exception {
    ReadBuilder rb = t.newReadBuilder();
    List<Split> splits = rb.newScan().plan().splits();
    List<String> out = new ArrayList<>();
    try (RecordReader<InternalRow> reader = rb.newRead().createReader(splits)) {
      RecordReader.RecordIterator<InternalRow> batch;
      while ((batch = reader.readBatch()) != null) {
        InternalRow r;
        while ((r = batch.next()) != null)
          out.add("(" + r.getInt(0) + "," + r.getString(1) + "," + r.getLong(2) + "," + r.getInt(3) + ")");
        batch.releaseBatch();
      }
    }
    return out.size() + " " + out;
  }
}
```

### Verdict: **REFUTED for `commit(identifier, messages)`; CONFIRMED for `filterAndCommit(Map)` under a `commitUser` stable across restarts**

For the API the brief actually tested, the assumption **failed**: `commit(42L, messages)`
replayed produced a second snapshot with duplicate data *and* duplicate changelog files,
so replay through that method is not idempotent.

The property the design needs does exist, but only through
`TableCommitImpl.filterAndCommit(Map<Long, List<CommitMessage>>)`, and only when the
`commitUser` is **stable across process restarts** — a random per-process UUID would
silently defeat the dedup, as the `other-user` case above shows. Task 11 and Task 13 must
be written against `filterAndCommit`, not `commit`.

---

## Spike 5 (GATE 2) — `-U` survives in the changelog and loses the merge

**Assumption under test.** A `-U`/`+U` pair in one commit for the same key, distinguished
only by `_op_seq`, is both physically retained (so a changelog consumer sees it) and
resolved by `sequence.field = _seq,_op_seq` so the `+U` value wins (spec §7).

**Scope of the evidence — read this before citing this section.** What was measured is
retention in the **changelog**: the `t$audit_log` streaming scan reads the changelog files
that `changelog-producer = input` writes, *not* the data files. So this section establishes
that a changelog consumer sees both `-U` and `+U`, and that a merged read resolves to `+U`.
It does **not** by itself establish what is in the data files. The separate supporting
evidence for the data files is the `_VALUE_KIND` Parquet schema dump under "Correction 3"
below, which shows the row kind is physically stored per record in the data file — but no
assertion in this spike reads those data-file values directly, because Paimon reserves the
`_VALUE_KIND` name and shades its Parquet reader (see that section). A task that needs to
assert on data-file contents pre-compaction will have to solve that separately.

### Command

```bash
javac -cp "$SPIKE_CP" -d "$SPIKE/jdbc" "$SPIKE/jdbc/Spike5.java"
java -cp "$SPIKE_CP:$SPIKE/jdbc" Spike5 "$SPIKE/wh5"
```

Written in one commit: `-U (id=1, v='old', _seq=200, _op_seq=0)` as
`RowKind.UPDATE_BEFORE` and `+U (id=1, v='new', _seq=200, _op_seq=1)` as
`RowKind.UPDATE_AFTER`.

### Output (complete)

```
snapshot=1
RAW  audit_log fields=[rowkind, id, v, _seq, _op_seq]
RAW  first plan() splits=0 (expected 0)
RAW  second plan() splits=1
RAW  rowkind=-U id=1 v=old _seq=200 _op_seq=0
RAW  rowkind=+U id=1 v=new _seq=200 _op_seq=1
RAW  total=2 kinds=[-U, +U]
MERGED id=1 v=new _seq=200 _op_seq=1 [rowKind=INSERT]
MERGED total=1 surviving_v=new
SPIKE5 OK
```

Both records are present pre-merge with their row kinds, and the merged read returns the
single `+U` row with `v='new'`. The gate holds.

### Ordering is by `sequence.field`, not by arrival order (the strongest result here)

The test above writes `-U` before `+U`, so passing it would also be consistent with
"last writer wins". `Spike5d.java` (extra spike) writes each pair in **reverse** arrival
order:

```
reverse-arrival, _op_seq decides -> 1 row(s) [v=new _seq=200 _op_seq=1]   (expect v=new)
reverse-arrival, _seq decides    -> 1 row(s) [v=seq300 _seq=300 _op_seq=0]   (expect v=seq300)
```

With `+U` written *first* and `-U` second, `+U` still wins; and with two `+U` rows at
`_seq=300` and `_seq=200` written newest-first, the `_seq=300` row wins. So the winner is
genuinely chosen by `sequence.field`, with `_op_seq` breaking ties on equal `_seq` — which
is exactly what §7 requires.

### Verdict: **CONFIRMED WITH CAVEAT**

The ordering property is confirmed, including independence from arrival order, which is
the load-bearing part for §7. Two caveats, neither of which touches that result:

1. **How to read raw.** The brief's prescribed pre-merge read technique does not work
   against a Paimon-API-written primary-key table. Task 15 (and the plan's step that cites
   `PaimonFeatureTest.java:308-346`) must use the `t$audit_log` streaming route recorded
   below instead.
2. **What "retained" was shown of.** Retention was demonstrated in the changelog, not in
   the data files — see "Scope of the evidence" above. This also means the assertion
   depends on `changelog-producer = input` being set; it is in the Global Constraints, so
   it holds today, but a future change to that option would break this test for reasons
   unrelated to what it checks.

---

## Paimon 1.4.1 API findings (for Task 11 and Task 15)

### Signatures that are exactly as the brief assumed

All of these compiled unchanged against `paimon-bundle:1.4.1`:

```java
Catalog CatalogFactory.createCatalog(CatalogContext)
CatalogContext CatalogContext.create(Options)
void    Catalog.createDatabase(String, boolean)
void    Catalog.createTable(Identifier, Schema, boolean)
Table   Catalog.getTable(Identifier)
Schema.newBuilder().column(String, DataType).primaryKey(String...).option(String, String).build()
TableWriteImpl<?>  FileStoreTable.newWrite(String commitUser)
TableCommitImpl    FileStoreTable.newCommit(String commitUser)
List<CommitMessage> TableWriteImpl.prepareCommit(boolean, long)
void                TableCommitImpl.commit(long, List<CommitMessage>)
Long                FileStoreTable.snapshotManager().latestSnapshotId()
GenericRow.of(Object...) / GenericRow.ofKind(RowKind, Object...)
```

`TableWriteImpl` and `TableCommitImpl` are both `AutoCloseable` (`close() throws
Exception`), so the brief's try-with-resources form is correct.

### Correction 1 — `write(InternalRow)` fails: PK tables default to dynamic bucket

The Global Constraints deliberately write **no `bucket` option**
(plan line 21: "`partitionKeys` is always `[]`. Data files go under `bucket-0/`. No
`bucket` option is written."). With no `bucket` option a primary-key table is in
**dynamic bucket** mode, and the single-argument write throws at runtime:

```
java.lang.IllegalArgumentException: Can't extract bucket from row in dynamic bucket mode,
  you should use 'TableWrite.write(InternalRow row, int bucket)' method.
	at org.apache.paimon.table.sink.DynamicBucketRowKeyExtractor.bucket(DynamicBucketRowKeyExtractor.java:44)
	at org.apache.paimon.table.sink.TableWriteImpl.toSinkRecord(TableWriteImpl.java:213)
	at org.apache.paimon.table.sink.TableWriteImpl.write(TableWriteImpl.java:157)
```

**What I changed:** `w.write(row)` → `w.write(row, 0)` in both spike 3 and spike 5. The
table options were left exactly as the Global Constraints specify — the point of the
spike was that configuration, so adding a `bucket` option to dodge the error would have
measured something else.

**The real signatures available** (`javap org.apache.paimon.table.sink.TableWriteImpl`):

```java
public void write(org.apache.paimon.data.InternalRow) throws Exception;
public void write(org.apache.paimon.data.InternalRow, int) throws Exception;
public void writeBundle(org.apache.paimon.data.BinaryRow, int, org.apache.paimon.io.BundleRecords) throws Exception;
public org.apache.paimon.table.sink.SinkRecord writeAndReturn(org.apache.paimon.data.InternalRow) throws Exception;
public org.apache.paimon.table.sink.SinkRecord writeAndReturn(org.apache.paimon.data.InternalRow, int) throws Exception;
public java.util.List<CommitMessage> prepareCommit(boolean, long) throws Exception;
public java.util.List<CommitMessage> prepareCommit() throws Exception;
```

**Decision Task 11 must make explicitly:** either keep dynamic-bucket mode and always
pass an explicit bucket (`write(row, 0)`, which is what produced the expected `bucket-0/`
layout here), or write a `bucket` option and use fixed-bucket mode. These spikes validate
the first. Writing everything to bucket 0 forgoes bucket-level write parallelism, which
is fine for a single-threaded writer but is a real constraint worth stating in the spec.

### Correction 2 — `TableCommitImpl` dedup entry point

```java
public void commit(long, java.util.List<CommitMessage>);                       // does NOT dedup
public int  filterAndCommit(java.util.Map<java.lang.Long, java.util.List<CommitMessage>>);  // dedups, returns #committed
public int  filterAndCommitMultiple(java.util.List<ManifestCommittable>);
public int  filterAndCommitMultiple(java.util.List<ManifestCommittable>, boolean);
public void commitMultiple(java.util.List<ManifestCommittable>, boolean);
public TableCommitImpl ignoreEmptyCommit(boolean);
public boolean forceCreatingSnapshot();
```

See spike 3 above. Use `filterAndCommit` with a stable `commitUser`.

### Correction 3 — the `readRows` / `_row_kind` pre-merge read does not transfer

The brief (and plan step citing `PaimonFeatureTest.java:308-346`) says to read raw by
stripping `merge-engine`, `changelog-producer` and `sequence.field`, setting `bucket=-1`,
and prepending a `_row_kind` TINYINT field at field id `-1`. Against a primary-key table
written through the Paimon Table API this fails **twice over**:

1. **There is no `_row_kind` column.** Paimon's own merge-tree physical layout stores the
   row kind as `_VALUE_KIND`, alongside `_KEY_<pk>` and `_SEQUENCE_NUMBER`. The Parquet
   schema actually on disk is:

   ```
   required int32 _KEY_id = 1073741823;
   required int64 _SEQUENCE_NUMBER = 2147483646;
   required int32 _VALUE_KIND (INTEGER(8,true)) = 2147483645;
   required int32 id = 0;
   optional binary v (STRING) = 1;
   required int64 _seq = 2;
   required int32 _op_seq = 3;
   ```

   so the read dies with
   `java.io.IOException: Required column is missing in data file. Col: [_row_kind]`.

2. **`_VALUE_KIND` cannot be named in a user schema either.** Substituting it raises:

   ```
   java.lang.IllegalStateException: Field name[_VALUE_KIND] in schema cannot be exist in
     [_VALUE_KIND, _ROW_ID, rowkind, _LEVEL, _SEQUENCE_NUMBER]
   ```

   That message is also the authoritative **reserved field-name list** for 1.4.1:
   `_VALUE_KIND`, `_ROW_ID`, `rowkind`, `_LEVEL`, `_SEQUENCE_NUMBER`. No replicated
   PostgreSQL column may be mapped to any of these names — a guardrail task should
   reject them.

3. **Stripping the options does not even bypass merging.** With the options removed but
   `primaryKeys` still set, the table is still a `PrimaryKeyFileStoreTable` and still
   merges; removing `merge-engine` just falls back to its default (`deduplicate`). The
   stripped read returned **1** merged row, not the 2 raw records:

   ```
   == route 1: stripped options, no row-kind field ==
     fields=[id, v, _seq, _op_seq]
     row: id=1 v=new _seq=200 _op_seq=1  [rowKind=UPDATE_AFTER]
     total=1
   ```

**The route that works** — the `t$audit_log` system table with a **streaming** scan pinned
to the snapshot, which exposes a `rowkind` STRING column (`+I` / `-U` / `+U` / `-D`) and
reads the changelog that `changelog-producer=input` wrote:

```java
Map<String, String> opts = new HashMap<>();
opts.put("scan.mode", "from-snapshot");
opts.put("scan.snapshot-id", "1");
Table al = cat.getTable(Identifier.create("db", "t$audit_log")).copy(opts);
ReadBuilder rb = al.newReadBuilder();
StreamTableScan scan = rb.newStreamScan();
scan.plan();                                   // FIRST plan() is EMPTY -- discard it
List<Split> splits = scan.plan().splits();     // SECOND plan() carries the changelog
```

**The first `plan()` of a `StreamTableScan` always returns 0 splits** — it only
establishes the starting position. Reading only the first plan is why this spike
initially reported `total=0`. Measured behaviour of the scan modes on this table:

| options | result |
| --- | --- |
| `scan.mode=from-snapshot`, `scan.snapshot-id=1` | 1st plan 0 splits; 2nd plan both `-U` and `+U` |
| `scan.snapshot-id=1` (no mode) | identical to the above |
| `scan.mode=from-snapshot-full`, `scan.snapshot-id=1` | 1 row, merged (`+U` only) |
| `scan.mode=latest-full` | 1 row, merged |
| `scan.mode=compacted-full` | 1 row, merged |

`t$binlog` also exposes `[rowkind, id, v, _seq, _op_seq]` and is worth evaluating if a
later task wants batched before/after pairs. `t$ro` exposes the value fields only.

### Other API notes

- A merged batch read reports `InternalRow.getRowKind() == INSERT` even for a row whose
  latest input was `+U` (see the spike 5 `MERGED` line). Do not assert on the row kind of
  a merged read.
- `Catalog.getTable` accepts the `table$system_table` naming (`t$audit_log`, `t$binlog`,
  `t$ro`, `t$files`) via `Identifier.create("db", "t$audit_log")`.
- `Table.copy(Map<String,String>)` is how read options are applied; it re-validates the
  schema, which is where the reserved-name check above fires.

---

## Summary

| Spike | Subject | Verdict |
| --- | --- | --- |
| 1 (gate) | `u` placeholder + old-tuple TOAST flattening under RI FULL | **CONFIRMED** |
| 2 | pgoutput `Message` records reach PgJDBC | **CONFIRMED** |
| 3 | commit-identifier dedup is a no-op on replay | **REFUTED** for `commit(identifier, messages)`; **CONFIRMED** for `filterAndCommit(Map)` under a `commitUser` stable across restarts |
| 4 | per-change LSN | **CONFIRMED** |
| 5 (gate) | `-U` survives in the changelog; `sequence.field` picks `+U` | **CONFIRMED WITH CAVEAT** — result holds (and is independent of arrival order), but retention was shown in the *changelog*, not the data files, and the prescribed raw-read technique does not work; use `t$audit_log` streaming |

Both gates pass. The design's load-bearing assumptions hold, with one assumption holding
only through a different API than the plan assumed (spike 3). Three corrections to the
plan's Paimon usage are recorded above and must be picked up by Tasks 11, 13 and 15:
explicit bucket on write, `filterAndCommit` for idempotent replay, and the
`t$audit_log` streaming route for pre-merge assertions.

The spike 3 result is the one that changes planned code rather than just confirming it:
the plan as written would have used `commit(identifier, messages)` and silently lost
idempotent replay.

### Throwaway artefacts kept for later tasks

Under `.superpowers/sdd/2026-10-09-logical-slot-writer/spike/` (git-ignored, not deleted):

| Path | Contents |
| --- | --- |
| `spike1.bin` | raw pgoutput stream, 256408 bytes — **Task 6's fixture source** |
| `spike1.hex` | xxd-format dump of the same bytes |
| `u-excerpt.txt` | the two `U` messages, formatted as in this note |
| `spike2.txt` | spike 2 / spike 4 output |
| `spike4b.txt` | spike 4's confirmation run: 5 transactions, 3 `R` messages |
| `spike3.txt`, `spike3b.txt` | spike 3 output |
| `spike5.txt`, `spike5d.txt` | spike 5 output |
| `cp.txt` | the 185-entry Maven classpath |
| `jdbc/Spike{2,3,3b,4b,5,5b,5c,5d}.java` | the spike sources as actually run |
| `pgdata/` | the PostgreSQL 18.6 cluster (stopped) |

Restart the cluster with:

```bash
/usr/lib/postgresql/18/bin/pg_ctl \
  -D /home/vadim/postgres-paimon/.superpowers/sdd/2026-10-09-logical-slot-writer/spike/pgdata \
  -l .../spike/pg.log start
```
