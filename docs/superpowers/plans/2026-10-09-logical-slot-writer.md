# postgres-paimon on Logical Replication Slots — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replicate PostgreSQL tables into Apache Paimon using a logical replication slot and an external JVM writer, producing the same Paimon warehouse that the `paimon_heap` table access method on `main` produces.

**Architecture:** A pure-SQL extension `paimon_slot` owns a registry, event-trigger guardrails, and DDL emission via `pg_logical_emit_message`. A separate JVM process consumes `pgoutput` over `PGReplicationStream`, resolves unchanged-TOAST columns from the `REPLICA IDENTITY FULL` old tuple, and writes `+I`/`-U`/`+U`/`-D` records into Paimon primary-key tables ordered by `sequence.field = _seq,_op_seq`.

**Tech Stack:** PostgreSQL 18 (official image, `wal_level = logical`), pure SQL + PL/pgSQL extension, Java 17, Maven, `paimon-bundle` 1.4.1, PgJDBC 42.7.3, JUnit Jupiter 5.10.2, Docker Compose, MinIO.

**Spec:** `docs/superpowers/specs/2026-10-09-logical-slot-writer-design.md`

## Global Constraints

- Branch is `feat/logical-slot-writer`. `extension/` is **not modified** by any task in this plan; it stays as the `main` architecture for comparison.
- `java/pom.xml` is **not modified**. It is `<packaging>jar</packaging>` with five shade profiles and no `<modules>`; the writer is an independent Maven project at `java/writer/`.
- Paimon version `1.4.1` (`paimon-bundle`). PgJDBC `42.7.3`. Hadoop `3.3.4` (`hadoop-client-api`, `hadoop-client-runtime`). JUnit Jupiter `5.10.2`. Java release target `17`.
- Paimon table options, byte-identical to what `main` writes at `legacy:extension/paimon_schema.cpp:220-222`, plus this architecture's two additions:
  `merge-engine = deduplicate`, `changelog-producer = input`, `sequence.field = _seq,_op_seq`, `ignore-update-before = false`.
- Reserved Paimon field ids, matching `main`: `_seq` = `32767`, `_xid` = `0xFF00` (65280), `_op_seq` = `0xFF01` (65281). `0xFF02` (65282) was `_tid` on `main` and must stay unused. User columns use `attnum` as field id.
- `partitionKeys` is always `[]`. Data files go under `bucket-0/`. No `bucket` option is written.
- Replication slot name `paimon`, publication name `paimon_pub`, logical message prefix `paimon`.
- Slot options, exactly: `proto_version '2'`, `publication_names 'paimon_pub'`, `messages 'true'`, `binary 'false'`, `streaming 'off'`.
- Every tracked table must have a primary key; `paimon.add_table()` raises otherwise.
- Column defaults stay blocked (`gaps.md` item 2 is carried over, not fixed). `gaps.md` items 1 and 3 are fixed.
- All new SQL and Java files start with `// SPDX-License-Identifier: Apache-2.0` (or `-- ` for SQL), matching the existing files.
- Commit messages end with `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.

## Review Focus

These are input classes the spec implies but which no task's happy-path tests would exercise. Each line's test is assigned to the task that owns the code, and appears in that task's steps.

1. **NULL versus empty string in text mode.** pgoutput writes `'n'` for NULL and `'t'` with length `0` for `''`. Conflating them turns every empty string into a NULL in the warehouse. Pinned in Task 6.
2. **A `u` column with no old tuple.** If a table's replica identity is not `FULL` — someone ran `ALTER TABLE ... REPLICA IDENTITY DEFAULT`, or the table was added to the publication by hand — an UPDATE delivers `u` and no old value. Writing NULL there is silent data loss; the writer must refuse the record and fail loudly. Pinned in Task 7.
3. **A transaction containing only DDL or only ignored relations.** It produces no Paimon records, so there is nothing to commit, so a naive writer never advances the LSN and replays it forever. The LSN must still advance. Pinned in Task 10.
4. **Reconnect in the middle of a transaction.** A half-buffered transaction must be discarded on reconnect, not flushed; flushing it would commit a transaction PostgreSQL may yet abort. Pinned in Task 12.
5. **A PostgreSQL type with no Paimon mapping.** `pg_oid_to_paimon_type` falls through to `STRING` for arrays, enums, ranges, `hstore`, `json`, `jsonb`, `inet` and domains. That is the right default, but it must be a deliberate, tested fallthrough with a `WARNING`, not an accident that silently stringifies a column someone expected to fail. Pinned in Task 2.

---

## File Structure

```
sql/paimon_slot/
  Makefile                      PGXS, no MODULES; EXTENSION + DATA + REGRESS
  paimon_slot.control           extension metadata
  paimon_slot--1.0.sql          registry, type mapping, add/remove_table, event triggers
  test/sql/paimon_slot.sql      pg_regress input
  test/expected/paimon_slot.out pg_regress expected output

java/writer/
  pom.xml                       independent project, coordinates pro.surpin.pgpaimon:paimon-writer
  src/main/java/pro/surpin/pgpaimon/writer/
    Main.java                   entry point, wiring, shutdown
    WriterConfig.java           env + argv configuration, validation
    PgEvent.java                sealed interface + records: Begin/Commit/Relation/Insert/
                                Update/Delete/Truncate/Message/Origin/TypeMsg
    ColumnValue.java            ColState enum + value record
    TupleData.java              list of ColumnValue
    RelationMeta.java           relid, namespace, name, column names, key flags
    PgOutputDecoder.java        protocol bytes -> PgEvent; relation cache
    ToastResolver.java          PgEvent -> List<PartialRecord>; u-fill; key-change split
    PartialRecord.java          relid, RowKind, text values
    SinkRecord.java             PartialRecord + _seq, _op_seq, _xid
    TxnBatcher.java             transaction assembly, _op_seq assignment, flush policy
    SchemaRegistry.java         relid -> FileStoreTable; DDL application; row building
    RowBuilder.java             name-keyed text values -> Paimon GenericRow
    TextDecoder.java            Paimon DataType + text -> Paimon internal object
    DdlMessage.java             parsed DDL JSON
    DdlMessageHandler.java      JSON -> SchemaRegistry calls
    PaimonSink.java             TableWriteImpl writes, commit with identifier
    OffsetStore.java            highest committed identifier from Paimon snapshots
    Backfiller.java             exported snapshot -> +I rows
    SlotConsumer.java           PGReplicationStream lifecycle, reconnect, status updates
  src/test/java/pro/surpin/pgpaimon/writer/
    PgOutputDecoderTest.java    byte fixtures for every message type
    ToastResolverTest.java      u-fill, key change, missing old tuple
    TxnBatcherTest.java         ordering, empty transactions, boundaries
    TextDecoderTest.java        every type in the Global Constraints mapping
    SchemaRegistryTest.java     DDL evolution against a local warehouse
    PaimonSinkTest.java         write + commit identifier dedup, raw read-back
    RowBuilderTest.java         name mapping, metadata columns

docker/slot-demo/
  docker-compose.yml            postgres:18, MinIO, writer
  writer/Dockerfile             JRE 17 + shaded writer jar
  postgres/init.sql             wal_level check, CREATE EXTENSION, demo tables
  run_acceptance.sh             builds, starts, runs the JDBC suites, reports

java/src/test/java/pro/surpin/pgpaimon/
  PaimonFeatureTest.java        setup adapted (Task 16), T4 assertion changed (Task 17)
  PaimonAdvancedTest.java       setup adapted; T10/T11/T16/T17 changed (Task 17)
  PaimonDdlTest.java            setup adapted (Task 16)
  PaimonOffloadTest.java        setup adapted (Task 18)
  PaimonSchemaIT.java           setup adapted (Task 16)
  PaimonTypeTest.java           setup adapted (Task 16)
  PaimonSlotKeyMoveTest.java    new, T20 (Task 17)

docs/superpowers/notes/
  2026-10-09-spike-findings.md  Task 1 output
```

Decomposition decisions locked in by that structure:

- **Type mapping lives in SQL, once.** `paimon.pg_type_to_paimon()` in `paimon_slot--1.0.sql` mirrors `legacy:extension/type_map_pg.h` and emits Paimon type strings into the DDL messages. The writer never owns an OID table, and the Relation message's type OIDs are used only for sanity checks. This is the single biggest duplication the architecture could have had.
- **Everything stays text until `RowBuilder`.** The decoder, resolver and batcher pass `String` values. One class converts to Paimon internals, so there is one place where a type bug can live.
- **`SchemaRegistry` owns the Paimon side; nothing else calls `Catalog`.** `PaimonSink` writes through tables the registry hands it.

---

## Task 1: Spikes

The five spikes from spec §12. No production code; the deliverable is a committed findings note. Spike 1 is a gate: if it fails, stop and return to the spec author.

**Files:**
- Create: `docs/superpowers/notes/2026-10-09-spike-findings.md`
- Create (throwaway, not committed): `/tmp/spike/*`

**Interfaces:**
- Consumes: nothing.
- Produces: the findings note. Task 6 reads its recorded `pg_recvlogical` output as the source for byte fixtures.

- [ ] **Step 1: Start a PostgreSQL 18 with logical WAL**

```bash
mkdir -p /tmp/spike && cd /tmp/spike
docker run -d --name spike-pg -p 5440:5432 \
  -e POSTGRES_PASSWORD=spike -e POSTGRES_DB=spike \
  postgres:18 -c wal_level=logical -c max_replication_slots=4
sleep 5
docker exec spike-pg psql -U postgres -d spike -c "SHOW wal_level"
```

Expected: `logical`.

- [ ] **Step 2: Spike 1 — `u` placeholder and old-tuple flattening (GATE)**

```bash
docker exec spike-pg psql -U postgres -d spike <<'SQL'
CREATE TABLE t (id int PRIMARY KEY, n int, body text);
ALTER TABLE t REPLICA IDENTITY FULL;
CREATE PUBLICATION p FOR TABLE t;
SELECT pg_create_logical_replication_slot('s', 'pgoutput');
INSERT INTO t VALUES (1, 1, repeat('x', 100000));   -- forces TOAST
INSERT INTO t VALUES (2, 2, 'short');
UPDATE t SET n = 99 WHERE id = 1;    -- does NOT touch body
UPDATE t SET n = 99 WHERE id = 2;    -- does NOT touch body, body is inline
SQL
docker exec spike-pg pg_recvlogical -U postgres -d spike -S s --start \
  -o proto_version=2 -o publication_names=p -o messages=true -o binary=false \
  -f - --no-loop | xxd | tee /tmp/spike/spike1.hex
```

Expected, to be confirmed by reading the hex dump:
- The `U` message for `id = 1` contains an `'O'` old tuple whose third column is `'t'` followed by length `100000` and the full value, **and** a new tuple whose third column is the single byte `'u'` (0x75).
- The `U` message for `id = 2` contains the full `'short'` in **both** tuples — a short inline value is never reported as `u`.

If the old tuple's `body` is `u`, **STOP**: §5's resolution does not hold. Record the finding and return to the spec author.

- [ ] **Step 3: Spike 2 — `Message` records reach PgJDBC**

```bash
mkdir -p /tmp/spike/jdbc && cd /tmp/spike/jdbc
cat > Spike2.java <<'EOF'
import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationStream;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Properties;

public class Spike2 {
  public static void main(String[] a) throws Exception {
    Properties p = new Properties();
    p.setProperty("user", "postgres");
    p.setProperty("password", "spike");
    p.setProperty("replication", "database");
    p.setProperty("preferQueryMode", "simple");
    p.setProperty("assumeMinServerVersion", "18");
    Connection c = DriverManager.getConnection("jdbc:postgresql://localhost:5440/spike", p);
    PGReplicationStream s = c.unwrap(PGConnection.class).getReplicationAPI()
        .replicationStream().logical().withSlotName("s2")
        .withSlotOption("proto_version", "2")
        .withSlotOption("publication_names", "p")
        .withSlotOption("messages", "true")
        .withSlotOption("binary", "false")
        .withSlotOption("streaming", "off")
        .start();
    for (int i = 0; i < 40; i++) {
      ByteBuffer b = s.readPending();
      if (b == null) { Thread.sleep(200); continue; }
      byte kind = b.get(b.position());
      System.out.println("kind=" + (char) kind + " lsn=" + s.getLastReceiveLSN()
                         + " remaining=" + b.remaining());
      s.setAppliedLSN(s.getLastReceiveLSN());
      s.setFlushedLSN(s.getLastReceiveLSN());
    }
  }
}
EOF
docker exec spike-pg psql -U postgres -d spike \
  -c "SELECT pg_create_logical_replication_slot('s2','pgoutput')"
mvn -q dependency:get -Dartifact=org.postgresql:postgresql:42.7.3
CP=$(mvn -q help:evaluate -Dexpression=settings.localRepository -DforceStdout)/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
javac -cp "$CP" Spike2.java
( sleep 2; docker exec spike-pg psql -U postgres -d spike \
    -c "SELECT pg_logical_emit_message(true, 'paimon', '{\"v\":1,\"op\":\"ping\"}'); INSERT INTO t VALUES (3,3,'z');" ) &
java -cp "$CP:." Spike2 | tee /tmp/spike/spike2.txt
```

Expected: a line with `kind=M`. Also record whether `getLastReceiveLSN()` differs between the `I` and the `M` lines — that is spike 4.

- [ ] **Step 4: Spike 3 — commit-identifier dedup is a no-op on replay**

Write `/tmp/spike/jdbc/Spike3.java` that opens a local Paimon warehouse, creates a PK table with the Global Constraints option set, and commits the same `commitIdentifier` twice under one `commitUser`:

```java
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.*;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.types.*;
import java.util.*;

public class Spike3 {
  public static void main(String[] a) throws Exception {
    Options o = new Options();
    o.set("warehouse", "/tmp/spike/wh");
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
    FileStoreTable t = (FileStoreTable) cat.getTable(id);
    for (int attempt = 0; attempt < 2; attempt++) {
      BatchWriteBuilder unused = t.newBatchWriteBuilder();
      try (TableWriteImpl<?> w = t.newWrite("spike-user");
           TableCommitImpl c = t.newCommit("spike-user")) {
        w.write(GenericRow.of(1, BinaryString.fromString("a"), 100L, 0));
        c.commit(42L, w.prepareCommit(false, 42L));
      }
      System.out.println("attempt " + attempt + " snapshot="
          + t.snapshotManager().latestSnapshotId());
    }
  }
}
```

Expected: the second attempt produces no new snapshot, or a snapshot that does not duplicate the row. Record exactly which, and the exception if it throws — Task 13 depends on this behaviour.

- [ ] **Step 5: Spike 4 — per-change LSN**

From the `/tmp/spike/spike2.txt` output of step 3, confirm `lsn=` changes between consecutive `I` lines produced by a multi-row `INSERT ... VALUES (..),(..),(..)`. Run:

```bash
docker exec spike-pg psql -U postgres -d spike \
  -c "INSERT INTO t VALUES (10,10,'a'),(11,11,'b'),(12,12,'c')"
```

with `Spike2` running, and record the three `lsn=` values. Expected: three distinct, increasing values.

- [ ] **Step 6: Spike 5 — `-U` survives to a raw read and loses the merge**

Extend `Spike3` to write, in one commit, `-U` at `(_seq=200,_op_seq=0)` and `+U` at `(_seq=200,_op_seq=1)` for the same key, then:
1. read raw with the `readRows` technique from `java/src/test/java/pro/surpin/pgpaimon/PaimonFeatureTest.java:308-346` (strip `merge-engine`, `changelog-producer`, `sequence.field`; prepend a `_row_kind` TINYINT field at id `-1`) and assert both records appear;
2. call `t.newCommit("spike-user")` with full compaction, or read through `t.newReadBuilder()` with options intact, and assert the surviving row is the `+U` value.

Expected: raw read shows 2 records with `_row_kind` 1 and 2; merged read shows the `+U` value. If the merged read shows the `-U` value or no row, **STOP** — `sequence.field` ordering is not working as §7 requires.

- [ ] **Step 7: Write the findings note**

Create `docs/superpowers/notes/2026-10-09-spike-findings.md` with one section per spike: the command run, the raw output excerpt, and a one-line verdict (`CONFIRMED` / `REFUTED` / `CONFIRMED WITH CAVEAT: …`). Paste the `U` message hex from spike 1 in full — Task 6 uses it as a byte fixture.

- [ ] **Step 8: Tear down and commit**

```bash
docker rm -f spike-pg
cd /home/vadim/postgres-paimon
git add docs/superpowers/notes/2026-10-09-spike-findings.md
git commit -m "$(printf 'spike: verify TOAST old-tuple flattening, pgoutput messages, Paimon commit dedup\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 2: Extension skeleton, registry and type mapping

**Files:**
- Create: `sql/paimon_slot/Makefile`
- Create: `sql/paimon_slot/paimon_slot.control`
- Create: `sql/paimon_slot/paimon_slot--1.0.sql`
- Create: `sql/paimon_slot/test/logical.conf`
- Create: `sql/paimon_slot/test/sql/paimon_slot.sql`
- Create: `sql/paimon_slot/test/expected/paimon_slot.out`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `paimon.pg_type_to_paimon(typid oid, typmod int, notnull boolean) RETURNS text`
  - `paimon.table_registry(relid oid PRIMARY KEY, schema_name text, table_name text, pk_cols text[], cols jsonb, backfill_due boolean, replica_identity text, changelog_producer text, registered_at timestamptz)`
  - `paimon.table_cols(relid oid) RETURNS jsonb` — ordered `[{"id":<attnum>,"name":<attname>,"type":<paimon type>}]`
  - `paimon.pk_cols(relid oid) RETURNS text[]`

Note on the spec: §4 names the registry column `field_ids` and describes it as `attname -> attnum`. It is named `cols` here and carries the Paimon type string as well, because the `ddl_command_end` diff in Task 3 needs the previous type to detect `ALTER COLUMN TYPE`. Same purpose, one more field.

- [ ] **Step 1: Write the failing regress test**

Create `sql/paimon_slot/test/sql/paimon_slot.sql`:

```sql
CREATE EXTENSION paimon_slot;

-- type mapping, mirroring legacy:extension/type_map_pg.h
SELECT paimon.pg_type_to_paimon('boolean'::regtype, -1, false)      AS b,
       paimon.pg_type_to_paimon('smallint'::regtype, -1, true)      AS i2,
       paimon.pg_type_to_paimon('integer'::regtype, -1, false)      AS i4,
       paimon.pg_type_to_paimon('bigint'::regtype, -1, false)       AS i8,
       paimon.pg_type_to_paimon('real'::regtype, -1, false)         AS f4,
       paimon.pg_type_to_paimon('double precision'::regtype, -1, false) AS f8;
SELECT paimon.pg_type_to_paimon('numeric'::regtype, -1, false)      AS num_default,
       paimon.pg_type_to_paimon('numeric'::regtype, 655366, false)  AS num_10_2;
SELECT paimon.pg_type_to_paimon('text'::regtype, -1, false)         AS t,
       paimon.pg_type_to_paimon('varchar'::regtype, 24, false)      AS vc,
       paimon.pg_type_to_paimon('uuid'::regtype, -1, false)         AS uu,
       paimon.pg_type_to_paimon('bytea'::regtype, -1, false)        AS ba,
       paimon.pg_type_to_paimon('date'::regtype, -1, false)         AS d,
       paimon.pg_type_to_paimon('timestamp'::regtype, -1, false)    AS ts,
       paimon.pg_type_to_paimon('timestamptz'::regtype, -1, false)  AS tstz;

-- Review Focus 5: an unmapped type falls through to STRING, with a warning
SET client_min_messages = warning;
SELECT paimon.pg_type_to_paimon('int4[]'::regtype, -1, false)  AS arr,
       paimon.pg_type_to_paimon('jsonb'::regtype, -1, false)   AS jb,
       paimon.pg_type_to_paimon('inet'::regtype, -1, false)    AS inet;
RESET client_min_messages;

-- column introspection
CREATE TABLE reg_t (id int PRIMARY KEY, amount numeric(10,2) NOT NULL, note text);
SELECT paimon.pk_cols('reg_t'::regclass);
SELECT jsonb_pretty(paimon.table_cols('reg_t'::regclass));

-- dropped columns are excluded and attnums are not renumbered
ALTER TABLE reg_t ADD COLUMN tmp int;
ALTER TABLE reg_t DROP COLUMN tmp;
ALTER TABLE reg_t ADD COLUMN kept int;
SELECT jsonb_pretty(paimon.table_cols('reg_t'::regclass));
```

Create `sql/paimon_slot/test/expected/paimon_slot.out` by running the test once the
implementation exists (step 4), then **reading every line** to confirm it is what the Global
Constraints require — in particular `DECIMAL(10, 2)` with the space after the comma, matching
`legacy:extension/type_map_pg.h:27`, and `TIMESTAMP WITH LOCAL TIME ZONE`.

Create `sql/paimon_slot/test/logical.conf`:

```
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/vadim/postgres-paimon/sql/paimon_slot
make installcheck
```

Expected: FAIL — no `Makefile` yet, so `make: *** No targets specified and no makefile found`.

- [ ] **Step 3: Write the Makefile, control file and SQL**

`sql/paimon_slot/Makefile`:

```makefile
EXTENSION   = paimon_slot
DATA        = paimon_slot--1.0.sql
REGRESS     = paimon_slot
REGRESS_OPTS = --inputdir=test --outputdir=test --temp-config=test/logical.conf

PG_CONFIG  ?= pg_config
PGXS       := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
```

`sql/paimon_slot/paimon_slot.control`:

```
# paimon_slot extension
comment = 'Replicate PostgreSQL tables to Apache Paimon via a logical replication slot'
default_version = '1.0'
relocatable = false
schema = paimon
superuser = true
```

`sql/paimon_slot/paimon_slot--1.0.sql` — this step writes only the registry and type
mapping; Tasks 3 and 4 append to the same file:

```sql
-- SPDX-License-Identifier: Apache-2.0
\echo Use "CREATE EXTENSION paimon_slot" to load this file. \quit

CREATE TABLE paimon.table_registry (
    relid              oid PRIMARY KEY,
    schema_name        text        NOT NULL,
    table_name         text        NOT NULL,
    pk_cols            text[]      NOT NULL,
    cols               jsonb       NOT NULL,
    backfill_due       boolean     NOT NULL DEFAULT true,
    replica_identity   text        NOT NULL DEFAULT 'full',
    changelog_producer text        NOT NULL DEFAULT 'input',
    registered_at      timestamptz NOT NULL DEFAULT now()
);
SELECT pg_catalog.pg_extension_config_dump('paimon.table_registry', '');

-- Mirror of legacy:extension/type_map_pg.h.  Returns a Paimon type string.
CREATE FUNCTION paimon.pg_type_to_paimon(typid oid, typmod int, notnull boolean)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    base text;
    raw  int;
    prec int;
    scal int;
BEGIN
    CASE typid
        WHEN 16   THEN base := 'BOOLEAN';
        WHEN 21   THEN base := 'SMALLINT';
        WHEN 23   THEN base := 'INT';
        WHEN 20   THEN base := 'BIGINT';
        WHEN 700  THEN base := 'FLOAT';
        WHEN 701  THEN base := 'DOUBLE';
        WHEN 1700 THEN
            IF typmod > 4 THEN
                raw  := typmod - 4;
                prec := (raw >> 16) & 65535;
                scal := raw & 65535;
            ELSE
                prec := 38;
                scal := 18;
            END IF;
            base := format('DECIMAL(%s, %s)', prec, scal);
        WHEN 25, 1043, 1042, 2950 THEN base := 'STRING';
        WHEN 17   THEN base := 'BYTES';
        WHEN 1082 THEN base := 'DATE';
        WHEN 1114 THEN base := 'TIMESTAMP';
        WHEN 1184 THEN base := 'TIMESTAMP WITH LOCAL TIME ZONE';
        ELSE
            RAISE WARNING 'paimon_slot: type %s has no Paimon mapping, using STRING',
                          format_type(typid, typmod);
            base := 'STRING';
    END CASE;

    IF notnull THEN
        RETURN base || ' NOT NULL';
    END IF;
    RETURN base;
END $$;

-- Ordered, live columns of a relation, with attnum as the Paimon field id.
CREATE FUNCTION paimon.table_cols(relid oid)
RETURNS jsonb LANGUAGE sql STABLE AS $$
    SELECT coalesce(jsonb_agg(jsonb_build_object(
               'id',   a.attnum,
               'name', a.attname,
               'type', paimon.pg_type_to_paimon(a.atttypid, a.atttypmod, a.attnotnull))
             ORDER BY a.attnum), '[]'::jsonb)
    FROM pg_attribute a
    WHERE a.attrelid = relid AND a.attnum > 0 AND NOT a.attisdropped;
$$;

CREATE FUNCTION paimon.pk_cols(relid oid)
RETURNS text[] LANGUAGE sql STABLE AS $$
    SELECT coalesce(array_agg(a.attname ORDER BY k.ord), '{}'::text[])
    FROM pg_constraint c
    CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
    WHERE c.conrelid = relid AND c.contype = 'p';
$$;
```

- [ ] **Step 4: Run the test and generate the expected output**

```bash
cd /home/vadim/postgres-paimon/sql/paimon_slot
make
sudo make install
make installcheck
```

The first run fails because `test/expected/paimon_slot.out` is empty or absent. Copy the
actual output into place, then **read it**:

```bash
cp test/results/paimon_slot.out test/expected/paimon_slot.out
cat test/expected/paimon_slot.out
```

Confirm by eye: `DECIMAL(38, 18)` for bare `numeric`, `DECIMAL(10, 2)` for `numeric(10,2)`
(typmod 655366 = `((10 << 16) | 2) + 4`), three `WARNING: paimon_slot: type ... has no Paimon
mapping, using STRING` lines for `int4[]`, `jsonb` and `inet`, and a `table_cols` result where
`kept` has `id: 5` — proving a dropped column does not renumber the field ids of later ones.

- [ ] **Step 5: Re-run to verify it passes**

```bash
make installcheck
```

Expected: `All 1 tests passed.`

- [ ] **Step 6: Commit**

```bash
cd /home/vadim/postgres-paimon
git add sql/paimon_slot
git commit -m "$(printf 'feat(paimon_slot): registry, type mapping and column introspection\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 3: `add_table`, `remove_table` and the publication

**Files:**
- Modify: `sql/paimon_slot/paimon_slot--1.0.sql` (append)
- Modify: `sql/paimon_slot/test/sql/paimon_slot.sql` (append)
- Modify: `sql/paimon_slot/test/expected/paimon_slot.out` (regenerate)

**Interfaces:**
- Consumes: `paimon.table_cols`, `paimon.pk_cols`, `paimon.table_registry` from Task 2.
- Produces:
  - `paimon.add_table(rel regclass, replica_identity text DEFAULT 'full', changelog_producer text DEFAULT 'input') RETURNS void`
  - `paimon.remove_table(rel regclass) RETURNS void`
  - `paimon.emit(payload jsonb) RETURNS void` — wraps `pg_logical_emit_message(true, 'paimon', payload::text)`
  - publication `paimon_pub`, created empty by the extension script
  - DDL message `create`, whose exact JSON shape Task 12 parses:
    `{"v":1,"op":"create","relid":<oid>,"schema":<text>,"table":<text>,"pk":[<text>],"cols":[{"id":<int>,"name":<text>,"type":<text>}],"options":{"changelog-producer":<text>}}`

- [ ] **Step 1: Write the failing test**

Append to `sql/paimon_slot/test/sql/paimon_slot.sql`:

```sql
-- add_table requires a primary key
CREATE TABLE nopk (a int);
SELECT paimon.add_table('nopk'::regclass);        -- expect ERROR

-- the v1 escapes are accepted but not implemented
SELECT paimon.add_table('reg_t'::regclass, 'default');          -- expect ERROR
SELECT paimon.add_table('reg_t'::regclass, 'full', 'lookup');   -- expect ERROR

-- happy path
SELECT pg_create_logical_replication_slot('paimon', 'pgoutput');
SELECT paimon.add_table('reg_t'::regclass);
SELECT schema_name, table_name, pk_cols, backfill_due, replica_identity, changelog_producer
FROM paimon.table_registry WHERE relid = 'reg_t'::regclass;
SELECT relreplident FROM pg_class WHERE oid = 'reg_t'::regclass;
SELECT count(*) FROM pg_publication_tables
WHERE pubname = 'paimon_pub' AND tablename = 'reg_t';

-- the create message reached the slot
SELECT data FROM pg_logical_slot_peek_changes('paimon', NULL, NULL,
       'proto_version', '2', 'publication_names', 'paimon_pub',
       'messages', 'true', 'binary', 'false') LIMIT 0;
SELECT (j->>'op') AS op, (j->>'table') AS tbl, (j->'cols'->0->>'name') AS first_col
FROM (SELECT paimon.peek_messages() AS j) s;

-- add_table twice is an error, not a silent no-op
SELECT paimon.add_table('reg_t'::regclass);       -- expect ERROR

-- remove_table
SELECT paimon.remove_table('reg_t'::regclass);
SELECT count(*) FROM paimon.table_registry WHERE relid = 'reg_t'::regclass;
SELECT count(*) FROM pg_publication_tables
WHERE pubname = 'paimon_pub' AND tablename = 'reg_t';
SELECT pg_drop_replication_slot('paimon');
```

`paimon.peek_messages()` is a test helper the implementation must also provide: it decodes
the slot with `test_decoding` and returns the `paimon`-prefixed payloads as `jsonb`. Decoding
the same slot with two plugins is not possible, so the helper creates its own slot. Write it
as part of step 3.

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/vadim/postgres-paimon/sql/paimon_slot && make installcheck
```

Expected: FAIL — `function paimon.add_table(regclass) does not exist`.

- [ ] **Step 3: Implement**

Append to `sql/paimon_slot/paimon_slot--1.0.sql`:

```sql
CREATE PUBLICATION paimon_pub;

CREATE FUNCTION paimon.emit(payload jsonb) RETURNS void
LANGUAGE sql AS $$
    SELECT pg_logical_emit_message(true, 'paimon', payload::text)::void;
$$;

CREATE FUNCTION paimon.add_table(rel regclass,
                                 replica_identity text DEFAULT 'full',
                                 changelog_producer text DEFAULT 'input')
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_relid  oid  := rel::oid;
    v_nsp    text;
    v_name   text;
    v_pk     text[];
    v_cols   jsonb;
    v_kind   "char";
BEGIN
    IF replica_identity <> 'full' THEN
        RAISE EXCEPTION 'paimon_slot: replica_identity => %L is not implemented in v1',
                        replica_identity
              USING HINT = 'only ''full'' is supported; see design spec section 5';
    END IF;
    IF changelog_producer <> 'input' THEN
        RAISE EXCEPTION 'paimon_slot: changelog_producer => %L is not implemented in v1',
                        changelog_producer
              USING HINT = 'only ''input'' is supported; see design spec section 5';
    END IF;

    SELECT n.nspname, c.relname, c.relkind
      INTO v_nsp, v_name, v_kind
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = v_relid;

    IF v_kind <> 'r' THEN
        RAISE EXCEPTION 'paimon_slot: % is not an ordinary table (relkind %)', rel, v_kind;
    END IF;

    IF EXISTS (SELECT 1 FROM paimon.table_registry WHERE relid = v_relid) THEN
        RAISE EXCEPTION 'paimon_slot: % is already tracked', rel;
    END IF;

    v_pk := paimon.pk_cols(v_relid);
    IF cardinality(v_pk) = 0 THEN
        RAISE EXCEPTION 'paimon_slot: % has no primary key', rel
              USING HINT = 'a primary key is required; see design spec section 4';
    END IF;

    v_cols := paimon.table_cols(v_relid);

    EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', rel::text);
    EXECUTE format('ALTER PUBLICATION paimon_pub ADD TABLE %s', rel::text);

    INSERT INTO paimon.table_registry
        (relid, schema_name, table_name, pk_cols, cols,
         backfill_due, replica_identity, changelog_producer)
    VALUES (v_relid, v_nsp, v_name, v_pk, v_cols,
            true, replica_identity, changelog_producer);

    PERFORM paimon.emit(jsonb_build_object(
        'v', 1, 'op', 'create', 'relid', v_relid,
        'schema', v_nsp, 'table', v_name,
        'pk', to_jsonb(v_pk), 'cols', v_cols,
        'options', jsonb_build_object('changelog-producer', changelog_producer)));
END $$;

CREATE FUNCTION paimon.remove_table(rel regclass) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM paimon.table_registry WHERE relid = rel::oid) THEN
        RAISE EXCEPTION 'paimon_slot: % is not tracked', rel;
    END IF;
    EXECUTE format('ALTER PUBLICATION paimon_pub DROP TABLE %s', rel::text);
    DELETE FROM paimon.table_registry WHERE relid = rel::oid;
END $$;

-- Test helper: decode paimon-prefixed logical messages through a private
-- test_decoding slot.  Not used by the writer.
CREATE FUNCTION paimon.peek_messages() RETURNS SETOF jsonb
LANGUAGE plpgsql AS $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT data FROM pg_logical_slot_get_changes('paimon_test', NULL, NULL)
    LOOP
        IF r.data LIKE 'message: transactional:%prefix: paimon%' THEN
            RETURN NEXT substring(r.data from position('content:' in r.data) + 9)::jsonb;
        END IF;
    END LOOP;
END $$;
```

Because `paimon.peek_messages()` reads a `test_decoding` slot named `paimon_test`, the regress
test must create it before `add_table`:

```sql
SELECT pg_create_logical_replication_slot('paimon_test', 'test_decoding');
```

Add that line before the `add_table` call in step 1's test, and
`SELECT pg_drop_replication_slot('paimon_test');` at the end.

- [ ] **Step 4: Run the test and regenerate expected output**

```bash
cd /home/vadim/postgres-paimon/sql/paimon_slot
make && sudo make install && make installcheck
cp test/results/paimon_slot.out test/expected/paimon_slot.out
cat test/expected/paimon_slot.out
```

Read it. Confirm: the no-PK error says `has no primary key`; `relreplident` is `f`;
`pg_publication_tables` has one row; the decoded message has `"op": "create"` and
`"first_col": "id"`; the duplicate `add_table` errors; after `remove_table` both counts are 0.

- [ ] **Step 5: Verify it passes**

```bash
make installcheck
```

Expected: `All 1 tests passed.`

- [ ] **Step 6: Commit**

```bash
cd /home/vadim/postgres-paimon
git add sql/paimon_slot
git commit -m "$(printf 'feat(paimon_slot): add_table, remove_table and the create message\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 4: Event-trigger guardrails and DDL messages

**Files:**
- Modify: `sql/paimon_slot/paimon_slot--1.0.sql` (append)
- Modify: `sql/paimon_slot/test/sql/paimon_slot.sql` (append)
- Modify: `sql/paimon_slot/test/expected/paimon_slot.out` (regenerate)

**Interfaces:**
- Consumes: everything from Tasks 2 and 3.
- Produces: event triggers `paimon_ddl_end` (`ddl_command_end`) and `paimon_sql_drop` (`sql_drop`), and these DDL messages, which Task 12 parses:

| op | payload |
|---|---|
| `create` | Task 3 |
| `add_col` | `{"v":1,"op":"add_col","relid":<oid>,"col":{"id":<int>,"name":<text>,"type":<text>}}` |
| `drop_col` | `{"v":1,"op":"drop_col","relid":<oid>,"name":<text>}` |
| `rename_col` | `{"v":1,"op":"rename_col","relid":<oid>,"id":<int>,"from":<text>,"to":<text>}` |
| `alter_col` | `{"v":1,"op":"alter_col","relid":<oid>,"id":<int>,"name":<text>,"type":<text>}` |
| `drop_table` | `{"v":1,"op":"drop_table","relid":<oid>,"schema":<text>,"table":<text>}` |

`TRUNCATE` is **not** a DDL message: pgoutput delivers a `T` message natively and
`paimon_pub` publishes truncate by default. `main` captured it via `ProcessUtility`
(`legacy:extension/paimon_ddl.cpp:164`); here the stream carries it.

- [ ] **Step 1: Write the failing test**

Append to `sql/paimon_slot/test/sql/paimon_slot.sql`:

```sql
CREATE TABLE ddl_t (id int PRIMARY KEY, a text);
SELECT paimon.add_table('ddl_t'::regclass);
SELECT paimon.peek_messages();   -- drain the create message

-- blocked: primary key changes
ALTER TABLE ddl_t DROP CONSTRAINT ddl_t_pkey;                 -- expect ERROR
CREATE TABLE ddl_u (id int, a text, PRIMARY KEY (id));
SELECT paimon.add_table('ddl_u'::regclass);
SELECT paimon.peek_messages();
ALTER TABLE ddl_u ADD COLUMN b int;                           -- allowed
SELECT paimon.peek_messages();

-- blocked: NOT NULL without a default
ALTER TABLE ddl_t ADD COLUMN nn int NOT NULL;                 -- expect ERROR
-- blocked: any default (gaps.md item 2 carried over)
ALTER TABLE ddl_t ADD COLUMN wd int DEFAULT 7;                -- expect ERROR
ALTER TABLE ddl_t ADD COLUMN wd2 int NOT NULL DEFAULT 7;      -- expect ERROR
-- the guardrail rolled the statement back, so the columns are absent
SELECT count(*) FROM pg_attribute
WHERE attrelid = 'ddl_t'::regclass AND attname IN ('nn','wd','wd2');

-- allowed and propagated
ALTER TABLE ddl_t ADD COLUMN extra bigint;
SELECT paimon.peek_messages();
ALTER TABLE ddl_t RENAME COLUMN extra TO renamed;
SELECT paimon.peek_messages();
ALTER TABLE ddl_t ALTER COLUMN a TYPE varchar(64);
SELECT paimon.peek_messages();
ALTER TABLE ddl_t DROP COLUMN renamed;
SELECT paimon.peek_messages();

-- the registry tracked every change
SELECT jsonb_pretty(cols) FROM paimon.table_registry WHERE relid = 'ddl_t'::regclass;

-- DROP TABLE
DROP TABLE ddl_t;
SELECT paimon.peek_messages();
SELECT count(*) FROM paimon.table_registry WHERE relid IS NOT NULL
  AND table_name = 'ddl_t';

-- untracked tables are untouched (D9)
CREATE TABLE plain_t (id int PRIMARY KEY);
ALTER TABLE plain_t ADD COLUMN whatever int NOT NULL DEFAULT 1;
SELECT paimon.peek_messages();
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/vadim/postgres-paimon/sql/paimon_slot && make installcheck
```

Expected: FAIL — the `ALTER TABLE ... DROP CONSTRAINT` succeeds instead of erroring, because
no event trigger exists yet.

- [ ] **Step 3: Implement**

Append to `sql/paimon_slot/paimon_slot--1.0.sql`:

```sql
/*
 * Guardrails and DDL emission run at ddl_command_end, not ddl_command_start.
 * Event triggers expose no parse tree at ddl_command_start -- only TG_TAG and
 * current_query() -- so blocking there would mean parsing SQL text.  At
 * ddl_command_end the catalog is readable in its new state and
 * pg_event_trigger_ddl_commands() gives structured object identities, so the
 * trigger diffs against paimon.table_registry.  ALTER TABLE is transactional,
 * so RAISE here rolls the whole statement back: the user-visible outcome is the
 * same as pre-execution blocking.
 */
CREATE FUNCTION paimon.on_ddl_end() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    obj       record;
    reg       paimon.table_registry;
    v_relid   oid;
    new_cols  jsonb;
    new_pk    text[];
    c         jsonb;
    old_c     jsonb;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        CONTINUE WHEN obj.classid <> 'pg_class'::regclass;
        v_relid := obj.objid;

        SELECT * INTO reg FROM paimon.table_registry WHERE relid = v_relid;
        CONTINUE WHEN NOT FOUND;

        new_pk   := paimon.pk_cols(v_relid);
        new_cols := paimon.table_cols(v_relid);

        /* Guardrail: primary key may not be added, dropped or changed. */
        IF new_pk <> reg.pk_cols THEN
            RAISE EXCEPTION 'paimon_slot: cannot change the primary key of %.%',
                            reg.schema_name, reg.table_name
                  USING HINT = 'the Paimon table key is fixed at add_table() time';
        END IF;

        /* Guardrails on newly added columns. */
        FOR c IN SELECT * FROM jsonb_array_elements(new_cols)
        LOOP
            CONTINUE WHEN EXISTS (
                SELECT 1 FROM jsonb_array_elements(reg.cols) o
                WHERE (o->>'id') = (c->>'id'));

            IF EXISTS (SELECT 1 FROM pg_attribute
                       WHERE attrelid = v_relid
                         AND attnum = (c->>'id')::int
                         AND atthasdef) THEN
                RAISE EXCEPTION
                    'paimon_slot: ADD COLUMN with a DEFAULT is not supported (%.%.%)',
                    reg.schema_name, reg.table_name, c->>'name'
                      USING HINT = 'add the column without a default, then UPDATE it';
            END IF;

            IF (c->>'type') LIKE '% NOT NULL' THEN
                RAISE EXCEPTION
                    'paimon_slot: ADD COLUMN ... NOT NULL without a default is not supported (%.%.%)',
                    reg.schema_name, reg.table_name, c->>'name';
            END IF;

            PERFORM paimon.emit(jsonb_build_object(
                'v', 1, 'op', 'add_col', 'relid', v_relid, 'col', c));
        END LOOP;

        /* Renames and type changes: same field id, different name or type. */
        FOR c IN SELECT * FROM jsonb_array_elements(new_cols)
        LOOP
            SELECT o INTO old_c
              FROM jsonb_array_elements(reg.cols) o
             WHERE (o->>'id') = (c->>'id');
            CONTINUE WHEN old_c IS NULL;

            IF (old_c->>'name') <> (c->>'name') THEN
                PERFORM paimon.emit(jsonb_build_object(
                    'v', 1, 'op', 'rename_col', 'relid', v_relid,
                    'id', (c->>'id')::int,
                    'from', old_c->>'name', 'to', c->>'name'));
            END IF;

            IF (old_c->>'type') <> (c->>'type') THEN
                PERFORM paimon.emit(jsonb_build_object(
                    'v', 1, 'op', 'alter_col', 'relid', v_relid,
                    'id', (c->>'id')::int,
                    'name', c->>'name', 'type', c->>'type'));
            END IF;
        END LOOP;

        UPDATE paimon.table_registry
           SET cols = new_cols
         WHERE relid = v_relid;
    END LOOP;
END $$;

CREATE EVENT TRIGGER paimon_ddl_end ON ddl_command_end
    EXECUTE FUNCTION paimon.on_ddl_end();

/*
 * Drops are handled in sql_drop rather than by diffing, because
 * pg_event_trigger_dropped_objects() reports the dropped column or table
 * directly and fires before the catalog row is gone from view.
 */
CREATE FUNCTION paimon.on_sql_drop() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    reg paimon.table_registry;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF obj.object_type = 'table' THEN
            SELECT * INTO reg FROM paimon.table_registry WHERE relid = obj.objid;
            CONTINUE WHEN NOT FOUND;
            PERFORM paimon.emit(jsonb_build_object(
                'v', 1, 'op', 'drop_table', 'relid', obj.objid,
                'schema', reg.schema_name, 'table', reg.table_name));
            DELETE FROM paimon.table_registry WHERE relid = obj.objid;

        ELSIF obj.object_type = 'table column' THEN
            SELECT * INTO reg FROM paimon.table_registry WHERE relid = obj.objid;
            CONTINUE WHEN NOT FOUND;
            PERFORM paimon.emit(jsonb_build_object(
                'v', 1, 'op', 'drop_col', 'relid', obj.objid,
                'name', split_part(obj.object_identity, '.', 3)));
            UPDATE paimon.table_registry
               SET cols = paimon.table_cols(obj.objid)
             WHERE relid = obj.objid;
        END IF;
    END LOOP;
END $$;

CREATE EVENT TRIGGER paimon_sql_drop ON sql_drop
    EXECUTE FUNCTION paimon.on_sql_drop();
```

- [ ] **Step 4: Run the test and regenerate expected output**

```bash
cd /home/vadim/postgres-paimon/sql/paimon_slot
make && sudo make install && make installcheck
cp test/results/paimon_slot.out test/expected/paimon_slot.out
cat test/expected/paimon_slot.out
```

Read it and confirm:
- the PK drop, the `NOT NULL` add and both `DEFAULT` adds all raise, with the messages above;
- `count(*)` for `nn`, `wd`, `wd2` is `0` — the rollback worked;
- `add_col`, `rename_col`, `alter_col`, `drop_col` and `drop_table` each appear exactly once with the right payload;
- `alter_col` for `a TYPE varchar(64)` carries `"type": "STRING"` — `varchar` and `text` both map to `STRING`, so this message is a no-op for Paimon but is still emitted, which Task 12 must tolerate;
- the registry `cols` after the drop no longer contains `renamed`;
- the `plain_t` statements emit nothing.

- [ ] **Step 5: Verify it passes**

```bash
make installcheck
```

Expected: `All 1 tests passed.`

- [ ] **Step 6: Commit**

```bash
cd /home/vadim/postgres-paimon
git add sql/paimon_slot
git commit -m "$(printf 'feat(paimon_slot): event-trigger guardrails and DDL messages\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 5: Writer project skeleton, configuration and value types

**Files:**
- Create: `java/writer/pom.xml`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/WriterConfig.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/ColumnValue.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/TupleData.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/RelationMeta.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/PgEvent.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/PartialRecord.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/SinkRecord.java`
- Test: `java/writer/src/test/java/pro/surpin/pgpaimon/writer/WriterConfigTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: every value type the later tasks pass around. Exact shapes:

```java
public enum ColState { NULL, UNCHANGED, TEXT }
public record ColumnValue(ColState state, String text) {
    public static ColumnValue ofNull();
    public static ColumnValue unchanged();
    public static ColumnValue of(String text);   // text may be "" and is never null
}
public record TupleData(List<ColumnValue> columns) {}
public record RelationMeta(int relid, String namespace, String relName,
                           List<String> columnNames, List<Boolean> isKey) {}
public record PartialRecord(int relid, org.apache.paimon.types.RowKind kind,
                            List<String> values) {}   // values: null element = SQL NULL
public record SinkRecord(int relid, org.apache.paimon.types.RowKind kind,
                         List<String> values, long seq, int opSeq, long xid) {}
public sealed interface PgEvent {
    record Begin(long finalLsn, long commitTimestamp, long xid)          implements PgEvent {}
    record Commit(long commitLsn, long endLsn, long commitTimestamp)     implements PgEvent {}
    record Relation(RelationMeta meta)                                   implements PgEvent {}
    record Insert(int relid, TupleData newTuple)                         implements PgEvent {}
    record Update(int relid, TupleData oldTuple, boolean oldIsKeyOnly,
                  TupleData newTuple)                                    implements PgEvent {}
    record Delete(int relid, TupleData oldTuple, boolean oldIsKeyOnly)   implements PgEvent {}
    record Truncate(List<Integer> relids, boolean cascade, boolean restartIdentity)
                                                                         implements PgEvent {}
    record Message(boolean transactional, long lsn, String prefix, byte[] content)
                                                                         implements PgEvent {}
    record Origin(long commitLsn, String name)                           implements PgEvent {}
    record TypeMsg(int typeOid, String namespace, String name)           implements PgEvent {}
}
public record WriterConfig(String jdbcUrl, String replicationUrl, String user,
                           String password, String warehouse, String database,
                           String slotName, String publicationName,
                           String messagePrefix, String commitUser,
                           String seqField, String opSeqField, String xidField,
                           int flushMaxRecords, long flushMaxIntervalMs,
                           Map<String, String> catalogOptions) {
    public static WriterConfig fromEnv(Map<String, String> env);
}
```

`Update.oldTuple` is `null` when pgoutput sent no old tuple at all. `oldIsKeyOnly` is `true`
for a `'K'` marker and `false` for `'O'`.

- [ ] **Step 1: Write the failing test**

`java/writer/src/test/java/pro/surpin/pgpaimon/writer/WriterConfigTest.java`:

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class WriterConfigTest {

    private static Map<String, String> minimal() {
        Map<String, String> e = new HashMap<>();
        e.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost:5432/testdb");
        e.put("PGPAIMON_USER", "postgres");
        e.put("PGPAIMON_PASSWORD", "secret");
        e.put("PGPAIMON_WAREHOUSE", "/tmp/wh");
        return e;
    }

    @Test
    void defaultsMatchTheSpec() {
        WriterConfig c = WriterConfig.fromEnv(minimal());
        assertEquals("paimon", c.slotName());
        assertEquals("paimon_pub", c.publicationName());
        assertEquals("paimon", c.messagePrefix());
        assertEquals("_seq", c.seqField());
        assertEquals("_op_seq", c.opSeqField());
        assertNull(c.xidField(), "_xid is optional and off by default");
        assertEquals("paimon-writer", c.commitUser());
        assertEquals("default", c.database());
    }

    @Test
    void replicationUrlForcesSimpleQueryMode() {
        WriterConfig c = WriterConfig.fromEnv(minimal());
        assertTrue(c.replicationUrl().contains("replication=database"));
        assertTrue(c.replicationUrl().contains("preferQueryMode=simple"));
    }

    @Test
    void missingWarehouseIsRejectedWithAUsefulMessage() {
        Map<String, String> e = minimal();
        e.remove("PGPAIMON_WAREHOUSE");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> WriterConfig.fromEnv(e));
        assertTrue(ex.getMessage().contains("PGPAIMON_WAREHOUSE"));
    }

    @Test
    void s3CatalogOptionsArePassedThrough() {
        Map<String, String> e = minimal();
        e.put("PGPAIMON_WAREHOUSE", "s3://bucket/wh");
        e.put("PGPAIMON_S3_ENDPOINT", "http://minio:9000");
        e.put("PGPAIMON_S3_ACCESS_KEY", "ak");
        e.put("PGPAIMON_S3_SECRET_KEY", "sk");
        WriterConfig c = WriterConfig.fromEnv(e);
        assertEquals("http://minio:9000", c.catalogOptions().get("s3.endpoint"));
        assertEquals("ak", c.catalogOptions().get("s3.access-key"));
        assertEquals("sk", c.catalogOptions().get("s3.secret-key"));
        assertEquals("true", c.catalogOptions().get("s3.path.style.access"));
    }

    @Test
    void columnValueDistinguishesNullFromEmpty() {
        assertEquals(ColState.NULL, ColumnValue.ofNull().state());
        assertNull(ColumnValue.ofNull().text());
        assertEquals(ColState.TEXT, ColumnValue.of("").state());
        assertEquals("", ColumnValue.of("").text());
        assertEquals(ColState.UNCHANGED, ColumnValue.unchanged().state());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test
```

Expected: FAIL — no `pom.xml`, so `mvn` reports `The goal you specified requires a project to execute but there is no POM in this directory`.

- [ ] **Step 3: Write the pom and the value types**

`java/writer/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>pro.surpin.pgpaimon</groupId>
    <artifactId>paimon-writer</artifactId>
    <version>1.0</version>
    <packaging>jar</packaging>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.release>17</maven.compiler.release>
        <paimon.version>1.4.1</paimon.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.paimon</groupId>
            <artifactId>paimon-bundle</artifactId>
            <version>${paimon.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client-api</artifactId>
            <version>3.3.4</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client-runtime</artifactId>
            <version>3.3.4</version>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>42.7.3</version>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.10.2</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.2.5</version>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.1</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals><goal>shade</goal></goals>
                        <configuration>
                            <createDependencyReducedPom>false</createDependencyReducedPom>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>pro.surpin.pgpaimon.writer.Main</mainClass>
                                </transformer>
                            </transformers>
                            <finalName>paimon-writer</finalName>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

The `ServicesResourceTransformer` is not optional: Paimon resolves catalogs, file IO and
field aggregators through `META-INF/services/org.apache.paimon.factories.Factory`, and a
shaded jar that concatenates those files wrongly fails at runtime with "Could not find any
factory for identifier 'filesystem'".

`ColumnValue.java`:

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

/** One column as pgoutput delivered it.  NULL and "" are different states. */
public record ColumnValue(ColState state, String text) {

    private static final ColumnValue NULL_VALUE = new ColumnValue(ColState.NULL, null);
    private static final ColumnValue UNCHANGED  = new ColumnValue(ColState.UNCHANGED, null);

    public static ColumnValue ofNull()    { return NULL_VALUE; }
    public static ColumnValue unchanged() { return UNCHANGED; }

    public static ColumnValue of(String text) {
        if (text == null) {
            throw new IllegalArgumentException("use ofNull() for a SQL NULL");
        }
        return new ColumnValue(ColState.TEXT, text);
    }
}
```

`ColState.java`, `TupleData.java`, `RelationMeta.java`, `PgEvent.java`, `PartialRecord.java`
and `SinkRecord.java` are the declarations listed in the Interfaces block above, each in its
own file with the SPDX header. `PgEvent` is a `sealed interface` whose `permits` clause is
implicit because every record is nested inside it.

`WriterConfig.java`:

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.*;

public record WriterConfig(String jdbcUrl, String replicationUrl, String user,
                           String password, String warehouse, String database,
                           String slotName, String publicationName,
                           String messagePrefix, String commitUser,
                           String seqField, String opSeqField, String xidField,
                           int flushMaxRecords, long flushMaxIntervalMs,
                           Map<String, String> catalogOptions) {

    public static WriterConfig fromEnv(Map<String, String> env) {
        String jdbc = require(env, "PGPAIMON_JDBC_URL");
        String wh   = require(env, "PGPAIMON_WAREHOUSE");

        Map<String, String> cat = new LinkedHashMap<>();
        cat.put("warehouse", wh);
        put(cat, "s3.endpoint",   env.get("PGPAIMON_S3_ENDPOINT"));
        put(cat, "s3.access-key", env.get("PGPAIMON_S3_ACCESS_KEY"));
        put(cat, "s3.secret-key", env.get("PGPAIMON_S3_SECRET_KEY"));
        if (env.containsKey("PGPAIMON_S3_ENDPOINT")) {
            cat.put("s3.path.style.access", "true");
        }

        return new WriterConfig(
            jdbc,
            jdbc + (jdbc.contains("?") ? "&" : "?")
                 + "replication=database&preferQueryMode=simple&assumeMinServerVersion=18",
            env.getOrDefault("PGPAIMON_USER", "postgres"),
            env.getOrDefault("PGPAIMON_PASSWORD", ""),
            wh,
            env.getOrDefault("PGPAIMON_DATABASE", "default"),
            env.getOrDefault("PGPAIMON_SLOT", "paimon"),
            env.getOrDefault("PGPAIMON_PUBLICATION", "paimon_pub"),
            env.getOrDefault("PGPAIMON_MESSAGE_PREFIX", "paimon"),
            env.getOrDefault("PGPAIMON_COMMIT_USER", "paimon-writer"),
            env.getOrDefault("PGPAIMON_SEQ_FIELD", "_seq"),
            env.getOrDefault("PGPAIMON_OP_SEQ_FIELD", "_op_seq"),
            env.get("PGPAIMON_XID_FIELD"),
            Integer.parseInt(env.getOrDefault("PGPAIMON_FLUSH_MAX_RECORDS", "10000")),
            Long.parseLong(env.getOrDefault("PGPAIMON_FLUSH_MAX_INTERVAL_MS", "1000")),
            Map.copyOf(cat));
    }

    private static String require(Map<String, String> env, String key) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException(key + " is required");
        }
        return v;
    }

    private static void put(Map<String, String> m, String k, String v) {
        if (v != null && !v.isBlank()) {
            m.put(k, v);
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test
```

Expected: 5 tests, all pass.

- [ ] **Step 5: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/writer
git commit -m "$(printf 'feat(writer): Maven project, configuration and protocol value types\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 6: `PgOutputDecoder`

**Files:**
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/PgOutputDecoder.java`
- Test: `java/writer/src/test/java/pro/surpin/pgpaimon/writer/PgOutputDecoderTest.java`

**Interfaces:**
- Consumes: `PgEvent`, `ColumnValue`, `ColState`, `TupleData`, `RelationMeta` from Task 5.
- Produces:
  - `PgOutputDecoder()` — stateful; holds the relation cache
  - `PgEvent decode(ByteBuffer buf)` — consumes exactly one message
  - `RelationMeta relation(int relid)` — the last `R` message for that relid, or `null`

The protocol is PostgreSQL's logical replication message format with `proto_version = 2`.
Every integer is big-endian. Column states are the four bytes from
`src/include/replication/logicalproto.h:96-99`: `'n'` NULL, `'u'` UNCHANGED, `'t'` TEXT,
`'b'` BINARY. Because the slot runs with `binary 'false'`, `'b'` must raise rather than be
guessed at.

- [ ] **Step 1: Write the failing test**

`java/writer/src/test/java/pro/surpin/pgpaimon/writer/PgOutputDecoderTest.java`:

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class PgOutputDecoderTest {

    /** Builds a pgoutput message body; mirrors src/backend/replication/logical/proto.c. */
    static class Msg {
        final ByteArrayOutputStream bo = new ByteArrayOutputStream();
        final DataOutputStream d = new DataOutputStream(bo);
        Msg b(int v)      { try { d.writeByte(v); }  catch (Exception e) { throw new RuntimeException(e); } return this; }
        Msg i16(int v)    { try { d.writeShort(v); } catch (Exception e) { throw new RuntimeException(e); } return this; }
        Msg i32(int v)    { try { d.writeInt(v); }   catch (Exception e) { throw new RuntimeException(e); } return this; }
        Msg i64(long v)   { try { d.writeLong(v); }  catch (Exception e) { throw new RuntimeException(e); } return this; }
        Msg cstr(String s){ try { d.write(s.getBytes(StandardCharsets.UTF_8)); d.writeByte(0); }
                            catch (Exception e) { throw new RuntimeException(e); } return this; }
        Msg text(String s){ byte[] x = s.getBytes(StandardCharsets.UTF_8);
                            b('t').i32(x.length);
                            try { d.write(x); } catch (Exception e) { throw new RuntimeException(e); }
                            return this; }
        ByteBuffer buf()  { return ByteBuffer.wrap(bo.toByteArray()); }
    }

    private static ByteBuffer relationOf(int relid, String... cols) {
        Msg m = new Msg().b('R').i32(relid).cstr("public").cstr("t").b('d').i16(cols.length);
        for (int i = 0; i < cols.length; i++) {
            m.b(i == 0 ? 1 : 0).cstr(cols[i]).i32(23).i32(-1);
        }
        return m.buf();
    }

    @Test
    void decodesBeginAndCommit() {
        PgOutputDecoder dec = new PgOutputDecoder();
        PgEvent.Begin b = (PgEvent.Begin) dec.decode(
            new Msg().b('B').i64(0x16B374D8L).i64(700000L).i32(4711).buf());
        assertEquals(0x16B374D8L, b.finalLsn());
        assertEquals(4711L, b.xid());

        PgEvent.Commit c = (PgEvent.Commit) dec.decode(
            new Msg().b('C').b(0).i64(0x16B374D8L).i64(0x16B374F0L).i64(700000L).buf());
        assertEquals(0x16B374D8L, c.commitLsn());
        assertEquals(0x16B374F0L, c.endLsn());
    }

    @Test
    void decodesRelationAndCachesIt() {
        PgOutputDecoder dec = new PgOutputDecoder();
        PgEvent.Relation r = (PgEvent.Relation) dec.decode(relationOf(16412, "id", "v"));
        assertEquals(16412, r.meta().relid());
        assertEquals("public", r.meta().namespace());
        assertEquals(List.of("id", "v"), r.meta().columnNames());
        assertEquals(List.of(true, false), r.meta().isKey());
        assertSame(r.meta(), dec.relation(16412));
    }

    @Test
    void decodesInsert() {
        PgOutputDecoder dec = new PgOutputDecoder();
        dec.decode(relationOf(16412, "id", "v"));
        PgEvent.Insert i = (PgEvent.Insert) dec.decode(
            new Msg().b('I').i32(16412).b('N').i16(2).text("1").text("hello").buf());
        assertEquals(16412, i.relid());
        assertEquals("1", i.newTuple().columns().get(0).text());
        assertEquals("hello", i.newTuple().columns().get(1).text());
    }

    /** Review Focus 1: 'n' is NULL, 't' with length 0 is the empty string. */
    @Test
    void nullAndEmptyStringAreDistinct() {
        PgOutputDecoder dec = new PgOutputDecoder();
        dec.decode(relationOf(16412, "id", "v"));
        PgEvent.Insert i = (PgEvent.Insert) dec.decode(
            new Msg().b('I').i32(16412).b('N').i16(2).text("").b('n').buf());
        ColumnValue empty = i.newTuple().columns().get(0);
        ColumnValue nul   = i.newTuple().columns().get(1);
        assertEquals(ColState.TEXT, empty.state());
        assertEquals("", empty.text());
        assertEquals(ColState.NULL, nul.state());
        assertNull(nul.text());
        assertNotEquals(empty, nul);
    }

    @Test
    void decodesUpdateWithFullOldTupleAndAnUnchangedColumn() {
        PgOutputDecoder dec = new PgOutputDecoder();
        dec.decode(relationOf(16412, "id", "n", "body"));
        PgEvent.Update u = (PgEvent.Update) dec.decode(
            new Msg().b('U').i32(16412)
                     .b('O').i16(3).text("1").text("1").text("big-old-value")
                     .b('N').i16(3).text("1").text("99").b('u')
                     .buf());
        assertFalse(u.oldIsKeyOnly());
        assertEquals("big-old-value", u.oldTuple().columns().get(2).text());
        assertEquals(ColState.UNCHANGED, u.newTuple().columns().get(2).state());
    }

    @Test
    void decodesUpdateWithKeyOnlyOldTuple() {
        PgOutputDecoder dec = new PgOutputDecoder();
        dec.decode(relationOf(16412, "id", "v"));
        PgEvent.Update u = (PgEvent.Update) dec.decode(
            new Msg().b('U').i32(16412)
                     .b('K').i16(2).text("1").b('n')
                     .b('N').i16(2).text("2").text("x")
                     .buf());
        assertTrue(u.oldIsKeyOnly());
        assertEquals("1", u.oldTuple().columns().get(0).text());
    }

    @Test
    void decodesUpdateWithNoOldTuple() {
        PgOutputDecoder dec = new PgOutputDecoder();
        dec.decode(relationOf(16412, "id", "v"));
        PgEvent.Update u = (PgEvent.Update) dec.decode(
            new Msg().b('U').i32(16412).b('N').i16(2).text("1").text("x").buf());
        assertNull(u.oldTuple());
    }

    @Test
    void decodesDeleteAndTruncate() {
        PgOutputDecoder dec = new PgOutputDecoder();
        dec.decode(relationOf(16412, "id", "v"));
        PgEvent.Delete d = (PgEvent.Delete) dec.decode(
            new Msg().b('D').i32(16412).b('K').i16(2).text("7").b('n').buf());
        assertEquals("7", d.oldTuple().columns().get(0).text());

        PgEvent.Truncate t = (PgEvent.Truncate) dec.decode(
            new Msg().b('T').i32(2).b(0).i32(16412).i32(16500).buf());
        assertEquals(List.of(16412, 16500), t.relids());
    }

    @Test
    void decodesLogicalMessage() {
        PgOutputDecoder dec = new PgOutputDecoder();
        byte[] body = "{\"v\":1,\"op\":\"create\"}".getBytes(StandardCharsets.UTF_8);
        Msg m = new Msg().b('M').b(1).i64(0x100L).cstr("paimon").i32(body.length);
        try { m.d.write(body); } catch (Exception e) { throw new RuntimeException(e); }
        PgEvent.Message msg = (PgEvent.Message) dec.decode(m.buf());
        assertTrue(msg.transactional());
        assertEquals("paimon", msg.prefix());
        assertEquals("{\"v\":1,\"op\":\"create\"}",
                     new String(msg.content(), StandardCharsets.UTF_8));
    }

    @Test
    void binaryColumnStateIsRejected() {
        PgOutputDecoder dec = new PgOutputDecoder();
        dec.decode(relationOf(16412, "id"));
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> dec.decode(new Msg().b('I').i32(16412).b('N').i16(1)
                                      .b('b').i32(1).b(0x41).buf()));
        assertTrue(e.getMessage().contains("binary"));
    }

    @Test
    void unknownMessageTypeIsRejectedWithTheByte() {
        PgOutputDecoder dec = new PgOutputDecoder();
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> dec.decode(new Msg().b('Z').buf()));
        assertTrue(e.getMessage().contains("Z"));
    }

    @Test
    void streamMessagesAreRejectedBecauseStreamingIsOff() {
        PgOutputDecoder dec = new PgOutputDecoder();
        for (char kind : new char[] { 'S', 'E', 'c', 'A' }) {
            assertThrows(IllegalStateException.class,
                () -> dec.decode(new Msg().b(kind).buf()),
                "stream message " + kind + " must be refused");
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=PgOutputDecoderTest
```

Expected: FAIL — `cannot find symbol: class PgOutputDecoder`.

- [ ] **Step 3: Implement**

`java/writer/src/main/java/pro/surpin/pgpaimon/writer/PgOutputDecoder.java`:

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Decodes pgoutput protocol version 2 messages.
 *
 * Column state bytes are the four from src/include/replication/logicalproto.h:
 *   'n' NULL, 'u' UNCHANGED (an unreassembled TOAST pointer), 't' TEXT, 'b' BINARY.
 * The slot runs with binary 'false', so 'b' is a protocol violation here, not a
 * case to guess at.
 */
public final class PgOutputDecoder {

    private final Map<Integer, RelationMeta> relations = new HashMap<>();

    public RelationMeta relation(int relid) {
        return relations.get(relid);
    }

    public PgEvent decode(ByteBuffer buf) {
        char kind = (char) buf.get();
        switch (kind) {
            case 'B': {
                long finalLsn = buf.getLong();
                long ts = buf.getLong();
                long xid = Integer.toUnsignedLong(buf.getInt());
                return new PgEvent.Begin(finalLsn, ts, xid);
            }
            case 'C': {
                buf.get();                      // flags, unused
                long commitLsn = buf.getLong();
                long endLsn = buf.getLong();
                long ts = buf.getLong();
                return new PgEvent.Commit(commitLsn, endLsn, ts);
            }
            case 'R': {
                int relid = buf.getInt();
                String nsp = cstring(buf);
                String rel = cstring(buf);
                buf.get();                      // replica identity setting
                int n = buf.getShort() & 0xFFFF;
                List<String> names = new ArrayList<>(n);
                List<Boolean> keys = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    keys.add((buf.get() & 1) == 1);
                    names.add(cstring(buf));
                    buf.getInt();               // type oid
                    buf.getInt();               // type modifier
                }
                RelationMeta meta = new RelationMeta(relid, nsp.isEmpty() ? "public" : nsp,
                                                     rel, List.copyOf(names), List.copyOf(keys));
                relations.put(relid, meta);
                return new PgEvent.Relation(meta);
            }
            case 'I': {
                int relid = buf.getInt();
                expect(buf.get(), 'N', "insert new-tuple marker");
                return new PgEvent.Insert(relid, tuple(buf));
            }
            case 'U': {
                int relid = buf.getInt();
                TupleData oldTuple = null;
                boolean keyOnly = false;
                char marker = (char) buf.get();
                if (marker == 'O' || marker == 'K') {
                    keyOnly = marker == 'K';
                    oldTuple = tuple(buf);
                    marker = (char) buf.get();
                }
                expect((byte) marker, 'N', "update new-tuple marker");
                return new PgEvent.Update(relid, oldTuple, keyOnly, tuple(buf));
            }
            case 'D': {
                int relid = buf.getInt();
                char marker = (char) buf.get();
                if (marker != 'O' && marker != 'K') {
                    throw new IllegalStateException(
                        "delete without an old tuple marker: got '" + marker + "'");
                }
                return new PgEvent.Delete(relid, tuple(buf), marker == 'K');
            }
            case 'T': {
                int n = buf.getInt();
                int flags = buf.get();
                List<Integer> relids = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    relids.add(buf.getInt());
                }
                return new PgEvent.Truncate(List.copyOf(relids),
                                            (flags & 1) != 0, (flags & 2) != 0);
            }
            case 'M': {
                boolean transactional = buf.get() != 0;
                long lsn = buf.getLong();
                String prefix = cstring(buf);
                int len = buf.getInt();
                byte[] content = new byte[len];
                buf.get(content);
                return new PgEvent.Message(transactional, lsn, prefix, content);
            }
            case 'O': {
                long commitLsn = buf.getLong();
                return new PgEvent.Origin(commitLsn, cstring(buf));
            }
            case 'Y': {
                int oid = buf.getInt();
                return new PgEvent.TypeMsg(oid, cstring(buf), cstring(buf));
            }
            case 'S': case 'E': case 'c': case 'A':
                throw new IllegalStateException(
                    "stream message '" + kind + "' received although streaming is off");
            default:
                throw new IllegalStateException("unknown pgoutput message type '" + kind + "'");
        }
    }

    private static TupleData tuple(ByteBuffer buf) {
        int n = buf.getShort() & 0xFFFF;
        List<ColumnValue> cols = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            char state = (char) buf.get();
            switch (state) {
                case 'n' -> cols.add(ColumnValue.ofNull());
                case 'u' -> cols.add(ColumnValue.unchanged());
                case 't' -> {
                    int len = buf.getInt();
                    byte[] v = new byte[len];
                    buf.get(v);
                    cols.add(ColumnValue.of(new String(v, StandardCharsets.UTF_8)));
                }
                case 'b' -> throw new IllegalStateException(
                    "binary column value received although the slot uses binary 'false'");
                default -> throw new IllegalStateException(
                    "unknown column state '" + state + "'");
            }
        }
        return new TupleData(List.copyOf(cols));
    }

    private static String cstring(ByteBuffer buf) {
        StringBuilder sb = new StringBuilder();
        byte b;
        while ((b = buf.get()) != 0) {
            sb.append((char) (b & 0xFF));
        }
        return new String(sb.toString().getBytes(StandardCharsets.ISO_8859_1),
                          StandardCharsets.UTF_8);
    }

    private static void expect(byte actual, char want, String what) {
        if (actual != (byte) want) {
            throw new IllegalStateException(
                what + ": expected '" + want + "', got '" + (char) actual + "'");
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=PgOutputDecoderTest
```

Expected: 11 tests, all pass.

- [ ] **Step 5: Cross-check against the spike fixture**

Take the `U` message hex recorded in `docs/superpowers/notes/2026-10-09-spike-findings.md`
(Task 1 step 2) and add one more test that feeds those exact bytes through `decode()`,
asserting the old tuple's `body` is 100000 characters long and the new tuple's `body` is
`UNCHANGED`. This is the only test that proves the decoder agrees with a real server rather
than with the test's own message builder.

```bash
mvn -q test -Dtest=PgOutputDecoderTest
```

Expected: 12 tests, all pass.

- [ ] **Step 6: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/writer
git commit -m "$(printf 'feat(writer): pgoutput protocol decoder\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 7: `ToastResolver`

**Files:**
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/ToastResolver.java`
- Test: `java/writer/src/test/java/pro/surpin/pgpaimon/writer/ToastResolverTest.java`

**Interfaces:**
- Consumes: `PgEvent`, `TupleData`, `ColumnValue`, `RelationMeta`, `PartialRecord` from Task 5.
- Produces:
  - `ToastResolver()`
  - `List<PartialRecord> resolve(PgEvent event, RelationMeta meta)` — returns `List.of()` for events that produce no records

This is the class that implements spec §5's write-rules table:

| event | records |
|---|---|
| `Insert` | `+I(new)` |
| `Update`, key unchanged | `-U(old)`, `+U(new)`; each `u` filled from `old` |
| `Update`, key changed | `-D(old)`, `+I(new)`; each `u` filled from `old` |
| `Delete` | `-D(old)` |
| anything else | `List.of()` |

Two rules that are easy to get wrong and are each pinned by a test:

- An old tuple's presence is **not** evidence that the key changed. `heap_update` logs the
  old key when `bms_overlap(modified_attrs, id_attrs) || id_has_external`
  (`heapam.c:4007-4010`), so a row with a TOASTed key column produces one on every UPDATE.
  Compare the key **values**.
- Review Focus 2: a `u` column with no old tuple to fill it from is unrecoverable. It means
  the table is not at `REPLICA IDENTITY FULL`. Throw; do not write NULL.

- [ ] **Step 1: Write the failing test**

`java/writer/src/test/java/pro/surpin/pgpaimon/writer/ToastResolverTest.java`:

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ToastResolverTest {

    static final RelationMeta META = new RelationMeta(
        16412, "public", "t",
        List.of("id", "n", "body"),
        List.of(true, false, false));

    static TupleData t(ColumnValue... c) { return new TupleData(List.of(c)); }
    static ColumnValue v(String s) { return ColumnValue.of(s); }

    private final ToastResolver r = new ToastResolver();

    @Test
    void insertProducesOneInsertRecord() {
        List<PartialRecord> out = r.resolve(
            new PgEvent.Insert(16412, t(v("1"), v("2"), v("big"))), META);
        assertEquals(1, out.size());
        assertEquals(RowKind.INSERT, out.get(0).kind());
        assertEquals(List.of("1", "2", "big"), out.get(0).values());
    }

    @Test
    void updateWithUnchangedKeyProducesBeforeAndAfter() {
        List<PartialRecord> out = r.resolve(
            new PgEvent.Update(16412,
                t(v("1"), v("2"), v("big")), false,
                t(v("1"), v("99"), ColumnValue.unchanged())), META);
        assertEquals(2, out.size());
        assertEquals(RowKind.UPDATE_BEFORE, out.get(0).kind());
        assertEquals(List.of("1", "2", "big"), out.get(0).values());
        assertEquals(RowKind.UPDATE_AFTER, out.get(1).kind());
        assertEquals(List.of("1", "99", "big"),
                     out.get(1).values(), "the u column must come from the old tuple");
    }

    @Test
    void updateThatMovesTheKeyBecomesDeleteThenInsert() {
        List<PartialRecord> out = r.resolve(
            new PgEvent.Update(16412,
                t(v("1"), v("2"), v("big")), false,
                t(v("5"), v("2"), ColumnValue.unchanged())), META);
        assertEquals(2, out.size());
        assertEquals(RowKind.DELETE, out.get(0).kind());
        assertEquals(List.of("1", "2", "big"), out.get(0).values());
        assertEquals(RowKind.INSERT, out.get(1).kind());
        assertEquals(List.of("5", "2", "big"), out.get(1).values());
    }

    /** An old tuple is present because of id_has_external, but the key did not move. */
    @Test
    void oldTuplePresenceAloneIsNotAKeyChange() {
        List<PartialRecord> out = r.resolve(
            new PgEvent.Update(16412,
                t(v("1"), v("2"), v("big")), false,
                t(v("1"), v("2"), v("big"))), META);
        assertEquals(RowKind.UPDATE_BEFORE, out.get(0).kind());
        assertEquals(RowKind.UPDATE_AFTER, out.get(1).kind());
    }

    @Test
    void deleteProducesOneDeleteRecord() {
        List<PartialRecord> out = r.resolve(
            new PgEvent.Delete(16412, t(v("1"), v("2"), v("big")), false), META);
        assertEquals(1, out.size());
        assertEquals(RowKind.DELETE, out.get(0).kind());
    }

    @Test
    void nullsSurviveAsNullsAndEmptyStringsAsEmptyStrings() {
        List<PartialRecord> out = r.resolve(
            new PgEvent.Insert(16412, t(v("1"), ColumnValue.ofNull(), v(""))), META);
        assertNull(out.get(0).values().get(1));
        assertEquals("", out.get(0).values().get(2));
    }

    /** Review Focus 2: a u column with no old tuple is unrecoverable; fail loudly. */
    @Test
    void unchangedColumnWithoutAnOldTupleIsRejected() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
            r.resolve(new PgEvent.Update(16412, null, false,
                t(v("1"), v("99"), ColumnValue.unchanged())), META));
        assertTrue(e.getMessage().contains("REPLICA IDENTITY FULL"),
                   "the message must say how to fix it, got: " + e.getMessage());
        assertTrue(e.getMessage().contains("body"), "the message must name the column");
    }

    /** A key-only old tuple cannot fill a u column either. */
    @Test
    void unchangedColumnWithAKeyOnlyOldTupleIsRejected() {
        assertThrows(IllegalStateException.class, () ->
            r.resolve(new PgEvent.Update(16412,
                t(v("1"), ColumnValue.ofNull(), ColumnValue.ofNull()), true,
                t(v("1"), v("99"), ColumnValue.unchanged())), META));
    }

    @Test
    void beginCommitRelationAndMessageProduceNoRecords() {
        assertTrue(r.resolve(new PgEvent.Begin(1, 2, 3), META).isEmpty());
        assertTrue(r.resolve(new PgEvent.Commit(1, 2, 3), META).isEmpty());
        assertTrue(r.resolve(new PgEvent.Relation(META), META).isEmpty());
        assertTrue(r.resolve(
            new PgEvent.Message(true, 1, "paimon", new byte[0]), META).isEmpty());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=ToastResolverTest
```

Expected: FAIL — `cannot find symbol: class ToastResolver`.

- [ ] **Step 3: Implement**

`java/writer/src/main/java/pro/surpin/pgpaimon/writer/ToastResolver.java`:

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.types.RowKind;
import java.util.*;

/**
 * Turns decoded events into the records of the design spec's write-rules table.
 *
 * The TOAST substitution rests on ExtractReplicaIdentity() in
 * src/backend/access/heap/heapam.c calling toast_flatten_tuple() before the old
 * tuple reaches WAL, in both of its branches.  Under REPLICA IDENTITY FULL the
 * old tuple therefore carries every column in full, TOASTed ones included, and an
 * UPDATE that did not touch a column has that column's current value sitting in
 * the old tuple: unchanged means old equals new.
 */
public final class ToastResolver {

    public List<PartialRecord> resolve(PgEvent event, RelationMeta meta) {
        if (event instanceof PgEvent.Insert i) {
            return List.of(new PartialRecord(i.relid(), RowKind.INSERT,
                                             plain(i.newTuple(), null, meta)));
        }
        if (event instanceof PgEvent.Delete d) {
            return List.of(new PartialRecord(d.relid(), RowKind.DELETE,
                                             plain(d.oldTuple(), null, meta)));
        }
        if (event instanceof PgEvent.Update u) {
            TupleData usableOld = u.oldIsKeyOnly() ? null : u.oldTuple();
            List<String> oldValues = u.oldTuple() == null
                ? null : plain(u.oldTuple(), null, meta);
            List<String> newValues = plain(u.newTuple(), usableOld, meta);

            if (oldValues != null && keyChanged(oldValues, newValues, meta)) {
                return List.of(
                    new PartialRecord(u.relid(), RowKind.DELETE, oldValues),
                    new PartialRecord(u.relid(), RowKind.INSERT, newValues));
            }
            if (oldValues == null) {
                return List.of(new PartialRecord(u.relid(), RowKind.UPDATE_AFTER, newValues));
            }
            return List.of(
                new PartialRecord(u.relid(), RowKind.UPDATE_BEFORE, oldValues),
                new PartialRecord(u.relid(), RowKind.UPDATE_AFTER, newValues));
        }
        return List.of();
    }

    /**
     * Flattens a tuple to plain text values.  A null element means SQL NULL; ""
     * means the empty string, which is a different thing.  An UNCHANGED column is
     * filled from {@code fallback}, and is a hard error when there is none.
     */
    private static List<String> plain(TupleData tuple, TupleData fallback, RelationMeta meta) {
        List<ColumnValue> cols = tuple.columns();
        List<String> out = new ArrayList<>(cols.size());
        for (int i = 0; i < cols.size(); i++) {
            ColumnValue c = cols.get(i);
            switch (c.state()) {
                case NULL -> out.add(null);
                case TEXT -> out.add(c.text());
                case UNCHANGED -> {
                    ColumnValue from = fallback == null ? null : fallback.columns().get(i);
                    if (from == null || from.state() == ColState.UNCHANGED) {
                        throw new IllegalStateException(
                            "column '" + columnName(meta, i) + "' of " + meta.namespace()
                            + "." + meta.relName() + " arrived unchanged (an unreassembled"
                            + " TOAST pointer) and no old tuple carries its value;"
                            + " the table must be at REPLICA IDENTITY FULL");
                    }
                    out.add(from.state() == ColState.NULL ? null : from.text());
                }
            }
        }
        return List.copyOf(new ArrayList<>(out) {{ }}.stream().toList());
    }

    private static boolean keyChanged(List<String> oldValues, List<String> newValues,
                                      RelationMeta meta) {
        List<Boolean> isKey = meta.isKey();
        for (int i = 0; i < isKey.size() && i < oldValues.size(); i++) {
            if (!isKey.get(i)) {
                continue;
            }
            if (!Objects.equals(oldValues.get(i), newValues.get(i))) {
                return true;
            }
        }
        return false;
    }

    private static String columnName(RelationMeta meta, int index) {
        List<String> names = meta.columnNames();
        return index < names.size() ? names.get(index) : "#" + index;
    }
}
```

Note: `List.copyOf` rejects null elements, and a null element is exactly how a SQL NULL is
represented here. Use `Collections.unmodifiableList(out)` instead — fix that line while
implementing rather than copying the sketch verbatim:

```java
        return Collections.unmodifiableList(out);
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=ToastResolverTest
```

Expected: 9 tests, all pass. If `nullsSurviveAsNullsAndEmptyStringsAsEmptyStrings` fails with
`NullPointerException`, the `List.copyOf` line was copied verbatim; apply the fix above.

- [ ] **Step 5: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/writer
git commit -m "$(printf 'feat(writer): TOAST resolution and key-change decomposition\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 8: `TextDecoder` and `RowBuilder`

**Files:**
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/TextDecoder.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/RowBuilder.java`
- Test: `java/writer/src/test/java/pro/surpin/pgpaimon/writer/TextDecoderTest.java`
- Test: `java/writer/src/test/java/pro/surpin/pgpaimon/writer/RowBuilderTest.java`

**Interfaces:**
- Consumes: `SinkRecord` from Task 5.
- Produces:
  - `TextDecoder.toPaimon(org.apache.paimon.types.DataType type, String text)` → `Object` suitable for `GenericRow`, or `null` for a SQL NULL
  - `RowBuilder(org.apache.paimon.types.RowType rowType, List<String> pgColumnOrder, String seqField, String opSeqField, String xidField)`
  - `RowBuilder.build(SinkRecord r)` → `org.apache.paimon.data.GenericRow` with `RowKind` set

`RowBuilder` maps by **name**, not by position: the Relation message's column order is the
live `attnum` order, while the Paimon schema also holds `_seq`, `_op_seq` and optionally
`_xid`, and a dropped column shifts positions on one side only.

- [ ] **Step 1: Write the failing tests**

`TextDecoderTest.java` covers every type in the Global Constraints mapping. The text forms
are PostgreSQL's own output, which is what `binary 'false'` delivers:

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.data.*;
import org.apache.paimon.types.*;
import org.junit.jupiter.api.Test;
import java.time.LocalDate;
import static org.junit.jupiter.api.Assertions.*;

class TextDecoderTest {

    @Test
    void nullTextBecomesNullForEveryType() {
        assertNull(TextDecoder.toPaimon(DataTypes.INT(), null));
        assertNull(TextDecoder.toPaimon(DataTypes.STRING(), null));
        assertNull(TextDecoder.toPaimon(DataTypes.TIMESTAMP(), null));
    }

    @Test
    void emptyStringIsAnEmptyStringNotNull() {
        Object v = TextDecoder.toPaimon(DataTypes.STRING(), "");
        assertEquals(BinaryString.fromString(""), v);
        assertNotNull(v);
    }

    @Test
    void booleans() {
        assertEquals(Boolean.TRUE,  TextDecoder.toPaimon(DataTypes.BOOLEAN(), "t"));
        assertEquals(Boolean.FALSE, TextDecoder.toPaimon(DataTypes.BOOLEAN(), "f"));
        assertEquals(Boolean.TRUE,  TextDecoder.toPaimon(DataTypes.BOOLEAN(), "true"));
    }

    @Test
    void integers() {
        assertEquals((short) -7, TextDecoder.toPaimon(DataTypes.SMALLINT(), "-7"));
        assertEquals(42, TextDecoder.toPaimon(DataTypes.INT(), "42"));
        assertEquals(9223372036854775807L,
                     TextDecoder.toPaimon(DataTypes.BIGINT(), "9223372036854775807"));
    }

    @Test
    void floatsRoundTripAtExtraFloatDigitsThree() {
        assertEquals(0.1f, (Float) TextDecoder.toPaimon(DataTypes.FLOAT(), "0.1"));
        assertEquals(1.2345678901234567e300,
                     (Double) TextDecoder.toPaimon(DataTypes.DOUBLE(), "1.2345678901234567e+300"));
        assertTrue(Double.isNaN((Double) TextDecoder.toPaimon(DataTypes.DOUBLE(), "NaN")));
        assertEquals(Double.POSITIVE_INFINITY,
                     TextDecoder.toPaimon(DataTypes.DOUBLE(), "Infinity"));
        assertEquals(Double.NEGATIVE_INFINITY,
                     TextDecoder.toPaimon(DataTypes.DOUBLE(), "-Infinity"));
    }

    @Test
    void decimalKeepsPrecisionAndScale() {
        Decimal d = (Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(10, 2), "123.45");
        assertEquals("123.45", d.toBigDecimal().toPlainString());
        assertEquals(10, d.precision());
        assertEquals(2, d.scale());
        Decimal z = (Decimal) TextDecoder.toPaimon(DataTypes.DECIMAL(10, 2), "0.1");
        assertEquals("0.10", z.toBigDecimal().toPlainString());
    }

    @Test
    void byteaHexBecomesBytes() {
        assertArrayEquals(new byte[] { 0x00, (byte) 0xff, 0x41 },
                          (byte[]) TextDecoder.toPaimon(DataTypes.BYTES(), "\\x00ff41"));
        assertArrayEquals(new byte[0], (byte[]) TextDecoder.toPaimon(DataTypes.BYTES(), "\\x"));
    }

    @Test
    void dateBecomesEpochDay() {
        assertEquals((int) LocalDate.of(2026, 10, 9).toEpochDay(),
                     TextDecoder.toPaimon(DataTypes.DATE(), "2026-10-09"));
    }

    @Test
    void timestampKeepsMicroseconds() {
        Timestamp ts = (Timestamp) TextDecoder.toPaimon(
            DataTypes.TIMESTAMP(6), "2026-10-09 13:46:40.123456");
        assertEquals("2026-10-09T13:46:40.123456", ts.toLocalDateTime().toString());
    }

    @Test
    void timestampWithoutFractionalPartWorks() {
        Timestamp ts = (Timestamp) TextDecoder.toPaimon(
            DataTypes.TIMESTAMP(6), "2026-10-09 13:46:40");
        assertEquals("2026-10-09T13:46:40", ts.toLocalDateTime().toString());
    }

    @Test
    void timestampWithLocalTimeZoneIsNormalisedToUtc() {
        Timestamp ts = (Timestamp) TextDecoder.toPaimon(
            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), "2026-10-09 13:46:40+02");
        assertEquals("2026-10-09T11:46:40", ts.toLocalDateTime().toString());
    }

    @Test
    void anUnparseableValueNamesTheTypeAndTheText() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> TextDecoder.toPaimon(DataTypes.INT(), "not-a-number"));
        assertTrue(e.getMessage().contains("INT"));
        assertTrue(e.getMessage().contains("not-a-number"));
    }
}
```

`RowBuilderTest.java`:

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RowBuilderTest {

    private static RowType paimonSchema() {
        return RowType.of(
            new DataType[] { DataTypes.INT().notNull(), DataTypes.STRING(),
                             DataTypes.BIGINT().notNull(), DataTypes.INT().notNull() },
            new String[]   { "id", "v", "_seq", "_op_seq" });
    }

    @Test
    void mapsPgColumnsByNameAndAppendsMetadata() {
        RowBuilder b = new RowBuilder(paimonSchema(), List.of("id", "v"),
                                      "_seq", "_op_seq", null);
        GenericRow row = b.build(new SinkRecord(16412, RowKind.UPDATE_AFTER,
                                 List.of("7", "hello"), 0x16B374D8L, 3, 4711L));
        assertEquals(4, row.getFieldCount());
        assertEquals(7, row.getInt(0));
        assertEquals("hello", row.getString(1).toString());
        assertEquals(0x16B374D8L, row.getLong(2));
        assertEquals(3, row.getInt(3));
        assertEquals(RowKind.UPDATE_AFTER, row.getRowKind());
    }

    @Test
    void pgColumnOrderDifferentFromPaimonOrderStillMapsCorrectly() {
        RowBuilder b = new RowBuilder(paimonSchema(), List.of("v", "id"),
                                      "_seq", "_op_seq", null);
        GenericRow row = b.build(new SinkRecord(16412, RowKind.INSERT,
                                 List.of("hello", "7"), 1L, 0, 1L));
        assertEquals(7, row.getInt(0));
        assertEquals("hello", row.getString(1).toString());
    }

    @Test
    void xidIsWrittenWhenTheSchemaHasIt() {
        RowType withXid = RowType.of(
            new DataType[] { DataTypes.INT().notNull(), DataTypes.BIGINT().notNull(),
                             DataTypes.INT().notNull(), DataTypes.BIGINT() },
            new String[]   { "id", "_seq", "_op_seq", "_xid" });
        RowBuilder b = new RowBuilder(withXid, List.of("id"), "_seq", "_op_seq", "_xid");
        GenericRow row = b.build(new SinkRecord(1, RowKind.INSERT, List.of("1"), 1L, 0, 4711L));
        assertEquals(4711L, row.getLong(3));
    }

    @Test
    void aPaimonColumnWithNoPgCounterpartIsNull() {
        RowBuilder b = new RowBuilder(paimonSchema(), List.of("id"),
                                      "_seq", "_op_seq", null);
        GenericRow row = b.build(new SinkRecord(1, RowKind.INSERT, List.of("1"), 1L, 0, 1L));
        assertTrue(row.isNullAt(1), "v was dropped in PG but is still in the Paimon schema");
    }

    @Test
    void aPgColumnWithNoPaimonCounterpartIsRejected() {
        RowBuilder b = new RowBuilder(paimonSchema(), List.of("id", "v", "surprise"),
                                      "_seq", "_op_seq", null);
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> b.build(new SinkRecord(1, RowKind.INSERT,
                          List.of("1", "x", "y"), 1L, 0, 1L)));
        assertTrue(e.getMessage().contains("surprise"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest='TextDecoderTest,RowBuilderTest'
```

Expected: FAIL — `cannot find symbol: class TextDecoder`.

- [ ] **Step 3: Implement `TextDecoder`**

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.data.*;
import org.apache.paimon.types.*;
import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/**
 * Converts PostgreSQL text output into Paimon internal values.
 *
 * The slot runs with binary 'false' because text output is round-trip exact for
 * every type in the mapping: extra_float_digits = 3 is the default since PG 12,
 * numeric is exact as text, and bytea is hex.
 */
public final class TextDecoder {

    private static final DateTimeFormatter TS = new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd HH:mm:ss")
        .appendFraction(java.time.temporal.ChronoField.MICRO_OF_SECOND, 0, 6, true)
        .toFormatter();

    private static final DateTimeFormatter TS_TZ = new DateTimeFormatterBuilder()
        .append(TS)
        .appendOffset("+HH:mm", "+00")
        .toFormatter();

    private TextDecoder() {
    }

    public static Object toPaimon(DataType type, String text) {
        if (text == null) {
            return null;
        }
        try {
            return convert(type, text);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "cannot parse " + type + " from \"" + text + "\"", e);
        }
    }

    private static Object convert(DataType type, String s) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return s.equals("t") || s.equals("true") || s.equals("1");
            case SMALLINT:
                return Short.parseShort(s);
            case INTEGER:
                return Integer.parseInt(s);
            case BIGINT:
                return Long.parseLong(s);
            case FLOAT:
                return Float.parseFloat(normaliseSpecial(s));
            case DOUBLE:
                return Double.parseDouble(normaliseSpecial(s));
            case DECIMAL: {
                DecimalType d = (DecimalType) type;
                return Decimal.fromBigDecimal(new BigDecimal(s), d.getPrecision(), d.getScale());
            }
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString(s);
            case BINARY:
            case VARBINARY:
                return fromHex(s);
            case DATE:
                return (int) LocalDate.parse(s).toEpochDay();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Timestamp.fromLocalDateTime(LocalDateTime.parse(s, TS));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.fromLocalDateTime(
                    OffsetDateTime.parse(s, TS_TZ)
                                  .withOffsetSameInstant(ZoneOffset.UTC)
                                  .toLocalDateTime());
            default:
                throw new UnsupportedOperationException("no text decoding for " + type);
        }
    }

    /** PostgreSQL prints these; Java's parsers spell them differently. */
    private static String normaliseSpecial(String s) {
        return switch (s) {
            case "NaN" -> "NaN";
            case "Infinity" -> "Infinity";
            case "-Infinity" -> "-Infinity";
            default -> s;
        };
    }

    /** bytea_output = hex is the default since PG 9.0: "\x" followed by pairs. */
    private static byte[] fromHex(String s) {
        String hex = s.startsWith("\\x") ? s.substring(2) : s;
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }
}
```

- [ ] **Step 4: Implement `RowBuilder`**

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import java.util.*;

/**
 * Builds a Paimon row from a SinkRecord by column name.
 *
 * Name mapping rather than position mapping: the pgoutput Relation message lists
 * live columns in attnum order, while the Paimon schema also carries _seq, _op_seq
 * and optionally _xid, and a DROP COLUMN shifts positions on one side only.
 */
public final class RowBuilder {

    private final RowType rowType;
    private final int[] paimonIndexForPgColumn;
    private final List<String> pgColumnOrder;
    private final int seqIndex;
    private final int opSeqIndex;
    private final int xidIndex;

    public RowBuilder(RowType rowType, List<String> pgColumnOrder,
                      String seqField, String opSeqField, String xidField) {
        this.rowType = rowType;
        this.pgColumnOrder = List.copyOf(pgColumnOrder);
        this.paimonIndexForPgColumn = new int[pgColumnOrder.size()];
        for (int i = 0; i < pgColumnOrder.size(); i++) {
            this.paimonIndexForPgColumn[i] = indexOf(rowType, pgColumnOrder.get(i));
        }
        this.seqIndex = indexOf(rowType, seqField);
        this.opSeqIndex = indexOf(rowType, opSeqField);
        this.xidIndex = xidField == null ? -1 : indexOf(rowType, xidField);
    }

    public GenericRow build(SinkRecord r) {
        GenericRow row = new GenericRow(r.kind(), rowType.getFieldCount());
        List<DataField> fields = rowType.getFields();

        for (int i = 0; i < paimonIndexForPgColumn.length; i++) {
            int target = paimonIndexForPgColumn[i];
            if (target < 0) {
                throw new IllegalStateException(
                    "PostgreSQL column '" + pgColumnOrder.get(i)
                    + "' has no counterpart in the Paimon schema " + rowType
                    + "; a DDL message was missed");
            }
            row.setField(target, TextDecoder.toPaimon(
                fields.get(target).type(), r.values().get(i)));
        }

        row.setField(seqIndex, r.seq());
        row.setField(opSeqIndex, r.opSeq());
        if (xidIndex >= 0) {
            row.setField(xidIndex, r.xid());
        }
        return row;
    }

    private static int indexOf(RowType rowType, String name) {
        List<String> names = rowType.getFieldNames();
        return names.indexOf(name);
    }
}
```

`GenericRow` has a constructor taking a `RowKind` and an arity; if the Paimon 1.4.1 API
instead requires `GenericRow.of(...)` plus `setRowKind`, use
`GenericRow row = new GenericRow(arity); row.setRowKind(r.kind());` — check
`org.apache.paimon.data.GenericRow` and use whichever exists. Both set the same state.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest='TextDecoderTest,RowBuilderTest'
```

Expected: 17 tests, all pass.

- [ ] **Step 6: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/writer
git commit -m "$(printf 'feat(writer): text-to-Paimon value decoding and name-keyed row building\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 9: `SchemaRegistry` and `DdlMessageHandler`

**Files:**
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/DdlMessage.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/SchemaRegistry.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/DdlMessageHandler.java`
- Test: `java/writer/src/test/java/pro/surpin/pgpaimon/writer/SchemaRegistryTest.java`

**Interfaces:**
- Consumes: `WriterConfig`, `RelationMeta`, `RowBuilder`.
- Produces:
  - `DdlMessage.parse(byte[] json)` → `DdlMessage(int version, String op, int relid, String schema, String table, List<String> pk, List<Col> cols, Col col, Integer id, String from, String to, String name, String type)` with `record Col(int id, String name, String type)`
  - `SchemaRegistry(org.apache.paimon.catalog.Catalog catalog, WriterConfig config)`
  - `void createTable(int relid, String schema, String table, List<String> pk, List<DdlMessage.Col> cols)` — idempotent
  - `void addColumn(int relid, DdlMessage.Col col)`, `void dropColumn(int relid, String name)`, `void renameColumn(int relid, String from, String to)`, `void alterColumn(int relid, String name, String type)`
  - `void dropTable(int relid, String schema, String table)`
  - `org.apache.paimon.table.FileStoreTable table(int relid)`
  - `RowBuilder rowBuilder(int relid, RelationMeta meta)` — cached per `(relid, column list)`
  - `void forget(int relid)` — drops cached state so the next access reloads
  - `boolean tracked(int relid)`
  - `DdlMessageHandler(SchemaRegistry registry, String expectedPrefix)` with `void handle(PgEvent.Message m)`

Paimon type strings arrive already formatted by `paimon.pg_type_to_paimon()`. Parse them with
Paimon's own parser — `org.apache.paimon.types.DataTypeJsonParser` for JSON, or
`org.apache.paimon.utils.TypeUtils.fromSqlString` / `DataTypes` lookup for SQL strings. Verify
which one exists in 1.4.1 during step 3 and use it; do not hand-roll a type-string parser.

`alter_col` can be a no-op — `varchar` and `text` both map to `STRING`, so
`ALTER COLUMN a TYPE varchar(64)` on a `text` column emits a message whose type equals the
stored one. The handler must compare and skip, not call `SchemaChange.updateColumnType` with
an identical type.

- [ ] **Step 1: Write the failing test**

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.catalog.*;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SchemaRegistryTest {

    @TempDir static Path warehouse;
    static Catalog catalog;
    static SchemaRegistry reg;
    static DdlMessageHandler handler;

    static final int RELID = 16412;

    @BeforeAll
    static void setUp() {
        Options o = new Options();
        o.set("warehouse", warehouse.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
        Map<String, String> env = new HashMap<>();
        env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost:5432/testdb");
        env.put("PGPAIMON_WAREHOUSE", warehouse.toString());
        reg = new SchemaRegistry(catalog, WriterConfig.fromEnv(env));
        handler = new DdlMessageHandler(reg, "paimon");
    }

    private static PgEvent.Message msg(String json) {
        return new PgEvent.Message(true, 1L, "paimon", json.getBytes(StandardCharsets.UTF_8));
    }

    @Test @Order(1)
    void createMakesAPrimaryKeyTableWithTheSpecOptions() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"create","relid":16412,"schema":"public","table":"orders",
             "pk":["id"],
             "cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                     {"id":2,"name":"amount","type":"DECIMAL(10, 2)"},
                     {"id":3,"name":"note","type":"STRING"}],
             "options":{"changelog-producer":"input"}}"""));

        FileStoreTable t = reg.table(RELID);
        assertEquals(List.of("id"), t.schema().primaryKeys());
        Map<String, String> opts = t.schema().options();
        assertEquals("deduplicate", opts.get("merge-engine"));
        assertEquals("input", opts.get("changelog-producer"));
        assertEquals("_seq,_op_seq", opts.get("sequence.field"));
        assertEquals("false", opts.get("ignore-update-before"));
        assertEquals(List.of(), t.schema().partitionKeys());

        List<String> names = t.schema().fieldNames();
        assertEquals(List.of("id", "amount", "note", "_seq", "_op_seq"), names);
    }

    @Test @Order(2)
    void createIsIdempotent() throws Exception {
        long before = reg.table(RELID).schema().id();
        handler.handle(msg("""
            {"v":1,"op":"create","relid":16412,"schema":"public","table":"orders",
             "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                     {"id":2,"name":"amount","type":"DECIMAL(10, 2)"},
                     {"id":3,"name":"note","type":"STRING"}],
             "options":{"changelog-producer":"input"}}"""));
        assertEquals(before, reg.table(RELID).schema().id());
    }

    @Test @Order(3)
    void addColumn() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"add_col","relid":16412,
             "col":{"id":4,"name":"qty","type":"BIGINT"}}"""));
        assertTrue(reg.table(RELID).schema().fieldNames().contains("qty"));
    }

    @Test @Order(4)
    void renameColumn() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"rename_col","relid":16412,"id":4,"from":"qty","to":"quantity"}"""));
        List<String> names = reg.table(RELID).schema().fieldNames();
        assertTrue(names.contains("quantity"));
        assertFalse(names.contains("qty"));
    }

    @Test @Order(5)
    void alterColumnType() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"alter_col","relid":16412,"id":2,
             "name":"amount","type":"DECIMAL(20, 2)"}"""));
        assertEquals("DECIMAL(20, 2)",
            reg.table(RELID).schema().fields().stream()
               .filter(f -> f.name().equals("amount")).findFirst()
               .orElseThrow().type().asSQLString());
    }

    @Test @Order(6)
    void alterColumnToTheSameTypeIsANoOp() throws Exception {
        long before = reg.table(RELID).schema().id();
        handler.handle(msg("""
            {"v":1,"op":"alter_col","relid":16412,"id":3,
             "name":"note","type":"STRING"}"""));
        assertEquals(before, reg.table(RELID).schema().id(),
                     "an identical type must not create a schema version");
    }

    /** gaps.md item 1: DROP COLUMN was never propagated on main. */
    @Test @Order(7)
    void dropColumnIsPropagated() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"drop_col","relid":16412,"name":"quantity"}"""));
        assertFalse(reg.table(RELID).schema().fieldNames().contains("quantity"));
    }

    /** gaps.md item 3: DROP TABLE left warehouse data behind on main. */
    @Test @Order(8)
    void dropTableRemovesTheWarehouseData() throws Exception {
        handler.handle(msg("""
            {"v":1,"op":"drop_table","relid":16412,
             "schema":"public","table":"orders"}"""));
        assertFalse(catalog.tableExists(Identifier.create("default", "orders")));
        assertFalse(reg.tracked(RELID));
    }

    @Test @Order(9)
    void aMessageWithAForeignPrefixIsIgnored() {
        assertDoesNotThrow(() -> handler.handle(new PgEvent.Message(
            true, 1L, "someone_else", "garbage".getBytes(StandardCharsets.UTF_8))));
    }

    @Test @Order(10)
    void anUnknownOpNamesItself() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> handler.handle(msg("{\"v\":1,\"op\":\"teleport\",\"relid\":1}")));
        assertTrue(e.getMessage().contains("teleport"));
    }

    @Test @Order(11)
    void aFutureWireVersionIsRefusedRatherThanMisread() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> handler.handle(msg("{\"v\":99,\"op\":\"create\",\"relid\":1}")));
        assertTrue(e.getMessage().contains("99"));
    }

    /** Spec section 8: DDL for an unregistered table is ignored with a warning. */
    @Test @Order(12)
    void ddlForAnUnregisteredTableIsIgnoredNotFatal() {
        assertDoesNotThrow(() -> handler.handle(msg("""
            {"v":1,"op":"add_col","relid":999999,
             "col":{"id":1,"name":"ghost","type":"INT"}}""")));
        assertDoesNotThrow(() -> handler.handle(msg("""
            {"v":1,"op":"drop_table","relid":999999,
             "schema":"public","table":"ghost"}""")));
        assertFalse(reg.tracked(999999));
    }
}
```

Add `@TestMethodOrder(MethodOrderer.OrderAnnotation.class)` to the class — the tests
deliberately build on one another, because schema evolution is inherently ordered.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=SchemaRegistryTest
```

Expected: FAIL — `cannot find symbol: class SchemaRegistry`.

- [ ] **Step 3: Implement**

`DdlMessage` parses with a dependency-free JSON reader. Paimon bundles Jackson under
`org.apache.paimon.shade.jackson2.com.fasterxml.jackson`; use it rather than adding a
dependency:

```java
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
```

`SchemaRegistry.createTable` builds the Paimon schema: every `Col` in order, then `_seq`
`BIGINT NOT NULL`, then `_op_seq` `INT NOT NULL`, then `_xid` `BIGINT` when
`config.xidField()` is non-null. Options exactly as the Global Constraints list. Use
`Schema.newBuilder()`, `catalog.createDatabase(config.database(), true)` and
`catalog.createTable(identifier, schema, /* ignoreIfExists */ true)` — the `true` is what
makes `create` idempotent, which matters because the writer re-reads
`paimon.table_registry` at startup and re-issues `create` for every row.

Schema evolution uses `org.apache.paimon.schema.SchemaChange`:

| op | call |
|---|---|
| `add_col` | `SchemaChange.addColumn(name, type)` |
| `drop_col` | `SchemaChange.dropColumn(name)` |
| `rename_col` | `SchemaChange.renameColumn(from, to)` |
| `alter_col` | `SchemaChange.updateColumnType(name, type)`, skipped when the stored type already equals the new one |

applied through `catalog.alterTable(identifier, List.of(change), false)`. `dropTable` calls
`catalog.dropTable(identifier, true)` — the second argument purges the warehouse data, which
is `gaps.md` item 3.

Field ids are **not** settable through `SchemaChange.addColumn`; Paimon assigns them from
`highestFieldId`. The `id` in the DDL message is therefore advisory: it is the PostgreSQL
`attnum`, kept in the message so the extension's diff and any future field-id-aware path have
it, and so a reader can correlate the two schemas. Record that in a comment on
`createTable` — it is a deliberate divergence from `main`, which writes `attnum` directly as
the Paimon field id at `legacy:extension/paimon_schema.cpp:131`. Column **names** are the contract
here, which is why `RowBuilder` maps by name.

`rowBuilder(relid, meta)` caches on `meta.columnNames()` so that a `Relation` message with a
changed column list invalidates the builder.

`DdlMessageHandler.handle` returns immediately when `m.prefix()` is not the configured prefix,
raises when `version != 1`, and dispatches on `op` with a `default` that names the op.

Every op except `create` logs a `WARNING` and returns when `registry.tracked(relid)` is false.
A writer that started after a table was dropped, or one pointed at a database whose publication
contains tables it has no Paimon schema for, must not die on the message — spec §8 requires it
to be ignored with a warning.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=SchemaRegistryTest
```

Expected: 12 tests, all pass.

- [ ] **Step 5: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/writer
git commit -m "$(printf 'feat(writer): Paimon schema registry and DDL message handling\n\nResolves gaps.md items 1 and 3: DROP COLUMN reaches the Paimon schema and\nDROP TABLE purges the warehouse data.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 10: `TxnBatcher`

**Files:**
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/TxnBatcher.java`
- Test: `java/writer/src/test/java/pro/surpin/pgpaimon/writer/TxnBatcherTest.java`

**Interfaces:**
- Consumes: `PgEvent`, `PartialRecord`, `SinkRecord`, `ToastResolver`, `SchemaRegistry`.
- Produces:
  - `interface Flusher { void flush(List<SinkRecord> records, long commitLsn, long endLsn) throws Exception; }`

`commitLsn` and `endLsn` are both needed and are not the same number. `commitLsn` is the
transaction's commit LSN and becomes Paimon's `commitIdentifier`. `endLsn` is the position
*after* the commit record and is what gets confirmed to the server, because that is where a
restart must resume.
  - `TxnBatcher(ToastResolver resolver, SchemaRegistry registry, Flusher flusher)`
  - `void onEvent(PgEvent event, long changeLsn, RelationMeta meta) throws Exception`
  - `void discardOpenTransaction()`
  - `boolean hasOpenTransaction()`

Rules, from spec §7:

- `_seq` is the **per-change LSN** passed in as `changeLsn`, not the commit LSN.
- `_op_seq` is an ordinal incremented once per **emitted record** within the transaction, so
  a `-U` always precedes its `+U`.
- No collapsing, no reordering, no deduplication inside a batch. Correctness rests entirely
  on LSN order surviving.
- On `Commit`, flush the whole transaction and nothing less.
- Review Focus 3: a transaction that produces no records still has to be flushed, with an
  empty list, so the consumer can advance the LSN. A DDL-only transaction is the common case.

- [ ] **Step 1: Write the failing test**

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class TxnBatcherTest {

    static final RelationMeta META = new RelationMeta(
        16412, "public", "t", List.of("id", "v"), List.of(true, false));

    static final class Recorder implements TxnBatcher.Flusher {
        final List<List<SinkRecord>> batches = new ArrayList<>();
        final List<Long> commitLsns = new ArrayList<>();
        final List<Long> endLsns = new ArrayList<>();
        @Override public void flush(List<SinkRecord> records, long commitLsn, long endLsn) {
            batches.add(List.copyOf(records));
            commitLsns.add(commitLsn);
            endLsns.add(endLsn);
        }
    }

    private static TupleData row(String id, String v) {
        return new TupleData(List.of(ColumnValue.of(id), ColumnValue.of(v)));
    }

    @Test
    void assignsPerChangeLsnAndAMonotoneOpSeq() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);

        b.onEvent(new PgEvent.Begin(500L, 0L, 4711L), 400L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 410L, META);
        b.onEvent(new PgEvent.Update(16412, row("1", "a"), false, row("1", "b")), 420L, META);
        b.onEvent(new PgEvent.Commit(500L, 510L, 0L), 500L, null);

        assertEquals(1, rec.batches.size());
        List<SinkRecord> batch = rec.batches.get(0);
        assertEquals(3, batch.size());

        assertEquals(RowKind.INSERT, batch.get(0).kind());
        assertEquals(410L, batch.get(0).seq());
        assertEquals(0, batch.get(0).opSeq());

        assertEquals(RowKind.UPDATE_BEFORE, batch.get(1).kind());
        assertEquals(420L, batch.get(1).seq());
        assertEquals(1, batch.get(1).opSeq());

        assertEquals(RowKind.UPDATE_AFTER, batch.get(2).kind());
        assertEquals(420L, batch.get(2).seq());
        assertEquals(2, batch.get(2).opSeq(),
                     "-U must sort before its +U, which is what _op_seq is for");

        assertEquals(500L, rec.commitLsns.get(0), "the commit identifier is the commit LSN");
        assertEquals(510L, rec.endLsns.get(0), "the confirmed position is after the commit");
        assertEquals(4711L, batch.get(0).xid());
    }

    @Test
    void opSeqRestartsEachTransaction() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        for (int i = 0; i < 2; i++) {
            b.onEvent(new PgEvent.Begin(100L * i, 0L, i), 100L * i, null);
            b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 100L * i + 1, META);
            b.onEvent(new PgEvent.Commit(100L * i, 0L, 0L), 100L * i + 2, null);
        }
        assertEquals(0, rec.batches.get(0).get(0).opSeq());
        assertEquals(0, rec.batches.get(1).get(0).opSeq());
    }

    /** Review Focus 3: a DDL-only transaction must still flush, or the LSN never advances. */
    @Test
    void aTransactionWithNoRecordsStillFlushes() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(600L, 0L, 1L), 600L, null);
        b.onEvent(new PgEvent.Message(true, 601L, "paimon",
                  "{\"v\":1,\"op\":\"add_col\"}".getBytes()), 601L, null);
        b.onEvent(new PgEvent.Commit(600L, 610L, 0L), 610L, null);
        assertEquals(1, rec.batches.size(), "the empty transaction must still be flushed");
        assertTrue(rec.batches.get(0).isEmpty());
        assertEquals(600L, rec.commitLsns.get(0));
        assertEquals(610L, rec.endLsns.get(0),
                     "the LSN must still advance or this transaction replays forever");
    }

    @Test
    void keyMovesInterleaveWithoutBeingReordered() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(700L, 0L, 1L), 700L, null);
        // UPDATE t SET id = id + 1 over keys 1 and 2, in server row order
        b.onEvent(new PgEvent.Update(16412, row("1", "a"), false, row("2", "a")), 710L, META);
        b.onEvent(new PgEvent.Update(16412, row("2", "b"), false, row("3", "b")), 720L, META);
        b.onEvent(new PgEvent.Commit(700L, 730L, 0L), 730L, null);

        List<SinkRecord> batch = rec.batches.get(0);
        assertEquals(4, batch.size());
        assertEquals(List.of(RowKind.DELETE, RowKind.INSERT, RowKind.DELETE, RowKind.INSERT),
                     batch.stream().map(SinkRecord::kind).toList());
        assertEquals(List.of(0, 1, 2, 3), batch.stream().map(SinkRecord::opSeq).toList());
    }

    @Test
    void discardThrowsAwayAnOpenTransaction() throws Exception {
        Recorder rec = new Recorder();
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec);
        b.onEvent(new PgEvent.Begin(800L, 0L, 1L), 800L, null);
        b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 810L, META);
        assertTrue(b.hasOpenTransaction());
        b.discardOpenTransaction();
        assertFalse(b.hasOpenTransaction());
        assertTrue(rec.batches.isEmpty());
    }

    @Test
    void eventsOutsideATransactionAreRejected() {
        TxnBatcher b = new TxnBatcher(new ToastResolver(), null, new Recorder());
        assertThrows(IllegalStateException.class,
            () -> b.onEvent(new PgEvent.Insert(16412, row("1", "a")), 1L, META));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=TxnBatcherTest
```

Expected: FAIL — `cannot find symbol: class TxnBatcher`.

- [ ] **Step 3: Implement**

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.*;

/**
 * Assembles complete transactions.
 *
 * Never collapses, reorders or deduplicates within a batch: correctness rests on
 * LSN order surviving intact, which is what lets an interleaved
 * "UPDATE t SET id = id + 1" converge.  _op_seq is assigned per emitted record,
 * not per change, so a -U always sorts before the +U it precedes.
 */
public final class TxnBatcher {

    public interface Flusher {
        void flush(List<SinkRecord> records, long commitLsn) throws Exception;
    }

    private final ToastResolver resolver;
    private final SchemaRegistry registry;
    private final Flusher flusher;

    private List<SinkRecord> open;
    private long xid;
    private int opSeq;

    public TxnBatcher(ToastResolver resolver, SchemaRegistry registry, Flusher flusher) {
        this.resolver = resolver;
        this.registry = registry;
        this.flusher = flusher;
    }

    public boolean hasOpenTransaction() {
        return open != null;
    }

    public void discardOpenTransaction() {
        open = null;
        opSeq = 0;
    }

    public void onEvent(PgEvent event, long changeLsn, RelationMeta meta) throws Exception {
        if (event instanceof PgEvent.Begin b) {
            open = new ArrayList<>();
            xid = b.xid();
            opSeq = 0;
            return;
        }
        if (event instanceof PgEvent.Commit c) {
            require();
            List<SinkRecord> batch = open;
            discardOpenTransaction();
            flusher.flush(batch, c.commitLsn(), c.endLsn());
            return;
        }
        if (event instanceof PgEvent.Relation || event instanceof PgEvent.TypeMsg
            || event instanceof PgEvent.Origin || event instanceof PgEvent.Message) {
            return;    // handled elsewhere; contributes no records
        }

        require();
        if (registry != null && !registry.tracked(relidOf(event))) {
            return;
        }
        for (PartialRecord p : resolver.resolve(event, meta)) {
            open.add(new SinkRecord(p.relid(), p.kind(), p.values(),
                                    changeLsn, opSeq++, xid));
        }
    }

    private void require() {
        if (open == null) {
            throw new IllegalStateException(
                "change event arrived outside a transaction; the stream began mid-transaction");
        }
    }

    private static int relidOf(PgEvent e) {
        if (e instanceof PgEvent.Insert i) return i.relid();
        if (e instanceof PgEvent.Update u) return u.relid();
        if (e instanceof PgEvent.Delete d) return d.relid();
        return -1;
    }
}
```

`PgEvent.Truncate` carries several relids and is not a row change; `TxnBatcher` passes it
through to the flusher as a marker. Add a `TruncateMarker` record to `SinkRecord`'s place in
the batch, or — simpler and what this plan chooses — handle `Truncate` in `Main`'s event loop
before `TxnBatcher` sees it, calling `PaimonSink.truncate(relid)` for each relid. Note that
choice in a comment in `onEvent`'s `Truncate` branch, and make `onEvent` ignore `Truncate`.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=TxnBatcherTest
```

Expected: 6 tests, all pass.

- [ ] **Step 5: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/writer
git commit -m "$(printf 'feat(writer): transaction batching with per-change LSN sequencing\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 11: `PaimonSink` and `OffsetStore`

**Files:**
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/PaimonSink.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/OffsetStore.java`
- Test: `java/writer/src/test/java/pro/surpin/pgpaimon/writer/PaimonSinkTest.java`

**Interfaces:**
- Consumes: `SchemaRegistry`, `RowBuilder`, `SinkRecord`, `WriterConfig`.
- Produces:
  - `PaimonSink(SchemaRegistry registry, WriterConfig config)` implementing `TxnBatcher.Flusher`
  - `void flush(List<SinkRecord> records, long commitLsn, long endLsn)` — groups by relid, writes, commits every touched table with `commitIdentifier = commitLsn`; `endLsn` is ignored here and used by `Main` to confirm the slot position
  - `void truncate(int relid)` — empty overwrite commit
  - `void setRelation(int relid, RelationMeta meta)` — supplies the column order `RowBuilder` needs
  - `void close()`
  - `OffsetStore(SchemaRegistry registry, WriterConfig config)` with `long highestCommittedLsn(Collection<Integer> relids)`, returning `0` when nothing has been committed

Effectively-once, from spec §7: commit with `commitIdentifier = commitLsn` under the fixed
`config.commitUser()`. Paimon discards a commit whose identifier it has already seen, so a
replayed transaction is idempotent. `OffsetStore` reads the highest committed identifier back
from the Paimon snapshots for that `commitUser` and the writer resumes the slot from it.

Use the exact behaviour Task 1 step 4 recorded. If the spike found that a duplicate identifier
throws rather than no-ops, catch that specific exception in `flush` and treat it as success,
and say so in a comment citing the spike note.

- [ ] **Step 1: Write the failing test**

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.catalog.*;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.*;
import org.apache.paimon.table.source.*;
import org.apache.paimon.types.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path as NioPath;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PaimonSinkTest {

    @TempDir static NioPath wh;
    static Catalog catalog;
    static SchemaRegistry reg;
    static PaimonSink sink;
    static WriterConfig cfg;

    static final int RELID = 16500;
    static final RelationMeta META = new RelationMeta(
        RELID, "public", "s", List.of("id", "v"), List.of(true, false));

    @BeforeAll
    static void setUp() throws Exception {
        Options o = new Options();
        o.set("warehouse", wh.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
        Map<String, String> env = new HashMap<>();
        env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost/db");
        env.put("PGPAIMON_WAREHOUSE", wh.toString());
        cfg = WriterConfig.fromEnv(env);
        reg = new SchemaRegistry(catalog, cfg);
        new DdlMessageHandler(reg, "paimon").handle(new PgEvent.Message(true, 1L, "paimon",
            ("""
             {"v":1,"op":"create","relid":16500,"schema":"public","table":"s",
              "pk":["id"],"cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                                  {"id":2,"name":"v","type":"STRING"}],
              "options":{"changelog-producer":"input"}}""").getBytes(StandardCharsets.UTF_8)));
        sink = new PaimonSink(reg, cfg);
        sink.setRelation(RELID, META);
    }

    @AfterAll
    static void tearDown() throws Exception {
        sink.close();
    }

    private static SinkRecord rec(RowKind k, String id, String v, long seq, int op) {
        return new SinkRecord(RELID, k, Arrays.asList(id, v), seq, op, 4711L);
    }

    @Test @Order(1)
    void writesEveryRecordIncludingUpdateBefore() throws Exception {
        sink.flush(List.of(
            rec(RowKind.INSERT, "1", "a", 100L, 0),
            rec(RowKind.UPDATE_BEFORE, "1", "a", 200L, 1),
            rec(RowKind.UPDATE_AFTER,  "1", "b", 200L, 2)), 200L, 210L);

        Map<Integer, Long> kinds = rawKinds("s");
        assertEquals(1L, kinds.getOrDefault(0, 0L), "+I");
        assertEquals(1L, kinds.getOrDefault(1, 0L), "-U must reach the data files");
        assertEquals(1L, kinds.getOrDefault(2, 0L), "+U");
    }

    @Test @Order(2)
    void theMergedReadKeepsTheUpdateAfterValue() throws Exception {
        FileStoreTable t = reg.table(RELID);
        List<String> values = new ArrayList<>();
        ReadBuilder rb = t.newReadBuilder();
        try (RecordReader<InternalRow> r =
                 rb.newRead().createReader(rb.newScan().plan().splits())) {
            r.forEachRemaining(row -> values.add(row.getString(1).toString()));
        }
        assertEquals(List.of("b"), values,
                     "sequence.field = _seq,_op_seq must make +U win over -U");
    }

    @Test @Order(3)
    void replayingTheSameCommitIdentifierDoesNotDuplicate() throws Exception {
        long before = reg.table(RELID).snapshotManager().latestSnapshotId();
        sink.flush(List.of(rec(RowKind.INSERT, "9", "z", 300L, 0)), 300L, 310L);
        long after = reg.table(RELID).snapshotManager().latestSnapshotId();
        sink.flush(List.of(rec(RowKind.INSERT, "9", "z", 300L, 0)), 300L, 310L);
        assertEquals(after, reg.table(RELID).snapshotManager().latestSnapshotId(),
                     "a replayed commitIdentifier must be a no-op");
        assertTrue(after > before);
    }

    @Test @Order(4)
    void offsetStoreReportsTheHighestCommittedIdentifier() throws Exception {
        OffsetStore store = new OffsetStore(reg, cfg);
        assertEquals(300L, store.highestCommittedLsn(List.of(RELID)));
    }

    @Test @Order(5)
    void offsetStoreReturnsZeroForAnUncommittedTable() throws Exception {
        OffsetStore store = new OffsetStore(reg, cfg);
        assertEquals(0L, store.highestCommittedLsn(List.of(999999)));
    }

    @Test @Order(6)
    void anEmptyFlushCommitsNothingAndThrowsNothing() throws Exception {
        long before = reg.table(RELID).snapshotManager().latestSnapshotId();
        assertDoesNotThrow(() -> sink.flush(List.of(), 400L, 410L));
        assertEquals(before, reg.table(RELID).snapshotManager().latestSnapshotId());
    }

    @Test @Order(7)
    void truncateEmptiesTheTable() throws Exception {
        sink.truncate(RELID);
        FileStoreTable t = reg.table(RELID);
        ReadBuilder rb = t.newReadBuilder();
        int[] count = { 0 };
        try (RecordReader<InternalRow> r =
                 rb.newRead().createReader(rb.newScan().plan().splits())) {
            r.forEachRemaining(row -> count[0]++);
        }
        assertEquals(0, count[0]);
    }

    /** The same raw read the acceptance suite performs; see PaimonFeatureTest:308. */
    private static Map<Integer, Long> rawKinds(String table) throws Exception {
        LocalFileIO io = LocalFileIO.create();
        Path tablePath = new Path(wh.toString() + "/default.db/" + table);
        TableSchema stored = new SchemaManager(io, tablePath).latest().orElseThrow();
        Map<String, String> opts = new HashMap<>(stored.options());
        opts.put("bucket", "-1");
        opts.remove("merge-engine");
        opts.remove("changelog-producer");
        opts.remove("sequence.field");
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(-1, "_row_kind", DataTypes.TINYINT().notNull()));
        fields.addAll(stored.fields());
        TableSchema schema = new TableSchema(stored.id(), fields, stored.highestFieldId(),
            stored.partitionKeys(), stored.primaryKeys(), opts, stored.comment());
        FileStoreTable t = FileStoreTableFactory.create(io, tablePath, schema);
        ReadBuilder rb = t.newReadBuilder();
        Map<Integer, Long> kinds = new HashMap<>();
        try (RecordReader<InternalRow> r =
                 rb.newRead().createReader(rb.newScan().plan().splits())) {
            r.forEachRemaining(row -> {
                if (!row.isNullAt(0)) {
                    kinds.merge((int) row.getByte(0), 1L, Long::sum);
                }
            });
        }
        return kinds;
    }
}
```

`import java.nio.file.Path as NioPath;` is not Java syntax — Java has no import aliasing. Use
the fully qualified `java.nio.file.Path` for the `@TempDir` field and import only
`org.apache.paimon.fs.Path`. Fix that while implementing.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=PaimonSinkTest
```

Expected: FAIL — `cannot find symbol: class PaimonSink`.

- [ ] **Step 3: Implement `PaimonSink`**

Structure:

- One `TableWriteImpl` and one `TableCommitImpl` per relid, created lazily through
  `table.newWrite(config.commitUser())` and `table.newCommit(config.commitUser())`, cached in
  a `Map<Integer, Entry>` and closed in `close()`.
- `flush(records, commitLsn, endLsn)`: group the records by relid **preserving order**, build each row
  with the `RowBuilder` from `registry.rowBuilder(relid, meta)`, `write()` each, then for every
  touched relid call `commit.commit(commitLsn, write.prepareCommit(false, commitLsn))`.
- An empty `records` list touches no relid, so it commits nothing and returns — that is what
  lets `Main` advance the LSN for a DDL-only transaction without creating an empty snapshot.
- `truncate(relid)`: use `table.newBatchWriteBuilder().withOverwrite()` to commit an empty
  overwrite, mirroring `main`'s `PaimonTableWriter::truncate()`
  (`legacy:extension/paimon_table_writer.cpp:302`). Invalidate the cached write/commit for that
  relid afterwards.
- A commit whose identifier Paimon has already seen must not fail the writer. Implement
  exactly what Task 1 step 4 recorded; if it throws, catch that type and log at `INFO` with the
  identifier.

`OffsetStore.highestCommittedLsn(relids)` iterates the relids, reads each table's
`snapshotManager().latestSnapshot()`, and takes the maximum `commitIdentifier` among snapshots
whose `commitUser` equals `config.commitUser()`. A table with no snapshot contributes nothing;
an empty result is `0`.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=PaimonSinkTest
```

Expected: 7 tests, all pass. `theMergedReadKeepsTheUpdateAfterValue` is the one that proves
spec §7's sequencing; if it fails, stop and re-read Task 1 step 6's findings.

- [ ] **Step 5: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/writer
git commit -m "$(printf 'feat(writer): Paimon sink with commit-identifier dedup and offset recovery\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 12: `SlotConsumer` and `Main`

**Files:**
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/SlotConsumer.java`
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/Main.java`
- Test: `java/writer/src/test/java/pro/surpin/pgpaimon/writer/SlotConsumerTest.java`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `interface SlotConsumer.Handler { void onEvent(PgEvent event, long changeLsn) throws Exception; void onDisconnect(); }`
  - `SlotConsumer(WriterConfig config, PgOutputDecoder decoder, Handler handler)`
  - `void run(long startLsn)` — loops until interrupted, reconnecting with backoff; `startLsn == 0` means resume from the slot's own confirmed position
  - `void stop()`
  - `void confirm(long lsn)` — sets flushed and applied LSN
  - `static long parseLsn(String text)` / `static String formatLsn(long lsn)` — the `X/Y` form
  - `Main.main(String[] args)`

Slot options are the Global Constraints list, verbatim. `confirm` is called **only** after the
Paimon commit returns, which is what makes crash recovery a property of the code.

Review Focus 4: on disconnect, a half-buffered transaction must be discarded. `onDisconnect()`
calls `TxnBatcher.discardOpenTransaction()`. Flushing it would commit a transaction PostgreSQL
may yet abort.

- [ ] **Step 1: Write the failing test**

Only the parts that do not need a server are unit-testable here; the rest is covered by the
acceptance suite in Task 15.

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SlotConsumerTest {

    @Test
    void lsnTextFormRoundTrips() {
        assertEquals(0x16B374D8L, SlotConsumer.parseLsn("0/16B374D8"));
        assertEquals(0x100000000L, SlotConsumer.parseLsn("1/0"));
        assertEquals("0/16B374D8", SlotConsumer.formatLsn(0x16B374D8L));
        assertEquals("1/00000000", SlotConsumer.formatLsn(0x100000000L));
    }

    @Test
    void zeroLsnMeansStartFromTheSlotsOwnPosition() {
        assertEquals("0/00000000", SlotConsumer.formatLsn(0L));
    }

    /** Review Focus 4: a disconnect mid-transaction discards, never flushes. */
    @Test
    void disconnectDiscardsAnOpenTransaction() throws Exception {
        List<List<SinkRecord>> flushed = new ArrayList<>();
        TxnBatcher batcher = new TxnBatcher(new ToastResolver(), null,
            (records, commitLsn, endLsn) -> flushed.add(List.copyOf(records)));

        RelationMeta meta = new RelationMeta(1, "public", "t",
            List.of("id"), List.of(true));
        batcher.onEvent(new PgEvent.Begin(10L, 0L, 1L), 10L, null);
        batcher.onEvent(new PgEvent.Insert(1,
            new TupleData(List.of(ColumnValue.of("1")))), 11L, meta);

        assertTrue(batcher.hasOpenTransaction());
        batcher.discardOpenTransaction();          // what onDisconnect() does
        assertFalse(batcher.hasOpenTransaction());
        assertTrue(flushed.isEmpty(),
                   "a transaction PostgreSQL may still abort must never be committed");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=SlotConsumerTest
```

Expected: FAIL — `cannot find symbol: class SlotConsumer`.

- [ ] **Step 3: Implement `SlotConsumer`**

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.logging.Logger;

/** Owns the replication stream: connect, read, confirm, reconnect. */
public final class SlotConsumer {

    public interface Handler {
        void onEvent(PgEvent event, long changeLsn) throws Exception;
        void onDisconnect();
    }

    private static final Logger LOG = Logger.getLogger(SlotConsumer.class.getName());

    private final WriterConfig config;
    private final PgOutputDecoder decoder;
    private final Handler handler;
    private volatile PGReplicationStream stream;
    private volatile boolean running = true;

    public SlotConsumer(WriterConfig config, PgOutputDecoder decoder, Handler handler) {
        this.config = config;
        this.decoder = decoder;
        this.handler = handler;
    }

    public void stop() {
        running = false;
    }

    public void confirm(long lsn) {
        PGReplicationStream s = stream;
        if (s != null) {
            LogSequenceNumber n = LogSequenceNumber.valueOf(lsn);
            s.setFlushedLSN(n);
            s.setAppliedLSN(n);
        }
    }

    public void run(long startLsn) throws Exception {
        long backoffMs = 500;
        while (running) {
            try (Connection conn = connect()) {
                stream = open(conn, startLsn);
                backoffMs = 500;
                while (running) {
                    ByteBuffer buf = stream.readPending();
                    if (buf == null) {
                        stream.forceUpdateStatus();
                        Thread.sleep(10);
                        continue;
                    }
                    long changeLsn = stream.getLastReceiveLSN().asLong();
                    handler.onEvent(decoder.decode(buf), changeLsn);
                }
            } catch (Exception e) {
                if (!running) {
                    return;
                }
                LOG.warning("replication stream lost: " + e
                            + "; discarding any open transaction and reconnecting in "
                            + backoffMs + "ms");
                handler.onDisconnect();
                stream = null;
                Thread.sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 30_000);
            }
        }
    }

    private Connection connect() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", config.user());
        p.setProperty("password", config.password());
        p.setProperty("replication", "database");
        p.setProperty("preferQueryMode", "simple");
        p.setProperty("assumeMinServerVersion", "18");
        return DriverManager.getConnection(config.jdbcUrl(), p);
    }

    private PGReplicationStream open(Connection conn, long startLsn) throws Exception {
        var b = conn.unwrap(PGConnection.class).getReplicationAPI().replicationStream()
                    .logical()
                    .withSlotName(config.slotName())
                    .withSlotOption("proto_version", "2")
                    .withSlotOption("publication_names", config.publicationName())
                    .withSlotOption("messages", "true")
                    .withSlotOption("binary", "false")
                    .withSlotOption("streaming", "off")
                    .withStatusInterval(10_000, java.util.concurrent.TimeUnit.MILLISECONDS);
        if (startLsn > 0) {
            b = b.withStartPosition(LogSequenceNumber.valueOf(startLsn));
        }
        return b.start();
    }

    public static long parseLsn(String text) {
        int slash = text.indexOf('/');
        long hi = Long.parseUnsignedLong(text.substring(0, slash), 16);
        long lo = Long.parseUnsignedLong(text.substring(slash + 1), 16);
        return (hi << 32) | lo;
    }

    public static String formatLsn(long lsn) {
        return String.format("%X/%08X", lsn >>> 32, lsn & 0xFFFFFFFFL);
    }
}
```

- [ ] **Step 4: Implement `Main`**

Startup order matters and is the part most easily got wrong:

1. Build `WriterConfig.fromEnv(System.getenv())`.
2. Open a plain JDBC connection. Create the slot if absent:
   `SELECT pg_create_logical_replication_slot(?, 'pgoutput')`. Record whether it was created
   now — that decides whether a backfill is due.
3. Read `paimon.table_registry` over that plain connection and, for each row, call
   `registry.createTable(...)` directly from `pk_cols` and `cols`. **This is why `create` is
   idempotent:** a table added before the writer ever ran emitted its `create` message into a
   slot that did not exist, so the message is gone. The registry, not the message, is the
   source of truth at startup; messages carry only subsequent changes.
4. `long resume = offsetStore.highestCommittedLsn(registryRelids)`.
5. Run `Backfiller` for every registry row with `backfill_due = true` (Task 13).
6. Construct the pipeline and `slotConsumer.run(resume)`.

The handler wires the pieces:

```java
Map<Integer, RelationMeta> relations = new HashMap<>();

SlotConsumer.Handler handler = new SlotConsumer.Handler() {
    @Override public void onEvent(PgEvent event, long changeLsn) throws Exception {
        if (event instanceof PgEvent.Relation r) {
            sink.setRelation(r.meta().relid(), r.meta());
            relations.put(r.meta().relid(), r.meta());
        } else if (event instanceof PgEvent.Message m) {
            ddlHandler.handle(m);
        } else if (event instanceof PgEvent.Truncate t) {
            for (int relid : t.relids()) {
                if (registry.tracked(relid)) {
                    sink.truncate(relid);
                }
            }
            return;
        }
        batcher.onEvent(event, changeLsn, relations.get(relidOf(event)));
    }
    @Override public void onDisconnect() {
        batcher.discardOpenTransaction();
    }
};
```

`relidOf(PgEvent)` is the same three-branch helper `TxnBatcher` has privately; put one
`static int relidOf(PgEvent e)` in `Main` returning the relid of an `Insert`, `Update` or
`Delete` and `-1` otherwise, . `relations` and `relidOf` must both exist before the handler is constructed.

and the flusher confirms the LSN only after the commit returns:

```java
TxnBatcher.Flusher flusher = (records, commitLsn, endLsn) -> {
    sink.flush(records, commitLsn, endLsn);
    consumer.confirm(endLsn);      // only after the Paimon commit returned
};
```

Register a shutdown hook that calls `consumer.stop()` and `sink.close()`.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test
```

Expected: every test from Tasks 5-12 passes, 3 of them new.

- [ ] **Step 6: Build the shaded jar**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q -DskipTests package
ls -la target/paimon-writer.jar
java -jar target/paimon-writer.jar 2>&1 | head -3
```

Expected: the jar exists and the run fails with `PGPAIMON_JDBC_URL is required` — which proves
the manifest main class and the shaded service files both resolved.

- [ ] **Step 7: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/writer
git commit -m "$(printf 'feat(writer): replication stream consumer and process entry point\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 13: `Backfiller`

**Files:**
- Create: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/Backfiller.java`
- Modify: `java/writer/src/main/java/pro/surpin/pgpaimon/writer/Main.java`
- Modify: `sql/paimon_slot/paimon_slot--1.0.sql` (add `paimon.mark_backfilled`)
- Test: `java/writer/src/test/java/pro/surpin/pgpaimon/writer/BackfillerTest.java`

**Interfaces:**
- Consumes: `PaimonSink`, `SchemaRegistry`, `WriterConfig`.
- Produces:
  - `Backfiller(WriterConfig config, SchemaRegistry registry, PaimonSink sink)`
  - `BackfillPlan createSlotAndExportSnapshot()` → `record BackfillPlan(String snapshotName, long consistentPoint)`, using the replication protocol's `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`
  - `void backfill(BackfillPlan plan, int relid, String schema, String table, List<String> columns)`
  - `paimon.mark_backfilled(rel regclass)` sets `backfill_due = false`

The sequence is spec §6's, and it is the reason this architecture can do an initial load at
all — `main`'s `ALTER TABLE ... SET ACCESS METHOD paimon_heap` rewrites through the inherited
heapam `relation_copy_for_cluster` and emits nothing.

Every backfilled row is `+I` with `_seq = consistentPoint` and `_op_seq` increasing across the
load. Streaming changes all carry a higher LSN, so they win. No gap, no duplicates, and no
lock on the table.

- [ ] **Step 1: Write the failing test**

The valuable assertions here need a live PostgreSQL, so this test is an integration test
guarded on an environment variable, in the style the acceptance suite already uses:

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class BackfillerTest {

    @Test
    void copyTextIsParsedWithTabsAndBackslashNForNull() {
        // COPY ... TO STDOUT text format: tab-separated, \N for NULL,
        // and \t \n \r \\ escaped inside values.
        assertEquals(List.of("1", null, "a\tb"),
                     Backfiller.parseCopyLine("1\t\\N\ta\\tb"));
        assertEquals(List.of("", "x"), Backfiller.parseCopyLine("\tx"));
        assertEquals(List.of("a\nb"), Backfiller.parseCopyLine("a\\nb"));
        assertEquals(List.of("a\\b"), Backfiller.parseCopyLine("a\\\\b"));
    }

    @Test
    void aNullMarkerIsNotTheTwoCharacterStringBackslashN() {
        assertNull(Backfiller.parseCopyLine("\\N").get(0));
        assertEquals("\\N", Backfiller.parseCopyLine("\\\\N").get(0));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "PGPAIMON_IT_JDBC_URL", matches = ".+")
    void backfillLoadsEveryRowAtTheConsistentPoint() throws Exception {
        String jdbc = System.getenv("PGPAIMON_IT_JDBC_URL");
        String wh   = System.getenv("PGPAIMON_IT_WAREHOUSE");

        try (Connection c = DriverManager.getConnection(jdbc);
             Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS bf_t");
            st.execute("CREATE TABLE bf_t (id int PRIMARY KEY, v text)");
            st.execute("INSERT INTO bf_t SELECT g, 'v' || g FROM generate_series(1, 1000) g");
            st.execute("SELECT paimon.add_table('bf_t'::regclass)");
        }
        int relid = relidOf(jdbc, "bf_t");

        Map<String, String> env = new HashMap<>();
        env.put("PGPAIMON_JDBC_URL", jdbc);
        env.put("PGPAIMON_WAREHOUSE", wh);
        WriterConfig cfg = WriterConfig.fromEnv(env);

        Options o = new Options();
        o.set("warehouse", wh);
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(o));
        SchemaRegistry reg = new SchemaRegistry(catalog, cfg);
        reg.createTable(relid, "public", "bf_t", List.of("id"),
            List.of(new DdlMessage.Col(1, "id", "INT NOT NULL"),
                    new DdlMessage.Col(2, "v", "STRING")));

        PaimonSink sink = new PaimonSink(reg, cfg);
        try {
            sink.setRelation(relid, new RelationMeta(relid, "public", "bf_t",
                List.of("id", "v"), List.of(true, false)));
            Backfiller bf = new Backfiller(cfg, reg, sink);
            Backfiller.BackfillPlan plan = bf.createSlotAndExportSnapshot();
            bf.backfill(plan, relid, "public", "bf_t", List.of("id", "v"));

            // Every record is +I and every _seq is the consistent point: a streaming
            // change always carries a higher LSN, so it wins with no coordination.
            List<RawRecord> raw = RawRead.read(wh, "bf_t");
            assertEquals(1000, raw.size());
            assertTrue(raw.stream().allMatch(r -> r.rowKind() == 0),
                       "every backfilled record must be RowKind.INSERT");
            assertTrue(raw.stream().allMatch(r -> r.seq() == plan.consistentPoint()),
                       "every backfilled _seq must equal the consistent point");
        } finally {
            sink.close();
        }

        try (Connection c = DriverManager.getConnection(jdbc);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT backfill_due FROM paimon.table_registry"
                 + " WHERE relid = 'bf_t'::regclass")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1), "mark_backfilled must have run");
        }
    }

    private static int relidOf(String jdbc, String table) throws Exception {
        try (Connection c = DriverManager.getConnection(jdbc);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT '" + table + "'::regclass::oid")) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=BackfillerTest
```

Expected: FAIL — `cannot find symbol: class Backfiller`.

`RawRead` and `RawRecord` are a small test helper extracted in this step from
`PaimonSinkTest.rawKinds` (Task 11): `record RawRecord(byte rowKind, long seq)` and
`RawRead.read(String warehouse, String table)`. Move that method into
`src/test/java/pro/surpin/pgpaimon/writer/RawRead.java`, have `PaimonSinkTest` call it, and
return the `_row_kind` and `_seq` of every physical record rather than a histogram. Two tests
need the same raw read, so it belongs in one place.

- [ ] **Step 3: Implement `parseCopyLine` and the backfill path**

`parseCopyLine` is the piece worth unit-testing because COPY's text format has exactly four
escapes plus the `\N` NULL marker, and treating `\N` as a two-character string silently turns
every NULL into the literal text `\N`.

```java
    /** COPY TO STDOUT text format: tab-separated, \N = NULL, \t \n \r \\ escaped. */
    static List<String> parseCopyLine(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean sawEscape = false;
        boolean isNull = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (sawEscape) {
                switch (c) {
                    case 'N' -> isNull = cur.isEmpty();
                    case 't' -> cur.append('\t');
                    case 'n' -> cur.append('\n');
                    case 'r' -> cur.append('\r');
                    case '\\' -> cur.append('\\');
                    default -> cur.append(c);
                }
                sawEscape = false;
            } else if (c == '\\') {
                sawEscape = true;
            } else if (c == '\t') {
                out.add(isNull ? null : cur.toString());
                cur.setLength(0);
                isNull = false;
            } else {
                cur.append(c);
            }
        }
        out.add(isNull ? null : cur.toString());
        return out;
    }
```

`isNull = cur.isEmpty()` is what distinguishes the marker from the escape: `\N` as a whole
field is NULL, while `\\N` produces the two-character string.

The rest of the class:

- `createSlotAndExportSnapshot()` opens a replication-mode connection and runs
  `CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput EXPORT_SNAPSHOT` as a simple query,
  reading `consistent_point` and `snapshot_name` from the single result row. Keep that
  connection open: the exported snapshot is only usable while it lives.
- `backfill(...)` opens a second, plain connection and runs
  `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`,
  `SET TRANSACTION SNAPSHOT '<snapshotName>'`,
  then `COPY <schema>.<table> (<columns>) TO STDOUT` through
  `org.postgresql.copy.CopyManager.copyOut`, feeding `parseCopyLine` output into
  `sink.flush(...)` in chunks of `config.flushMaxRecords()`, every record `RowKind.INSERT`
  with `_seq = plan.consistentPoint()` and a running `_op_seq`, then `COMMIT`.
- Each chunk is committed with `commitIdentifier = plan.consistentPoint()`. Because the
  identifier repeats across chunks, use `plan.consistentPoint() - chunkCount + index` or a
  per-table monotone identifier derived from it; pick one and state it in a comment. The
  constraint is only that identifiers are monotone per `commitUser` and never collide with a
  streaming commit LSN, so **subtracting** from the consistent point is safe while adding is
  not.
- Finally `SELECT paimon.mark_backfilled(?::regclass)` on the plain connection.

Append to `sql/paimon_slot/paimon_slot--1.0.sql`:

```sql
CREATE FUNCTION paimon.mark_backfilled(rel regclass) RETURNS void
LANGUAGE sql AS $$
    UPDATE paimon.table_registry SET backfill_due = false WHERE relid = rel::oid;
$$;
```

and to the regress test, after `add_table`:

```sql
SELECT paimon.mark_backfilled('reg_t'::regclass);
SELECT backfill_due FROM paimon.table_registry WHERE relid = 'reg_t'::regclass;
```

- [ ] **Step 4: Run the unit tests to verify they pass**

```bash
cd /home/vadim/postgres-paimon/java/writer && mvn -q test -Dtest=BackfillerTest
```

Expected: the two `parseCopyLine` tests pass; `backfillLoadsEveryRowAtTheConsistentPoint` is
reported as skipped, because `PGPAIMON_IT_JDBC_URL` is unset until Task 14 brings up the
harness. A skip here is the expected result, not a gap.

- [ ] **Step 5: Regenerate the regress expected output**

```bash
cd /home/vadim/postgres-paimon/sql/paimon_slot
make && sudo make install && make installcheck
cp test/results/paimon_slot.out test/expected/paimon_slot.out
make installcheck
```

Expected: `All 1 tests passed.`, with `backfill_due` showing `f`.

- [ ] **Step 6: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/writer sql/paimon_slot
git commit -m "$(printf 'feat(writer): initial backfill from an exported snapshot\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 14: Docker harness

**Files:**
- Create: `docker/slot-demo/docker-compose.yml`
- Create: `docker/slot-demo/writer/Dockerfile`
- Create: `docker/slot-demo/postgres/init.sql`
- Create: `docker/slot-demo/run_acceptance.sh`
- Modify: `docker/slot-demo/run_acceptance.sh` (export `PGPAIMON_IT_JDBC_URL`, which enables the Task 13 integration test)

**Interfaces:**
- Consumes: the shaded `java/writer/target/paimon-writer.jar` from Task 12.
- Produces: `./docker/slot-demo/run_acceptance.sh` — the single command that brings up the stack and runs the JDBC suites. Tasks 15-18 all invoke it.

The point of this task is that it removes most of the project's CI cost: `docker/demo/postgres/Dockerfile` builds PostgreSQL against aws-sdk-cpp, libhdfs3 and Arrow, and takes about thirty minutes on a cold cache. Here PostgreSQL is the official image and nothing is compiled into it.

The warehouse must be a path both the writer container and the host-side JDBC test suites can see, because the suites read the warehouse directly from the filesystem (`PaimonFeatureTest:309`). Use a bind mount to a host directory, not a named volume.

- [ ] **Step 1: Write the failing harness check**

Create `docker/slot-demo/run_acceptance.sh` with the suite list and an explicit readiness gate, then run it. It fails because nothing else exists yet.

```bash
#!/usr/bin/env bash
# postgres-paimon logical-slot acceptance run
# Usage: ./run_acceptance.sh [suite ...]      (default: all)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
WAREHOUSE="${WAREHOUSE:-$HERE/warehouse}"
JDBC="jdbc:postgresql://localhost:5435/testdb?user=postgres&password=paimon"

SUITES=("$@")
if [ ${#SUITES[@]} -eq 0 ]; then
    SUITES=(PaimonFeatureTest PaimonAdvancedTest PaimonDdlTest PaimonSchemaIT
            PaimonTypeTest PaimonSlotKeyMoveTest)
fi

echo "▶  Building the writer jar"
( cd "$ROOT/java/writer" && mvn -q -DskipTests package )

echo "▶  Building the reader/test jars"
# -DskipTests skips execution but still compiles src/test, which is where these
# suites live: this pom has no src/main, so target/test-classes must be on the
# classpath alongside the shaded dependency jar.
( cd "$ROOT/java" && mvn -q -DskipTests package -Pfeature-test )
TESTCP="$ROOT/java/target/paimon-feature-test.jar:$ROOT/java/target/test-classes"

echo "▶  Starting the stack"
rm -rf "$WAREHOUSE" && mkdir -p "$WAREHOUSE"
WAREHOUSE="$WAREHOUSE" docker compose -f "$HERE/docker-compose.yml" up -d --build

echo "▶  Waiting for PostgreSQL"
until docker compose -f "$HERE/docker-compose.yml" exec -T postgres \
        pg_isready -U postgres -d testdb >/dev/null 2>&1; do printf '.'; sleep 1; done
echo " ready."

echo "▶  Waiting for the writer to attach to the slot"
for _ in $(seq 1 60); do
    active=$(docker compose -f "$HERE/docker-compose.yml" exec -T postgres \
        psql -U postgres -d testdb -tAc \
        "SELECT active FROM pg_replication_slots WHERE slot_name = 'paimon'" 2>/dev/null || true)
    [ "$active" = "t" ] && break
    printf '.'; sleep 1
done
[ "$active" = "t" ] || { echo " writer never attached"; exit 1; }
echo " attached."

rc=0
for suite in "${SUITES[@]}"; do
    echo ""
    echo "══ $suite ══"
    java -cp "$TESTCP" "pro.surpin.pgpaimon.$suite" "$WAREHOUSE" "$JDBC" || rc=1
done

echo ""
echo "▶  Writer log tail"
docker compose -f "$HERE/docker-compose.yml" logs --tail 40 writer
exit $rc
```

```bash
chmod +x docker/slot-demo/run_acceptance.sh
./docker/slot-demo/run_acceptance.sh PaimonFeatureTest
```

Expected: FAIL — `no configuration file provided: not found`, because `docker-compose.yml` does not exist.

- [ ] **Step 2: Write the compose file, Dockerfile and init.sql**

`docker/slot-demo/docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_PASSWORD: paimon
      POSTGRES_DB: testdb
    command:
      - -c
      - wal_level=logical
      - -c
      - max_replication_slots=8
      - -c
      - max_wal_senders=8
      - -c
      - shared_buffers=256MB
    ports:
      - "5435:5432"
    volumes:
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/10-paimon.sql:ro
      - ../../sql/paimon_slot:/paimon_slot:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d testdb"]
      interval: 2s
      timeout: 3s
      retries: 30

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9100:9000"
      - "9101:9001"

  writer:
    build:
      context: ../..
      dockerfile: docker/slot-demo/writer/Dockerfile
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      PGPAIMON_JDBC_URL: jdbc:postgresql://postgres:5432/testdb
      PGPAIMON_USER: postgres
      PGPAIMON_PASSWORD: paimon
      PGPAIMON_WAREHOUSE: /warehouse
      PGPAIMON_XID_FIELD: ""
    volumes:
      - ${WAREHOUSE:?set WAREHOUSE}:/warehouse
    restart: unless-stopped
```

The extension's SQL files are bind-mounted rather than installed with `make install`, because
the official image has no build toolchain. `init.sql` installs them by hand.

`docker/slot-demo/postgres/init.sql`:

```sql
-- SPDX-License-Identifier: Apache-2.0
-- The official image has no PGXS, so the pure-SQL extension is installed by
-- copying its files into the share directory before CREATE EXTENSION.
\! cp /paimon_slot/paimon_slot.control /usr/share/postgresql/18/extension/
\! cp /paimon_slot/paimon_slot--1.0.sql /usr/share/postgresql/18/extension/
CREATE SCHEMA IF NOT EXISTS paimon;
CREATE EXTENSION paimon_slot;
```

`docker/slot-demo/writer/Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY java/writer/pom.xml ./pom.xml
RUN mvn -q -B dependency:go-offline
COPY java/writer/src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre
COPY --from=build /src/target/paimon-writer.jar /app/paimon-writer.jar
ENTRYPOINT ["java", "-jar", "/app/paimon-writer.jar"]
```

The warehouse directory is written by the container as its own user and read by the host-side
test suites, so add `user: "${UID:-1000}:${GID:-1000}"` to the `writer` service if the
acceptance run reports permission errors reading the warehouse.

- [ ] **Step 3: Run the harness and watch the writer attach**

```bash
cd /home/vadim/postgres-paimon
./docker/slot-demo/run_acceptance.sh PaimonFeatureTest || true
docker compose -f docker/slot-demo/docker-compose.yml logs writer | tail -30
```

Expected at this point: PostgreSQL is healthy, the writer attaches to the slot, and
`PaimonFeatureTest` **fails** — the suite still runs `USING paimon_heap`, which that image
does not have. That failure is the Task 15 starting point. What must already work here is the
writer attaching: `pg_replication_slots.active` becomes `t`.

- [ ] **Step 4: Prove the end-to-end path by hand**

```bash
docker compose -f docker/slot-demo/docker-compose.yml exec -T postgres \
  psql -U postgres -d testdb <<'SQL'
CREATE TABLE smoke (id int PRIMARY KEY, v text, body text);
SELECT paimon.add_table('smoke'::regclass);
INSERT INTO smoke VALUES (1, 'a', repeat('x', 100000));
UPDATE smoke SET v = 'b' WHERE id = 1;          -- does not touch body
DELETE FROM smoke WHERE id = 1;
SQL
sleep 5
ls -la docker/slot-demo/warehouse/default.db/smoke/
```

Expected: a `schema/schema-0`, a `snapshot/` directory and `bucket-0/` with Parquet files.
Then read it back:

```bash
java -cp java/target/paimon-feature-test.jar:java/target/test-classes \
     pro.surpin.pgpaimon.PaimonRead docker/slot-demo/warehouse smoke
```

Expected: 4 records — `+I`, `-U`, `+U`, `-D` — and the `-U`/`+U` pair both carrying the
100000-character `body`. That last point is the whole TOAST design working end to end; if
`body` is NULL on the `+U`, go back to Task 7.

- [ ] **Step 5: Run the `Backfiller` integration test, which the harness now enables**

`BackfillerTest.backfillLoadsEveryRowAtTheConsistentPoint` was written in full in Task 13 and
skipped because `PGPAIMON_IT_JDBC_URL` was unset. Setting it here is what turns it on. Add to
`run_acceptance.sh`, after the writer-attached gate:

```bash
echo "▶  Writer integration tests"
PGPAIMON_IT_JDBC_URL="$JDBC" PGPAIMON_IT_WAREHOUSE="$WAREHOUSE" \
  ( cd "$ROOT/java/writer" && mvn -q test ) || rc=1
```

- [ ] **Step 6: Commit**

```bash
cd /home/vadim/postgres-paimon
git add docker/slot-demo java/writer
git commit -m "$(printf 'feat(harness): official postgres:18 compose stack and acceptance runner\n\nReplaces the thirty-minute aws-sdk-cpp PostgreSQL build with CREATE EXTENSION.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 15: Port the acceptance suites — setup only

**Files:**
- Modify: `java/src/test/java/pro/surpin/pgpaimon/PaimonFeatureTest.java`
- Modify: `java/src/test/java/pro/surpin/pgpaimon/PaimonDdlTest.java`
- Modify: `java/src/test/java/pro/surpin/pgpaimon/PaimonSchemaIT.java`
- Modify: `java/src/test/java/pro/surpin/pgpaimon/PaimonTypeTest.java`

**Interfaces:**
- Consumes: the harness from Task 14.
- Produces: the suites running against the slot architecture with **assertions unchanged**.

The suites are architecture-agnostic by construction: they drive SQL into PostgreSQL and
assert what a Paimon reader sees. What changes is setup only:

| `main` | here |
|---|---|
| `CREATE TABLE ... USING paimon_heap` | `CREATE TABLE ...` then `SELECT paimon.add_table('t'::regclass)` |
| `setGuc(conn, "paimon_heap.meta_field_xid", "_xid")` | no-op; metadata columns are writer-side configuration |
| `shared_preload_libraries = paimon_heap` | the writer process |
| warehouse path `<wh>/<table>` | warehouse path `<wh>/default.db/<table>` |

That last row touches every `readRows`, `readSchema`, `waitForFile` and `waitForParquet` call,
because Paimon's filesystem catalog puts tables under `<database>.db/`. Change the path
helpers, not each call site.

- [ ] **Step 1: Run the suites to see them fail**

```bash
cd /home/vadim/postgres-paimon
./docker/slot-demo/run_acceptance.sh PaimonFeatureTest PaimonDdlTest || true
```

Expected: every test fails on `CREATE TABLE ... USING paimon_heap` with
`access method "paimon_heap" does not exist`.

- [ ] **Step 2: Add the shared setup helpers**

In each suite, add two helpers and route the existing code through them:

```java
    /** Creates a tracked table.  Replaces "USING paimon_heap" from the TAM architecture. */
    static void createTracked(Connection conn, String tbl, String columns) throws Exception {
        exec(conn, "DROP TABLE IF EXISTS " + tbl);
        exec(conn, "CREATE TABLE " + tbl + " (" + columns + ")");
        exec(conn, "SELECT paimon.add_table('" + tbl + "'::regclass)");
    }

    /** Paimon's filesystem catalog nests tables under <database>.db/. */
    static String tableDir(String tbl) {
        return warehouse + "/default.db/" + tbl;
    }
```

Then: replace every `CREATE TABLE ... USING paimon_heap` with `createTracked`; replace every
`warehouse + "/" + tbl` with `tableDir(tbl)`; and make `setGuc(conn, "paimon_heap.…", …)` a
no-op that logs once, so the call sites do not all have to change in this task.

- [ ] **Step 3: Run the suites again**

```bash
./docker/slot-demo/run_acceptance.sh PaimonFeatureTest PaimonDdlTest PaimonSchemaIT PaimonTypeTest
```

Expected: T1, T2, T3, T6, T7, T8 pass. T4 fails on its `_tid` assertion — that is Task 16.
T5 passes. D1-D6 and D9 pass; D7 and D8 pass with the error now raised by an event trigger.
`PaimonSchemaIT` and `PaimonTypeTest` pass.

Any other failure is a real defect: fix it in the writer, not in the assertion. The whole point
of keeping these assertions is that they are the definition of "works".

- [ ] **Step 4: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/src/test
git commit -m "$(printf 'test: port the acceptance suites to the slot architecture, setup only\n\nAssertions are unchanged; only table creation, the warehouse path and the\nnow-irrelevant GUC calls differ.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 16: The assertions that genuinely change, and T20

**Files:**
- Modify: `java/src/test/java/pro/surpin/pgpaimon/PaimonFeatureTest.java` (T4)
- Modify: `java/src/test/java/pro/surpin/pgpaimon/PaimonAdvancedTest.java` (T10, T11, T16, T17)
- Create: `java/src/test/java/pro/surpin/pgpaimon/PaimonSlotKeyMoveTest.java` (T20)

`java/pom.xml` is **not** modified, which the Global Constraints require. A new class in
`src/test/java/pro/surpin/pgpaimon/` is picked up by `mvn -DskipTests package -Pfeature-test`
with no pom change, and `run_acceptance.sh` already puts `target/test-classes` on the
classpath next to the shaded jar. Confirm in step 2 that the class is found; if the project's
own `java -jar java/target/paimon-feature-test.jar` form documented in `CONTRIBUTING.md:44`
works, the `-cp` form works too.

**Interfaces:**
- Consumes: the harness and the ported suites.
- Produces: the four changed assertions and the new T20, which is the named acceptance test for spec §7's intra-transaction key-collision rule.

Changes, each a documented behaviour difference rather than a defect:

| test | change |
|---|---|
| T4 `test4_metadataFields` | `_xid` and `_op_seq` assertions kept. The `_tid` assertion is replaced by asserting `_seq` is present and strictly increasing — no physical tuple identifier survives the reorder buffer, and `_seq` is the per-change LSN that replaces it. |
| T10, T11 (vacuum, vacuum freeze) | Become assertions that **nothing** appears: VACUUM and VACUUM FREEZE emit no logical changes, so the record count must be unchanged across them. |
| T16 (crash recovery) | Strengthened: kill the **writer** mid-load with `docker compose kill writer`, restart it, assert zero loss and zero duplicates. `main` cannot promise this; the slot plus `commitIdentifier` can. |
| T17 `test17_defaultTam` | Loses its meaning — there is no TAM. Becomes a check that a table which was never passed to `paimon.add_table()` reaches neither `paimon_pub` nor the warehouse. |

- [ ] **Step 1: Write T20 as a failing test**

```java
// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.ReadBuilder;

import java.sql.*;
import java.util.*;

/**
 * T20 — intra-transaction key collisions.
 *
 * "UPDATE t SET id = id + 1" rewrites every key in one transaction.  Each row
 * decomposes to -D(old_key) + +I(new_key), and those pairs interleave: the +I for
 * key 3 may arrive before the -D that vacates it.  Correctness rests entirely on
 * per-change LSN ordering surviving, so this is the test that any writer-side
 * collapsing, reordering or per-key deduplication fails.
 *
 * Usage:
 *   java -cp paimon-feature-test.jar pro.surpin.pgpaimon.PaimonSlotKeyMoveTest \
 *        [warehouse] [jdbc-url]
 */
public class PaimonSlotKeyMoveTest {

    static final int N = 200;

    static String warehouse;
    static String jdbcUrl;
    static final LocalFileIO fileIO = LocalFileIO.create();
    static int passed = 0, failed = 0;

    public static void main(String[] args) throws Exception {
        warehouse = args.length > 0 ? args[0] : "/tmp/paimon-warehouse";
        jdbcUrl   = args.length > 1 ? args[1]
                  : "jdbc:postgresql://localhost:5435/testdb?user=postgres&password=paimon";

        System.out.println("── T20: intra-transaction key moves ──────────────────");

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            exec(conn, "DROP TABLE IF EXISTS km_t");
            exec(conn, "CREATE TABLE km_t (id int PRIMARY KEY, v text)");
            exec(conn, "SELECT paimon.add_table('km_t'::regclass)");
            try (PreparedStatement ps =
                     conn.prepareStatement("INSERT INTO km_t VALUES (?, ?)")) {
                for (int i = 1; i <= N; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, "row" + i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }

        Map<Integer, String> loaded = waitForMerged("km_t", N);
        check("T20 initial load: " + N + " keys", loaded.size() == N);

        /*
         * One statement, one transaction.  PostgreSQL evaluates the whole UPDATE
         * against a single snapshot, so no unique violation occurs even though the
         * key ranges overlap; the server picks the row order, which is the point.
         */
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            conn.setAutoCommit(false);
            exec(conn, "UPDATE km_t SET id = id + 1");
            conn.commit();
        }

        Map<Integer, String> expected = new TreeMap<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, v FROM km_t ORDER BY id")) {
            while (rs.next()) {
                expected.put(rs.getInt(1), rs.getString(2));
            }
        }
        check("T20 PostgreSQL holds " + N + " keys after the move", expected.size() == N);

        Map<Integer, String> actual = waitForMergedEquals("km_t", expected);

        check("T20 key sets match", actual.keySet().equals(expected.keySet()));
        check("T20 values match", actual.equals(expected));
        check("T20 no key duplicated or lost", actual.size() == N);
        if (!actual.equals(expected)) {
            SortedSet<Integer> missing = new TreeSet<>(expected.keySet());
            missing.removeAll(actual.keySet());
            SortedSet<Integer> extra = new TreeSet<>(actual.keySet());
            extra.removeAll(expected.keySet());
            System.out.println("  missing keys: " + missing);
            System.out.println("  extra keys  : " + extra);
        }

        System.out.printf("%n  %d passed, %d failed%n", passed, failed);
        if (failed > 0) {
            System.exit(1);
        }
    }

    /** Merged read: the table's own options are left intact, unlike readRows(). */
    static Map<Integer, String> readMerged(String tbl) throws Exception {
        FileStoreTable table = FileStoreTableFactory.create(
            fileIO, new Path(warehouse + "/default.db/" + tbl));
        ReadBuilder rb = table.newReadBuilder();
        Map<Integer, String> out = new TreeMap<>();
        try (RecordReader<InternalRow> reader =
                 rb.newRead().createReader(rb.newScan().plan().splits())) {
            reader.forEachRemaining(r ->
                out.put(r.getInt(0), r.isNullAt(1) ? null : r.getString(1).toString()));
        }
        return out;
    }

    static Map<Integer, String> waitForMerged(String tbl, int want) throws Exception {
        for (int i = 0; i < 120; i++) {
            try {
                Map<Integer, String> m = readMerged(tbl);
                if (m.size() >= want) {
                    return m;
                }
            } catch (Exception ignored) {
                // table not created yet
            }
            Thread.sleep(500);
        }
        return readMerged(tbl);
    }

    static Map<Integer, String> waitForMergedEquals(String tbl, Map<Integer, String> want)
            throws Exception {
        Map<Integer, String> last = Map.of();
        for (int i = 0; i < 120; i++) {
            last = readMerged(tbl);
            if (last.equals(want)) {
                return last;
            }
            Thread.sleep(500);
        }
        return last;
    }

    static void exec(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    static void check(String name, boolean ok) {
        System.out.printf("  %s  %s%n", ok ? "PASS" : "FAIL", name);
        if (ok) {
            passed++;
        } else {
            failed++;
        }
    }
}
```

`readMerged` leaves the table's own options intact — `merge-engine`, `changelog-producer` and
`sequence.field` — unlike `readRows` in the other suites, which strips them to see raw
pre-merge records. Both reads matter: the raw one proves every event was written, the merged
one proves they were ordered correctly.

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /home/vadim/postgres-paimon
./docker/slot-demo/run_acceptance.sh PaimonSlotKeyMoveTest
```

Expected: FAIL — the class has no implementation yet.

- [ ] **Step 3: Implement T20 and run it**

```bash
./docker/slot-demo/run_acceptance.sh PaimonSlotKeyMoveTest
```

Expected: PASS, 200 keys, every value shifted by one. If keys are missing, the writer collapsed
or reordered within the batch — go back to Task 10 and re-read spec §7.

- [ ] **Step 4: Change T4, T10, T11, T16 and T17**

Apply the four changes from the table above. For T16, the kill-and-restart sequence is:

```bash
docker compose -f docker/slot-demo/docker-compose.yml kill writer
# insert more rows while it is down; the slot retains the WAL
docker compose -f docker/slot-demo/docker-compose.yml up -d writer
```

The test drives that through `Runtime.getRuntime().exec(...)` guarded by an environment
variable, so the suite still runs when Docker is not available, skipping T16 with a printed
note rather than failing.

- [ ] **Step 5: Run every suite**

```bash
./docker/slot-demo/run_acceptance.sh
```

Expected: all suites green except `PaimonOffloadTest`, which is Task 17.

- [ ] **Step 6: Commit**

```bash
cd /home/vadim/postgres-paimon
git add java/src/test
git commit -m "$(printf 'test: adapt T4/T10/T11/T16/T17 and add T20 for key moves\n\nT4 asserts _seq instead of _tid: no physical tuple identifier survives the\nreorder buffer.  T16 is strengthened to kill the writer mid-load.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 17: S3 and HDFS warehouses

**Files:**
- Modify: `docker/slot-demo/docker-compose.yml` (MinIO init, writer S3 env)
- Modify: `docker/slot-demo/run_acceptance.sh` (an `--s3` mode)
- Modify: `java/src/test/java/pro/surpin/pgpaimon/PaimonOffloadTest.java`

**Interfaces:**
- Consumes: `WriterConfig.catalogOptions()` from Task 5, which already carries `s3.endpoint`, `s3.access-key`, `s3.secret-key` and `s3.path.style.access`.
- Produces: T18 and T19 green.

This is where the architecture pays off most visibly. On `main`, offload is bespoke C++:
`legacy:extension/paimon_offload_s3.cpp` drives aws-sdk-cpp directly and, after uploading, deletes
local Parquet files from `bucket-0/` (`legacy:extension/paimon_offload_s3.h:20`). Here the warehouse
simply *is* `s3://bucket/wh`, Paimon's own `FileIO` handles it, and there is no offload step,
no local staging and no deletion pass. `PaimonOffloadTest`'s assertions about what ends up in
S3 stay; the mechanism underneath them disappears.

- [ ] **Step 1: Run T18 to see it fail**

```bash
cd /home/vadim/postgres-paimon
./docker/slot-demo/run_acceptance.sh PaimonOffloadTest || true
```

Expected: FAIL — the suite configures `paimon_heap.s3_*` GUCs that do not exist.

- [ ] **Step 2: Add the MinIO bucket initialiser**

Append to `docker/slot-demo/docker-compose.yml`:

```yaml
  minio-init:
    image: minio/mc
    depends_on: [minio]
    entrypoint: >
      /bin/sh -c "
      until mc alias set local http://minio:9000 minioadmin minioadmin; do sleep 1; done;
      mc mb --ignore-existing local/paimon;
      "
```

and a second writer service for the S3 mode:

```yaml
  writer-s3:
    build:
      context: ../..
      dockerfile: docker/slot-demo/writer/Dockerfile
    profiles: ["s3"]
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      PGPAIMON_JDBC_URL: jdbc:postgresql://postgres:5432/testdb
      PGPAIMON_USER: postgres
      PGPAIMON_PASSWORD: paimon
      PGPAIMON_WAREHOUSE: s3://paimon/wh
      PGPAIMON_S3_ENDPOINT: http://minio:9000
      PGPAIMON_S3_ACCESS_KEY: minioadmin
      PGPAIMON_S3_SECRET_KEY: minioadmin
      PGPAIMON_SLOT: paimon
```

`paimon-bundle` includes the S3 `FileIOLoader`; the shaded jar's
`META-INF/services/org.apache.paimon.fs.FileIOLoader` must survive, which is what the
`ServicesResourceTransformer` from Task 5 guarantees. If `s3://` fails with "Could not find a
file io implementation for scheme s3", that transformer is missing or misconfigured.

- [ ] **Step 3: Adapt `PaimonOffloadTest`**

Replace the GUC-based S3 setup with the warehouse URI, keep every `ofKind` assertion
(`PaimonOffloadTest:185-206`), and point the reader at `s3://paimon/wh` through a Paimon
catalog configured with the same options. Add `--s3` handling to `run_acceptance.sh`:

```bash
if [ "${1:-}" = "--s3" ]; then
    shift
    PROFILE="--profile s3"
    WAREHOUSE_URI="s3://paimon/wh"
    SUITES=(PaimonOffloadTest)
fi
```

For HDFS (T19), reuse the existing `hadoop-minicluster` dependency in `java/pom.xml` rather
than adding an HDFS container: the suite starts a minicluster in-process and the writer is
pointed at its URI via `PGPAIMON_WAREHOUSE=hdfs://localhost:<port>/wh`. That keeps the compose
stack to three services.

- [ ] **Step 4: Run T18 and T19**

```bash
./docker/slot-demo/run_acceptance.sh --s3
./docker/slot-demo/run_acceptance.sh PaimonOffloadTest
```

Expected: T18's four `ofKind` assertions and T19's HDFS equivalents pass.

- [ ] **Step 5: Commit**

```bash
cd /home/vadim/postgres-paimon
git add docker/slot-demo java/src/test
git commit -m "$(printf 'feat: S3 and HDFS warehouses through Paimon FileIO\n\nReplaces the bespoke aws-sdk-cpp offload path with a warehouse URI: there is\nno staging directory and no deletion pass any more.\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```

---

## Task 18: Documentation and the limitations list

**Files:**
- Create: `docs/logical-slot-architecture.md`
- Modify: `README.md`
- Modify: `gaps.md`

**Interfaces:**
- Consumes: everything.
- Produces: the operational documentation the spec's §8 requires, in particular the abandoned-slot hazard, which is a new way to take a production database down and must not be discovered by a user.

- [ ] **Step 1: Write `docs/logical-slot-architecture.md`**

Sections, each short:

1. **What this is** — the parallel architecture, and that `extension/` on the same branch is the other one.
2. **Setup** — `wal_level = logical`, `CREATE EXTENSION paimon_slot`, run the writer, `paimon.add_table()`.
3. **Operational hazards.** The abandoned slot comes first and is stated plainly: if the writer stops and nobody notices, `pg_wal` grows until `max_slot_wal_keep_size` invalidates the slot, and an invalidated slot means a full re-bootstrap. Give the monitoring query:
   ```sql
   SELECT slot_name, active, wal_status,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS behind,
          pg_size_pretty(safe_wal_size) AS safe_wal_size
   FROM pg_replication_slots WHERE slot_name = 'paimon';
   ```
   and the recommendation to set `max_slot_wal_keep_size`.
4. **`REPLICA IDENTITY FULL`** — what it costs, the `jsonb` pathology, and that the escape exists in the design but not in v1.
5. **Differences from the TAM architecture** — the §8 limitations list verbatim, plus `meta_field_tid` having no counterpart and `_tid` being replaced by `_seq`.
6. **Troubleshooting** — the exact text of the three errors a user can actually hit: the `REPLICA IDENTITY FULL` message from `ToastResolver`, the no-primary-key message from `add_table`, and the `ADD COLUMN ... DEFAULT` guardrail with its workaround.

- [ ] **Step 2: Update `README.md`**

Add a short section pointing at the new doc and stating which architecture a reader is looking
at, since both now live on the same branch. Do not rewrite the existing `paimon_heap`
documentation.

- [ ] **Step 3: Update `gaps.md`**

Mark items 1 and 3 fixed, naming the task and the mechanism (`SchemaChange.dropColumn`;
`Catalog.dropTable(identifier, true)`). Leave item 2 open and record why: `deduplicate` cannot
populate one column on existing records, the only correct fix is a full table refill, and
column defaults therefore stay blocked. Note that the detection improved —
`pg_attribute.atthasdef` at `ddl_command_end` rather than a parse-tree walk over
`ColumnDef->constraints`.

- [ ] **Step 4: Verify every documented command actually works**

Run each command block from the new doc against the running stack, including the monitoring
query and all three error cases. A troubleshooting section whose error strings do not match
the code is worse than none.

```bash
cd /home/vadim/postgres-paimon
docker compose -f docker/slot-demo/docker-compose.yml exec -T postgres \
  psql -U postgres -d testdb <<'SQL'
CREATE TABLE doc_nopk (a int);
SELECT paimon.add_table('doc_nopk'::regclass);
CREATE TABLE doc_t (id int PRIMARY KEY, v text);
SELECT paimon.add_table('doc_t'::regclass);
ALTER TABLE doc_t ADD COLUMN d int DEFAULT 1;
SQL
```

Expected: the two error texts match the doc character for character.

- [ ] **Step 5: Final full run**

```bash
./docker/slot-demo/run_acceptance.sh
./docker/slot-demo/run_acceptance.sh --s3
( cd sql/paimon_slot && make installcheck )
( cd java/writer && mvn -q test )
```

Expected: every suite green, `All 1 tests passed.` from pg_regress, and every writer unit test
passing.

- [ ] **Step 6: Commit**

```bash
cd /home/vadim/postgres-paimon
git add README.md gaps.md docs/logical-slot-architecture.md
git commit -m "$(printf 'docs: logical-slot architecture, operations and limitations\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>')"
```
