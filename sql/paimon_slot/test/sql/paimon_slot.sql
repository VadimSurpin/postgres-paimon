\set SHOW_CONTEXT never
CREATE EXTENSION paimon_slot;

-- type mapping, mirroring extension/type_map_pg.h
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

-- types with no Paimon mapping fall through to STRING, each with a WARNING
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

-- composite PK declared in non-catalog order: pk_cols must follow index order {c,a}, not column order {a,c}
CREATE TABLE pk_order_t (a int, b int, c int, PRIMARY KEY (c, a));
SELECT paimon.pk_cols('pk_order_t'::regclass);

-- no primary key: pk_cols returns an empty array, never NULL
CREATE TABLE no_pk_t (x int);
SELECT paimon.pk_cols('no_pk_t'::regclass) AS pk,
       paimon.pk_cols('no_pk_t'::regclass) IS NULL AS is_null,
       cardinality(paimon.pk_cols('no_pk_t'::regclass)) AS len;

-- no user columns: table_cols returns an empty JSON array, never NULL
CREATE TABLE no_cols_t ();
SELECT paimon.table_cols('no_cols_t'::regclass) AS cols,
       paimon.table_cols('no_cols_t'::regclass) IS NULL AS is_null;

-- add_table requires a primary key
CREATE TABLE nopk (a int);
SELECT paimon.add_table('nopk'::regclass);        -- expect ERROR

-- the v1 escapes are accepted but not implemented
SELECT paimon.add_table('reg_t'::regclass, 'default');          -- expect ERROR
SELECT paimon.add_table('reg_t'::regclass, 'full', 'lookup');   -- expect ERROR

-- happy path
SELECT slot_name FROM pg_create_logical_replication_slot('paimon', 'pgoutput');
SELECT slot_name FROM pg_create_logical_replication_slot('paimon_test', 'test_decoding');
SELECT paimon.add_table('reg_t'::regclass);
SELECT schema_name, table_name, pk_cols, backfill_due, replica_identity, changelog_producer
FROM paimon.table_registry WHERE relid = 'reg_t'::regclass;
SELECT relreplident FROM pg_class WHERE oid = 'reg_t'::regclass;
SELECT count(*) FROM pg_publication_tables
WHERE pubname = 'paimon_pub' AND tablename = 'reg_t';

-- the create message reached the slot
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
SELECT pg_drop_replication_slot('paimon_test');

-- Task 4: the slots above were dropped; recreate the decoding slot
SELECT slot_name FROM pg_create_logical_replication_slot('paimon_test', 'test_decoding');
CREATE TABLE ddl_t (id int PRIMARY KEY, a text);
SELECT paimon.add_table('ddl_t'::regclass);
SELECT paimon.peek_messages() - 'relid';   -- drain the create message

-- blocked: primary key changes
ALTER TABLE ddl_t DROP CONSTRAINT ddl_t_pkey;                 -- expect ERROR
CREATE TABLE ddl_u (id int, a text, PRIMARY KEY (id));
SELECT paimon.add_table('ddl_u'::regclass);
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE ddl_u ADD COLUMN b int;                           -- allowed
SELECT paimon.peek_messages() - 'relid';

-- blocked: Paimon's reserved field names (ruling R4)
ALTER TABLE ddl_t ADD COLUMN "_VALUE_KIND" int;               -- expect ERROR
ALTER TABLE ddl_t ADD COLUMN rowkind int;                     -- expect ERROR
ALTER TABLE ddl_t ADD COLUMN "_Level" int;                    -- expect ERROR, case-insensitive
ALTER TABLE ddl_t RENAME COLUMN a TO _sequence_number;        -- expect ERROR
CREATE TABLE reserved_t (id int PRIMARY KEY, _row_id text);
SELECT paimon.add_table('reserved_t'::regclass);              -- expect ERROR

-- blocked: NOT NULL without a default
ALTER TABLE ddl_t ADD COLUMN nn int NOT NULL;                 -- expect ERROR
-- blocked: any default (gaps.md item 2 carried over)
ALTER TABLE ddl_t ADD COLUMN wd int DEFAULT 7;                -- expect ERROR
ALTER TABLE ddl_t ADD COLUMN wd2 int NOT NULL DEFAULT 7;      -- expect ERROR
-- the guardrail rolled the statement back, so the columns are absent
SELECT count(*) FROM pg_attribute
WHERE attrelid = 'ddl_t'::regclass AND attname IN ('nn','wd','wd2');

-- allowed and propagated; relid is an unstable oid, so check it separately
ALTER TABLE ddl_t ADD COLUMN extra bigint;
SELECT (j->>'relid')::oid = 'ddl_t'::regclass AS relid_ok, j - 'relid' AS msg
FROM (SELECT paimon.peek_messages() AS j) s;
ALTER TABLE ddl_t RENAME COLUMN extra TO renamed;
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE ddl_t ALTER COLUMN a TYPE varchar(64);   -- STRING -> STRING: no message
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE ddl_t ADD COLUMN n int;
ALTER TABLE ddl_t ALTER COLUMN n TYPE bigint;
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE ddl_t DROP COLUMN n;
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE ddl_t DROP COLUMN renamed;
SELECT paimon.peek_messages() - 'relid';

-- the registry tracked every change
SELECT jsonb_pretty(cols) FROM paimon.table_registry WHERE relid = 'ddl_t'::regclass;

-- DROP TABLE
DROP TABLE ddl_t;
SELECT paimon.peek_messages() - 'relid';
SELECT count(*) FROM paimon.table_registry WHERE relid IS NOT NULL
  AND table_name = 'ddl_t';

-- untracked tables are untouched (D9)
CREATE TABLE plain_t (id int PRIMARY KEY);
ALTER TABLE plain_t ADD COLUMN whatever int NOT NULL DEFAULT 1;
SELECT paimon.peek_messages() - 'relid';

-- combined actions are judged on the statement's net effect
CREATE TABLE combo_t (id int PRIMARY KEY, b int, y int, w int, x int, k int);
SELECT paimon.add_table('combo_t'::regclass);
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE combo_t DROP COLUMN b, ADD COLUMN z int DEFAULT 5;       -- expect ERROR
ALTER TABLE combo_t DROP COLUMN y, ADD COLUMN nn int NOT NULL;       -- expect ERROR
ALTER TABLE combo_t DROP COLUMN w, ADD COLUMN rowkind int;           -- expect ERROR
SELECT paimon.peek_messages() - 'relid';                             -- nothing emitted
SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute
WHERE attrelid = 'combo_t'::regclass AND attnum > 0 AND NOT attisdropped;
ALTER TABLE combo_t DROP COLUMN x, ADD COLUMN w2 int;                -- allowed
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE combo_t DROP COLUMN w, ALTER COLUMN k TYPE bigint;       -- allowed
SELECT paimon.peek_messages() - 'relid';
DROP TABLE combo_t;
SELECT paimon.peek_messages() - 'relid';

-- dropping a schema emits drop_table only, no per-column messages
CREATE SCHEMA s4;
CREATE TABLE s4.t (id int PRIMARY KEY, a int, b int);
SELECT paimon.add_table('s4.t'::regclass);
SELECT paimon.peek_messages() - 'relid';
DROP SCHEMA s4 CASCADE;
SELECT paimon.peek_messages() - 'relid';

-- columns lost to a cascade from another object are synced in the same statement
CREATE DOMAIN dd AS int;
CREATE TYPE ct AS (f int);
CREATE TABLE casc_t (id int PRIMARY KEY, a int);
SELECT paimon.add_table('casc_t'::regclass);
ALTER TABLE casc_t ADD COLUMN dcol dd;
ALTER TABLE casc_t ADD COLUMN tcol ct;
SELECT paimon.peek_messages() - 'relid';
DROP DOMAIN dd CASCADE;
SELECT paimon.peek_messages() - 'relid';
DROP TYPE ct CASCADE;
SELECT paimon.peek_messages() - 'relid';
SELECT jsonb_path_query_array(cols, '$[*].name') FROM paimon.table_registry
WHERE relid = 'casc_t'::regclass;
-- same, for a table whose only cascaded column is a non-PK one
CREATE TYPE ct2 AS (f int);
CREATE TABLE casc2_t (id int PRIMARY KEY, c ct2);
SELECT paimon.add_table('casc2_t'::regclass);
SELECT paimon.peek_messages() - 'relid';
DROP TYPE ct2 CASCADE;
SELECT paimon.peek_messages() - 'relid';
DROP TABLE casc_t;
SELECT paimon.peek_messages() - 'relid';
-- a table dropped in the same cascade as the type its column uses: drop_table only
CREATE SCHEMA s5;
CREATE DOMAIN s5.d AS int;
CREATE TABLE s5.t (id int PRIMARY KEY, c s5.d);
SELECT paimon.add_table('s5.t'::regclass);
SELECT paimon.peek_messages() - 'relid';
DROP SCHEMA s5 CASCADE;
SELECT paimon.peek_messages() - 'relid';

-- RENAME TO: the Paimon table name is fixed at registration; tracking survives
CREATE TABLE ren_t (id int PRIMARY KEY);
SELECT paimon.add_table('ren_t'::regclass);
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE ren_t RENAME TO ren_t2;
SELECT paimon.peek_messages() - 'relid';
SELECT table_name FROM paimon.table_registry WHERE relid = 'ren_t2'::regclass;
ALTER TABLE ren_t2 ADD COLUMN after_rename int;
SELECT paimon.peek_messages() - 'relid';
DROP TABLE ren_t2;
SELECT paimon.peek_messages() - 'relid';

-- replica identity of a tracked table must stay FULL
CREATE TABLE ri_t (id int PRIMARY KEY, u int NOT NULL);
CREATE UNIQUE INDEX ri_t_u ON ri_t (u);
SELECT paimon.add_table('ri_t'::regclass);
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE ri_t REPLICA IDENTITY DEFAULT;          -- expect ERROR
SELECT relreplident FROM pg_class WHERE oid = 'ri_t'::regclass;
ALTER TABLE ri_t REPLICA IDENTITY NOTHING;          -- expect ERROR
SELECT relreplident FROM pg_class WHERE oid = 'ri_t'::regclass;
ALTER TABLE ri_t REPLICA IDENTITY USING INDEX ri_t_u;   -- expect ERROR
SELECT relreplident FROM pg_class WHERE oid = 'ri_t'::regclass;
ALTER TABLE ri_t REPLICA IDENTITY FULL;             -- allowed, no message
SELECT relreplident FROM pg_class WHERE oid = 'ri_t'::regclass;
SELECT paimon.peek_messages() - 'relid';
-- untracked tables are never interfered with
CREATE TABLE ri_plain (id int PRIMARY KEY, u int NOT NULL);
CREATE UNIQUE INDEX ri_plain_u ON ri_plain (u);
ALTER TABLE ri_plain REPLICA IDENTITY DEFAULT;
ALTER TABLE ri_plain REPLICA IDENTITY NOTHING;
ALTER TABLE ri_plain REPLICA IDENTITY USING INDEX ri_plain_u;
SELECT relreplident FROM pg_class WHERE oid = 'ri_plain'::regclass;
SELECT paimon.peek_messages() - 'relid';
DROP TABLE ri_t;
SELECT paimon.peek_messages() - 'relid';

SELECT pg_drop_replication_slot('paimon_test');

-- Task 13: mark_backfilled.  The slot above was dropped; recreate the decoding slot.
SELECT slot_name FROM pg_create_logical_replication_slot('paimon_test', 'test_decoding');
CREATE TABLE bf_t (id int PRIMARY KEY, v text);
INSERT INTO bf_t VALUES (1, 'x');   -- non-empty, so backfill_due is true and mark_backfilled has work
SELECT paimon.add_table('bf_t'::regclass);
SELECT paimon.peek_messages() - 'relid';
SELECT backfill_due FROM paimon.table_registry WHERE relid = 'bf_t'::regclass;
SELECT paimon.mark_backfilled('bf_t'::regclass);
SELECT backfill_due FROM paimon.table_registry WHERE relid = 'bf_t'::regclass;
-- idempotent, emits no message, and an untracked table is simply a no-op
SELECT paimon.mark_backfilled('bf_t'::regclass);
SELECT paimon.mark_backfilled('no_pk_t'::regclass);
SELECT count(*) FROM paimon.peek_messages();
DROP TABLE bf_t;
SELECT paimon.peek_messages() - 'relid';
-- backfill_due follows whether the table has rows at registration
CREATE TABLE bf_empty (id int PRIMARY KEY);
CREATE TABLE bf_rows (id int PRIMARY KEY);
INSERT INTO bf_rows VALUES (1);
SELECT paimon.add_table('bf_empty'::regclass);
SELECT paimon.add_table('bf_rows'::regclass);
SELECT table_name, backfill_due FROM paimon.table_registry
WHERE table_name IN ('bf_empty', 'bf_rows') ORDER BY table_name;
SELECT j->>'op' AS op, j->>'table' AS tbl FROM (SELECT paimon.peek_messages() AS j) s
ORDER BY 2;
DROP TABLE bf_empty, bf_rows;
SELECT paimon.peek_messages() - 'relid';

-- a DEFERRABLE primary key cannot be represented in a Paimon pk table
CREATE TABLE df_t (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED);
SELECT paimon.add_table('df_t'::regclass);                  -- expect ERROR
SELECT count(*) FROM paimon.table_registry WHERE table_name = 'df_t';
CREATE TABLE df_ok (id int PRIMARY KEY);
SELECT paimon.add_table('df_ok'::regclass);
SELECT paimon.peek_messages() - 'relid';
-- ALTER CONSTRAINT cannot change a primary key's deferrability in place
ALTER TABLE df_ok ALTER CONSTRAINT df_ok_pkey DEFERRABLE;   -- expect ERROR (PostgreSQL)
-- but drop + re-add in one statement keeps pk_cols, so the DDL path checks too
ALTER TABLE df_ok DROP CONSTRAINT df_ok_pkey,
  ADD CONSTRAINT df_ok_pkey PRIMARY KEY (id) DEFERRABLE;    -- expect ERROR
SELECT condeferrable FROM pg_constraint WHERE conrelid = 'df_ok'::regclass;
SELECT paimon.peek_messages() - 'relid';
DROP TABLE df_ok, df_t;
SELECT paimon.peek_messages() - 'relid';

SELECT pg_drop_replication_slot('paimon_test');

-- Finding 3 of the whole-branch review: two ordinary ALTERs passed every
-- guardrail and then killed the writer permanently, because Paimon refuses both
-- in a transaction PostgreSQL has already committed.
SELECT slot_name FROM pg_create_logical_replication_slot('paimon_test', 'test_decoding');
CREATE TABLE key_t (id int PRIMARY KEY, v text, nn int NOT NULL, n int);
SELECT paimon.add_table('key_t'::regclass);
SELECT paimon.peek_messages() - 'relid';

-- the primary key's NAME is unchanged, so the pk guardrail passes; its TYPE is not
ALTER TABLE key_t ALTER COLUMN id TYPE bigint;                    -- expect ERROR
SELECT format_type(atttypid, atttypmod) FROM pg_attribute
WHERE attrelid = 'key_t'::regclass AND attname = 'id';
-- and in a combined statement, before anything is emitted
ALTER TABLE key_t ALTER COLUMN id TYPE bigint, ADD COLUMN q int;  -- expect ERROR
SELECT paimon.peek_messages() - 'relid';                          -- nothing emitted

-- nullable -> NOT NULL: Paimon never rewrites the rows already written
ALTER TABLE key_t ALTER COLUMN n SET NOT NULL;                    -- expect ERROR
ALTER TABLE key_t ALTER COLUMN n SET NOT NULL, ADD COLUMN q int;  -- expect ERROR
SELECT attnotnull FROM pg_attribute
WHERE attrelid = 'key_t'::regclass AND attname = 'n';
SELECT paimon.peek_messages() - 'relid';                          -- nothing emitted

-- what stays allowed: a non-key type change, and NOT NULL -> nullable
ALTER TABLE key_t ALTER COLUMN v TYPE varchar(32);   -- STRING -> STRING: no message
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE key_t ALTER COLUMN n TYPE bigint;        -- non-key: allowed
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE key_t ALTER COLUMN nn DROP NOT NULL;     -- Paimon accepts this direction
SELECT paimon.peek_messages() - 'relid';

-- a composite key is covered on every column
CREATE TABLE key2_t (a int, b int, v text, PRIMARY KEY (a, b));
SELECT paimon.add_table('key2_t'::regclass);
SELECT paimon.peek_messages() - 'relid';
ALTER TABLE key2_t ALTER COLUMN b TYPE bigint;                    -- expect ERROR
SELECT paimon.peek_messages() - 'relid';

-- untracked tables are not interfered with
CREATE TABLE key_plain (id int PRIMARY KEY, n int);
ALTER TABLE key_plain ALTER COLUMN id TYPE bigint;
ALTER TABLE key_plain ALTER COLUMN n SET NOT NULL;
SELECT paimon.peek_messages() - 'relid';

DROP TABLE key_t;
SELECT paimon.peek_messages() - 'relid';
DROP TABLE key2_t;
SELECT paimon.peek_messages() - 'relid';
DROP TABLE key_plain;
SELECT paimon.peek_messages() - 'relid';
-- numeric must have explicit precision <= 38
CREATE TABLE nm_plain (id int PRIMARY KEY, amount numeric);
CREATE TABLE nm_big (id int PRIMARY KEY, amount numeric(50, 2));
CREATE TABLE nm_dec (id int PRIMARY KEY, amount decimal);
CREATE TABLE nm_dec2 (id int PRIMARY KEY, amount dec);
CREATE TABLE nm_decp (id int PRIMARY KEY, amount decimal(40, 2));
CREATE TABLE nm_ok1 (id int PRIMARY KEY, amount numeric(10, 2));
CREATE TABLE nm_ok2 (id int PRIMARY KEY, amount numeric(38, 18));
SELECT paimon.add_table('nm_plain'::regclass);    -- expect ERROR
SELECT paimon.add_table('nm_big'::regclass);      -- expect ERROR
SELECT paimon.add_table('nm_dec'::regclass);      -- expect ERROR (alias)
SELECT paimon.add_table('nm_dec2'::regclass);     -- expect ERROR (alias)
SELECT paimon.add_table('nm_decp'::regclass);     -- expect ERROR (alias, p > 38)
SELECT paimon.add_table('nm_ok1'::regclass);
SELECT paimon.add_table('nm_ok2'::regclass);
SELECT j->>'table' AS tbl, j->'cols'->1->>'type' AS amount_type
FROM (SELECT paimon.peek_messages() AS j) s ORDER BY 1;
ALTER TABLE nm_ok1 ADD COLUMN bad numeric;        -- expect ERROR
ALTER TABLE nm_ok1 ADD COLUMN bad2 numeric(39);   -- expect ERROR
ALTER TABLE nm_ok1 ALTER COLUMN amount TYPE numeric;   -- expect ERROR
ALTER TABLE nm_ok1 DROP COLUMN amount, ADD COLUMN bad3 decimal;   -- expect ERROR
SELECT count(*) FROM pg_attribute
WHERE attrelid = 'nm_ok1'::regclass AND attname LIKE 'bad%';
SELECT format_type(atttypid, atttypmod) FROM pg_attribute
WHERE attrelid = 'nm_ok1'::regclass AND attname = 'amount';
ALTER TABLE nm_ok1 ADD COLUMN good numeric(12, 3);
SELECT paimon.peek_messages() - 'relid';
DROP TABLE nm_plain, nm_big, nm_dec, nm_dec2, nm_decp, nm_ok1, nm_ok2;
SELECT count(*) FROM paimon.peek_messages();

SELECT pg_drop_replication_slot('paimon_test');
