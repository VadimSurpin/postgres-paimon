\set SHOW_CONTEXT never
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

-- a table without a primary key is accepted (replicated as an operation log); see section (12)
CREATE TABLE nopk (a int);
SELECT paimon.add_table('nopk'::regclass);
SELECT pk_cols, hist_table FROM paimon.table_registry WHERE relid = 'nopk'::regclass;
SELECT paimon.remove_table('nopk'::regclass);
SELECT paimon.forget_history('public', 'nopk_hist');

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

-- Static (fixed) bucketing: the count add_table() resolves, and the override.
SELECT slot_name FROM pg_create_logical_replication_slot('paimon_test', 'test_decoding');
-- the heuristic: clamp(4, ceil(total_size / 1 GiB), 64).  An empty table is 16 kB, so the
-- ordinary registration path -- which REQUIRES an empty table -- always lands on the floor.
CREATE TABLE bk_empty (id int PRIMARY KEY, v text);
SELECT pg_total_relation_size('bk_empty'::regclass) = 0 AS truly_zero,
       paimon.default_buckets('bk_empty'::regclass) AS buckets;
-- the clamp's shape, without needing multi-gigabyte tables: the function is pure arithmetic on
-- pg_total_relation_size, so exercise the arithmetic directly
SELECT greatest(4, least(64, ceil(sz::numeric / (1024*1024*1024))::int)) AS buckets, sz
FROM (VALUES (0), (16384), (1073741823), (1073741824), (1073741825),
             (4294967296), (8589934592), (68719476736), (274877906944)) v(sz)
ORDER BY sz;
-- the resolved count is recorded and emitted
SELECT paimon.add_table('bk_empty'::regclass);
SELECT table_name, buckets FROM paimon.table_registry WHERE table_name = 'bk_empty';
SELECT j->'options' AS options FROM (SELECT paimon.peek_messages() AS j) s;
-- the override
CREATE TABLE bk_over (id int PRIMARY KEY);
SELECT paimon.add_table('bk_over'::regclass, buckets => 8);
SELECT table_name, buckets FROM paimon.table_registry WHERE table_name = 'bk_over';
SELECT j->'options'->>'bucket' AS bucket FROM (SELECT paimon.peek_messages() AS j) s;
-- a non-positive override is refused, and nothing is registered
CREATE TABLE bk_bad (id int PRIMARY KEY);
SELECT paimon.add_table('bk_bad'::regclass, buckets => 0);     -- expect ERROR
SELECT paimon.add_table('bk_bad'::regclass, buckets => -1);    -- expect ERROR
SELECT count(*) FROM paimon.table_registry WHERE table_name = 'bk_bad';
SELECT count(*) FROM paimon.peek_messages();
-- 1 is allowed: a deliberate single bucket is a choice, not an error
SELECT paimon.add_table('bk_bad'::regclass, buckets => 1);
SELECT table_name, buckets FROM paimon.table_registry WHERE table_name = 'bk_bad';
SELECT count(*) FROM paimon.peek_messages();
DROP TABLE bk_empty, bk_over, bk_bad;
SELECT count(*) FROM paimon.peek_messages();
SELECT pg_drop_replication_slot('paimon_test');

-- =====================================================================================
-- Poison records and silent data loss: every guardrail below refuses, before PostgreSQL
-- commits, a change the writer could never apply or would silently lose. Each refusal is
-- paired with the accepted neighbour it must not catch.
-- =====================================================================================
SELECT slot_name FROM pg_create_logical_replication_slot('paimon_test', 'test_decoding');
\set VERBOSITY default

-- (1) value guards: numeric NaN, and timestamps beyond Paimon's largest writable instant
CREATE TABLE vg_t (id int PRIMARY KEY, n numeric(10,2), ts timestamp, tz timestamptz,
                   d date, t text);
SELECT paimon.add_table('vg_t'::regclass);
SELECT paimon.peek_messages() - 'relid';
SELECT conname, pg_get_expr(conbin, conrelid), convalidated FROM pg_constraint
WHERE conrelid = 'vg_t'::regclass AND contype = 'c' ORDER BY conname;
INSERT INTO vg_t (id, n) VALUES (1, 'NaN');                                     -- expect ERROR
INSERT INTO vg_t (id, n) VALUES (1, 'Infinity');            -- expect ERROR (PostgreSQL itself)
INSERT INTO vg_t (id, ts) VALUES (2, '294247-01-10 04:00:54.775808');           -- expect ERROR
INSERT INTO vg_t (id, tz) VALUES (3, '294247-01-10 04:00:54.775808+00');        -- expect ERROR
INSERT INTO vg_t (id, ts) VALUES (4, '294276-12-31 23:59:59.999999');           -- expect ERROR
-- the neighbours: the largest writable instant, infinity (encoded by the writer), dates at
-- PostgreSQL's own extremes (Paimon holds its whole date range), ordinary values
INSERT INTO vg_t VALUES (5, 99999999.99, '294247-01-10 04:00:54.775807',
                         '294247-01-10 04:00:54.775807+00', '5874897-12-31', 'x');
INSERT INTO vg_t VALUES (6, -1.5, 'infinity', '-infinity', '4714-11-24 BC', NULL);
INSERT INTO vg_t VALUES (7, NULL, '4714-11-24 00:00:00 BC', NULL, NULL, NULL);
SET TimeZone = 'America/New_York';
INSERT INTO vg_t (id, tz) VALUES (8, '294247-01-09 23:00:54.775807');   -- = the maximum in UTC
INSERT INTO vg_t (id, tz) VALUES (9, '294247-01-09 23:00:54.775808');           -- expect ERROR
RESET TimeZone;
SELECT id FROM vg_t ORDER BY id;
-- the guard is recognised under any DateStyle and TimeZone (its constants deparse through them)
SET DateStyle = 'SQL, DMY';
SET TimeZone = 'Asia/Kathmandu';
ALTER TABLE vg_t ADD COLUMN after_style int;
RESET DateStyle;
RESET TimeZone;
SELECT paimon.peek_messages() - 'relid';
-- a guard cannot be dropped, renamed, or replaced by a same-named constraint
ALTER TABLE vg_t DROP CONSTRAINT paimon_guard_2;                                -- expect ERROR
ALTER TABLE vg_t RENAME CONSTRAINT paimon_guard_3 TO mine;                      -- expect ERROR
ALTER TABLE vg_t DROP CONSTRAINT paimon_guard_4,
  ADD CONSTRAINT paimon_guard_4 CHECK (true);                                   -- expect ERROR
-- a column added later gets its guard, NOT VALID: the column is NULL in every existing row
ALTER TABLE vg_t ADD COLUMN n2 numeric(5,1), ADD COLUMN ts2 timestamptz, ADD COLUMN i2 int;
SELECT conname, convalidated FROM pg_constraint
WHERE conrelid = 'vg_t'::regclass AND contype = 'c' ORDER BY conname;
INSERT INTO vg_t (id, n2) VALUES (10, 'NaN');                                   -- expect ERROR
SELECT paimon.peek_messages() - 'relid';
-- an existing bad value refuses add_table, naming the way to find it
CREATE TABLE vg_bad (id int PRIMARY KEY, n numeric(5,2));
INSERT INTO vg_bad VALUES (1, 'NaN');
SELECT paimon.add_table('vg_bad'::regclass);                                    -- expect ERROR
SELECT count(*) FROM paimon.table_registry WHERE table_name = 'vg_bad';
SELECT count(*) FROM pg_constraint WHERE conrelid = 'vg_bad'::regclass AND contype = 'c';
-- a same-named constraint that is not the guard refuses add_table rather than pass for one
CREATE TABLE vg_named (id int PRIMARY KEY, n numeric(5,2) CONSTRAINT paimon_guard_2 CHECK (n > 0));
SELECT paimon.add_table('vg_named'::regclass);                                  -- expect ERROR
-- remove_table drops the guards (and only them), after which they are ordinary DDL again
ALTER TABLE vg_t ADD CONSTRAINT users_own CHECK (id > 0);
SELECT paimon.peek_messages() - 'relid';
SELECT paimon.remove_table('vg_t'::regclass);
SELECT conname FROM pg_constraint WHERE conrelid = 'vg_t'::regclass AND contype = 'c';
INSERT INTO vg_t (id, n) VALUES (11, 'NaN');                         -- allowed: not replicated
DROP TABLE vg_t, vg_bad, vg_named;
SELECT count(*) FROM paimon.peek_messages();

-- (2) numeric typmods Paimon's DECIMAL cannot take (PostgreSQL 15+)
SELECT paimon.pg_type_to_paimon(1700, (5 << 16 | (-2 & 2047)) + 4, false) AS neg_scale,
       paimon.pg_type_to_paimon(1700, (3 << 16 | 5) + 4, false)          AS scale_over;
CREATE TABLE ns_neg (id int PRIMARY KEY, a numeric(5,-2));
CREATE TABLE ns_over (id int PRIMARY KEY, a numeric(3,5));
CREATE TABLE ns_ok (id int PRIMARY KEY, a numeric(3,3));
SELECT paimon.add_table('ns_neg'::regclass);                                    -- expect ERROR
SELECT paimon.add_table('ns_over'::regclass);                                   -- expect ERROR
SELECT paimon.add_table('ns_ok'::regclass);
ALTER TABLE ns_ok ADD COLUMN b numeric(2,-1);                                   -- expect ERROR
SELECT paimon.peek_messages() - 'relid';
DROP TABLE ns_neg, ns_over, ns_ok;
SELECT count(*) FROM paimon.peek_messages();

-- (3) columns the stream cannot carry: generated columns and reg* types
CREATE TABLE gc_stored (id int PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a * 2) STORED);
CREATE TABLE gc_virtual (id int PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a * 2));
CREATE TABLE gc_ident (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, a int);
SELECT paimon.add_table('gc_stored'::regclass);                                 -- expect ERROR
SELECT paimon.add_table('gc_virtual'::regclass);                                -- expect ERROR
SELECT paimon.add_table('gc_ident'::regclass);           -- an identity column is an ordinary value
CREATE DOMAIN rt_dom AS regtype;
CREATE TABLE rc_t (id int PRIMARY KEY, r regclass);
CREATE TABLE rc_arr (id int PRIMARY KEY, r rt_dom[]);
CREATE TABLE rc_oid (id int PRIMARY KEY, r oid);
SET client_min_messages = error;
SELECT paimon.add_table('rc_t'::regclass);                                      -- expect ERROR
SELECT paimon.add_table('rc_arr'::regclass);                                    -- expect ERROR
SELECT paimon.add_table('rc_oid'::regclass);
ALTER TABLE rc_oid ADD COLUMN r2 regrole;                                       -- expect ERROR
RESET client_min_messages;
SELECT j->>'op' AS op, j->>'table' AS tbl FROM (SELECT paimon.peek_messages() AS j) s ORDER BY 2;
DROP TABLE gc_stored, gc_virtual, gc_ident, rc_t, rc_arr, rc_oid;
DROP DOMAIN rt_dom;
SELECT count(*) FROM paimon.peek_messages();

-- (4) names Paimon or the warehouse cannot take
CREATE TABLE "id$x" (id int PRIMARY KEY);
CREATE TABLE "id:x" (id int PRIMARY KEY);
CREATE TABLE "id/x" (id int PRIMARY KEY);
CREATE TABLE "." (id int PRIMARY KEY);
CREATE TABLE ".." (id int PRIMARY KEY);
CREATE SCHEMA sys;
CREATE TABLE sys.t (id int PRIMARY KEY);
CREATE SCHEMA "s:x";
CREATE TABLE "s:x".t (id int PRIMARY KEY);
SELECT paimon.add_table('"id$x"'::regclass);                                    -- expect ERROR
SELECT paimon.add_table('"id:x"'::regclass);                                    -- expect ERROR
SELECT paimon.add_table('"id/x"'::regclass);                                    -- expect ERROR
SELECT paimon.add_table('"."'::regclass);                                       -- expect ERROR
SELECT paimon.add_table('".."'::regclass);                                      -- expect ERROR
SELECT paimon.add_table('sys.t'::regclass);                                     -- expect ERROR
SELECT paimon.add_table('"s:x".t'::regclass);                                   -- expect ERROR
-- the neighbours, all measured to work in Paimon: a dot, upper case, non-ASCII, a schema
-- whose name merely contains "sys"
CREATE TABLE "a.b" (id int PRIMARY KEY);
CREATE TABLE "Upper" (id int PRIMARY KEY);
CREATE TABLE "naïve_表" (id int PRIMARY KEY);
CREATE SCHEMA "sys2";
CREATE TABLE sys2.t (id int PRIMARY KEY);
SELECT paimon.add_table('"a.b"'::regclass);
SELECT paimon.add_table('"Upper"'::regclass);
SELECT paimon.add_table('"naïve_表"'::regclass);
SELECT paimon.add_table('sys2.t'::regclass);
SELECT j->>'schema' AS sch, j->>'table' AS tbl FROM (SELECT paimon.peek_messages() AS j) s ORDER BY 1, 2;
-- column names: Paimon's _KEY_ prefix (case-sensitive), no visible character, the writer's
-- metadata fields; then the accepted neighbours
CREATE TABLE cn_t (id int PRIMARY KEY);
SELECT paimon.add_table('cn_t'::regclass);
ALTER TABLE cn_t ADD COLUMN "_KEY_a" int;                                       -- expect ERROR
ALTER TABLE cn_t ADD COLUMN " " int;                                            -- expect ERROR
ALTER TABLE cn_t ADD COLUMN _seq int;                                           -- expect ERROR
ALTER TABLE cn_t ADD COLUMN _op_seq int;                                        -- expect ERROR
ALTER TABLE cn_t ADD COLUMN _xid int;                                           -- expect ERROR
ALTER TABLE cn_t ADD COLUMN a int;
ALTER TABLE cn_t RENAME COLUMN a TO _seq;                                       -- expect ERROR
ALTER TABLE cn_t ADD COLUMN "_key_a" int, ADD COLUMN "_SEQ" int, ADD COLUMN " x" int;
SELECT jsonb_path_query_array(cols, '$[*].name') FROM paimon.table_registry
WHERE relid = 'cn_t'::regclass;
DROP TABLE "id$x", "id:x", "id/x", ".", "..", sys.t, "s:x".t, "a.b", "Upper", "naïve_表",
           sys2.t, cn_t;
DROP SCHEMA sys, "s:x", sys2;
SELECT count(*) FROM paimon.peek_messages();

-- (5) ALTER COLUMN ... TYPE: only exact widenings
SET client_min_messages = error;     -- json's unmapped-type WARNING repeats on every ALTER
CREATE TABLE tc_t (id int PRIMARY KEY, i2 smallint, i4 int, i8 bigint, f4 real, f8 float8,
                   n numeric(10,2), t text, v varchar(5), c char(5), ts timestamp(3),
                   tz timestamptz, j json, k int);
SELECT paimon.add_table('tc_t'::regclass);
SELECT count(*) FROM paimon.peek_messages();
ALTER TABLE tc_t ALTER COLUMN i4 TYPE smallint;                                 -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN i8 TYPE int;                                      -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN i4 TYPE numeric(9,0);                             -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN f8 TYPE real;                                     -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN f4 TYPE numeric(10,2);                            -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN n TYPE numeric(9,2);                              -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN n TYPE numeric(10,1);                             -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN n TYPE numeric(10,4);                             -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN n TYPE float8;                                    -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN n TYPE text;  -- expect ERROR (PostgreSQL: the guard does not fit text)
ALTER TABLE tc_t ALTER COLUMN t TYPE char(10);                                  -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN c TYPE char(10);                                  -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN c TYPE text;                                      -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN ts TYPE timestamp(0);                             -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN ts TYPE timestamptz;                              -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN tz TYPE timestamp;                                -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN j TYPE jsonb;                                     -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN t TYPE int USING t::int;                          -- expect ERROR
-- USING is refused even when the type does not change: the values do
ALTER TABLE tc_t ALTER COLUMN k TYPE int USING k + 1;                           -- expect ERROR
ALTER TABLE tc_t ALTER COLUMN k TYPE bigint USING k;                            -- expect ERROR
DO $$ BEGIN EXECUTE 'ALTER TABLE tc_t ALTER COLUMN k TYPE bigint USING k * 2'; END $$; -- expect ERROR
SELECT count(*) FROM paimon.peek_messages();                         -- nothing was emitted
-- the widenings
ALTER TABLE tc_t ALTER COLUMN i2 TYPE int;
ALTER TABLE tc_t ALTER COLUMN i4 TYPE bigint;
ALTER TABLE tc_t ALTER COLUMN i8 TYPE numeric(19,0);
ALTER TABLE tc_t ALTER COLUMN k TYPE text;
ALTER TABLE tc_t ALTER COLUMN f4 TYPE float8;
ALTER TABLE tc_t ALTER COLUMN n TYPE numeric(14,4);
ALTER TABLE tc_t ALTER COLUMN v TYPE varchar(10);
ALTER TABLE tc_t ALTER COLUMN v TYPE text;
ALTER TABLE tc_t ALTER COLUMN t TYPE varchar(20);
ALTER TABLE tc_t ALTER COLUMN ts TYPE timestamp(6);
SELECT paimon.peek_messages() - 'relid';
-- the retyped numeric kept its guard; i8, now numeric, gained one
SELECT conname, pg_get_expr(conbin, conrelid) FROM pg_constraint
WHERE conrelid = 'tc_t'::regclass AND contype = 'c' ORDER BY conname;
DROP TABLE tc_t;
RESET client_min_messages;
SELECT count(*) FROM paimon.peek_messages();

-- (6) inheritance and partitions
CREATE TABLE ih_par (id int PRIMARY KEY, v text);
SELECT paimon.add_table('ih_par'::regclass);
CREATE TABLE ih_child () INHERITS (ih_par);                                     -- expect ERROR
CREATE TABLE ih_other (id int PRIMARY KEY, v text);
ALTER TABLE ih_other INHERIT ih_par;                                            -- expect ERROR
CREATE TABLE ih_p2 (id int PRIMARY KEY, v text);
CREATE TABLE ih_c2 () INHERITS (ih_p2);
SELECT paimon.add_table('ih_p2'::regclass);                                     -- expect ERROR
-- a replicated CHILD is fine, and its parent's ALTER now reaches its registry
CREATE TABLE ih_rc (id int PRIMARY KEY, v text);
SELECT paimon.add_table('ih_rc'::regclass);
ALTER TABLE ih_rc INHERIT ih_p2;
ALTER TABLE ih_p2 ADD COLUMN z int;
ALTER TABLE ih_p2 ADD COLUMN zz int DEFAULT 1;                                  -- expect ERROR
CREATE TABLE pt_par (id int, k int, PRIMARY KEY (id, k)) PARTITION BY LIST (k);
CREATE TABLE pt_1 PARTITION OF pt_par FOR VALUES IN (1);
SELECT paimon.add_table('pt_1'::regclass);
ALTER TABLE pt_par ADD COLUMN extra numeric(10,2);
ALTER TABLE pt_par ALTER COLUMN extra TYPE numeric(8,2);                        -- expect ERROR
SELECT j->>'op' AS op, j->>'table' AS tbl, j->'col'->>'name' AS col
FROM (SELECT paimon.peek_messages() AS j) s;
SELECT jsonb_path_query_array(cols, '$[*].name') FROM paimon.table_registry
WHERE relid IN ('ih_rc'::regclass, 'pt_1'::regclass) ORDER BY table_name;
SELECT conname FROM pg_constraint WHERE conrelid = 'pt_1'::regclass AND contype = 'c';
DROP TABLE ih_par, ih_other, ih_c2, ih_rc, ih_p2, pt_par;
SELECT count(*) FROM paimon.peek_messages();

-- (7) types whose every value changes text without a row change
CREATE TYPE ty_comp AS (a int);
CREATE TYPE ty_free AS (a int);
CREATE TYPE ty_mood AS ENUM ('a', 'b');
CREATE TYPE ty_mood2 AS ENUM ('a', 'b');
CREATE TYPE ty_unused AS ENUM ('a');
SET client_min_messages = error;
CREATE TABLE ty_t (id int PRIMARY KEY, c ty_comp, m ty_mood[]);
SELECT paimon.add_table('ty_t'::regclass);
RESET client_min_messages;
ALTER TYPE ty_comp ADD ATTRIBUTE b int;                                         -- expect ERROR
ALTER TYPE ty_mood RENAME VALUE 'a' TO 'A';              -- expect ERROR (used inside an array)
ALTER TYPE ty_mood ADD VALUE 'c';
ALTER TYPE ty_mood RENAME TO ty_mood_renamed;           -- a type rename changes no value text
ALTER TYPE ty_free ADD ATTRIBUTE b int;
ALTER TYPE ty_unused RENAME VALUE 'a' TO 'A';
ALTER TYPE ty_mood2 RENAME VALUE 'a' TO 'A';
SELECT count(*) FROM paimon.peek_messages();
DROP TABLE ty_t;
DROP TYPE ty_comp, ty_free, ty_mood_renamed, ty_mood2, ty_unused;
SELECT count(*) FROM paimon.peek_messages();

-- (8) the publication cannot be changed in a way that silently stops changes
CREATE TABLE pub_t (id int PRIMARY KEY, a int, b text);
SELECT paimon.add_table('pub_t'::regclass);
ALTER PUBLICATION paimon_pub DROP TABLE pub_t;                                  -- expect ERROR
ALTER PUBLICATION paimon_pub SET (publish = 'insert');                          -- expect ERROR
ALTER PUBLICATION paimon_pub SET (publish = 'insert, update, delete');          -- expect ERROR
ALTER PUBLICATION paimon_pub SET (publish_via_partition_root = true);           -- expect ERROR
-- SET TABLE restating every replicated table, with pub_t given a column list, a row filter,
-- or neither (the last is a no-op restatement, and allowed)
CREATE FUNCTION pg_temp.restate(pub_t_spec text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'ALTER PUBLICATION paimon_pub SET TABLE '
        || (SELECT string_agg(CASE WHEN relid = 'pub_t'::regclass THEN pub_t_spec
                                   ELSE 'ONLY ' || relid::regclass::text END, ', ')
              FROM paimon.table_registry);
END $$;
SELECT pg_temp.restate('ONLY pub_t (id, a)');                                   -- expect ERROR
SELECT pg_temp.restate('ONLY pub_t WHERE (a > 0)');                             -- expect ERROR
SELECT pg_temp.restate('ONLY pub_t');
ALTER PUBLICATION paimon_pub RENAME TO renamed_pub;                             -- expect ERROR
DROP PUBLICATION paimon_pub;                                                    -- expect ERROR
-- the neighbours: an untracked table may come and go, the owner may change
CREATE TABLE pub_plain (id int PRIMARY KEY);
ALTER PUBLICATION paimon_pub ADD TABLE pub_plain;
ALTER PUBLICATION paimon_pub DROP TABLE pub_plain;
ALTER PUBLICATION paimon_pub OWNER TO CURRENT_USER;
SELECT * FROM paimon.publication_problems();
-- remove_table and DROP TABLE still leave the publication, legitimately
SELECT paimon.remove_table('pub_t'::regclass);
SELECT count(*) FROM pg_publication_tables WHERE pubname = 'paimon_pub' AND tablename = 'pub_t';
-- its history table stays behind, so the same name cannot simply be registered again
SELECT paimon.add_table('pub_t'::regclass);                                    -- expect ERROR
SELECT paimon.forget_history('public', 'pub_t_hist');
SELECT paimon.add_table('pub_t'::regclass);
DROP TABLE pub_t, pub_plain;
SELECT count(*) FROM pg_publication_tables WHERE pubname = 'paimon_pub';
SELECT count(*) > 0 AS drained FROM paimon.peek_messages();

-- (9) session_replication_role = replica no longer silences the guardrails
CREATE TABLE srr_t (id int PRIMARY KEY);
SELECT paimon.add_table('srr_t'::regclass);
SELECT evtname, evtenabled FROM pg_event_trigger WHERE evtname LIKE 'paimon%' ORDER BY 1;
SET session_replication_role = replica;
ALTER TABLE srr_t ADD COLUMN d int DEFAULT 5;                                   -- expect ERROR
ALTER TABLE srr_t REPLICA IDENTITY DEFAULT;                                     -- expect ERROR
ALTER TABLE srr_t ADD COLUMN e int;
RESET session_replication_role;
SELECT paimon.peek_messages() - 'relid';

-- (10) paimon.health(): what the guardrails keep true, checked from the catalog
SELECT * FROM paimon.health();
ALTER EVENT TRIGGER paimon_ddl_end ENABLE;   -- back to ORIGIN: silent under replica role
ALTER EVENT TRIGGER paimon_sql_drop DISABLE;
SELECT * FROM paimon.health();
ALTER EVENT TRIGGER paimon_ddl_end DISABLE;
ALTER TABLE srr_t ADD COLUMN unseen int;     -- bypasses the trigger: the registry goes stale
ALTER TABLE srr_t REPLICA IDENTITY DEFAULT;
SELECT * FROM paimon.health();
ALTER TABLE srr_t REPLICA IDENTITY FULL;
ALTER TABLE srr_t DROP COLUMN unseen;
ALTER EVENT TRIGGER paimon_ddl_end ENABLE ALWAYS;
ALTER EVENT TRIGGER paimon_sql_drop ENABLE ALWAYS;
SELECT * FROM paimon.health();
DROP TABLE srr_t;
SELECT count(*) > 0 AS drained FROM paimon.peek_messages();

-- (11) DDL messages carry the installation's credential; nobody else can emit one
CREATE TABLE au_t (id int PRIMARY KEY);
SELECT paimon.add_table('au_t'::regclass);
SELECT position(format('"auth": "%s"', (SELECT token FROM paimon.secret)) IN data) > 0 AS has_auth
FROM pg_logical_slot_get_changes('paimon_test', NULL, NULL)
WHERE data LIKE 'message:%';
SELECT length(token) >= 64 AS long_token FROM paimon.secret;
CREATE ROLE paimon_nobody;
SELECT has_table_privilege('paimon_nobody', 'paimon.secret', 'SELECT') AS can_read_secret,
       has_function_privilege('paimon_nobody', 'paimon.emit(jsonb)', 'EXECUTE') AS can_emit;
DROP ROLE paimon_nobody;
DROP TABLE au_t;
SELECT count(*) FROM paimon.peek_messages();
SELECT pg_drop_replication_slot('paimon_test');

-- (12) operation history tables and key-less operation logs
SELECT slot_name FROM pg_create_logical_replication_slot('paimon_test', 'test_decoding');
-- key-less: accepted, REPLICA IDENTITY FULL set, the create message carries an empty key
CREATE TABLE kl_t (a int, b text NOT NULL);
SELECT paimon.add_table('kl_t'::regclass);
SELECT pk_cols, hist_table, backfill_due FROM paimon.table_registry WHERE relid = 'kl_t'::regclass;
SELECT relreplident FROM pg_class WHERE oid = 'kl_t'::regclass;
SELECT paimon.peek_messages() - 'relid';
-- the guardrails apply to it as to any table
ALTER TABLE kl_t REPLICA IDENTITY DEFAULT;                                      -- expect ERROR
ALTER TABLE kl_t ADD COLUMN d int DEFAULT 1;                                    -- expect ERROR
ALTER TABLE kl_t ADD PRIMARY KEY (b);                                           -- expect ERROR
-- the log's own field names are reserved on a key-less table, and only there
ALTER TABLE kl_t ADD COLUMN _lsn int;                                           -- expect ERROR
ALTER TABLE kl_t RENAME COLUMN a TO _op;                                        -- expect ERROR
ALTER TABLE kl_t ADD COLUMN _ts timestamptz;                                    -- expect ERROR
ALTER TABLE kl_t ADD COLUMN _commit_lsn bigint;                                 -- expect ERROR
CREATE TABLE kl_bad (x int, _ts timestamptz);
SELECT paimon.add_table('kl_bad'::regclass);                                    -- expect ERROR
CREATE TABLE pk_ts (id int PRIMARY KEY, _ts timestamptz, _lsn bigint);
SELECT paimon.add_table('pk_ts'::regclass);
ALTER TABLE kl_t ADD COLUMN c int;
SELECT paimon.peek_messages() - 'relid';
-- name collisions: T_hist is a replicated table, or T is a replicated table's history
CREATE TABLE col_a (id int PRIMARY KEY);
CREATE TABLE col_a_hist (id int PRIMARY KEY);
SELECT paimon.add_table('col_a'::regclass);
SELECT paimon.add_table('col_a_hist'::regclass);                                -- expect ERROR
CREATE TABLE col_b_hist (id int PRIMARY KEY);
CREATE TABLE col_b (id int PRIMARY KEY);
SELECT paimon.add_table('col_b_hist'::regclass);
SELECT paimon.add_table('col_b'::regclass);                                     -- expect ERROR
-- other schemas are other Paimon databases: no collision
CREATE SCHEMA s12;
CREATE TABLE s12.col_a_hist (id int PRIMARY KEY);
SELECT paimon.add_table('s12.col_a_hist'::regclass);
-- DROP TABLE leaves the history table: re-creating the name is refused, with the remedy
DROP TABLE col_a;
SELECT schema_name, hist_table, table_name, reason FROM paimon.retained_history
 WHERE hist_table LIKE 'col\_%' ORDER BY 1, 2;
SELECT count(*) FROM paimon.retained_history WHERE hist_table = 'reg_t_hist' AND reason = 'remove_table';
CREATE TABLE col_a (id int PRIMARY KEY, v text);
SELECT paimon.add_table('col_a'::regclass);                                     -- expect ERROR
-- and a table named like a retained history table would turn it into a main table
SELECT paimon.add_table('col_a_hist'::regclass);                                -- expect ERROR
SELECT paimon.forget_history('public', 'col_a_hist');
SELECT paimon.forget_history('public', 'col_a_hist');                           -- expect ERROR
SELECT paimon.add_table('col_a'::regclass);
SELECT table_name, hist_table FROM paimon.table_registry WHERE table_name LIKE 'col_%' ORDER BY 1;
-- health() reports a collision made behind the guardrails' back
SELECT * FROM paimon.health();
UPDATE paimon.table_registry SET table_name = 'col_b_hist_hist' WHERE table_name = 'col_a';
SELECT * FROM paimon.health();
UPDATE paimon.table_registry SET table_name = 'col_a' WHERE table_name = 'col_b_hist_hist';
SELECT * FROM paimon.health();
DROP TABLE kl_t, kl_bad, pk_ts, col_a, col_a_hist, col_b, col_b_hist, s12.col_a_hist;
DROP SCHEMA s12;
SELECT count(*) > 0 AS drained FROM paimon.peek_messages();
SELECT pg_drop_replication_slot('paimon_test');
