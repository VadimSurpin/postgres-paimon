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
