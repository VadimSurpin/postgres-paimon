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
