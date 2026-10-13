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
    buckets            int         NOT NULL DEFAULT 4,
    registered_at      timestamptz NOT NULL DEFAULT now()
);
SELECT pg_catalog.pg_extension_config_dump('paimon.table_registry', '');

-- Mirror of legacy:extension/type_map_pg.h.  Returns a Paimon type string.
-- quiet => true suppresses the unmapped-type WARNING, for health(), which re-derives every
-- table's columns on each call and would otherwise repeat it each time.
CREATE FUNCTION paimon.pg_type_to_paimon(typid oid, typmod int, not_null boolean,
                                         quiet boolean DEFAULT false)
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
                -- numeric's typmod (numeric.c, make_numeric_typmod): precision in the high
                -- 16 bits, scale in the low 11 bits as a SIGNED value. Since PostgreSQL 15
                -- the scale may be negative (numeric(5,-2)) or exceed the precision
                -- (numeric(3,5)); reading it as "raw & 65535" turned -2 into 2046 and
                -- produced DECIMAL(5, 2046). Decoded faithfully here so that
                -- reject_unsafe_numeric can refuse both forms by name.
                raw  := typmod - 4;
                prec := (raw >> 16) & 65535;
                scal := ((raw & 2047) # 1024) - 1024;
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
            IF NOT quiet THEN
                RAISE WARNING 'paimon_slot: type % has no Paimon mapping, using STRING',
                              format_type(typid, typmod);
            END IF;
            base := 'STRING';
    END CASE;

    IF not_null THEN
        RETURN base || ' NOT NULL';
    END IF;
    RETURN base;
END $$;

-- Ordered, live columns of a relation, with attnum as the Paimon field id.
CREATE FUNCTION paimon.table_cols(relid oid, quiet boolean DEFAULT false)
RETURNS jsonb LANGUAGE sql STABLE AS $$
    -- typid/typmod are the PostgreSQL type, kept beside the Paimon type so that sync_table
    -- can judge a type change by what PostgreSQL does to the values (see
    -- reject_unsafe_type_change), which the Paimon type alone cannot tell: varchar(5) and
    -- char(5) are both STRING. The writer ignores both keys.
    SELECT coalesce(jsonb_agg(jsonb_build_object(
               'id',     a.attnum,
               'name',   a.attname,
               'type',   paimon.pg_type_to_paimon(a.atttypid, a.atttypmod, a.attnotnull, quiet),
               'typid',  a.atttypid::bigint,
               'typmod', a.atttypmod)
             ORDER BY a.attnum), '[]'::jsonb)
    FROM pg_attribute a
    WHERE a.attrelid = relid AND a.attnum > 0 AND NOT a.attisdropped;
$$;

-- The columns as DDL messages carry them: the PostgreSQL type is the registry's business.
CREATE FUNCTION paimon.wire_cols(cols jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
    SELECT coalesce(jsonb_agg(c - 'typid' - 'typmod' ORDER BY o), '[]'::jsonb)
    FROM jsonb_array_elements(cols) WITH ORDINALITY AS x(c, o);
$$;

CREATE FUNCTION paimon.pk_cols(relid oid)
RETURNS text[] LANGUAGE sql STABLE AS $$
    SELECT coalesce(array_agg(a.attname ORDER BY k.ord), '{}'::text[])
    FROM pg_constraint c
    CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
    WHERE c.conrelid = relid AND c.contype = 'p';
$$;

/*
 * How many Paimon buckets a table gets, from the only signal available at registration:
 * its current total size on disk.
 *
 *     buckets = clamp(4, ceil(total_size / 1 GiB), 64)
 *
 * Fixed (static) bucketing is what removes Paimon's dynamic-bucket key index, which on a
 * measured TPC-C run was 1.2 GB of a 1.8 GB warehouse -- 63% of it -- for 88 MB of data.
 *
 * The floor of 4 is doing almost all the work in practice, and deliberately. The ordinary
 * registration path requires an EMPTY table (a non-empty one cannot be registered before the
 * writer's slot exists), where pg_total_relation_size is 16 kB and there is no signal at all;
 * one bucket would then give every new table a single-bucket hotspot. The size only says
 * anything when a populated table is re-registered after remove_table().
 *
 * The ceiling of 64 is because the writer is single-threaded: beyond that, more buckets only
 * mean more open files per commit.
 *
 * A guess, in other words, with buckets => N on add_table() as the control for when it is
 * wrong. The count is alterable afterwards, but Paimon does not redistribute existing data, so
 * changing it costs a rewrite of the table.
 */
CREATE FUNCTION paimon.default_buckets(relid oid) RETURNS int
LANGUAGE sql STABLE AS $$
    SELECT greatest(4, least(64,
        ceil(pg_total_relation_size(relid)::numeric / (1024 * 1024 * 1024))::int));
$$;

/*
 * Every option is spelled out although each is the default: check_publication() holds the
 * publication to exactly these, so the definition and the check say the same thing.
 */
CREATE PUBLICATION paimon_pub
    WITH (publish = 'insert, update, delete, truncate', publish_via_partition_root = false);

/*
 * The DDL messages' credential.
 *
 * pg_logical_emit_message() is executable by PUBLIC, so ANY role that can connect could put a
 * message with the 'paimon' prefix into the writer's stream: measured, one
 * SELECT pg_logical_emit_message(true, 'paimon', 'not json') by a role with no privileges
 * at all stopped the writer permanently (it replays on every restart), and a well-formed
 * forged {"op":"drop_table",...} would have purged a Paimon table. The writer therefore acts
 * only on messages carrying this token, which it reads at startup, and ignores (with a
 * WARNING) everything else under the prefix. Only roles that can read this table can emit a
 * message the writer accepts: the extension owner, and whoever is granted SELECT -- the
 * writer's own role must be.
 */
CREATE TABLE paimon.secret (
    token text NOT NULL
);
INSERT INTO paimon.secret VALUES (gen_random_uuid()::text || gen_random_uuid()::text);
REVOKE ALL ON paimon.secret FROM PUBLIC;

CREATE FUNCTION paimon.emit(payload jsonb) RETURNS void
LANGUAGE sql AS $$
    SELECT pg_logical_emit_message(true, 'paimon',
        (payload || jsonb_build_object('auth', (SELECT token FROM paimon.secret)))::text);
$$;
-- Not a public entry point: it is the extension's own path to the writer.
REVOKE ALL ON FUNCTION paimon.emit(jsonb) FROM PUBLIC;

/*
 * numeric guardrail.  numeric(p,s) with p <= 38 maps exactly to DECIMAL(p,s), so
 * no value it holds can be unrepresentable.  Plain numeric maps to DECIMAL(38,18)
 * but PostgreSQL holds far more; one such value would be a record the writer
 * refuses forever, stopping replication.  p > 38 exceeds Paimon's maximum.
 * The precision is read from pg_type_to_paimon's output so the typmod
 * arithmetic lives in one place.
 */
CREATE FUNCTION paimon.reject_unsafe_numeric(v_relid oid, v_attnum int) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    a    pg_attribute;
    prec int;
    scal int;
BEGIN
    SELECT * INTO a FROM pg_attribute WHERE attrelid = v_relid AND attnum = v_attnum;
    IF NOT FOUND OR a.atttypid <> 1700 THEN
        RETURN;
    END IF;

    IF a.atttypmod <= 4 THEN
        RAISE EXCEPTION 'paimon_slot: column "%" is numeric without precision', a.attname
              USING HINT = 'Paimon maps it to DECIMAL(38, 18), which cannot hold every value PostgreSQL allows; a value outside that range would stop replication permanently. Use numeric(p,s) with p <= 38. See design spec section 5.';
    END IF;

    prec := substring(paimon.pg_type_to_paimon(a.atttypid, a.atttypmod, false)
                      from 'DECIMAL\((\d+),')::int;
    scal := substring(paimon.pg_type_to_paimon(a.atttypid, a.atttypmod, false)
                      from 'DECIMAL\(\d+, (-?\d+)\)')::int;
    IF prec > 38 THEN
        RAISE EXCEPTION 'paimon_slot: column "%" is numeric(%) but Paimon DECIMAL supports at most 38 digits',
                        a.attname, prec
              USING HINT = 'Use numeric(p,s) with p <= 38. See design spec section 5.';
    END IF;

    /*
     * PostgreSQL 15 allows a negative scale (numeric(5,-2) rounds to hundreds) and a scale
     * above the precision (numeric(3,5) holds 0.00123). Paimon's DECIMAL allows neither, so the
     * writer's createTable or addColumn would fail -- after PostgreSQL has committed, which
     * stops replication for good. Refused here, naming the type that holds the same values.
     */
    IF scal < 0 THEN
        RAISE EXCEPTION 'paimon_slot: column "%" is numeric(%,%), a negative scale, which Paimon DECIMAL cannot represent',
                        a.attname, prec, scal
              USING HINT = format('Use numeric(%s,0), which holds the same values. See design spec section 5.',
                                  least(38, prec - scal));
    END IF;
    IF scal > prec THEN
        RAISE EXCEPTION 'paimon_slot: column "%" is numeric(%,%), a scale larger than the precision, which Paimon DECIMAL cannot represent',
                        a.attname, prec, scal
              USING HINT = format('Use numeric(%s,%s), which holds the same values. See design spec section 5.',
                                  scal, scal);
    END IF;
END $$;

/*
 * Columns the stream cannot carry faithfully, whatever their values.
 *
 * A generated column (STORED or, the PostgreSQL 18 default, VIRTUAL) is never in the stream
 * paimon_pub publishes (publish_generated_columns = none), so its Paimon column would read
 * NULL for every streamed row; the initial load fails outright on it ("column ... is a
 * generated column", measured). Both are refused rather than half-replicated.
 *
 * A reg* column (regclass, regtype, ...) is stored as an OID and printed as a NAME looked up
 * at output time. Rename the object it points to and every such value prints differently,
 * with no row change and so nothing in the stream: measured, a regclass value read
 * 'target_t' in Paimon and 'renamed_t' in PostgreSQL after one ALTER TABLE ... RENAME. Store
 * the oid instead (the cast r::oid), which is stable.
 */
CREATE FUNCTION paimon.reject_unsupported_column(v_relid oid, v_attnum int) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    a pg_attribute;
    t oid;
BEGIN
    SELECT * INTO a FROM pg_attribute WHERE attrelid = v_relid AND attnum = v_attnum;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    IF a.attgenerated <> '' THEN
        RAISE EXCEPTION 'paimon_slot: column "%" of % is a generated column, which logical replication does not carry',
                        a.attname, v_relid::regclass
              USING HINT = 'paimon_pub does not publish generated columns, so Paimon would hold NULL there for every row; drop the expression (ALTER COLUMN ... DROP EXPRESSION) or leave the column out of the replicated table';
    END IF;
    -- the column's own type, an array's element type, and down a chain of domains
    t := a.atttypid;
    LOOP
        IF t IN ('regclass'::regtype, 'regtype'::regtype, 'regproc'::regtype,
                 'regprocedure'::regtype, 'regoper'::regtype, 'regoperator'::regtype,
                 'regconfig'::regtype, 'regdictionary'::regtype, 'regnamespace'::regtype,
                 'regrole'::regtype, 'regcollation'::regtype) THEN
            RAISE EXCEPTION 'paimon_slot: column "%" of % has type %, whose text form is a catalog lookup',
                            a.attname, v_relid::regclass, format_type(a.atttypid, a.atttypmod)
                  USING HINT = 'renaming the object a value points to changes how every such value prints, with nothing in the replication stream, so Paimon would silently keep the old names; store the oid instead (an oid column, or the cast value::oid)';
        END IF;
        SELECT CASE WHEN typtype = 'd' THEN typbasetype
                    WHEN typelem <> 0 AND typlen = -1 THEN typelem END
          INTO t FROM pg_type WHERE oid = t;
        EXIT WHEN t IS NULL;
    END LOOP;
END $$;

/*
 * Names Paimon or the warehouse filesystem cannot take as a table's identity. The Paimon
 * identity is fixed at add_table() (RENAME TO and SET SCHEMA deliberately do not move it),
 * so this is the only place to check it. Each case is measured against Paimon 1.4.1 on a
 * local warehouse:
 *   - '$' in a table name makes it a system-table reference ("Cannot 'createTable' for
 *     system table"): the writer could never create it;
 *   - ':' in a table or schema name is a URI scheme separator ("Relative path in absolute
 *     URI");
 *   - '/' nests the table INSIDE another table's directory: 'a/b' lives in table a's
 *     directory, and dropping table a deleted a/b's data with it;
 *   - '.' and '..' resolve to the database directory and the warehouse root;
 *   - a schema named 'sys' is Paimon's system database ("Can't do operation on system
 *     database").
 * All of these would be found by the writer only after PostgreSQL committed, which stops
 * replication; refusing here costs a rename.
 */
CREATE FUNCTION paimon.reject_unsafe_identity(v_schema text, v_table text) RETURNS void
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v_table ~ '[$:/]' OR v_table IN ('.', '..') THEN
        RAISE EXCEPTION 'paimon_slot: table name "%" cannot be a Paimon table name', v_table
              USING HINT = 'Paimon reads "$" as a system-table suffix, ":" as a URI scheme, and "/", "." and ".." as paths in the warehouse; rename the table before paimon.add_table() (a later RENAME TO does not change the Paimon name)';
    END IF;
    IF v_schema ~ '[:/]' OR v_schema = 'sys' THEN
        RAISE EXCEPTION 'paimon_slot: schema name "%" cannot be a Paimon database name', v_schema
              USING HINT = 'the schema becomes the Paimon database; Paimon reserves "sys" and reads ":" and "/" as parts of a path; move the table to another schema before paimon.add_table()';
    END IF;
END $$;

/*
 * Value guards: a CHECK constraint, owned by paimon_slot, on every column whose type can
 * hold a value Paimon cannot represent. Without it, one ordinary INSERT of such a value is a
 * poison record: PostgreSQL commits it, the writer can never apply it, and replication stops
 * for the whole database (design spec section 8). Refusing it before commit is the only
 * place that cannot happen.
 *
 *   numeric(p,s)  NaN. (Infinity cannot reach a numeric(p,s) column: PostgreSQL itself
 *                 refuses it, "cannot hold an infinite value". Out-of-range digits cannot
 *                 either, because p <= 38 is enforced and DECIMAL(p,s) is exact.)
 *   timestamp,    anything after 294247-01-10 04:00:54.775807 (UTC for timestamptz), the
 *   timestamptz   largest instant Paimon can write (TextDecoder explains why), except
 *                 'infinity', which the writer encodes. PostgreSQL's own maximum is about
 *                 thirty years later. The lower end needs nothing: PostgreSQL's minimum,
 *                 4714-11-24 BC, is far inside Paimon's range.
 *   date          nothing: PostgreSQL's whole range fits Paimon's (measured both extremes).
 *
 * Named paimon_guard_<attnum>, one per column. sync_table() refuses dropping or renaming one
 * on a replicated column; remove_table() drops them; add_guards() adds them.
 */
CREATE FUNCTION paimon.guard_expr(v_relid oid, v_attnum int) RETURNS text
LANGUAGE plpgsql STABLE AS $$
DECLARE
    a pg_attribute;
    c text;
BEGIN
    SELECT * INTO a FROM pg_attribute
     WHERE attrelid = v_relid AND attnum = v_attnum AND NOT attisdropped;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    c := quote_ident(a.attname);
    -- Spelled the way pg_get_expr() prints it back, so guard_present() can compare the two.
    -- The timestamp constants go through ::text, the same output function the deparser
    -- uses, so the comparison holds under any DateStyle or TimeZone.
    RETURN CASE a.atttypid
        WHEN 'numeric'::regtype THEN
            format('(%s <> ''NaN''::numeric)', c)
        WHEN 'timestamp'::regtype THEN
            format('((%1$s <= %2$L::timestamp without time zone) OR (%1$s = ''infinity''::timestamp without time zone))',
                   c, ('294247-01-10 04:00:54.775807'::timestamp)::text)
        WHEN 'timestamptz'::regtype THEN
            format('((%1$s <= %2$L::timestamp with time zone) OR (%1$s = ''infinity''::timestamp with time zone))',
                   c, ('294247-01-10 04:00:54.775807+00'::timestamptz)::text)
    END;
END $$;

CREATE FUNCTION paimon.guard_name(v_attnum int) RETURNS name
LANGUAGE sql IMMUTABLE AS $$ SELECT ('paimon_guard_' || v_attnum)::name $$;

/* Whether the column's guard is in place, by name AND by expression: a same-named
 * CHECK (true) is not a guard. */
CREATE FUNCTION paimon.guard_present(v_relid oid, v_attnum int) RETURNS boolean
LANGUAGE plpgsql STABLE AS $$
DECLARE
    want text := paimon.guard_expr(v_relid, v_attnum);
BEGIN
    IF want IS NULL THEN
        RETURN true;
    END IF;
    RETURN EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conrelid = v_relid AND contype = 'c'
                      AND conname = paimon.guard_name(v_attnum)
                      AND pg_get_expr(conbin, conrelid) = want);
END $$;

/*
 * Adds the guards the given columns need and do not have yet, in ONE ALTER TABLE: that
 * statement fires the event triggers in turn, and sync_table must find every guard of the
 * table in place when it does.
 *
 * validate => true (add_table) checks the rows already in the table, once, under the lock
 * add_table holds anyway, and turns a violation into an error naming the problem: a value
 * already present would otherwise poison the initial load. validate => false (sync_table, for
 * a column just added or retyped on a live table) adds them NOT VALID: new and updated rows
 * are checked from this statement on, which is everything the stream will carry, and the
 * existing rows need no scan -- an added column is NULL in all of them (ADD COLUMN with a
 * default is refused), and a retyped column only arrives here from a type that could not hold
 * the value either (type_change_problem allows only exact widenings).
 */
CREATE FUNCTION paimon.add_guards(v_relid oid, attnums int[], validate boolean) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    n       int;
    clauses text[] := '{}';
    exprs   text[] := '{}';
BEGIN
    FOREACH n IN ARRAY attnums
    LOOP
        CONTINUE WHEN paimon.guard_expr(v_relid, n) IS NULL OR paimon.guard_present(v_relid, n);
        IF EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = v_relid AND conname = paimon.guard_name(n)) THEN
            RAISE EXCEPTION 'paimon_slot: constraint name "%" on % is reserved for paimon_slot',
                            paimon.guard_name(n), v_relid::regclass
                  USING HINT = 'paimon_slot names the value guard of a column paimon_guard_<attnum>; rename or drop that constraint first';
        END IF;
        clauses := clauses || format('ADD CONSTRAINT %I CHECK %s%s',
                                     paimon.guard_name(n), paimon.guard_expr(v_relid, n),
                                     CASE WHEN validate THEN '' ELSE ' NOT VALID' END);
        exprs := exprs || paimon.guard_expr(v_relid, n);
    END LOOP;
    IF cardinality(clauses) = 0 THEN
        RETURN;
    END IF;
    BEGIN
        EXECUTE format('ALTER TABLE %s %s', v_relid::regclass, array_to_string(clauses, ', '));
    EXCEPTION WHEN check_violation THEN
        RAISE EXCEPTION 'paimon_slot: % already holds a value Paimon cannot represent', v_relid::regclass
              USING HINT = 'numeric NaN, or a timestamp after 294247-01-10 04:00:54.775807 (''infinity'' is fine); find the rows with SELECT * FROM '
                           || v_relid::regclass || ' WHERE NOT (' || array_to_string(exprs, ' AND ')
                           || '), correct them, and add the table again';
    END;
END $$;

/*
 * Which ALTER COLUMN ... TYPE a replicated column may take. NULL when allowed, otherwise
 * the reason.
 *
 * PostgreSQL rewrites the table for a type change and NOTHING of that rewrite reaches the
 * stream, so every row already in Paimon keeps its old value, read back through Paimon's own
 * cast. Measured on Paimon 1.4.1 across every pair of mapped types: that cast silently wraps
 * narrowing integers (INT 2147483647 read back as SMALLINT -1, BIGINT max as INT -1), makes
 * the writer's own compaction throw on a narrowed DECIMAL or a STRING -> BOOLEAN, reinterprets
 * integers as timestamps, and Paimon refuses outright about a third of the pairs -- each of
 * those after PostgreSQL has committed, which stops replication. And a change that keeps the
 * Paimon type can still change the value in PostgreSQL alone: char(n) pads, timestamp(p)
 * rounds, json -> jsonb normalises.
 *
 * So only the changes whose old values are, exactly, what PostgreSQL now holds:
 *   - smallint -> int -> bigint;
 *   - an integer -> numeric(p,s) with room for all its digits, or -> text/varchar;
 *   - real -> double precision;
 *   - numeric(p,s) -> numeric(p',s') with s' >= s and p'-s' >= p-s;
 *   - text <-> varchar(n), and varchar(n) -> varchar(m);
 *   - timestamp(p) -> timestamp(q) for q >= p, and the same for timestamptz.
 * ALTER COLUMN ... TYPE ... USING is refused separately (on_table_rewrite), because USING can
 * change the values of any of these.
 */
CREATE FUNCTION paimon.type_change_problem(old_typid oid, old_typmod int,
                                           new_typid oid, new_typmod int)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    ints    oid[] := ARRAY['smallint'::regtype, 'integer'::regtype, 'bigint'::regtype]::oid[];
    digits  int[] := ARRAY[5, 10, 19];
    strs    oid[] := ARRAY['text'::regtype, 'varchar'::regtype]::oid[];
    op int; os int; np int; ns int;
BEGIN
    IF old_typid = new_typid AND old_typmod = new_typmod THEN
        RETURN NULL;
    END IF;
    IF old_typid = ANY (ints) AND new_typid = ANY (ints) THEN
        IF array_position(ints, new_typid) > array_position(ints, old_typid) THEN
            RETURN NULL;
        END IF;
        RETURN 'it narrows an integer';
    END IF;
    IF old_typid = ANY (ints) AND new_typid = 'numeric'::regtype THEN
        np := ((new_typmod - 4) >> 16) & 65535;
        ns := (((new_typmod - 4) & 2047) # 1024) - 1024;
        IF new_typmod > 4 AND np - ns >= digits[array_position(ints, old_typid)] THEN
            RETURN NULL;
        END IF;
        RETURN 'the numeric has too few integer digits for every value of the integer type';
    END IF;
    IF old_typid = ANY (ints) AND new_typid = ANY (strs) THEN
        RETURN NULL;
    END IF;
    IF old_typid = 'real'::regtype AND new_typid = 'double precision'::regtype THEN
        RETURN NULL;
    END IF;
    IF old_typid = 'numeric'::regtype AND new_typid = 'numeric'::regtype THEN
        IF old_typmod > 4 AND new_typmod > 4 THEN
            op := ((old_typmod - 4) >> 16) & 65535;
            os := (((old_typmod - 4) & 2047) # 1024) - 1024;
            np := ((new_typmod - 4) >> 16) & 65535;
            ns := (((new_typmod - 4) & 2047) # 1024) - 1024;
            IF ns >= os AND np - ns >= op - os THEN
                RETURN NULL;
            END IF;
        END IF;
        RETURN 'it removes integer digits or decimal places';
    END IF;
    IF old_typid = ANY (strs) AND new_typid = ANY (strs) THEN
        RETURN NULL;
    END IF;
    IF old_typid = new_typid AND old_typid IN ('timestamp'::regtype, 'timestamptz'::regtype) THEN
        IF new_typmod = -1 OR (old_typmod <> -1 AND new_typmod >= old_typmod) THEN
            RETURN NULL;
        END IF;
        RETURN 'it rounds the fractional seconds';
    END IF;
    RETURN 'Paimon could not apply it to the rows it already holds without changing them';
END $$;

/*
 * The publication's integrity, as a list of problems (empty when sound). A table missing
 * from paimon_pub, filtered by a row filter or a column list, or an action not published, is
 * data the writer never receives and cannot know it missed: measured, after
 * ALTER PUBLICATION paimon_pub SET (publish = 'insert') an UPDATE never reached Paimon; with
 * a column list the unlisted column arrived NULL; with a row filter or with the table dropped
 * from the publication the rows were simply absent. publish_via_partition_root would publish
 * a replicated partition's changes under its parent's identity, which the writer does not
 * track.
 */
CREATE FUNCTION paimon.publication_problems() RETURNS SETOF text
LANGUAGE plpgsql STABLE AS $$
DECLARE
    p   pg_publication;
    r   record;
    pt  record;
BEGIN
    SELECT * INTO p FROM pg_publication WHERE pubname = 'paimon_pub';
    IF NOT FOUND THEN
        RETURN NEXT 'publication paimon_pub does not exist (dropped or renamed)';
        RETURN;
    END IF;
    IF NOT (p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate) THEN
        RETURN NEXT 'publication paimon_pub does not publish every action (publish must be ''insert, update, delete, truncate'')';
    END IF;
    IF p.pubviaroot THEN
        RETURN NEXT 'publication paimon_pub has publish_via_partition_root = true';
    END IF;
    FOR r IN SELECT g.relid, n.nspname, c.relname
               FROM paimon.table_registry g
               JOIN pg_class c ON c.oid = g.relid
               JOIN pg_namespace n ON n.oid = c.relnamespace
              ORDER BY g.relid
    LOOP
        SELECT * INTO pt FROM pg_publication_tables
         WHERE pubname = 'paimon_pub' AND schemaname = r.nspname AND tablename = r.relname;
        IF NOT FOUND THEN
            RETURN NEXT format('replicated table %s is not in publication paimon_pub', r.relid::regclass);
        ELSIF pt.rowfilter IS NOT NULL THEN
            RETURN NEXT format('replicated table %s has a row filter in publication paimon_pub', r.relid::regclass);
        ELSIF EXISTS (SELECT 1 FROM pg_attribute a
                       WHERE a.attrelid = r.relid AND a.attnum > 0 AND NOT a.attisdropped
                         AND NOT (a.attname = ANY (pt.attnames))) THEN
            RETURN NEXT format('replicated table %s has a column list in publication paimon_pub', r.relid::regclass);
        END IF;
    END LOOP;
END $$;

CREATE FUNCTION paimon.check_publication() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    problems text;
BEGIN
    SELECT string_agg(x, '; ') INTO problems FROM paimon.publication_problems() x;
    IF problems IS NOT NULL THEN
        RAISE EXCEPTION 'paimon_slot: this change to the publication would silently stop changes reaching Paimon: %', problems
              USING HINT = 'paimon_pub is managed by paimon.add_table() and paimon.remove_table(); it must publish every action of every replicated table, with no row filter or column list. To stop replicating a table use paimon.remove_table()';
    END IF;
END $$;

/*
 * Whether a type is used, directly or inside another type, by a replicated table's column;
 * returns the first such column as text, or NULL. "Inside" follows arrays, domains, ranges
 * and composite attributes, because each prints its contents' text.
 */
CREATE FUNCTION paimon.type_used_by_replicated(v_typid oid) RETURNS text
LANGUAGE sql STABLE AS $$
    WITH RECURSIVE users(typid) AS (
        SELECT v_typid
        UNION
        SELECT t.oid FROM pg_type t JOIN users u
            ON t.typelem = u.typid AND t.typlen = -1
            OR t.typbasetype = u.typid
            OR t.typrelid IN (SELECT a.attrelid FROM pg_attribute a
                               WHERE a.atttypid = u.typid AND a.attnum > 0 AND NOT a.attisdropped)
               AND t.typtype = 'c'
            OR t.oid IN (SELECT r.rngtypid FROM pg_range r WHERE r.rngsubtype = u.typid)
    )
    SELECT format('%s.%I', g.relid::regclass, a.attname)
      FROM paimon.table_registry g
      JOIN pg_attribute a ON a.attrelid = g.relid AND a.attnum > 0 AND NOT a.attisdropped
     WHERE a.atttypid IN (SELECT typid FROM users)
     ORDER BY g.relid, a.attnum
     LIMIT 1;
$$;

CREATE FUNCTION paimon.add_table(rel regclass,
                                 replica_identity text DEFAULT 'full',
                                 changelog_producer text DEFAULT 'input',
                                 buckets int DEFAULT NULL)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_relid  oid  := rel::oid;
    v_nsp    text;
    v_name   text;
    v_pk     text[];
    v_cols   jsonb;
    v_kind   "char";
    v_has_rows boolean;
    v_buckets  int;
BEGIN
    IF replica_identity <> 'full' THEN
        RAISE EXCEPTION 'paimon_slot: replica_identity => % is not implemented in v1',
                        replica_identity
              USING HINT = 'only ''full'' is supported; see design spec section 5';
    END IF;
    IF changelog_producer <> 'input' THEN
        RAISE EXCEPTION 'paimon_slot: changelog_producer => % is not implemented in v1',
                        changelog_producer
              USING HINT = 'only ''input'' is supported; see design spec section 5';
    END IF;
    IF buckets IS NOT NULL AND buckets < 1 THEN
        RAISE EXCEPTION 'paimon_slot: buckets => % must be a positive integer', buckets
              USING HINT = 'omit the argument to size it from the table (see paimon.default_buckets), or give a count of 1 or more';
    END IF;

    SELECT n.nspname, c.relname, c.relkind
      INTO v_nsp, v_name, v_kind
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = v_relid;

    IF v_kind <> 'r' THEN
        RAISE EXCEPTION 'paimon_slot: % is not an ordinary table (relkind %)', rel, v_kind;
    END IF;

    PERFORM paimon.reject_unsafe_identity(v_nsp, v_name);

    IF EXISTS (SELECT 1 FROM paimon.table_registry WHERE relid = v_relid) THEN
        RAISE EXCEPTION 'paimon_slot: % is already tracked', rel;
    END IF;

    -- Lock before reading pk/cols so a concurrent ADD COLUMN cannot leave stale cols in the registry.
    EXECUTE format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE', rel::text);

    v_pk := paimon.pk_cols(v_relid);
    IF cardinality(v_pk) = 0 THEN
        RAISE EXCEPTION 'paimon_slot: % has no primary key', rel
              USING HINT = 'a primary key is required; see design spec section 4';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conrelid = v_relid AND contype = 'p' AND condeferrable) THEN
        RAISE EXCEPTION 'paimon_slot: the primary key of % must not be DEFERRABLE', rel
              USING HINT = 'a deferred key permits transient duplicate keys that a Paimon primary-key table cannot represent; see design spec section 7';
    END IF;

    v_cols := paimon.table_cols(v_relid);
    PERFORM paimon.reject_reserved_name(c->>'name') FROM jsonb_array_elements(v_cols) c;
    PERFORM paimon.reject_unsafe_numeric(v_relid, (c->>'id')::int) FROM jsonb_array_elements(v_cols) c;
    PERFORM paimon.reject_unsupported_column(v_relid, (c->>'id')::int) FROM jsonb_array_elements(v_cols) c;
    PERFORM paimon.reject_inheritance(v_relid);

    -- ORDER IS LOAD-BEARING: do both ALTERs BEFORE inserting the registry row.
    -- The ddl_command_end event trigger decides what to emit by looking the
    -- table up in paimon.table_registry.  With no row yet it stays silent about
    -- these two ALTERs.  Insert the row first and the trigger sees our own
    -- ALTER TABLE as a change to a tracked table, emits a spurious message and
    -- may trip the PK guardrail from inside the statement that establishes the
    -- table.  Do not "tidy" this order.
    -- backfill_due reflects whether rows exist that the writer could have missed.
    -- The SHARE ROW EXCLUSIVE lock taken above conflicts with ROW EXCLUSIVE, so no
    -- concurrent INSERT/UPDATE/DELETE can commit between this check and the registry
    -- row; the emptiness test is therefore reliable, not a race.  That is why the lock
    -- must stay where it is.  An empty table has nothing to backfill: the create
    -- message plus streaming covers it fully.
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', rel::text) INTO v_has_rows;

    -- The resolved count, not the argument: the registry records what the table actually got,
    -- so the writer's startup bootstrap replays the same value it was created with.
    v_buckets := coalesce(buckets, paimon.default_buckets(v_relid));

    EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', rel::text);
    -- The value guards, validated against the rows already there (see add_guards).
    PERFORM paimon.add_guards(v_relid,
                              ARRAY(SELECT (c->>'id')::int FROM jsonb_array_elements(v_cols) c),
                              true);
    -- ONLY: without it PostgreSQL also publishes every inheritance child, whose rows the
    -- writer does not track. (reject_inheritance has refused a table with children anyway.)
    EXECUTE format('ALTER PUBLICATION paimon_pub ADD TABLE ONLY %s', rel::text);

    INSERT INTO paimon.table_registry
        (relid, schema_name, table_name, pk_cols, cols,
         backfill_due, replica_identity, changelog_producer, buckets)
    VALUES (v_relid, v_nsp, v_name, v_pk, v_cols,
            v_has_rows, replica_identity, changelog_producer, v_buckets);

    PERFORM paimon.emit(jsonb_build_object(
        'v', 1, 'op', 'create', 'relid', v_relid,
        'schema', v_nsp, 'table', v_name,
        'pk', to_jsonb(v_pk), 'cols', paimon.wire_cols(v_cols),
        'options', jsonb_build_object('changelog-producer', changelog_producer,
                                      'bucket', v_buckets)));
END $$;

CREATE FUNCTION paimon.remove_table(rel regclass) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    g name;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM paimon.table_registry WHERE relid = rel::oid) THEN
        RAISE EXCEPTION 'paimon_slot: % is not tracked', rel;
    END IF;
    -- ORDER IS LOAD-BEARING: the registry row goes FIRST. The event triggers guard every
    -- replicated table, i.e. every registry row: with the row still present, leaving the
    -- publication is refused by check_publication() and dropping a value guard by
    -- sync_table(). Once the row is gone the table is ordinary and both are allowed.
    DELETE FROM paimon.table_registry WHERE relid = rel::oid;
    EXECUTE format('ALTER PUBLICATION paimon_pub DROP TABLE %s', rel::text);
    -- The table is no longer replicated, so its value guards no longer have a reason.
    FOR g IN SELECT conname FROM pg_constraint
              WHERE conrelid = rel::oid AND contype = 'c' AND conname ~ '^paimon_guard_[0-9]+$'
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', rel::text, g);
    END LOOP;
END $$;

-- Called by the writer once a table's initial load has committed.
CREATE FUNCTION paimon.mark_backfilled(rel regclass) RETURNS void
LANGUAGE sql AS $$
    UPDATE paimon.table_registry SET backfill_due = false WHERE relid = rel::oid;
$$;

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
        IF r.data LIKE 'message: transactional:%prefix: paimon, sz:%' THEN
            -- The credential is random per installation, so it is not part of what tests see.
            RETURN NEXT substring(r.data from position('content:' in r.data) + 8)::jsonb - 'auth';
        END IF;
    END LOOP;
END $$;

-- Test-only: destructively drains slot paimon_test, so keep it away from non-superusers.
REVOKE ALL ON FUNCTION paimon.peek_messages() FROM PUBLIC;

/*
 * Paimon 1.4.1 reserves these field names and rejects any schema containing
 * one (ruling R4).  Compared case-insensitively.  The single definition of the
 * rule: add_table() and the ddl_command_end trigger both call it.
 */
CREATE FUNCTION paimon.reject_reserved_name(col_name text) RETURNS void
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF lower(col_name) = ANY (ARRAY['_value_kind', '_row_id', 'rowkind',
                                    '_level', '_sequence_number']) THEN
        RAISE EXCEPTION
            'paimon_slot: column name "%" is reserved by Paimon and cannot be replicated',
            col_name
              USING HINT = 'choose a different column name';
    END IF;
    /*
     * Also refused by Paimon 1.4.1 at createTable/addColumn, measured: a field name starting
     * with "_KEY_" (case-sensitive, Paimon's key-field prefix) and one with no non-whitespace
     * character. And the writer's own metadata fields, at their default names: a column
     * called _seq or _op_seq (or _xid, when PGPAIMON_XID_FIELD enables it) duplicates a field
     * the writer adds to every table ("must not contain duplicate fields").
     */
    IF col_name LIKE '\_KEY\_%' THEN
        RAISE EXCEPTION 'paimon_slot: column name "%" starts with _KEY_, which Paimon reserves', col_name
              USING HINT = 'choose a different column name';
    END IF;
    IF col_name !~ '[^[:space:]]' THEN
        RAISE EXCEPTION 'paimon_slot: column name "%" has no visible character, which Paimon refuses', col_name
              USING HINT = 'choose a different column name';
    END IF;
    IF col_name IN ('_seq', '_op_seq', '_xid') THEN
        RAISE EXCEPTION 'paimon_slot: column name "%" is a metadata field the writer adds to every Paimon table', col_name
              USING HINT = 'choose a different column name (the writer''s fields are set by PGPAIMON_SEQ_FIELD, PGPAIMON_OP_SEQ_FIELD and PGPAIMON_XID_FIELD; these are their defaults)';
    END IF;
END $$;

/*
 * Plain inheritance (not partitioning). A replicated table with inheritance CHILDREN is
 * refused: their rows are part of what "SELECT * FROM parent" returns but are published
 * under the children's own identities, which the writer does not track, so Paimon would
 * silently hold only the parent's own rows (measured). A replicated table may itself be a
 * child or a partition; on_ddl_end keeps it in step when its parent's ALTER reaches it.
 */
CREATE FUNCTION paimon.reject_inheritance(v_relid oid) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    child oid;
BEGIN
    SELECT inhrelid INTO child FROM pg_inherits WHERE inhparent = v_relid LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'paimon_slot: % has inheritance child % and cannot be replicated', v_relid::regclass, child::regclass
              USING HINT = 'rows of an inheritance child belong to the child: they would not reach the parent''s Paimon table although SELECT on the parent returns them; replicate each table separately (paimon.add_table on each), with no inheritance between replicated tables';
    END IF;
END $$;

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
/*
 * Diff one tracked table's registry state against the live table and emit the
 * resulting messages.  Called from both event triggers so that whenever the
 * registry's cols stops matching the live table, a message goes out in the
 * same transaction.  A no-op for untracked tables.
 */
CREATE FUNCTION paimon.sync_table(v_relid oid) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    reg       paimon.table_registry;
    new_cols  jsonb;
    new_pk    text[];
    c         jsonb;
    o         jsonb;
BEGIN
    SELECT * INTO reg FROM paimon.table_registry WHERE relid = v_relid;
    IF NOT FOUND THEN RETURN; END IF;

    new_pk   := paimon.pk_cols(v_relid);
    new_cols := paimon.table_cols(v_relid);

    /* Guardrail: no inheritance children (CREATE TABLE ... INHERITS, ALTER TABLE ... INHERIT;
     * on_ddl_end calls this for the parent when the child is the object of the command). */
    PERFORM paimon.reject_inheritance(v_relid);

    /* Guardrail: primary key may not be added, dropped or changed. */
    IF new_pk <> reg.pk_cols THEN
        RAISE EXCEPTION 'paimon_slot: cannot change the primary key of %.%',
                        reg.schema_name, reg.table_name
              USING HINT = 'the Paimon table key is fixed at add_table() time';
    END IF;

    /* Guardrail: a deferrable key (reachable by DROP CONSTRAINT + ADD PRIMARY KEY
     * ... DEFERRABLE in one statement, which leaves pk_cols unchanged). */
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conrelid = v_relid AND contype = 'p' AND condeferrable) THEN
        RAISE EXCEPTION 'paimon_slot: the primary key of %.% must not be DEFERRABLE',
                        reg.schema_name, reg.table_name
              USING HINT = 'a deferred key permits transient duplicate keys that a Paimon primary-key table cannot represent; see design spec section 7';
    END IF;

    /*
     * Guardrail: the writer needs the complete old row, so a tracked table's
     * replica identity must stay FULL.  This is why table_registry.replica_identity
     * is always 'full': add_table() sets it and this check keeps it so.  Checked
     * on the resulting pg_class.relreplident, which catches every spelling.
     */
    IF (SELECT relreplident FROM pg_class WHERE oid = v_relid) <> 'f' THEN
        RAISE EXCEPTION 'paimon_slot: replica identity of a replicated table must stay FULL'
              USING HINT = 'the writer needs the complete old row; see design spec section 5';
    END IF;

    /*
     * Everything below is derived from one before/after diff of the whole
     * statement's net effect (registry cols vs live cols, keyed by field
     * id), so combined actions such as DROP COLUMN x, ADD COLUMN y cannot
     * hide one another.  All guardrails run before anything is emitted.
     */
    FOR c IN SELECT * FROM jsonb_array_elements(new_cols)
    LOOP
        SELECT x INTO o FROM jsonb_array_elements(reg.cols) x
         WHERE (x->>'id') = (c->>'id');

        IF o IS NULL OR (o->>'type') <> (c->>'type') THEN
            PERFORM paimon.reject_unsafe_numeric(v_relid, (c->>'id')::int);
        END IF;

        IF o IS NULL OR (o->>'typid') IS DISTINCT FROM (c->>'typid')
                     OR (o->>'typmod') IS DISTINCT FROM (c->>'typmod') THEN
            PERFORM paimon.reject_unsupported_column(v_relid, (c->>'id')::int);
        ELSIF NOT paimon.guard_present(v_relid, (c->>'id')::int) THEN
            /*
             * Guardrail: a value guard may not be dropped or renamed (or replaced by a
             * same-named constraint that checks something else) while the column is
             * replicated. Without it the next NaN or out-of-range timestamp is a poison
             * record. remove_table() drops them once the table is no longer replicated.
             */
            RAISE EXCEPTION
                'paimon_slot: constraint "%" on %.% is the value guard of column "%" and must stay while the table is replicated',
                paimon.guard_name((c->>'id')::int), reg.schema_name, reg.table_name, c->>'name'
                  USING HINT = 'it keeps values Paimon cannot represent (numeric NaN, a timestamp after 294247-01-10 04:00:54.775807) out of the replication stream, where one would stop replication for good; paimon.remove_table() drops it';
        END IF;

        IF o IS NULL THEN
            /* added column */
            IF EXISTS (SELECT 1 FROM pg_attribute
                       WHERE attrelid = v_relid
                         AND attnum = (c->>'id')::int
                         AND atthasdef) THEN
                RAISE EXCEPTION
                    'paimon_slot: ADD COLUMN with a DEFAULT is not supported (%.%.%)',
                    reg.schema_name, reg.table_name, c->>'name'
                      USING HINT = 'add the column without a default, then UPDATE it';
            END IF;

            PERFORM paimon.reject_reserved_name(c->>'name');

            IF (c->>'type') LIKE '% NOT NULL' THEN
                RAISE EXCEPTION
                    'paimon_slot: ADD COLUMN ... NOT NULL without a default is not supported (%.%.%)',
                    reg.schema_name, reg.table_name, c->>'name';
            END IF;
        ELSE
            IF (o->>'name') <> (c->>'name') THEN
                PERFORM paimon.reject_reserved_name(c->>'name');
            END IF;

            /*
             * Guardrail: the type of a primary-key column may not change.  The key
             * guardrail above compares pk_cols, which are NAMES, so ALTER COLUMN <pk>
             * TYPE bigint leaves the key "unchanged" and passes it.  Paimon then refuses
             * the schema change with "UnsupportedOperationException: Cannot update
             * primary key" -- in a transaction PostgreSQL has already committed, so the
             * writer replays it forever and replication stops for the whole database.
             * Comparison is on the type without the NOT NULL suffix, so this reports a
             * real type change and not a nullability one (a key column is always NOT
             * NULL anyway).
             */
            IF (o->>'name') = ANY (reg.pk_cols)
               AND regexp_replace(o->>'type', ' NOT NULL$', '')
                   <> regexp_replace(c->>'type', ' NOT NULL$', '') THEN
                RAISE EXCEPTION
                    'paimon_slot: cannot change the type of primary-key column %.%.% (% -> %)',
                    reg.schema_name, reg.table_name, c->>'name',
                    regexp_replace(o->>'type', ' NOT NULL$', ''),
                    regexp_replace(c->>'type', ' NOT NULL$', '')
                      USING HINT = 'Paimon cannot alter the type of a primary-key field; the key is fixed at add_table() time, so re-register the table (paimon.remove_table, drop the Paimon table, paimon.add_table) to change it';
            END IF;

            /*
             * Guardrail: a column may not go from nullable to NOT NULL.  Paimon refuses
             * it with "Cannot update column type from nullable to non nullable", again
             * after PostgreSQL has committed.  Paimon does offer the table option
             * alter-column-null-to-not-null.disabled = false, and this REFUSES rather
             * than enabling it: Paimon only rewrites the declared schema, never the data
             * files, so every row already written -- the backfill, and every -U
             * before-image in the changelog -- may hold NULL there, and a schema claiming
             * NOT NULL over them is a silent lie to every reader.  PostgreSQL's own SET
             * NOT NULL validates the live table, not the history.  (The option is also
             * per table and read from the stored options, so enabling it would not even
             * cover tables created by an earlier run.)
             */
            IF (o->>'type') NOT LIKE '% NOT NULL'
               AND (c->>'type') LIKE '% NOT NULL' THEN
                RAISE EXCEPTION
                    'paimon_slot: SET NOT NULL on a replicated column is not supported (%.%.%)',
                    reg.schema_name, reg.table_name, c->>'name'
                      USING HINT = 'rows already written to Paimon may hold NULL there and Paimon never rewrites them, so the column must stay nullable; re-register the table to change it';
            END IF;

            /*
             * Guardrail: only type changes Paimon can apply to the rows it already holds
             * without changing them (type_change_problem has the measured reasons). Registry
             * rows written before typid/typmod were recorded carry neither and are not judged.
             */
            IF (o->>'typid') IS NOT NULL
               AND paimon.type_change_problem((o->>'typid')::oid, (o->>'typmod')::int,
                                              (c->>'typid')::oid, (c->>'typmod')::int) IS NOT NULL THEN
                RAISE EXCEPTION
                    'paimon_slot: cannot change the type of replicated column %.%.% from % to %: %',
                    reg.schema_name, reg.table_name, c->>'name',
                    format_type((o->>'typid')::oid, (o->>'typmod')::int),
                    format_type((c->>'typid')::oid, (c->>'typmod')::int),
                    paimon.type_change_problem((o->>'typid')::oid, (o->>'typmod')::int,
                                               (c->>'typid')::oid, (c->>'typmod')::int)
                      USING HINT = 'PostgreSQL rewrites the table without sending the rewritten rows, so Paimon keeps the old values and reads them through its own cast. Only exact widenings are allowed (smallint->int->bigint, integer->wide-enough numeric or text, real->double precision, numeric(p,s)->numeric with no fewer integer digits or decimal places, text<->varchar, timestamp precision up). Otherwise: ADD COLUMN the new type, UPDATE it from the old one, DROP the old column, RENAME the new one -- each step replicates.';
            END IF;
        END IF;
    END LOOP;

    /* Emit: drops first, then renames and alters, then adds, so a dropped
     * name can be reused by an added column. */
    FOR o IN SELECT x FROM jsonb_array_elements(reg.cols) x
              WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(new_cols) n
                                 WHERE (n->>'id') = (x->>'id'))
    LOOP
        PERFORM paimon.emit(jsonb_build_object(
            'v', 1, 'op', 'drop_col', 'relid', v_relid, 'name', o->>'name'));
    END LOOP;

    FOR c IN SELECT * FROM jsonb_array_elements(new_cols)
    LOOP
        SELECT x INTO o FROM jsonb_array_elements(reg.cols) x
         WHERE (x->>'id') = (c->>'id');
        CONTINUE WHEN o IS NULL;

        IF (o->>'name') <> (c->>'name') THEN
            PERFORM paimon.emit(jsonb_build_object(
                'v', 1, 'op', 'rename_col', 'relid', v_relid,
                'id', (c->>'id')::int,
                'from', o->>'name', 'to', c->>'name'));
        END IF;

        IF (o->>'type') <> (c->>'type') THEN
            PERFORM paimon.emit(jsonb_build_object(
                'v', 1, 'op', 'alter_col', 'relid', v_relid,
                'id', (c->>'id')::int,
                'name', c->>'name', 'type', c->>'type'));
        END IF;
    END LOOP;

    FOR c IN SELECT n FROM jsonb_array_elements(new_cols) n
              WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(reg.cols) x
                                 WHERE (x->>'id') = (n->>'id'))
    LOOP
        PERFORM paimon.emit(jsonb_build_object(
            'v', 1, 'op', 'add_col', 'relid', v_relid, 'col', c - 'typid' - 'typmod'));
    END LOOP;

    UPDATE paimon.table_registry
       SET cols = new_cols
     WHERE relid = v_relid;

    /* Value guards for the columns this statement added or retyped. After the registry
     * UPDATE, deliberately: the ALTER TABLE in add_guards fires this trigger again, and it
     * must then find nothing left to emit. */
    PERFORM paimon.add_guards(v_relid,
        ARRAY(SELECT (nc->>'id')::int FROM jsonb_array_elements(new_cols) nc
               WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(reg.cols) x
                                  WHERE (x->>'id') = (nc->>'id')
                                    AND (x->>'typid') IS NOT DISTINCT FROM (nc->>'typid')
                                    AND (x->>'typmod') IS NOT DISTINCT FROM (nc->>'typmod'))),
        false);
END $$;

CREATE FUNCTION paimon.on_ddl_end() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    obj   record;
    t     oid;
    used  text;
BEGIN
    FOR obj IN SELECT DISTINCT objid FROM pg_event_trigger_ddl_commands()
                WHERE classid = 'pg_class'::regclass
    LOOP
        PERFORM paimon.sync_table(obj.objid);

        /*
         * An ALTER of a parent reaches its children and partitions (ADD/DROP COLUMN, ALTER
         * COLUMN TYPE recurse), but the command reports only the parent. A replicated child
         * was therefore left with a stale registry: measured, ALTER TABLE parent ADD COLUMN
         * gave a replicated partition a column whose every value the writer discarded. Sync
         * every replicated descendant too, which also runs its guardrails.
         */
        FOR t IN WITH RECURSIVE d(relid) AS (
                     SELECT inhrelid FROM pg_inherits WHERE inhparent = obj.objid
                     UNION
                     SELECT i.inhrelid FROM pg_inherits i JOIN d ON i.inhparent = d.relid)
                 SELECT relid FROM d
        LOOP
            PERFORM paimon.sync_table(t);
        END LOOP;

        /* CREATE TABLE ... INHERITS (replicated) and ALTER TABLE ... INHERIT replicated report
         * the child; the parent's guardrail is what refuses it. */
        FOR t IN SELECT inhparent FROM pg_inherits WHERE inhrelid = obj.objid
        LOOP
            PERFORM paimon.sync_table(t);
        END LOOP;

        /*
         * ALTER TYPE ... ADD/DROP/ALTER ATTRIBUTE on a composite type reports the composite's
         * pg_class entry. Every value of the type then prints differently -- measured,
         * (1,x) became (1,x,) in PostgreSQL and stayed (1,x) in Paimon -- with no row
         * change, so nothing reaches the stream.
         */
        SELECT reltype INTO t FROM pg_class WHERE oid = obj.objid AND relkind = 'c';
        IF FOUND THEN
            used := paimon.type_used_by_replicated(t);
            IF used IS NOT NULL THEN
                RAISE EXCEPTION 'paimon_slot: composite type % is used by replicated column %', t::regtype, used
                      USING HINT = 'changing its attributes changes the text of every value already replicated, without any row change reaching Paimon; create a new type and migrate the column (ADD COLUMN, UPDATE, DROP COLUMN, RENAME COLUMN)';
            END IF;
        END IF;
    END LOOP;

    /*
     * ALTER TYPE ... RENAME VALUE on an enum: every row holding that label now prints the new
     * one, and no row change reaches the stream (measured: Paimon kept 'a', PostgreSQL said
     * 'A'). The event trigger cannot see which ALTER TYPE form ran, so the statement text is
     * consulted; ADD VALUE, the common case, stays allowed.
     */
    FOR obj IN SELECT DISTINCT objid FROM pg_event_trigger_ddl_commands()
                WHERE classid = 'pg_type'::regclass AND command_tag = 'ALTER TYPE'
    LOOP
        IF (SELECT typtype FROM pg_type WHERE oid = obj.objid) = 'e'
           AND current_query() ~* 'rename[[:space:]]+value' THEN
            used := paimon.type_used_by_replicated(obj.objid);
            IF used IS NOT NULL THEN
                RAISE EXCEPTION 'paimon_slot: cannot rename a value of enum type % while replicated column % uses it', obj.objid::regtype, used
                      USING HINT = 'rows already replicated keep the old label in Paimon, because a rename changes no row; add the new value (ALTER TYPE ... ADD VALUE), UPDATE the rows to it, then stop using the old one';
            END IF;
        END IF;
    END LOOP;

    /* ALTER TABLE ... RENAME CONSTRAINT reports the constraint, not the table: sync the
     * table, whose guardrail refuses renaming a value guard away. */
    FOR obj IN SELECT DISTINCT objid FROM pg_event_trigger_ddl_commands()
                WHERE classid = 'pg_constraint'::regclass
    LOOP
        PERFORM paimon.sync_table(conrelid) FROM pg_constraint WHERE oid = obj.objid;
    END LOOP;

    /* Any change to a publication: paimon_pub must keep carrying every replicated table. */
    IF EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands()
                WHERE classid IN ('pg_publication'::regclass, 'pg_publication_rel'::regclass,
                                  'pg_publication_namespace'::regclass)) THEN
        PERFORM paimon.check_publication();
    END IF;
END $$;

CREATE EVENT TRIGGER paimon_ddl_end ON ddl_command_end
    EXECUTE FUNCTION paimon.on_ddl_end();

/*
 * ALTER COLUMN ... TYPE ... USING <expression>: the expression can change the values of
 * any type change, even one to the same type (ALTER COLUMN a TYPE int USING a + 1), and the
 * rewritten rows never reach the stream, so Paimon would keep every old value. The rewrite
 * event is the only place this is visible at all, and it does not say whether USING was
 * given, so the statement text is consulted. A false positive (USING elsewhere in the same
 * statement string) refuses, never loses; deliberately hiding the word, e.g. by building the
 * statement in EXECUTE from fragments, is not caught (documented residual risk).
 */
CREATE FUNCTION paimon.on_table_rewrite() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    reg paimon.table_registry;
BEGIN
    SELECT * INTO reg FROM paimon.table_registry
     WHERE relid = pg_event_trigger_table_rewrite_oid();
    IF NOT FOUND THEN
        RETURN;
    END IF;
    -- AT_REWRITE_COLUMN_REWRITE (src/include/nodes/parsenodes.h)
    IF (pg_event_trigger_table_rewrite_reason() & 4) <> 0
       AND current_query() ~* '(^|[^[:alnum:]_])using([^[:alnum:]_]|$)' THEN
        RAISE EXCEPTION 'paimon_slot: ALTER COLUMN ... TYPE ... USING is not supported on replicated table %.%',
                        reg.schema_name, reg.table_name
              USING HINT = 'PostgreSQL rewrites the rows without sending them, so values a USING expression changes would never reach Paimon. Change the type without USING (only exact widenings are allowed), or ADD COLUMN, UPDATE it with the expression, DROP the old column and RENAME the new one -- each step replicates.';
    END IF;
END $$;

CREATE EVENT TRIGGER paimon_table_rewrite ON table_rewrite
    EXECUTE FUNCTION paimon.on_table_rewrite();

/*
 * Table drops emit drop_table.  A column of a table that is itself dropped
 * needs no message; a column lost from a surviving table is synced via the same
 * diff on_ddl_end() uses.  The Paimon table name is deliberately fixed at
 * registration: ALTER TABLE ... RENAME TO emits nothing and the registry keeps
 * the original name (tracking is keyed on relid, so it survives the rename).
 */
CREATE FUNCTION paimon.on_sql_drop() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    obj     record;
    reg     paimon.table_registry;
    dropped oid[];
BEGIN
    SELECT coalesce(array_agg(objid), '{}') INTO dropped
      FROM pg_event_trigger_dropped_objects() WHERE object_type = 'table';

    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
                WHERE object_type = 'table'
    LOOP
        SELECT * INTO reg FROM paimon.table_registry WHERE relid = obj.objid;
        CONTINUE WHEN NOT FOUND;
        PERFORM paimon.emit(jsonb_build_object(
            'v', 1, 'op', 'drop_table', 'relid', obj.objid,
            'schema', reg.schema_name, 'table', reg.table_name));
        DELETE FROM paimon.table_registry WHERE relid = obj.objid;
    END LOOP;

    /* A column dropped by a cascade (DROP DOMAIN/TYPE ... CASCADE) is reported
     * against the dropped object, so on_ddl_end never sees its table.  Sync
     * every surviving table that lost a column. */
    FOR obj IN SELECT DISTINCT objid FROM pg_event_trigger_dropped_objects()
                WHERE object_type = 'table column' AND NOT (objid = ANY (dropped))
    LOOP
        PERFORM paimon.sync_table(obj.objid);
    END LOOP;

    /* ALTER PUBLICATION ... DROP TABLE and DROP PUBLICATION report only here. After the
     * registry rows of dropped tables are gone, so dropping a replicated table still works. */
    IF EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects()
                WHERE object_type IN ('publication', 'publication relation',
                                      'publication namespace')) THEN
        PERFORM paimon.check_publication();
    END IF;
END $$;

CREATE EVENT TRIGGER paimon_sql_drop ON sql_drop
    EXECUTE FUNCTION paimon.on_sql_drop();

/*
 * ENABLE ALWAYS, all three. An event trigger in the default (ORIGIN) state does not fire
 * while session_replication_role = replica, a setting DBAs use for bulk loads: measured,
 * under it ALTER TABLE ... ADD COLUMN ... DEFAULT and REPLICA IDENTITY DEFAULT on a
 * replicated table both went through, and the registry went stale. ALWAYS fires in every
 * mode. (Nothing can stop a superuser disabling them outright; paimon.health() reports it and
 * the writer refuses to run on it.)
 */
ALTER EVENT TRIGGER paimon_ddl_end ENABLE ALWAYS;
ALTER EVENT TRIGGER paimon_sql_drop ENABLE ALWAYS;
ALTER EVENT TRIGGER paimon_table_rewrite ENABLE ALWAYS;

/*
 * Everything the guardrails above are meant to keep true, checked from the catalog. Returns
 * one row per problem; none means healthy. For monitoring, and the writer runs it at startup
 * and periodically and stops on any problem (design spec section 8): each one means changes
 * may be reaching PostgreSQL that the writer cannot see or cannot apply, and the guardrails
 * that would have refused them have been bypassed -- an event trigger disabled or dropped,
 * a catalog edited by hand.
 */
CREATE FUNCTION paimon.health() RETURNS SETOF text
LANGUAGE plpgsql STABLE AS $$
DECLARE
    e   text;
    r   paimon.table_registry;
    c   jsonb;
BEGIN
    FOREACH e IN ARRAY ARRAY['paimon_ddl_end', 'paimon_sql_drop', 'paimon_table_rewrite']
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = e) THEN
            RETURN NEXT format('event trigger %s is missing: DDL on replicated tables is no longer checked or replicated', e);
        ELSIF (SELECT evtenabled FROM pg_event_trigger WHERE evtname = e) <> 'A' THEN
            RETURN NEXT format('event trigger %s is not ENABLE ALWAYS: DDL on replicated tables is not checked or replicated while it does not fire (ALTER EVENT TRIGGER %s ENABLE ALWAYS)', e, e);
        END IF;
    END LOOP;

    RETURN QUERY SELECT * FROM paimon.publication_problems();

    FOR r IN SELECT * FROM paimon.table_registry ORDER BY relid
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = r.relid) THEN
            RETURN NEXT format('registry row for %I.%I names relation %s, which no longer exists', r.schema_name, r.table_name, r.relid);
            CONTINUE;
        END IF;
        IF (SELECT relreplident FROM pg_class WHERE oid = r.relid) <> 'f' THEN
            RETURN NEXT format('replicated table %s does not have REPLICA IDENTITY FULL', r.relid::regclass);
        END IF;
        IF r.cols IS DISTINCT FROM paimon.table_cols(r.relid, true) THEN
            RETURN NEXT format('replicated table %s has changed without its DDL being replicated: paimon.table_registry.cols no longer matches the table', r.relid::regclass);
        END IF;
        FOR c IN SELECT * FROM jsonb_array_elements(paimon.table_cols(r.relid, true))
        LOOP
            IF NOT paimon.guard_present(r.relid, (c->>'id')::int) THEN
                RETURN NEXT format('replicated table %s has lost value guard %s on column %I', r.relid::regclass, paimon.guard_name((c->>'id')::int), c->>'name');
            END IF;
        END LOOP;
        IF EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = r.relid) THEN
            RETURN NEXT format('replicated table %s has inheritance children, whose rows are not replicated', r.relid::regclass);
        END IF;
    END LOOP;
END $$;
