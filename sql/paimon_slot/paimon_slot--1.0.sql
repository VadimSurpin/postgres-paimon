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

-- Mirror of extension/type_map_pg.h.  Returns a Paimon type string.
CREATE FUNCTION paimon.pg_type_to_paimon(typid oid, typmod int, not_null boolean)
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
            RAISE WARNING 'paimon_slot: type % has no Paimon mapping, using STRING',
                          format_type(typid, typmod);
            base := 'STRING';
    END CASE;

    IF not_null THEN
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

CREATE PUBLICATION paimon_pub;

CREATE FUNCTION paimon.emit(payload jsonb) RETURNS void
LANGUAGE sql AS $$
    SELECT pg_logical_emit_message(true, 'paimon', payload::text);
$$;

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
    IF prec > 38 THEN
        RAISE EXCEPTION 'paimon_slot: column "%" is numeric(%) but Paimon DECIMAL supports at most 38 digits',
                        a.attname, prec
              USING HINT = 'Use numeric(p,s) with p <= 38. See design spec section 5.';
    END IF;
END $$;

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
    v_has_rows boolean;
    v_other_nsp text;
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

    -- The writer's Paimon table identity is (database, table name) and ignores the
    -- PostgreSQL schema, so same-named tables in different schemas would interleave
    -- into one Paimon table.  Compare by name only, against a DIFFERENT relid, so
    -- re-registering after remove_table() is unaffected.
    SELECT schema_name INTO v_other_nsp FROM paimon.table_registry
     WHERE table_name = v_name AND relid <> v_relid LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'paimon_slot: table name "%" is already replicated from schema "%"',
                        v_name, v_other_nsp
              USING HINT = 'the Paimon table identity does not include the PostgreSQL schema, so two same-named tables would interleave into one Paimon table. Rename one, or replicate only one of them. See design spec section 4.';
    END IF;

    v_cols := paimon.table_cols(v_relid);
    PERFORM paimon.reject_reserved_name(c->>'name') FROM jsonb_array_elements(v_cols) c;
    PERFORM paimon.reject_unsafe_numeric(v_relid, (c->>'id')::int) FROM jsonb_array_elements(v_cols) c;

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

    EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', rel::text);
    EXECUTE format('ALTER PUBLICATION paimon_pub ADD TABLE %s', rel::text);

    INSERT INTO paimon.table_registry
        (relid, schema_name, table_name, pk_cols, cols,
         backfill_due, replica_identity, changelog_producer)
    VALUES (v_relid, v_nsp, v_name, v_pk, v_cols,
            v_has_rows, replica_identity, changelog_producer);

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
    -- Order is add_table in reverse: leave the publication while the registry row still exists,
    -- so the DDL event trigger still recognises the table; only then delete the row.
    EXECUTE format('ALTER PUBLICATION paimon_pub DROP TABLE %s', rel::text);
    DELETE FROM paimon.table_registry WHERE relid = rel::oid;
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
            RETURN NEXT substring(r.data from position('content:' in r.data) + 8)::jsonb;
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
            'v', 1, 'op', 'add_col', 'relid', v_relid, 'col', c));
    END LOOP;

    UPDATE paimon.table_registry
       SET cols = new_cols
     WHERE relid = v_relid;
END $$;

CREATE FUNCTION paimon.on_ddl_end() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT DISTINCT objid FROM pg_event_trigger_ddl_commands()
                WHERE classid = 'pg_class'::regclass
    LOOP
        PERFORM paimon.sync_table(obj.objid);
    END LOOP;
END $$;

CREATE EVENT TRIGGER paimon_ddl_end ON ddl_command_end
    EXECUTE FUNCTION paimon.on_ddl_end();

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
END $$;

CREATE EVENT TRIGGER paimon_sql_drop ON sql_drop
    EXECUTE FUNCTION paimon.on_sql_drop();
