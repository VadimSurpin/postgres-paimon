-- TPC-C registration, run between BenchBase's --create phase and its --load phase.
--
-- THE STOCK BENCHBASE SCHEMA IS LEFT EXACTLY AS IT SHIPS. Nothing here alters a TPC-C table: no
-- surrogate key, no added column, no DDL edit. The workload stays recognisably TPC-C, which is the
-- point of running TPC-C at all.
--
-- ORDER IS LOAD-BEARING, for two separate reasons.
--
--  1. add_table() must run while each table is still EMPTY and before the writer's slot exists.
--     add_table derives backfill_due from whether the table has rows, and a non-empty table
--     registered against a live slot cannot be loaded consistently (see
--     docs/logical-slot-architecture.md section 5). So BenchBase creates the schema, this script
--     registers it, the writer starts, and only then does BenchBase load. The rows therefore
--     arrive through the replication stream rather than through the initial backfill, which is
--     the point: it exercises the streaming write path under a bulk insert.
--
--  2. The Paimon tables are created by the extension's own `create` messages, emitted by
--     add_table below. Nothing here, and nothing in the harness, touches the warehouse.
--
-- ALL NINE TPC-C TABLES ARE REGISTERED. TPC-C's `history` has no primary key -- two foreign keys
-- and nothing else -- and is replicated as an OPERATION LOG (docs/superpowers/specs/
-- 2026-10-14-operation-history-design.md): its Paimon table has no key and one row per operation,
-- since without a key there is nothing to merge on. Payment inserts into it throughout the run.
-- DO NOT add a key to `history` to make it look like the others: the stock schema is the point,
-- and the comparison (TpccCompare.compareLog) checks the log as a multiset.
--
-- Every registered table T also gets a history table T_hist in Paimon (every row-level change,
-- before and after, never collapsed); the comparison checks each one against PostgreSQL.

CREATE EXTENSION IF NOT EXISTS paimon_slot;

-- Registration order follows the foreign keys, so the tables appear in a sensible order in the
-- log; the extension does not care.
SELECT paimon.add_table('warehouse'::regclass);
SELECT paimon.add_table('item'::regclass);
SELECT paimon.add_table('stock'::regclass);
SELECT paimon.add_table('district'::regclass);
SELECT paimon.add_table('customer'::regclass);
SELECT paimon.add_table('oorder'::regclass);
SELECT paimon.add_table('new_order'::regclass);
SELECT paimon.add_table('order_line'::regclass);
SELECT paimon.add_table('history'::regclass);

-- Show what was registered and how wide each key is. Composite keys are the common case here:
-- district and stock are two columns, customer, oorder and new_order three, order_line four;
-- history none.
SELECT table_name, cardinality(pk_cols) AS pk_width, pk_cols, hist_table
  FROM paimon.table_registry ORDER BY table_name;

DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM paimon.table_registry;
    IF n <> 9 THEN
        RAISE EXCEPTION 'expected 9 registered tables, found %', n;
    END IF;
    -- Exactly one key-less table, and it is history: an operation log.
    SELECT count(*) INTO n FROM paimon.table_registry WHERE cardinality(pk_cols) = 0;
    IF n <> 1 OR NOT EXISTS (SELECT 1 FROM paimon.table_registry
                             WHERE table_name = 'history' AND cardinality(pk_cols) = 0) THEN
        RAISE EXCEPTION 'expected exactly history to be registered without a key';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conrelid = 'history'::regclass AND contype = 'p') THEN
        RAISE EXCEPTION 'history has a primary key: the stock TPC-C schema has been altered';
    END IF;
    IF (SELECT relreplident FROM pg_class WHERE oid = 'history'::regclass) <> 'f' THEN
        RAISE EXCEPTION 'history is not REPLICA IDENTITY FULL';
    END IF;
    -- Every table names its history table.
    IF EXISTS (SELECT 1 FROM paimon.table_registry WHERE hist_table <> table_name || '_hist') THEN
        RAISE EXCEPTION 'a registry row names an unexpected history table';
    END IF;
    -- Every table was empty at registration, so none is awaiting a backfill: the whole load
    -- travels through the slot.
    SELECT count(*) INTO n FROM paimon.table_registry WHERE backfill_due;
    IF n <> 0 THEN
        RAISE EXCEPTION '% table(s) were not empty at add_table(); the load would not stream', n;
    END IF;
    SELECT count(*) INTO n FROM paimon.health();
    IF n <> 0 THEN
        RAISE EXCEPTION 'paimon.health() reports % problem(s)', n;
    END IF;
END $$;
