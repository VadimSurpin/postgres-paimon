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
-- EIGHT OF THE NINE TPC-C TABLES ARE REGISTERED. `history` is not, and that is the design behaving
-- as specified rather than falling short of it: TPC-C's `history` has no primary key -- two foreign
-- keys and nothing else -- and paimon_slot requires a primary key on every replicated table,
-- because the write target is a Paimon primary-key table and without a key there is nothing to
-- merge on. add_table() refuses such a table on purpose. So `history` cannot be replicated, stays
-- out of the publication, and never appears in the warehouse.
--
-- DO NOT "FIX" THIS by adding a key to `history`. It is a visible, intended consequence of the
-- primary-key requirement, and the comparison asserts the exclusion holds (TpccCompare's
-- checkExcluded) rather than assuming it. What it costs in coverage is one table's breadth, not a
-- class of behaviour: `history` is insert-only in TPC-C, and the other eight tables exercise
-- inserts, composite-key updates and deletes far harder.

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

-- Show what was registered and how wide each key is. Composite keys are the common case here:
-- district and stock are two columns, customer, oorder and new_order three, order_line four.
SELECT table_name, cardinality(pk_cols) AS pk_width, pk_cols
  FROM paimon.table_registry ORDER BY table_name;

DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM paimon.table_registry;
    IF n <> 8 THEN
        RAISE EXCEPTION 'expected 8 registered tables, found %', n;
    END IF;
    SELECT count(*) INTO n FROM paimon.table_registry WHERE cardinality(pk_cols) = 0;
    IF n <> 0 THEN
        RAISE EXCEPTION '% registered table(s) have no primary key', n;
    END IF;
    -- Every table was empty at registration, so none is awaiting a backfill: the whole load
    -- travels through the slot.
    SELECT count(*) INTO n FROM paimon.table_registry WHERE backfill_due;
    IF n <> 0 THEN
        RAISE EXCEPTION '% table(s) were not empty at add_table(); the load would not stream', n;
    END IF;

    -- `history` must be out, and out for the stated reason. Checked here as well as in the
    -- comparison, so a mistake is caught before the workload runs rather than after it.
    IF EXISTS (SELECT 1 FROM paimon.table_registry WHERE table_name = 'history') THEN
        RAISE EXCEPTION 'history is registered, but it must not be';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_publication_tables
               WHERE pubname = 'paimon_pub' AND tablename = 'history') THEN
        RAISE EXCEPTION 'history is published by paimon_pub, but it must not be';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conrelid = 'history'::regclass AND contype = 'p') THEN
        RAISE EXCEPTION 'history has a primary key: the stock TPC-C schema has been altered, '
                        'so the documented reason for excluding it no longer holds';
    END IF;
END $$;

-- And prove the refusal is real, not merely unexercised: registering history must fail with the
-- primary-key error. Caught so this script still succeeds, and re-raised if it does NOT fail.
DO $$
BEGIN
    PERFORM paimon.add_table('history'::regclass);
    RAISE EXCEPTION 'add_table(history) succeeded, but a table with no primary key must be refused';
EXCEPTION
    WHEN sqlstate 'P0001' THEN
        IF position('has no primary key' in SQLERRM) = 0 THEN
            RAISE;
        END IF;
        RAISE NOTICE 'add_table(history) refused as designed: %', SQLERRM;
END $$;
