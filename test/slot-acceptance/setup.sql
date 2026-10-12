-- Minimal schema to prove the pipeline runs. Tasks 15-17 own suite-specific schemas.
-- ORDER MATTERS: add_table must run before the writer starts (a table cannot be added
-- once the slot exists).
CREATE EXTENSION paimon_slot;

CREATE TABLE harness_smoke (
    id    int PRIMARY KEY,
    label text,
    amt   numeric(12,2)
);
INSERT INTO harness_smoke VALUES (1, 'one', 1.50), (2, 'two', 2.25);
SELECT paimon.add_table('harness_smoke'::regclass);

-- Two same-named tables in DIFFERENT PostgreSQL schemas. Each must land in its own Paimon
-- database (<warehouse>/<schema>.db/twin) and never interleave with the other; with the schema
-- dropped from the Paimon identity they resolved to one table and merged on the shared key.
CREATE SCHEMA billing;
CREATE TABLE public.twin  (id int PRIMARY KEY, who text);
CREATE TABLE billing.twin (id int PRIMARY KEY, who text);
INSERT INTO public.twin  VALUES (1, 'public-one');
INSERT INTO billing.twin VALUES (1, 'billing-one');
SELECT paimon.add_table('public.twin'::regclass);
SELECT paimon.add_table('billing.twin'::regclass);
