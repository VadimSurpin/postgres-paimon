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
