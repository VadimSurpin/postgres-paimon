-- paimon_heap regression tests
-- Run with: make installcheck PGUSER=<superuser>
-- Requires: shared_preload_libraries = 'paimon_heap' in postgresql.conf

SET paimon_heap.warehouse = '/tmp/paimon-regress-warehouse';

-- 0. Access method is registered
SELECT amname FROM pg_am WHERE amname = 'paimon_heap';

-- 1. Basic table lifecycle
CREATE TABLE t1 (id int, payload text) USING paimon_heap;

INSERT INTO t1 VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');
SELECT * FROM t1 ORDER BY id;

UPDATE t1 SET payload = 'ALPHA' WHERE id = 1;
SELECT * FROM t1 ORDER BY id;

DELETE FROM t1 WHERE id = 3;
SELECT COUNT(*) FROM t1;

TRUNCATE t1;
SELECT COUNT(*) FROM t1;

DROP TABLE t1;

-- 2. Multi-column scalar type coverage
CREATE TABLE t2 (
    id      bigint,
    ts      timestamptz,
    flag    boolean,
    amt     numeric(12,4),
    label   varchar(64)
) USING paimon_heap;

INSERT INTO t2 VALUES
    (1, '2024-01-01 00:00:00+00', true,  100.5000, 'first'),
    (2, '2024-06-15 12:00:00+00', false, 200.7500, 'second');

SELECT id, flag, amt, label FROM t2 ORDER BY id;

DROP TABLE t2;

-- 3. DDL: column add / drop
CREATE TABLE t3 (id int, a text) USING paimon_heap;
INSERT INTO t3 VALUES (1, 'x');
ALTER TABLE t3 ADD COLUMN b int DEFAULT 42;
SELECT * FROM t3 ORDER BY id;
ALTER TABLE t3 DROP COLUMN a;
SELECT * FROM t3 ORDER BY id;
DROP TABLE t3;

-- 4. Sequence / serial column
CREATE TABLE t4 (id serial, v text) USING paimon_heap;
INSERT INTO t4 (v) VALUES ('one'), ('two'), ('three');
SELECT v FROM t4 ORDER BY id;
DROP TABLE t4;
