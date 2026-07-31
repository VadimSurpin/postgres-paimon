\echo Use "CREATE EXTENSION paimon_heap" to load this file. \quit

CREATE FUNCTION paimon_heap_handler(internal)
RETURNS table_am_handler
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'paimon_heap_handler';

CREATE ACCESS METHOD paimon_heap
TYPE TABLE
HANDLER paimon_heap_handler;
