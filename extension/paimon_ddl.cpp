// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_ddl.cpp — ProcessUtility hook for DDL capture on paimon_heap tables.
 *
 * Intercepts and forwards to the Paimon lake writer (via ring buffer):
 *   DROP TABLE            → paimon_client_ddl_drop_table  (pre-execute)
 *   ALTER TABLE ADD COLUMN → paimon_client_ddl_add_col    (post-execute)
 *   ALTER TABLE DROP COLUMN → paimon_client_ddl_drop_col  (pre-execute)
 *   ALTER TABLE RENAME COLUMN → paimon_client_ddl_rename  (pre-execute, via RenameStmt)
 *   ALTER TABLE ALTER COLUMN TYPE → paimon_client_ddl_type_change (post-execute)
 *   ALTER TABLE ADD/DROP PRIMARY KEY → blocked with ERROR
 *   TRUNCATE              → paimon_client_ddl_truncate     (pre-execute)
 *
 * Pre-execute is used when catalog state needed (OIDs, attnums, names) is
 * only available before the DDL modifies it.  Post-execute is used when the
 * new state (new column attnum/type) must be read from the updated catalog.
 *
 * Non-paimon_heap relations are passed through untouched.
 */

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "tcop/utility.h"
#include "nodes/parsenodes.h"
#include "commands/tablecmds.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_constraint.h"

void paimon_ddl_init(void);
}

#include "paimon_client.h"
#include "type_map_pg.h"

/* ── hook chain ──────────────────────────────────────────────────────── */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/* ── AM OID cache ────────────────────────────────────────────────────── */
static Oid s_paimon_am_oid = InvalidOid;

static Oid
get_paimon_am_oid(void)
{
    if (!OidIsValid(s_paimon_am_oid)) {
        HeapTuple ht = SearchSysCache1(AMNAME, CStringGetDatum("paimon_heap"));
        if (HeapTupleIsValid(ht)) {
            s_paimon_am_oid = ((Form_pg_am) GETSTRUCT(ht))->oid;
            ReleaseSysCache(ht);
        }
    }
    return s_paimon_am_oid;
}

static bool
is_paimon_heap_rel(Oid relid)
{
    Oid am = get_rel_relam(relid);
    if (!OidIsValid(am))
        return false;
    Oid pam = get_paimon_am_oid();
    return OidIsValid(pam) && am == pam;
}

/* Convert a PG type OID + typmod into the wire type code. */
static PaimonTypeCode
pg_type_to_ptype(Oid typoid, int32_t typmod, uint32_t *typmod_out)
{
    return paimon::pg_oid_to_paimon_type((unsigned int) typoid,
                                          typmod_out, (uint32_t) typmod);
}

/* ── pre-execute handlers ────────────────────────────────────────────── */

static void
handle_drop_pre(DropStmt *stmt)
{
    if (stmt->removeType != OBJECT_TABLE)
        return;

    ListCell *lc;
    foreach(lc, stmt->objects) {
        List     *namelist = castNode(List, lfirst(lc));
        RangeVar *rv       = makeRangeVarFromNameList(namelist);
        Oid       relid    = RangeVarGetRelid(rv, NoLock, stmt->missing_ok);
        if (!OidIsValid(relid) || !is_paimon_heap_rel(relid))
            continue;

        char *relname = get_rel_name(relid);
        if (!relname)
            continue;

        paimon_client_ddl_drop_table((uint32_t) relid, relname);
    }
}

static void
handle_truncate_pre(TruncateStmt *stmt)
{
    ListCell *lc;
    foreach(lc, stmt->relations) {
        RangeVar *rv    = castNode(RangeVar, lfirst(lc));
        Oid       relid = RangeVarGetRelid(rv, NoLock, false);
        if (!OidIsValid(relid) || !is_paimon_heap_rel(relid))
            continue;

        char *relname = get_rel_name(relid);
        if (!relname)
            continue;

        paimon_client_ddl_truncate((uint32_t) relid, relname);
    }
}

/*
 * If conname on relid is a PRIMARY KEY constraint, raise an error.
 * Used by AT_DropConstraint to block dropping the PK.
 */
static void
block_if_pk_drop(Oid relid, const char *conname, bool missing_ok)
{
    Oid con_oid = get_relation_constraint_oid(relid, conname, missing_ok);
    if (!OidIsValid(con_oid))
        return;
    HeapTuple tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(con_oid));
    if (!HeapTupleIsValid(tup))
        return;
    char contype = ((Form_pg_constraint) GETSTRUCT(tup))->contype;
    ReleaseSysCache(tup);
    if (contype == CONSTRAINT_PRIMARY)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("paimon_heap: changing the primary key is not supported"),
                 errhint("Recreate the table with the desired primary key.")));
}

static void
handle_alter_pre(AlterTableStmt *stmt)
{
    Oid relid = RangeVarGetRelid(stmt->relation, NoLock, stmt->missing_ok);
    if (!OidIsValid(relid) || !is_paimon_heap_rel(relid))
        return;

    ListCell *lc;
    foreach(lc, stmt->cmds) {
        AlterTableCmd *cmd = castNode(AlterTableCmd, lfirst(lc));

        switch (cmd->subtype) {
            case AT_DropColumn: {
                AttrNumber attnum = get_attnum(relid, cmd->name);
                if (attnum == InvalidAttrNumber)
                    break;
                paimon_client_ddl_drop_col((uint32_t) relid, (uint16_t) attnum);
                break;
            }
            case AT_AddConstraint: {
                Constraint *con = castNode(Constraint, cmd->def);
                if (con->contype == CONSTR_PRIMARY)
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("paimon_heap: changing the primary key is not supported"),
                             errhint("Recreate the table with the desired primary key.")));
                break;
            }
            case AT_DropConstraint:
                block_if_pk_drop(relid, cmd->name, cmd->missing_ok);
                break;

            default:
                break;
        }
    }
}

/* RENAME COLUMN comes as a RenameStmt, not AlterTableStmt. */
static void
handle_rename_pre(RenameStmt *stmt)
{
    if (stmt->renameType != OBJECT_COLUMN)
        return;

    Oid relid = RangeVarGetRelid(stmt->relation, NoLock, stmt->missing_ok);
    if (!OidIsValid(relid) || !is_paimon_heap_rel(relid))
        return;

    AttrNumber attnum = get_attnum(relid, stmt->subname);
    if (attnum == InvalidAttrNumber)
        return;

    paimon_client_ddl_rename((uint32_t) relid, (uint16_t) attnum, stmt->newname);
}

/* ── post-execute handlers ───────────────────────────────────────────── */

static void
handle_alter_post(AlterTableStmt *stmt)
{
    Oid relid = RangeVarGetRelid(stmt->relation, NoLock, stmt->missing_ok);
    if (!OidIsValid(relid) || !is_paimon_heap_rel(relid))
        return;

    ListCell *lc;
    foreach(lc, stmt->cmds) {
        AlterTableCmd *cmd = castNode(AlterTableCmd, lfirst(lc));

        switch (cmd->subtype) {
            case AT_AddColumn: {
                ColumnDef  *cdef   = castNode(ColumnDef, cmd->def);
                AttrNumber  attnum = get_attnum(relid, cdef->colname);
                if (attnum == InvalidAttrNumber)
                    break;

                HeapTuple atttup = SearchSysCache2(ATTNUM,
                                                    ObjectIdGetDatum(relid),
                                                    Int16GetDatum(attnum));
                if (!HeapTupleIsValid(atttup))
                    break;
                Form_pg_attribute att = (Form_pg_attribute) GETSTRUCT(atttup);
                Oid     typid    = att->atttypid;
                int32_t typmod   = att->atttypmod;
                bool    not_null = att->attnotnull;
                ReleaseSysCache(atttup);

                uint32_t       typmod_wire = 0;
                PaimonTypeCode tc = pg_type_to_ptype(typid, typmod, &typmod_wire);
                paimon_client_ddl_add_col((uint32_t) relid, (uint16_t) attnum,
                                           (uint8_t) tc, typmod_wire,
                                           not_null ? 1 : 0, cdef->colname);
                break;
            }
            case AT_AlterColumnType: {
                AttrNumber attnum = get_attnum(relid, cmd->name);
                if (attnum == InvalidAttrNumber)
                    break;
                HeapTuple atttup2 = SearchSysCache2(ATTNUM,
                                                     ObjectIdGetDatum(relid),
                                                     Int16GetDatum(attnum));
                if (!HeapTupleIsValid(atttup2))
                    break;
                Form_pg_attribute att2 = (Form_pg_attribute) GETSTRUCT(atttup2);
                Oid     typid  = att2->atttypid;
                int32_t typmod = att2->atttypmod;
                ReleaseSysCache(atttup2);
                uint32_t       typmod_wire = 0;
                PaimonTypeCode tc = pg_type_to_ptype(typid, typmod, &typmod_wire);
                paimon_client_ddl_type_change((uint32_t) relid, (uint16_t) attnum,
                                              (uint8_t) tc, typmod_wire);
                break;
            }
            default:
                break;
        }
    }
}

/* ── main hook ───────────────────────────────────────────────────────── */

static void
paimon_process_utility(PlannedStmt *pstmt,
                        const char *queryString,
                        bool readOnlyTree,
                        ProcessUtilityContext context,
                        ParamListInfo params,
                        QueryEnvironment *queryEnv,
                        DestReceiver *dest,
                        QueryCompletion *qc)
{
    Node *parsetree = pstmt->utilityStmt;

    /*
     * Pre-execute: capture names/attnums before the DDL modifies the catalog,
     * and block forbidden operations (PK changes) before they happen.
     */
    switch (nodeTag(parsetree)) {
        case T_DropStmt:
            handle_drop_pre(castNode(DropStmt, parsetree));
            break;
        case T_TruncateStmt:
            handle_truncate_pre(castNode(TruncateStmt, parsetree));
            break;
        case T_AlterTableStmt:
            handle_alter_pre(castNode(AlterTableStmt, parsetree));
            break;
        case T_RenameStmt:
            handle_rename_pre(castNode(RenameStmt, parsetree));
            break;
        default:
            break;
    }

    /* Execute the DDL (chain to any prior hook or the standard function). */
    if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, readOnlyTree,
                            context, params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree,
                                context, params, queryEnv, dest, qc);

    /*
     * Post-execute: read updated catalog state for ADD COLUMN and ALTER TYPE,
     * which must be done after PostgreSQL has recorded the new column/type.
     */
    if (nodeTag(parsetree) == T_AlterTableStmt)
        handle_alter_post(castNode(AlterTableStmt, parsetree));
}

/* ── registration (called from _PG_init) ────────────────────────────── */

extern "C" void
paimon_ddl_init(void)
{
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = paimon_process_utility;
}
