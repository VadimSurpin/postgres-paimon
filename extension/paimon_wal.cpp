// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_wal.cpp — Custom WAL resource manager for paimon_heap durability.
 *
 * Write path (backend, XACT_EVENT_PRE_COMMIT):
 *   paimon_wal_write() writes a single WAL record containing all framed ring
 *   messages for that transaction, then calls XLogFlush(lsn).  Because this
 *   fires at XACT_EVENT_PRE_COMMIT (before RecordTransactionCommit()), the
 *   paimon record is included in the same WAL flush that makes the transaction
 *   commit durable — no extra fsync.  At high concurrency multiple backends
 *   share one XLogFlush call (group commit).
 *
 * Recovery path (PostgreSQL startup process, crash recovery):
 *   paimon_wal_redo() appends each paimon WAL record's framed messages to
 *   $PGDATA/paimon_recovery.journal as raw bytes.
 *
 * Replay path (bgworker startup):
 *   paimon_wal_replay_journal() reads the journal, sends each framed message
 *   to the ring buffer, then deletes the journal.  The bgworker then processes
 *   the ring normally (writing Parquet, offloading to S3/HDFS, etc.).
 */

#include <cstdio>
#include <cstring>
#include <cerrno>
#include <vector>
#include <string>

extern "C" {
#include "postgres.h"
#include "access/rmgr.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/elog.h"
}

#include "paimon_wal.h"
#include "paimon_ring.h"

/* ── Custom RMGR ──────────────────────────────────────────────────────── */

#define PAIMON_WAL_RMGR_ID    RM_MIN_CUSTOM_ID    /* 128 */
#define PAIMON_WAL_INFO_BATCH 0x00

static void        paimon_wal_redo    (XLogReaderState *record);
static void        paimon_wal_desc    (StringInfo buf, XLogReaderState *record);
static const char *paimon_wal_identify(uint8 info);

static const RmgrData PaimonRmgr = {
    "paimon_heap",        /* rm_name */
    paimon_wal_redo,      /* rm_redo */
    paimon_wal_desc,      /* rm_desc */
    paimon_wal_identify,  /* rm_identify */
    NULL,                 /* rm_startup */
    NULL,                 /* rm_cleanup */
    NULL,                 /* rm_mask */
    NULL,                 /* rm_decode */
};

/* ── GUC ──────────────────────────────────────────────────────────────── */

static char *guc_wal_level = NULL;   /* "off" | "local" */

/* ── Recovery journal path ────────────────────────────────────────────── */

static void
journal_path(char *path)
{
    snprintf(path, MAXPGPATH, "%s/paimon_recovery.journal", DataDir);
}

/* ── Framed message length from raw bytes (little-endian uint32) ──────── */

static uint32_t
read_le_u32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

/* ── RMGR callbacks ───────────────────────────────────────────────────── */

static void
paimon_wal_redo(XLogReaderState *record)
{
    /*
     * Append framed ring messages verbatim to the recovery journal.
     * Called by the startup process during crash recovery.
     */
    const char  *data  = XLogRecGetData(record);
    uint32_t     total = XLogRecGetDataLen(record);

    if (!data || total == 0)
        return;

    char path[MAXPGPATH];
    journal_path(path);

    FILE *f = fopen(path, "ab");
    if (!f)
    {
        ereport(WARNING,
                errmsg("paimon: could not open recovery journal \"%s\": %m", path));
        return;
    }

    /* Walk the concatenated framed messages and write each one. */
    const uint8_t *p   = (const uint8_t *) data;
    const uint8_t *end = p + total;

    while (p + 4 <= end)
    {
        uint32_t msg_len = read_le_u32(p);
        if (msg_len < 5 || msg_len > (uint32_t)(end - p))
            break;   /* corrupt or truncated record */

        if (fwrite(p, 1, msg_len, f) != msg_len)
        {
            ereport(WARNING, errmsg("paimon: short write to recovery journal"));
            break;
        }
        p += msg_len;
    }

    fclose(f);
}

static void
paimon_wal_desc(StringInfo buf, XLogReaderState *record)
{
    appendStringInfoString(buf, "paimon batch");
}

static const char *
paimon_wal_identify(uint8 info)
{
    return "BATCH";
}

/* ── Public API ───────────────────────────────────────────────────────── */

void
paimon_wal_init(void)
{
    RegisterCustomRmgr(PAIMON_WAL_RMGR_ID, &PaimonRmgr);

    DefineCustomStringVariable(
        "paimon_heap.wal_level",
        "Durability level for paimon_heap writes: 'off' (default, fire-and-forget) "
        "or 'local' (WAL record at commit, crash-safe recovery journal)",
        NULL,
        &guc_wal_level,
        "off",
        PGC_SIGHUP, 0, NULL, NULL, NULL);
}

bool
paimon_wal_is_local(void)
{
    return guc_wal_level != NULL &&
           pg_strcasecmp(guc_wal_level, "local") == 0;
}

void
paimon_wal_write(const std::vector<std::string> &msgs)
{
    if (msgs.empty())
        return;
    if (!XLogInsertAllowed())
        return;

    XLogBeginInsert();
    for (const auto &msg : msgs)
        XLogRegisterData(msg.data(), (uint32) msg.size());
    /*
     * Insert the WAL record but do NOT call XLogFlush here.
     * This function runs at XACT_EVENT_PRE_COMMIT — before
     * RecordTransactionCommit() writes the commit record.
     * RecordTransactionCommit then calls XLogFlush(commit_lsn),
     * which flushes everything up to the commit record, including
     * our record (commit_lsn > our lsn).  No extra fsync needed.
     *
     * Durability guarantee: crash-safe when synchronous_commit = on
     * (the default).  With synchronous_commit = off the commit flush
     * is also skipped, so paimon WAL is flushed asynchronously by the
     * WAL writer — same semantics as PostgreSQL itself.
     */
    XLogInsert((RmgrId) PAIMON_WAL_RMGR_ID, PAIMON_WAL_INFO_BATCH);
}

void
paimon_wal_replay_journal(void)
{
    char path[MAXPGPATH];
    journal_path(path);

    FILE *f = fopen(path, "rb");
    if (!f)
        return;    /* no journal — clean startup */

    ereport(LOG, errmsg("paimon_bgworker: replaying recovery journal %s", path));

    uint32_t n_replayed = 0;

    for (;;)
    {
        uint8_t len_bytes[4];
        if (fread(len_bytes, 4, 1, f) != 1)
            break;   /* EOF or read error */

        uint32_t msg_len = read_le_u32(len_bytes);

        if (msg_len < 5 || msg_len > (uint32_t)(32 * 1024 * 1024))
        {
            ereport(WARNING,
                    errmsg("paimon_bgworker: corrupt journal entry (len=%u), "
                           "stopping replay", msg_len));
            break;
        }

        char *buf = (char *) palloc(msg_len);
        memcpy(buf, len_bytes, 4);

        size_t to_read = msg_len - 4;
        if (fread(buf + 4, 1, to_read, f) != to_read)
        {
            pfree(buf);
            ereport(WARNING, errmsg("paimon_bgworker: truncated journal entry"));
            break;
        }

        if (!paimon_ring_write(buf, msg_len))
            ereport(WARNING,
                    errmsg("paimon_bgworker: ring write failed during journal replay "
                           "(ring full — message lost)"));

        pfree(buf);
        n_replayed++;
    }

    fclose(f);

    if (n_replayed > 0)
        ereport(LOG,
                errmsg("paimon_bgworker: replayed %u messages from recovery journal",
                       n_replayed));

    if (unlink(path) != 0 && errno != ENOENT)
        ereport(WARNING,
                errmsg("paimon_bgworker: could not remove recovery journal "
                       "\"%s\": %m", path));
}
