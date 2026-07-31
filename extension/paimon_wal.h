// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * paimon_wal.h — WAL-level durability for paimon_heap.
 *
 * When paimon_heap.wal_level = 'local', each commit writes a custom WAL record
 * containing all ring-buffer messages for that transaction and then calls
 * XLogFlush() to guarantee they are on disk before COMMIT returns to the client.
 * Because paimon_wal_write() is called at XACT_EVENT_PRE_COMMIT — before
 * RecordTransactionCommit() flushes the COMMIT record — our record is included
 * in the same WAL flush at no extra I/O cost (group commit still applies).
 *
 * On crash, the startup process calls paimon_wal_redo() for each paimon WAL
 * record, which appends the framed messages to $PGDATA/paimon_recovery.journal.
 * When the bgworker starts it calls paimon_wal_replay_journal() to re-inject
 * those messages into the ring buffer before resuming normal operation.
 */

#ifdef __cplusplus
#include <vector>
#include <string>

extern "C" {
#endif

#include "postgres.h"

/*
 * Register the paimon WAL resource manager and define the wal_level GUC.
 * Must be called from _PG_init before any process fork.
 */
void paimon_wal_init(void);

/* Returns true when paimon_heap.wal_level = 'local'. */
bool paimon_wal_is_local(void);

/*
 * Write framed ring messages as one WAL record and flush to disk.
 * No-op when wal_level != 'local' or WAL inserts are not allowed.
 * Only safe to call from a backend (e.g. an xact PRE_COMMIT callback).
 */
#ifdef __cplusplus
void paimon_wal_write(const std::vector<std::string> &msgs);
#endif

/*
 * Replay any pending recovery journal into the ring buffer.
 * Called from paimon_bgworker_main() after paimon_shmem_init(),
 * before entering the normal ring-read loop.
 */
void paimon_wal_replay_journal(void);

#ifdef __cplusplus
} /* extern "C" */
#endif
