// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * paimon_shmem.h — shared memory ring buffer for paimon_heap.
 *
 * The ring buffer lives in PostgreSQL shared memory and is the only IPC
 * channel between backends (producers) and the paimon_bgworker (consumer).
 *
 * Layout in shared memory:
 *   [PaimonRingHdr]  — control block (positions, lock, CV)
 *   [uint8 data[ring_size]]  — circular byte array
 *
 * Message framing inside the ring (same as the old socket wire protocol):
 *   [uint32 total_len]  — total bytes including this field
 *   [uint8  msg_type]   — PaimonMsgType
 *   [payload...]
 *
 * Producers write whole framed messages atomically under the LWLock.
 * If the ring is full the message is dropped (the offload is best-effort;
 * PostgreSQL heap remains the authoritative store).
 *
 * IMPORTANT: The extension must be listed in shared_preload_libraries for
 * shared memory and the background worker to be available.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "storage/lwlock.h"
#include "storage/condition_variable.h"
#include "port/atomics.h"

#define PAIMON_RING_MAGIC         0x50414952U   /* 'PAIR' */
#define PAIMON_SHMEM_NAME         "paimon_heap_shmem"

/* Default ring buffer size (can be overridden by GUC paimon_heap.ring_size_mb). */
#define PAIMON_RING_SIZE_DEFAULT  (32 * 1024 * 1024)

typedef struct PaimonRingHdr {
    uint32              magic;
    uint32              ring_size;       /* byte capacity, set at startup */
    uint64              write_pos;       /* producer position (monotonic) */
    uint64              read_pos;        /* consumer position (monotonic) */
    ConditionVariable   has_data_cv;     /* bgworker sleeps here when ring empty */
    pg_atomic_uint32    dropped_msgs;    /* messages dropped due to ring-full */
    pg_atomic_uint32    bgworker_pid;    /* bgworker PID for latch signaling; 0 = not running */
} PaimonRingHdr;

/*
 * Named-tranche LWLock for the ring buffer.
 * Obtained from GetNamedLWLockTranche() and stored process-locally.
 * Valid after paimon_shmem_init().
 */
extern LWLock *paimon_ring_lock(void);

/* Total shared memory size needed for the ring. */
extern Size paimon_shmem_size(void);

/* Define ring_size_mb GUC — call from _PG_init. */
extern void paimon_shmem_define_ring_guc(void);

/* Called from shmem_request_hook (PG 15+). */
extern void paimon_shmem_request(void);

/* Called from shmem_startup_hook: allocates and initialises the segment. */
extern void paimon_shmem_init(void);

/* Returns NULL if the extension was not in shared_preload_libraries. */
extern PaimonRingHdr *paimon_ring_hdr(void);

/* Pointer to the data array that immediately follows the header. */
extern uint8_t       *paimon_ring_data(void);

#ifdef __cplusplus
}
#endif
