// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_shmem.cpp — shared memory setup for paimon_heap ring buffer.
 *
 * In PostgreSQL 15+ RequestAddinShmemSpace / RequestNamedLWLockTranche
 * must be called from shmem_request_hook, NOT from _PG_init.
 */

extern "C" {
#include "postgres.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
}

#include "paimon_shmem.h"

/* ring_size_mb GUC — defined in _PG_init so it is available before the
 * shmem_request_hook fires. */
static int guc_ring_size_mb = PAIMON_RING_SIZE_DEFAULT / (1024 * 1024);

static PaimonRingHdr *g_ring_hdr    = NULL;
static LWLockPadded  *g_ring_tranche = NULL;

void
paimon_shmem_define_ring_guc(void)
{
    DefineCustomIntVariable(
        "paimon_heap.ring_size_mb",
        "Size of the paimon_heap shared-memory ring buffer (MB)",
        NULL,
        &guc_ring_size_mb,
        PAIMON_RING_SIZE_DEFAULT / (1024 * 1024),
        1, 512,
        PGC_POSTMASTER,
        GUC_UNIT_MB,
        NULL, NULL, NULL);
}

Size
paimon_shmem_size(void)
{
    Size ring_bytes = (Size)guc_ring_size_mb * 1024 * 1024;
    return MAXALIGN(sizeof(PaimonRingHdr)) + ring_bytes;
}

/* Called from shmem_request_hook. */
void
paimon_shmem_request(void)
{
    RequestAddinShmemSpace(paimon_shmem_size());
    RequestNamedLWLockTranche("paimon_heap_ring", 1);
}

/* Called from shmem_startup_hook. */
void
paimon_shmem_init(void)
{
    bool found;
    Size ring_bytes = (Size)guc_ring_size_mb * 1024 * 1024;
    Size total      = MAXALIGN(sizeof(PaimonRingHdr)) + ring_bytes;

    g_ring_hdr     = (PaimonRingHdr *) ShmemInitStruct(PAIMON_SHMEM_NAME, total, &found);
    g_ring_tranche = GetNamedLWLockTranche("paimon_heap_ring");

    if (!found)
    {
        memset(g_ring_hdr, 0, total);
        g_ring_hdr->magic     = PAIMON_RING_MAGIC;
        g_ring_hdr->ring_size = (uint32) ring_bytes;
        ConditionVariableInit(&g_ring_hdr->has_data_cv);
        g_ring_hdr->write_pos = 0;
        g_ring_hdr->read_pos  = 0;
        pg_atomic_init_u32(&g_ring_hdr->dropped_msgs, 0);
    }
}

PaimonRingHdr *
paimon_ring_hdr(void)
{
    return g_ring_hdr;
}

uint8_t *
paimon_ring_data(void)
{
    if (!g_ring_hdr) return NULL;
    return (uint8_t *)g_ring_hdr + MAXALIGN(sizeof(PaimonRingHdr));
}

LWLock *
paimon_ring_lock(void)
{
    return g_ring_tranche ? &g_ring_tranche[0].lock : NULL;
}
