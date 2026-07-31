// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * paimon_bgworker.h — background worker entry point.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "fmgr.h"

/* Entry point registered with RegisterBackgroundWorker.
 * Must be discoverable by dlsym — requires default symbol visibility. */
extern PGDLLEXPORT void paimon_bgworker_main(Datum arg);
extern PGDLLEXPORT void paimon_bgworker_define_gucs(void);

#ifdef __cplusplus
}
#endif
