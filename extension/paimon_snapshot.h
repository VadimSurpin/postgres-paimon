// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <string>
#include <cstdint>

namespace paimon {

/*
 * Atomically commit a new snapshot to <table_root>/snapshot/snapshot-N.
 *
 * base_manifest_list: manifest-list covering all data files committed in
 *   prior snapshots (empty string → written as JSON null).  For the very
 *   first snapshot pass write_manifest_list(dir, {}) and supply the name.
 * delta_manifest_list: manifest-list covering only this commit's files.
 *
 * Returns the committed snapshot ID on success, -1 on failure.
 */
int64_t commit_snapshot(const std::string& table_root,
                         int64_t          last_snapshot_id,
                         int64_t          schema_id,
                         const std::string& base_manifest_list,
                         const std::string& delta_manifest_list,
                         int64_t          total_record_count,
                         int64_t          delta_record_count);

/* Read the latest snapshot ID from snapshot/LATEST, -1 if absent. */
int64_t read_latest_snapshot(const std::string& table_root);

/*
 * Read the delta manifest-list filename from snapshot-N.
 * Returns "" if absent or unreadable.
 */
std::string read_snapshot_delta_manifest(const std::string& table_root,
                                          int64_t snapshot_id);

} // namespace paimon
