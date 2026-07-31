// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <string>
#include <vector>
#include <cstdint>

namespace paimon {

struct DataFileStat {
    std::string file_name;   /* basename only, e.g. "data-xxx.parquet" */
    int64_t     file_size;
    int64_t     row_count;
    int64_t     delete_row_count = 0; /* rows with _VALUE_KIND=1 (DELETE/UPDATE_BEFORE) */
    int64_t     schema_id;
    int64_t     creation_time_ms;
};

/*
 * Write one manifest file (list of data files being ADDed in this commit).
 * Returns the manifest file basename (manifest-<uuid>).
 */
std::string write_manifest(const std::string& manifest_dir,
                            const std::vector<DataFileStat>& files,
                            int64_t schema_id);

/* Entry in a manifest-list file (one per manifest file). */
struct ManifestRef {
    std::string name;
    int64_t     num_added  = 0;
    int64_t     schema_id  = 0;
};

/*
 * Write a manifest-list file referencing one manifest file (delta use).
 * Returns the manifest-list basename (manifest-list-<uuid>-0).
 */
std::string write_manifest_list(const std::string& manifest_dir,
                                 const std::string& manifest_name,
                                 int64_t num_added,
                                 int64_t schema_id);

/*
 * Write a manifest-list file referencing multiple manifest files.
 * An empty vector produces a valid Avro file with 0 records (for base on
 * the first snapshot).
 * Returns the manifest-list basename, or "" on I/O error.
 */
std::string write_manifest_list(const std::string& manifest_dir,
                                 const std::vector<ManifestRef>& entries);

/*
 * Read all manifest file names listed in a manifest-list Avro file.
 * Returns an empty vector if the file is absent or unreadable.
 */
std::vector<std::string> read_manifest_names(const std::string& manifest_list_path);

} // namespace paimon
