// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <string>

namespace paimon {

struct S3Config {
    std::string bucket;
    std::string prefix;    // key prefix inside bucket, no trailing slash
    std::string region;    // empty → SDK credential/config chain
    std::string endpoint;  // empty → AWS; set to MinIO/Ceph URL otherwise
};

// SDK lifecycle — call once at bgworker start/stop.
void s3_sdk_init();
void s3_sdk_shutdown();

// Sync local_dir/<table_name>/ → s3://<bucket>/<prefix>/<table_name>/
// Skips files already uploaded with the same byte-length (HeadObject check).
// After a successful full sync, removes .parquet files from bucket-0/ to
// keep local disk usage bounded.
// Returns true on success; logs WARNING and returns false on error.
bool s3_upload_table(const S3Config &cfg,
                     const std::string &ware_dir,
                     const std::string &table_name);

} // namespace paimon
