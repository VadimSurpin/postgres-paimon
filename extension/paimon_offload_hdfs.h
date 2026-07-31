// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <string>

namespace paimon {

struct HdfsConfig {
    /* WebHDFS namenode HTTP(S) host and port, e.g. "namenode1:9870". */
    std::string namenode;       // empty → try hdfs-site.xml auto-discovery
    bool        use_https = false;

    /* Base HDFS path under which table dirs are placed. */
    std::string base_path;      // e.g. "/warehouse/paimon"

    /* HDFS user name (empty = current OS user). */
    std::string user;

    /* Directory containing hdfs-site.xml (empty = search standard locations). */
    std::string conf_dir;

    /* Kerberos: both must be set together; empty = simple auth. */
    std::string krb_principal;  // e.g. "hdfs/host@REALM"
    std::string krb_keytab;     // absolute path to keytab file
};

// Sync local_dir/<table_name>/ → hdfs://<namenode>/<base_path>/<table_name>/
// Skips files already on HDFS with the same byte length (GETFILESTATUS check).
// After a successful sync, removes local .parquet files from bucket-0/.
// Returns true on success; logs WARNING and returns false on error.
bool hdfs_upload_table(const HdfsConfig  &cfg,
                       const std::string &ware_dir,
                       const std::string &table_name);

} // namespace paimon
