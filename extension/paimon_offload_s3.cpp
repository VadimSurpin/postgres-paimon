// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_offload_s3.cpp — S3 offload for paimon_heap via AWS SDK C++.
 *
 * Sync semantics:
 *   For every regular file under <ware_dir>/<table_name>/ we do a HeadObject
 *   check; if the key already exists with the same byte length we skip it.
 *   After all files are uploaded we remove local .parquet files from bucket-0/
 *   so disk usage stays bounded.
 */

#include "paimon_offload_s3.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

/* PostgreSQL logging — included AFTER all C++ headers. */
extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace paimon {

namespace fs = std::filesystem;

/* ── SDK lifecycle ──────────────────────────────────────────────────────── */

static Aws::SDKOptions g_sdk_options;

void s3_sdk_init()
{
    /* Don't let the SDK install a SIGPIPE handler — PostgreSQL owns signals. */
    g_sdk_options.httpOptions.installSigPipeHandler = false;
    Aws::InitAPI(g_sdk_options);
}

void s3_sdk_shutdown()
{
    Aws::ShutdownAPI(g_sdk_options);
}

/* ── Internal helpers ───────────────────────────────────────────────────── */

static std::unique_ptr<Aws::S3::S3Client>
make_client(const S3Config &cfg)
{
    Aws::Client::ClientConfiguration cc;
    if (!cfg.region.empty())
        cc.region = cfg.region;
    if (!cfg.endpoint.empty()) {
        cc.endpointOverride = cfg.endpoint;
        /* Path-style addressing is required for non-AWS endpoints (e.g. MinIO). */
        cc.scheme = Aws::Http::Scheme::HTTP;
    }
    /* Use the full credential chain: env vars, ~/.aws/credentials, IAM role. */
    return std::make_unique<Aws::S3::S3Client>(
        cc,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        /*useVirtualAddressing=*/cfg.endpoint.empty());
}

/* Returns true if the S3 key already exists with exactly expected_size bytes. */
static bool
remote_file_matches(Aws::S3::S3Client &client,
                    const std::string &bucket,
                    const std::string &key,
                    int64_t            expected_size)
{
    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    auto outcome = client.HeadObject(req);
    if (!outcome.IsSuccess()) return false;
    return outcome.GetResult().GetContentLength() == expected_size;
}

/* Upload a single local file to S3. Returns true on success. */
static bool
put_file(Aws::S3::S3Client &client,
         const std::string  &bucket,
         const std::string  &key,
         const fs::path     &local_path)
{
    auto stream = Aws::MakeShared<Aws::FStream>(
        "PaimonS3Upload",
        local_path.string().c_str(),
        std::ios_base::in | std::ios_base::binary);

    if (!stream->good()) {
        ereport(WARNING, errmsg("paimon_s3: cannot open %s for upload",
                                local_path.c_str()));
        return false;
    }

    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    req.SetBody(stream);

    auto outcome = client.PutObject(req);
    if (!outcome.IsSuccess()) {
        ereport(WARNING,
                errmsg("paimon_s3: PutObject failed for key %s: %s",
                       key.c_str(),
                       outcome.GetError().GetMessage().c_str()));
        return false;
    }
    return true;
}

/* ── Public API ─────────────────────────────────────────────────────────── */

bool
s3_upload_table(const S3Config     &cfg,
                const std::string  &ware_dir,
                const std::string  &table_name)
{
    if (cfg.bucket.empty()) return true;

    fs::path local_root = fs::path(ware_dir) / table_name;
    std::error_code ec;
    if (!fs::exists(local_root, ec)) return true; /* nothing to upload */

    std::string key_prefix = cfg.prefix.empty()
                           ? table_name
                           : cfg.prefix + "/" + table_name;

    auto client = make_client(cfg);

    bool all_ok = true;
    std::vector<fs::path> uploaded_parquets;

    /* Walk the entire table directory tree. */
    for (const auto &entry : fs::recursive_directory_iterator(local_root, ec)) {
        if (!entry.is_regular_file()) continue;

        fs::path rel  = fs::relative(entry.path(), local_root, ec);
        std::string key = key_prefix + "/" + rel.generic_string();
        int64_t   size  = (int64_t)entry.file_size(ec);

        /* LATEST is mutable (snapshot id changes each flush) — always re-upload.
         * All other Paimon metadata files are content-addressed by name, so the
         * size check is a valid dedup guard. */
        bool mutable_file = (entry.path().filename() == "LATEST");
        if (!mutable_file && remote_file_matches(*client, cfg.bucket, key, size)) continue;

        if (put_file(*client, cfg.bucket, key, entry.path())) {
            if (entry.path().extension() == ".parquet")
                uploaded_parquets.push_back(entry.path());
        } else {
            all_ok = false;
        }
    }

    if (all_ok) {
        /* Remove local Parquet files — schema/manifest/snapshot stay locally. */
        for (const auto &p : uploaded_parquets)
            fs::remove(p, ec);
    }

    return all_ok;
}

} // namespace paimon
