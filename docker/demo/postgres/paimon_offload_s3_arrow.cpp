// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_offload_s3.cpp — S3 offload via Arrow's S3FileSystem (no raw AWS SDK needed).
 *
 * Sync semantics identical to the AWS-SDK version:
 *   HeadObject-equivalent via GetFileInfo size check (skip if already uploaded).
 *   PutObject-equivalent via OpenOutputStream + chunked copy.
 *   Removes local .parquet files after a successful full sync.
 */

#include "paimon_offload_s3.h"

#include <filesystem>
#include <string>
#include <vector>

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/status.h>

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace paimon {

namespace lfs = std::filesystem;

static std::shared_ptr<arrow::fs::S3FileSystem> g_s3fs;
static bool g_s3_ready = false;

/* ── SDK lifecycle ──────────────────────────────────────────────────────── */

void s3_sdk_init()
{
    arrow::fs::S3GlobalOptions gopts;
    gopts.log_level = arrow::fs::S3LogLevel::Fatal; /* suppress SDK noise */
    auto st = arrow::fs::InitializeS3(gopts);
    if (!st.ok())
        ereport(WARNING, errmsg("paimon_s3: InitializeS3: %s", st.ToString().c_str()));
    else
        g_s3_ready = true;
}

void s3_sdk_shutdown()
{
    g_s3fs.reset();
    if (g_s3_ready) {
        (void)arrow::fs::FinalizeS3();
        g_s3_ready = false;
    }
}

/* ── Internal helpers ───────────────────────────────────────────────────── */

static arrow::fs::S3FileSystem *
get_fs(const S3Config &cfg)
{
    if (g_s3fs) return g_s3fs.get();

    arrow::fs::S3Options opts = arrow::fs::S3Options::Defaults();
    if (!cfg.region.empty())
        opts.region = cfg.region;
    if (!cfg.endpoint.empty()) {
        opts.endpoint_override = cfg.endpoint;
        opts.scheme = "http"; /* MinIO is plain HTTP in the demo */
    }

    auto result = arrow::fs::S3FileSystem::Make(opts);
    if (!result.ok()) {
        ereport(WARNING, errmsg("paimon_s3: S3FileSystem::Make: %s",
                                result.status().ToString().c_str()));
        return nullptr;
    }
    g_s3fs = *result;
    return g_s3fs.get();
}

/* Returns true if the remote object already exists with the expected byte count. */
static bool
remote_matches(arrow::fs::FileSystem &fs,
               const std::string     &path,
               int64_t                expected_size)
{
    auto info = fs.GetFileInfo(path);
    if (!info.ok()) return false;
    if (info->type() == arrow::fs::FileType::NotFound) return false;
    return info->size() == expected_size;
}

/* Upload src_path → dest_path on the given Arrow filesystem. */
static bool
put_file(arrow::fs::FileSystem  &fs,
         const std::string      &dest_path,
         const lfs::path        &src_path)
{
    auto local = arrow::io::ReadableFile::Open(src_path.string());
    if (!local.ok()) {
        ereport(WARNING, errmsg("paimon_s3: open %s: %s",
                                src_path.c_str(), local.status().ToString().c_str()));
        return false;
    }

    auto out = fs.OpenOutputStream(dest_path);
    if (!out.ok()) {
        ereport(WARNING, errmsg("paimon_s3: OpenOutputStream %s: %s",
                                dest_path.c_str(), out.status().ToString().c_str()));
        return false;
    }

    constexpr int64_t kChunk = 8 << 20; /* 8 MiB */
    for (;;) {
        auto chunk = (*local)->Read(kChunk);
        if (!chunk.ok()) {
            ereport(WARNING, errmsg("paimon_s3: read %s: %s",
                                    src_path.c_str(), chunk.status().ToString().c_str()));
            return false;
        }
        if ((*chunk)->size() == 0) break;
        auto ws = (*out)->Write((*chunk)->data(), (*chunk)->size());
        if (!ws.ok()) {
            ereport(WARNING, errmsg("paimon_s3: write %s: %s",
                                    dest_path.c_str(), ws.ToString().c_str()));
            return false;
        }
    }

    auto cs = (*out)->Close();
    if (!cs.ok()) {
        ereport(WARNING, errmsg("paimon_s3: close %s: %s",
                                dest_path.c_str(), cs.ToString().c_str()));
        return false;
    }
    return true;
}

/* ── Public API ─────────────────────────────────────────────────────────── */

bool
s3_upload_table(const S3Config    &cfg,
                const std::string &ware_dir,
                const std::string &table_name)
{
    if (cfg.bucket.empty()) return true;

    lfs::path local_root = lfs::path(ware_dir) / table_name;
    std::error_code ec;
    if (!lfs::exists(local_root, ec)) return true;

    auto *fs = get_fs(cfg);
    if (!fs) return false;

    /* Arrow S3FileSystem paths are "bucket/key". */
    std::string key_prefix = cfg.prefix.empty()
                           ? table_name
                           : cfg.prefix + "/" + table_name;
    std::string path_prefix = cfg.bucket + "/" + key_prefix;

    bool all_ok = true;
    std::vector<lfs::path> uploaded_parquets;

    for (const auto &entry : lfs::recursive_directory_iterator(local_root, ec)) {
        if (!entry.is_regular_file()) continue;

        lfs::path rel  = lfs::relative(entry.path(), local_root, ec);
        std::string dest = path_prefix + "/" + rel.generic_string();
        int64_t size = (int64_t)entry.file_size(ec);

        /* LATEST is a mutable pointer file — always re-upload.
         * All other Paimon files are content-addressed, so size equality is
         * a safe dedup guard. */
        bool mutable_file = (entry.path().filename() == "LATEST");
        if (!mutable_file && remote_matches(*fs, dest, size)) continue;

        if (put_file(*fs, dest, entry.path())) {
            if (entry.path().extension() == ".parquet")
                uploaded_parquets.push_back(entry.path());
        } else {
            all_ok = false;
        }
    }

    if (all_ok) {
        for (const auto &p : uploaded_parquets)
            lfs::remove(p, ec);
    }

    return all_ok;
}

} // namespace paimon
