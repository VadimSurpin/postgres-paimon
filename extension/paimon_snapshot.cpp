// SPDX-License-Identifier: Apache-2.0
#include "paimon_snapshot.h"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <chrono>

namespace fs = std::filesystem;
namespace paimon {

static int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
}

static std::string json_string_or_null(const std::string& s) {
    if (s.empty()) return "null";
    return "\"" + s + "\"";
}

int64_t read_latest_snapshot(const std::string& table_root) {
    fs::path p = fs::path(table_root) / "snapshot" / "LATEST";
    std::ifstream f(p);
    if (!f) return -1;
    int64_t id = -1;
    f >> id;
    return id;
}

std::string read_snapshot_delta_manifest(const std::string& table_root,
                                          int64_t snapshot_id) {
    if (snapshot_id < 0) return "";
    fs::path p = fs::path(table_root) / "snapshot" /
                 ("snapshot-" + std::to_string(snapshot_id));
    std::ifstream f(p);
    if (!f) return "";
    std::string line;
    while (std::getline(f, line)) {
        auto pos = line.find("\"deltaManifestList\"");
        if (pos == std::string::npos) continue;
        auto q1 = line.find('"', pos + 19);
        if (q1 == std::string::npos) continue;
        auto q2 = line.find('"', q1 + 1);
        if (q2 == std::string::npos) continue;
        return line.substr(q1 + 1, q2 - q1 - 1);
    }
    return "";
}

int64_t commit_snapshot(const std::string& table_root,
                         int64_t          last_snapshot_id,
                         int64_t          schema_id,
                         const std::string& base_manifest_list,
                         const std::string& delta_manifest_list,
                         int64_t          total_record_count,
                         int64_t          delta_record_count) {
    int64_t new_id = last_snapshot_id + 1;
    fs::path snap_dir = fs::path(table_root) / "snapshot";
    fs::create_directories(snap_dir);

    std::ostringstream o;
    o << "{\n"
      << "  \"version\" : 3,\n"
      << "  \"id\" : " << new_id << ",\n"
      << "  \"schemaId\" : " << schema_id << ",\n"
      << "  \"baseManifestList\" : " << json_string_or_null(base_manifest_list) << ",\n"
      << "  \"deltaManifestList\" : " << json_string_or_null(delta_manifest_list) << ",\n"
      << "  \"changelogManifestList\" : null,\n"
      << "  \"indexManifest\" : null,\n"
      << "  \"commitUser\" : \"clouddb\",\n"
      << "  \"commitIdentifier\" : " << new_id << ",\n"
      << "  \"commitKind\" : \"APPEND\",\n"
      << "  \"timeMillis\" : " << now_ms() << ",\n"
      << "  \"logOffsets\" : {},\n"
      << "  \"totalRecordCount\" : " << total_record_count << ",\n"
      << "  \"deltaRecordCount\" : " << delta_record_count << ",\n"
      << "  \"changelogRecordCount\" : 0,\n"
      << "  \"watermark\" : -9223372036854775808,\n"
      << "  \"statistics\" : null\n"
      << "}\n";

    std::string snap_name = "snapshot-" + std::to_string(new_id);
    fs::path tmp  = snap_dir / (snap_name + ".tmp");
    fs::path dest = snap_dir / snap_name;
    {
        std::ofstream f(tmp);
        if (!f) return -1;
        f << o.str();
    }
    fs::rename(tmp, dest);

    fs::path latest_tmp  = snap_dir / "LATEST.tmp";
    fs::path latest_dest = snap_dir / "LATEST";
    {
        std::ofstream f(latest_tmp);
        if (!f) return -1;
        f << new_id << "\n";
    }
    fs::rename(latest_tmp, latest_dest);

    return new_id;
}

} // namespace paimon
