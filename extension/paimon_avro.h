// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * Minimal hand-written Avro binary encoder.
 *
 * Covers exactly the schema needed for Paimon manifest-list and manifest
 * files.  No external avro-c dependency required.
 *
 * Avro file layout:
 *   magic(4) | file-meta-map | sync-marker(16) | blocks...
 * Each block:
 *   count(zigzag-long) | byte-size(zigzag-long) | records | sync-marker(16)
 */
#include <array>
#include <string>
#include <vector>
#include <cstdint>
#include <random>
#include <cstring>

namespace paimon {
namespace avro {

/* ── zigzag varint ─────────────────────────────────────────────────── */
inline void encode_long(std::string& buf, int64_t v) {
    uint64_t n = (uint64_t)((v << 1) ^ (v >> 63));
    while (n & ~0x7F) {
        buf.push_back((char)((n & 0x7F) | 0x80));
        n >>= 7;
    }
    buf.push_back((char)(n & 0x7F));
}
inline void encode_int(std::string& buf, int32_t v) {
    encode_long(buf, (int64_t)v);
}

/* ── primitives ────────────────────────────────────────────────────── */
inline void encode_string(std::string& buf, const std::string& s) {
    encode_long(buf, (int64_t)s.size());
    buf.append(s);
}
inline void encode_bytes(std::string& buf, const std::string& b) {
    encode_long(buf, (int64_t)b.size());
    buf.append(b);
}
inline void encode_null(std::string&) {} /* null = 0 bytes */

/* Array block: encode count then items, then terminator 0 */
inline void begin_array(std::string& buf, int64_t count) {
    encode_long(buf, count);
}
inline void end_array(std::string& buf) {
    encode_long(buf, 0);
}
/* Union: encode index, then value */
inline void encode_union_null(std::string& buf, int null_index) {
    encode_int(buf, null_index);
}
inline void encode_union_index(std::string& buf, int idx) {
    encode_int(buf, idx);
}

/* ── Sync marker ───────────────────────────────────────────────────── */
using SyncMarker = std::array<uint8_t, 16>;

inline SyncMarker make_sync() {
    SyncMarker m;
    std::mt19937_64 rng(std::random_device{}());
    for (int i = 0; i < 16; i += 8) {
        uint64_t v = rng();
        memcpy(&m[i], &v, 8);
    }
    return m;
}

/* ── AvroFile builder ──────────────────────────────────────────────── */
class AvroFile {
public:
    AvroFile(const std::string& schema_json, const std::string& path);

    /* Append a pre-encoded record (raw Avro binary) */
    void addRecord(const std::string& record_bytes);

    /* Flush all buffered records as one block and write to file */
    bool close();

private:
    std::string        path_;
    SyncMarker         sync_;
    std::string        schema_json_;
    std::vector<std::string> records_;

    void writeHeader(std::string& out);
    void writeBlock(std::string& out, const std::vector<std::string>& recs);
};

/*
 * Schema JSON strings for our two Avro file types.
 * Defined as raw strings to keep paimon_manifest.cpp self-contained.
 */
const char* manifest_list_schema();
const char* manifest_file_schema();

} // namespace avro
} // namespace paimon
