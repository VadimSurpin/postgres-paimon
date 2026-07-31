// SPDX-License-Identifier: Apache-2.0
#include "paimon_manifest.h"
#include "paimon_avro.h"

#include <filesystem>
#include <fstream>
#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>

namespace fs = std::filesystem;
using namespace paimon::avro;

namespace paimon {

/* ── UUID generator ──────────────────────────────────────────────── */
static std::string make_uuid() {
    std::mt19937_64 rng(std::random_device{}());
    uint64_t a = rng(), b = rng();
    /* RFC-4122 version 4 */
    a = (a & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
    b = (b & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;
    std::ostringstream ss;
    ss << std::hex << std::setfill('0')
       << std::setw(8) << (uint32_t)(a >> 32) << '-'
       << std::setw(4) << (uint32_t)((a >> 16) & 0xFFFF) << '-'
       << std::setw(4) << (uint32_t)(a & 0xFFFF) << '-'
       << std::setw(4) << (uint32_t)(b >> 48) << '-'
       << std::setw(12) << (b & 0x0000FFFFFFFFFFFFULL);
    return ss.str();
}

static int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
}

/* Write a _NULL_COUNTS array with 64 null elements.
   SimpleStats.fromRow crashes on null _NULL_COUNTS (NPE), and crashes on
   0-element array when fieldIndex > 0 (OOB).  Writing 64 null longs means
   getLong(fieldIndex) returns 0 for any fieldIndex < 64, which != rowCount
   for a non-empty file, so the null-count check is skipped and we proceed
   to the all-null min/max comparison (which keeps the file). */
static void encode_null_counts_64(std::string& rec) {
    encode_long(rec, 1LL);             /* union idx 1 = non-null array     */
    encode_long(rec, 64LL);            /* block count = 64 elements        */
    for (int i = 0; i < 64; i++)
        encode_long(rec, 0LL);         /* each element: union idx 0 = null */
    encode_long(rec, 0LL);             /* end of array                     */
}

/* All-null BinaryRow covering up to 32 fields.
   Paimon's LeafPredicate.test returns true (keep file) when min or max
   values are null — so this effectively disables stats-based file pruning,
   which is correct when we have no real statistics to report. */
static std::string make_allnull_binaryrow() {
    static const int arity = 32;
    std::string result;
    result.reserve(4 + 8 + arity * 8);
    /* 4-byte arity (little-endian) */
    result.push_back((char)(arity & 0xFF));
    result.push_back('\0'); result.push_back('\0'); result.push_back('\0');
    /* 8-byte null bitset: bits 0-31 set (fields 0-31 null) */
    result.append("\xFF\xFF\xFF\xFF\x00\x00\x00\x00", 8);
    /* field slots: arity * 8 bytes (values irrelevant when null) */
    result.append(arity * 8, '\0');
    return result;
}

/* ── write_manifest ──────────────────────────────────────────────── */
std::string write_manifest(const std::string& manifest_dir,
                            const std::vector<DataFileStat>& files,
                            int64_t schema_id) {
    fs::create_directories(manifest_dir);
    std::string name = "manifest-" + make_uuid();
    std::string path = manifest_dir + "/" + name;

    AvroFile avro(manifest_file_schema(), path);

    /* 12 zero bytes = serialized BinaryRow.EMPTY_ROW for the partition field
       (unpartitioned table → always empty partition). */
    static const std::string empty_binaryrow(12, '\0');
    /* All-null stats BinaryRow: prevents Paimon from incorrectly pruning files
       when we have no real min/max statistics. */
    static const std::string allnull_row = make_allnull_binaryrow();

    for (const auto& f : files) {
        std::string rec;

        encode_int   (rec, 2);                     /* _VERSION = 2              */
        encode_int   (rec, 0);                     /* _KIND: ADD = 0            */
        encode_bytes (rec, empty_binaryrow);       /* _PARTITION: empty BinaryRow */
        encode_int   (rec, 0);                     /* _BUCKET: 0                */
        encode_int   (rec, 1);                     /* _TOTAL_BUCKETS: 1         */

        /* _FILE: DataFileMeta (Paimon 0.9.0 field order) */
        encode_string(rec, f.file_name);           /* _FILE_NAME                */
        encode_long  (rec, f.file_size);           /* _FILE_SIZE                */
        encode_long  (rec, f.row_count);           /* _ROW_COUNT                */
        encode_bytes (rec, allnull_row);           /* _MIN_KEY: all-null BinaryRow */
        encode_bytes (rec, allnull_row);           /* _MAX_KEY: all-null BinaryRow */
        /* _KEY_STATS: SimpleStats record — null min/max avoids filterByStats crash */
        encode_bytes (rec, allnull_row);           /* _MIN_VALUES               */
        encode_bytes (rec, allnull_row);           /* _MAX_VALUES               */
        encode_null_counts_64(rec);                /* _NULL_COUNTS: 64 nulls    */
        /* _VALUE_STATS: SimpleStats record */
        encode_bytes (rec, allnull_row);           /* _MIN_VALUES               */
        encode_bytes (rec, allnull_row);           /* _MAX_VALUES               */
        encode_null_counts_64(rec);                /* _NULL_COUNTS: 64 nulls    */
        encode_long  (rec, 0LL);                   /* _MIN_SEQUENCE_NUMBER: 0   */
        encode_long  (rec, 0LL);                   /* _MAX_SEQUENCE_NUMBER: 0   */
        encode_long  (rec, schema_id);             /* _SCHEMA_ID                */
        encode_int   (rec, 0);                     /* _LEVEL: 0                 */
        encode_long  (rec, 0LL);                   /* _EXTRA_FILES: empty array */
        encode_long  (rec, 1LL);                   /* _CREATION_TIME: union idx 1 (long) */
        encode_long  (rec, now_ms());              /* _CREATION_TIME: millis    */
        encode_long      (rec, 1LL);               /* _DELETE_ROW_COUNT: union idx 1 (long) */
        encode_long      (rec, f.delete_row_count);/* _DELETE_ROW_COUNT: value  */
        encode_union_null(rec, 0);                 /* _EMBEDDED_FILE_INDEX: null*/
        encode_union_null(rec, 0);                 /* _FILE_SOURCE: null        */

        avro.addRecord(rec);
    }
    avro.close();
    return name;
}

/* ── write_manifest_list ─────────────────────────────────────────── */
std::string write_manifest_list(const std::string& manifest_dir,
                                 const std::string& manifest_name,
                                 int64_t num_added,
                                 int64_t schema_id) {
    fs::create_directories(manifest_dir);
    std::string uuid = make_uuid();
    std::string name = "manifest-list-" + uuid + "-0";
    std::string path = manifest_dir + "/" + name;

    AvroFile avro(manifest_list_schema(), path);

    /* One ManifestFileMeta record (VersionedObjectSerializer version 2) */
    /* _MIN_VALUES/_MAX_VALUES: serialized BinaryRow.EMPTY_ROW (arity=0).
       SerializationUtils.serializeBinaryRow prepends 4-byte arity, then row data.
       For arity=0: 4 bytes arity(0) + 8 bytes null-bitset = 12 zero bytes. */
    static const std::string empty_binaryrow(12, '\0');

    int64_t manifest_file_size = 0;
    {
        std::error_code ec;
        manifest_file_size = (int64_t)fs::file_size(manifest_dir + "/" + manifest_name, ec);
    }

    std::string rec;
    encode_int   (rec, 2);                      /* _VERSION = 2        */
    encode_string(rec, manifest_name);          /* _FILE_NAME          */
    encode_long  (rec, manifest_file_size);     /* _FILE_SIZE          */
    encode_long  (rec, num_added);              /* _NUM_ADDED_FILES    */
    encode_long  (rec, 0LL);             /* _NUM_DELETED_FILES  */
    /* _PARTITION_STATS: SimpleStats record (empty for unpartitioned tables) */
    encode_bytes (rec, empty_binaryrow); /* _MIN_VALUES: empty BinaryRow  */
    encode_bytes (rec, empty_binaryrow); /* _MAX_VALUES: empty BinaryRow  */
    encode_null_counts_64(rec);          /* _NULL_COUNTS: 64 nulls            */
    encode_long  (rec, schema_id);       /* _SCHEMA_ID          */

    avro.addRecord(rec);
    avro.close();
    return name;
}

/* ── write_manifest_list (multi-entry) ──────────────────────────────── */
std::string write_manifest_list(const std::string& manifest_dir,
                                 const std::vector<ManifestRef>& entries) {
    fs::create_directories(manifest_dir);
    std::string uuid = make_uuid();
    std::string name = "manifest-list-" + uuid + "-0";
    std::string path = manifest_dir + "/" + name;

    AvroFile avro(manifest_list_schema(), path);

    /* 12 zero bytes = serialized BinaryRow.EMPTY_ROW (arity prefix + null bitset) */
    static const std::string empty_binaryrow(12, '\0');

    for (const auto& e : entries) {
        int64_t entry_file_size = 0;
        {
            std::error_code ec;
            entry_file_size = (int64_t)fs::file_size(manifest_dir + "/" + e.name, ec);
        }
        std::string rec;
        encode_int   (rec, 2);                     /* _VERSION = 2        */
        encode_string(rec, e.name);                /* _FILE_NAME          */
        encode_long  (rec, entry_file_size);       /* _FILE_SIZE          */
        encode_long  (rec, e.num_added);           /* _NUM_ADDED_FILES    */
        encode_long  (rec, 0LL);             /* _NUM_DELETED_FILES  */
        /* _PARTITION_STATS: SimpleStats record (empty for unpartitioned tables) */
        encode_bytes (rec, empty_binaryrow); /* _MIN_VALUES: empty BinaryRow      */
        encode_bytes (rec, empty_binaryrow); /* _MAX_VALUES: empty BinaryRow      */
        encode_null_counts_64(rec);          /* _NULL_COUNTS: 64 nulls            */
        encode_long  (rec, e.schema_id);     /* _SCHEMA_ID          */
        avro.addRecord(rec);
    }
    avro.close();
    return name;
}

/* ── read_manifest_names ─────────────────────────────────────────────── */

static int64_t avro_decode_long(const uint8_t* buf, size_t len, size_t& pos) {
    uint64_t n = 0; int shift = 0;
    while (pos < len) {
        uint8_t b = buf[pos++];
        n |= (uint64_t)(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    return (int64_t)((n >> 1) ^ -(int64_t)(n & 1));
}

static std::string avro_decode_string(const uint8_t* buf, size_t len, size_t& pos) {
    int64_t slen = avro_decode_long(buf, len, pos);
    if (slen <= 0 || pos + (size_t)slen > len) return "";
    std::string s((const char*)buf + pos, (size_t)slen);
    pos += (size_t)slen;
    return s;
}

std::vector<std::string> read_manifest_names(const std::string& path) {
    std::vector<std::string> result;

    std::ifstream f(path, std::ios::binary);
    if (!f) return result;
    std::string content((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());
    const uint8_t* buf = (const uint8_t*)content.data();
    size_t len = content.size(), pos = 0;

    /* Magic: Obj\x01 */
    if (len < 4 || buf[0] != 0x4F || buf[1] != 0x62 ||
        buf[2] != 0x6A || buf[3] != 0x01) return result;
    pos = 4;

    /* Skip Avro file-metadata map */
    for (;;) {
        int64_t count = avro_decode_long(buf, len, pos);
        if (count == 0) break;
        int64_t abs = (count < 0) ? -count : count;
        if (count < 0) avro_decode_long(buf, len, pos); /* skip byte size */
        for (int64_t i = 0; i < abs; i++) {
            /* key: string */
            int64_t klen = avro_decode_long(buf, len, pos);
            if (klen > 0) pos += (size_t)klen;
            /* value: bytes */
            int64_t vlen = avro_decode_long(buf, len, pos);
            if (vlen > 0) pos += (size_t)vlen;
        }
    }

    /* Skip 16-byte sync marker */
    if (pos + 16 > len) return result;
    pos += 16;

    /* Read data blocks */
    while (pos < len) {
        int64_t block_count = avro_decode_long(buf, len, pos);
        if (block_count == 0) break;
        int64_t byte_size = avro_decode_long(buf, len, pos);
        if (byte_size <= 0 || pos + (size_t)byte_size > len) break;
        size_t block_end = pos + (size_t)byte_size;

        for (int64_t i = 0; i < block_count && pos < block_end; i++) {
            /* _VERSION: int */
            avro_decode_long(buf, block_end, pos);
            /* _FILE_NAME: string — the only field we need */
            result.push_back(avro_decode_string(buf, block_end, pos));
            /* skip _FILE_SIZE, _NUM_ADDED_FILES, _NUM_DELETED_FILES */
            avro_decode_long(buf, block_end, pos);
            avro_decode_long(buf, block_end, pos);
            avro_decode_long(buf, block_end, pos);
            /* _PARTITION_STATS: SimpleStats record
               _MIN_VALUES: bytes, _MAX_VALUES: bytes, _NULL_COUNTS: union[null,array] */
            {
                int64_t blen;
                blen = avro_decode_long(buf, block_end, pos); /* _MIN_VALUES len */
                if (blen > 0) pos += (size_t)blen;
                blen = avro_decode_long(buf, block_end, pos); /* _MAX_VALUES len */
                if (blen > 0) pos += (size_t)blen;
                /* _NULL_COUNTS: union[null, array<union[null,long]>]
                   We write union index 1 (non-null) + empty array end marker */
                int64_t union_idx = avro_decode_long(buf, block_end, pos);
                if (union_idx != 0) {
                    /* index 1 = array; drain all blocks */
                    int64_t arr = avro_decode_long(buf, block_end, pos);
                    while (arr != 0) {
                        if (arr < 0) avro_decode_long(buf, block_end, pos);
                        int64_t n = (arr < 0) ? -arr : arr;
                        for (int64_t k = 0; k < n; k++) {
                            int64_t idx = avro_decode_long(buf, block_end, pos);
                            if (idx != 0) avro_decode_long(buf, block_end, pos);
                        }
                        arr = avro_decode_long(buf, block_end, pos);
                    }
                }
                /* if union_idx == 0 (null), no array data follows */
            }
            /* _SCHEMA_ID */
            avro_decode_long(buf, block_end, pos);
        }

        pos = block_end;
        if (pos + 16 <= len) pos += 16; /* sync marker */
    }

    return result;
}

} // namespace paimon
