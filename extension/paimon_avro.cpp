// SPDX-License-Identifier: Apache-2.0
#include "paimon_avro.h"
#include <fstream>
#include <stdexcept>

namespace paimon {
namespace avro {

/* ── Avro file magic + header ─────────────────────────────────────── */
static const uint8_t AVRO_MAGIC[4] = {0x4F, 0x62, 0x6A, 0x01}; /* Obj\1 */

void AvroFile::writeHeader(std::string& out) {
    out.append((const char*)AVRO_MAGIC, 4);

    /* File metadata: map<string,bytes> with avro.schema and avro.codec */
    encode_long(out, 2); /* 2 entries */
    encode_string(out, "avro.schema");
    encode_bytes(out, schema_json_);
    encode_string(out, "avro.codec");
    encode_bytes(out, "null"); /* no compression */
    encode_long(out, 0); /* end of map */

    /* 16-byte sync marker */
    out.append((const char*)sync_.data(), 16);
}

void AvroFile::writeBlock(std::string& out, const std::vector<std::string>& recs) {
    if (recs.empty()) return;
    std::string data;
    for (const auto& r : recs) data.append(r);

    encode_long(out, (int64_t)recs.size()); /* object count */
    encode_long(out, (int64_t)data.size()); /* byte size */
    out.append(data);
    out.append((const char*)sync_.data(), 16);
}

AvroFile::AvroFile(const std::string& schema_json, const std::string& path)
    : path_(path), schema_json_(schema_json) {
    sync_ = make_sync();
}

void AvroFile::addRecord(const std::string& record_bytes) {
    records_.push_back(record_bytes);
}

bool AvroFile::close() {
    std::string out;
    writeHeader(out);
    writeBlock(out, records_);

    std::ofstream f(path_, std::ios::binary | std::ios::trunc);
    if (!f) return false;
    f.write(out.data(), out.size());
    return f.good();
}

/* ── Avro schemas ────────────────────────────────────────────────── */

/*
 * ManifestFileMeta schema (written into manifest-list files).
 * ManifestFileMetaSerializer extends VersionedObjectSerializer (version 2),
 * so the first field is _VERSION (int).  _PARTITION_STATS is SimpleStats.
 * Matches AvroSchemaConverter.convertToSchema(versionType(ManifestFileMeta.SCHEMA)).
 */
const char* manifest_list_schema() {
    return R"({
  "type": "record",
  "name": "ManifestFileMeta",
  "fields": [
    {"name": "_VERSION",           "type": "int"},
    {"name": "_FILE_NAME",         "type": "string"},
    {"name": "_FILE_SIZE",         "type": "long"},
    {"name": "_NUM_ADDED_FILES",   "type": "long"},
    {"name": "_NUM_DELETED_FILES", "type": "long"},
    {"name": "_PARTITION_STATS",   "type": {
        "type": "record", "name": "SimpleStats",
        "fields": [
          {"name": "_MIN_VALUES",  "type": "bytes"},
          {"name": "_MAX_VALUES",  "type": "bytes"},
          {"name": "_NULL_COUNTS",
           "type": ["null", {"type": "array", "items": ["null", "long"]}],
           "default": null}
        ]}},
    {"name": "_SCHEMA_ID", "type": "long"}
  ]
})";
}

/*
 * ManifestEntry Avro schema for manifest files.
 * Matches the exact schema produced by AvroSchemaConverter for
 * VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA) in Paimon 0.9.0.
 * Key points:
 *  - First field _VERSION (int) = 2
 *  - _KIND is int (not enum): 0=ADD, 1=DELETE
 *  - DataFileMeta fields in Paimon 0.9.0 order (no _FILE_FORMAT; _MIN_KEY/_MAX_KEY
 *    before stats; added _MIN/_MAX_SEQUENCE_NUMBER; _CREATION_TIME nullable long)
 *  - _KEY_STATS/_VALUE_STATS are SimpleStats records (ROW<BYTES,BYTES,ARRAY<BIGINT>?>)
 */
const char* manifest_file_schema() {
    return R"({
  "type": "record",
  "name": "ManifestEntry",
  "fields": [
    {"name": "_VERSION",       "type": "int"},
    {"name": "_KIND",          "type": "int"},
    {"name": "_PARTITION",     "type": "bytes"},
    {"name": "_BUCKET",        "type": "int"},
    {"name": "_TOTAL_BUCKETS", "type": "int"},
    {"name": "_FILE", "type": {
      "type": "record", "name": "DataFileMeta",
      "fields": [
        {"name": "_FILE_NAME",           "type": "string"},
        {"name": "_FILE_SIZE",           "type": "long"},
        {"name": "_ROW_COUNT",           "type": "long"},
        {"name": "_MIN_KEY",             "type": "bytes"},
        {"name": "_MAX_KEY",             "type": "bytes"},
        {"name": "_KEY_STATS", "type": {
          "type": "record", "name": "SimpleStats_KEY",
          "fields": [
            {"name": "_MIN_VALUES",  "type": "bytes"},
            {"name": "_MAX_VALUES",  "type": "bytes"},
            {"name": "_NULL_COUNTS",
             "type": ["null", {"type": "array", "items": ["null", "long"]}],
             "default": null}
          ]}},
        {"name": "_VALUE_STATS", "type": {
          "type": "record", "name": "SimpleStats_VALUE",
          "fields": [
            {"name": "_MIN_VALUES",  "type": "bytes"},
            {"name": "_MAX_VALUES",  "type": "bytes"},
            {"name": "_NULL_COUNTS",
             "type": ["null", {"type": "array", "items": ["null", "long"]}],
             "default": null}
          ]}},
        {"name": "_MIN_SEQUENCE_NUMBER", "type": "long"},
        {"name": "_MAX_SEQUENCE_NUMBER", "type": "long"},
        {"name": "_SCHEMA_ID",           "type": "long"},
        {"name": "_LEVEL",               "type": "int"},
        {"name": "_EXTRA_FILES",
         "type": {"type": "array", "items": "string"}},
        {"name": "_CREATION_TIME",
         "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null},
        {"name": "_DELETE_ROW_COUNT",
         "type": ["null", "long"], "default": null},
        {"name": "_EMBEDDED_FILE_INDEX",
         "type": ["null", "bytes"], "default": null},
        {"name": "_FILE_SOURCE",
         "type": ["null", "int"], "default": null}
      ]}}
  ]
})";
}

} // namespace avro
} // namespace paimon
