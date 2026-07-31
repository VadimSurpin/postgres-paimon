// SPDX-License-Identifier: Apache-2.0
#include "paimon_parquet.h"
#include "type_map.h"

#include <arrow/api.h>

/* Configurable system column names set by bgworker GUCs. */
extern char *g_paimon_seq_col_guc;
extern char *g_paimon_rk_col_guc;
#define SEQ_COL (g_paimon_seq_col_guc && *g_paimon_seq_col_guc ? g_paimon_seq_col_guc : "_seq")
#define RK_COL  (g_paimon_rk_col_guc  && *g_paimon_rk_col_guc  ? g_paimon_rk_col_guc  : "_row_kind")
#include <arrow/array/builder_base.h>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <filesystem>
#include <stdexcept>
#include <random>
#include <sstream>
#include <iomanip>
#include <cstring>
#include <algorithm>
#include <numeric>

namespace fs = std::filesystem;
namespace paimon {

static std::string make_uuid() {
    std::mt19937_64 rng(std::random_device{}());
    uint64_t a = rng(), b = rng();
    a = (a & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
    b = (b & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;
    std::ostringstream ss;
    ss << std::hex << std::setfill('0')
       << std::setw(8)  << (uint32_t)(a >> 32)            << '-'
       << std::setw(4)  << (uint32_t)((a >> 16) & 0xFFFF) << '-'
       << std::setw(4)  << (uint32_t)(a & 0xFFFF)         << '-'
       << std::setw(4)  << (uint32_t)(b >> 48)            << '-'
       << std::setw(12) << (b & 0x0000FFFFFFFFFFFFULL);
    return ss.str();
}

/* ── Arrow schema ────────────────────────────────────────────────── */

std::shared_ptr<arrow::Schema> make_arrow_schema(const SchemaVersion& sv) {
    arrow::FieldVector fields;

    /* 1. _row_kind — CDC row kind, prepended for readRawCdcCounts compatibility */
    fields.push_back(arrow::field(RK_COL, arrow::int8(), false));

    bool has_pk = !sv.primary_keys.empty();
    if (has_pk) {
        /* 2. _KEY_<pk> — key copies required by Paimon merge-tree reader */
        for (const auto& pk_name : sv.primary_keys) {
            for (const auto& f : sv.fields) {
                if (f.name == pk_name && f.field_id != 32767) {
                    fields.push_back(arrow::field("_KEY_" + pk_name,
                                                  arrow_type(f.type_code, f.typmod), false));
                    break;
                }
            }
        }
        /* 3. _SEQUENCE_NUMBER — sequence for merge ordering (same value as SEQ_COL) */
        fields.push_back(arrow::field("_SEQUENCE_NUMBER", arrow::int64(), false));
        /* 4. _VALUE_KIND — 0=ADD/PUT, 1=DELETE */
        fields.push_back(arrow::field("_VALUE_KIND", arrow::int8(), false));
    }

    /* 5. User columns (exclude _seq which is field_id 32767) */
    for (const auto& f : sv.fields) {
        if (f.field_id == 32767) continue;
        auto dtype = arrow_type(f.type_code, f.typmod);
        fields.push_back(arrow::field(f.name, dtype, !f.not_null));
    }

    /* 6. _seq — internal sequence column */
    fields.push_back(arrow::field(SEQ_COL, arrow::int64(), false));

    return arrow::schema(fields);
}

/* ── Decode a column value from wire bytes into an Arrow builder ─── */

static void append_value(arrow::ArrayBuilder* builder,
                          PaimonTypeCode tc,
                          int32_t len,
                          const std::string& val) {
    if (len < 0) {
        ARROW_CHECK_OK(builder->AppendNull());
        return;
    }
    const char* p = val.data();
    switch (tc) {
        case PTYPE_BOOLEAN: {
            ARROW_CHECK_OK(static_cast<arrow::BooleanBuilder*>(builder)->Append(p[0] != 0));
            break;
        }
        case PTYPE_SMALLINT: {
            int16_t v; memcpy(&v, p, 2);
            ARROW_CHECK_OK(static_cast<arrow::Int16Builder*>(builder)->Append(v));
            break;
        }
        case PTYPE_INT: {
            int32_t v; memcpy(&v, p, 4);
            ARROW_CHECK_OK(static_cast<arrow::Int32Builder*>(builder)->Append(v));
            break;
        }
        case PTYPE_BIGINT: {
            int64_t v; memcpy(&v, p, 8);
            ARROW_CHECK_OK(static_cast<arrow::Int64Builder*>(builder)->Append(v));
            break;
        }
        case PTYPE_FLOAT: {
            float v; memcpy(&v, p, 4);
            ARROW_CHECK_OK(static_cast<arrow::FloatBuilder*>(builder)->Append(v));
            break;
        }
        case PTYPE_DOUBLE: {
            double v; memcpy(&v, p, 8);
            ARROW_CHECK_OK(static_cast<arrow::DoubleBuilder*>(builder)->Append(v));
            break;
        }
        case PTYPE_DATE: {
            int32_t v; memcpy(&v, p, 4);
            ARROW_CHECK_OK(static_cast<arrow::Date32Builder*>(builder)->Append(v));
            break;
        }
        case PTYPE_TIMESTAMP:
        case PTYPE_TIMESTAMPTZ: {
            int64_t v; memcpy(&v, p, 8);
            ARROW_CHECK_OK(static_cast<arrow::TimestampBuilder*>(builder)->Append(v));
            break;
        }
        case PTYPE_DECIMAL: {
            std::string s(p, len);
            arrow::Decimal128 dec;
            int32_t prec, scale;
            ARROW_CHECK_OK(arrow::Decimal128::FromString(s, &dec, &prec, &scale));
            ARROW_CHECK_OK(static_cast<arrow::Decimal128Builder*>(builder)->Append(dec));
            break;
        }
        case PTYPE_BYTES: {
            ARROW_CHECK_OK(static_cast<arrow::LargeBinaryBuilder*>(builder)->Append(p, len));
            break;
        }
        default: {
            ARROW_CHECK_OK(static_cast<arrow::StringBuilder*>(builder)->Append(p, len));
            break;
        }
    }
}

/* Compare two values of type tc. Returns negative/0/positive. */
static int cmp_pk_val(PaimonTypeCode tc,
                      const std::string& va, int32_t la,
                      const std::string& vb, int32_t lb) {
    if (la < 0 && lb < 0) return 0;
    if (la < 0) return -1;
    if (lb < 0) return  1;
    switch (tc) {
        case PTYPE_SMALLINT: { int16_t a, b; memcpy(&a, va.data(), 2); memcpy(&b, vb.data(), 2); return (a>b)-(a<b); }
        case PTYPE_INT:      { int32_t a, b; memcpy(&a, va.data(), 4); memcpy(&b, vb.data(), 4); return (a>b)-(a<b); }
        case PTYPE_BIGINT:   { int64_t a, b; memcpy(&a, va.data(), 8); memcpy(&b, vb.data(), 8); return (a>b)-(a<b); }
        case PTYPE_FLOAT:    { float   a, b; memcpy(&a, va.data(), 4); memcpy(&b, vb.data(), 4); return (a>b)-(a<b); }
        case PTYPE_DOUBLE:   { double  a, b; memcpy(&a, va.data(), 8); memcpy(&b, vb.data(), 8); return (a>b)-(a<b); }
        default:
            return va.compare(0, (size_t)la, vb.data(), (size_t)lb);
    }
}

/* ── write_parquet_file ──────────────────────────────────────────── */

ParquetFileResult write_parquet_file(
    const std::string&                 file_dir,
    const SchemaVersion&               schema,
    const std::vector<PaimonRowKind>&  row_kinds,
    const std::vector<uint64_t>&       seqs,
    const std::vector<ColumnValues>&   col_data) {

    int64_t nrows = (int64_t)row_kinds.size();

    /* Build a sort permutation: primary keys (ascending) then sequence number.
       Paimon's compaction sort-merge assumes each input file is already sorted
       by key.  Without sorting here, SortMergeReader never deduplicates rows
       with the same key that are non-adjacent (event-order). */
    std::vector<size_t> perm(nrows);
    std::iota(perm.begin(), perm.end(), 0);
    if (!schema.primary_keys.empty()) {
        /* Find each PK column: (ColumnValues index, type_code) */
        struct PkCol { size_t cv_idx; PaimonTypeCode tc; };
        std::vector<PkCol> pk_cols;
        for (const auto& pk_name : schema.primary_keys) {
            for (const auto& f : schema.fields) {
                if (f.name != pk_name || f.field_id == 32767) continue;
                for (size_t i = 0; i < col_data.size(); i++) {
                    if (col_data[i].attnum == f.field_id) {
                        pk_cols.push_back({i, f.type_code});
                        break;
                    }
                }
                break;
            }
        }
        std::stable_sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
            for (const auto& pk : pk_cols) {
                const auto& cv = col_data[pk.cv_idx];
                int c = cmp_pk_val(pk.tc,
                                   cv.values[a], cv.lengths[a],
                                   cv.values[b], cv.lengths[b]);
                if (c != 0) return c < 0;
            }
            return seqs[a] < seqs[b]; /* same key: earlier seq first */
        });
    }

    auto arrow_schema = make_arrow_schema(schema);

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    /* _row_kind builder */
    auto rk_builder = std::make_shared<arrow::Int8Builder>(pool);
    ARROW_CHECK_OK(rk_builder->Reserve(nrows));
    for (int64_t i = 0; i < nrows; i++)
        ARROW_CHECK_OK(rk_builder->Append((int8_t)row_kinds[perm[i]]));

    /* Per-field builders keyed by field_id (= attnum) */
    std::unordered_map<uint16_t, std::unique_ptr<arrow::ArrayBuilder>> builders;
    std::vector<uint16_t> field_order;
    for (const auto& f : schema.fields) {
        if (f.field_id == 32767) continue;
        auto dtype = arrow_type(f.type_code, f.typmod);
        std::unique_ptr<arrow::ArrayBuilder> b;
        ARROW_CHECK_OK(arrow::MakeBuilder(pool, dtype, &b));
        ARROW_CHECK_OK(b->Reserve(nrows));
        builders[f.field_id] = std::move(b);
        field_order.push_back(f.field_id);
    }

    /* Build lookup: attnum → ColumnValues index */
    std::unordered_map<uint16_t, size_t> col_idx;
    for (size_t i = 0; i < col_data.size(); ++i)
        col_idx[col_data[i].attnum] = i;

    for (int64_t i = 0; i < nrows; ++i) {
        size_t row = perm[i];
        for (uint16_t field_id : field_order) {
            auto bit = builders.find(field_id);
            if (bit == builders.end()) continue;
            arrow::ArrayBuilder* b = bit->second.get();

            PaimonTypeCode tc = PTYPE_STRING;
            for (const auto& f : schema.fields)
                if (f.field_id == field_id) { tc = f.type_code; break; }

            auto cit = col_idx.find(field_id);
            if (cit == col_idx.end()) {
                ARROW_CHECK_OK(b->AppendNull());
                continue;
            }
            const auto& cv = col_data[cit->second];
            append_value(b, tc, cv.lengths[row], cv.values[row]);
        }
    }

    /* _seq builder */
    auto seq_builder = std::make_shared<arrow::Int64Builder>(pool);
    ARROW_CHECK_OK(seq_builder->Reserve(nrows));
    for (int64_t i = 0; i < nrows; i++)
        ARROW_CHECK_OK(seq_builder->Append((int64_t)seqs[perm[i]]));

    /* Finish _row_kind */
    std::shared_ptr<arrow::Array> rk_array;
    ARROW_CHECK_OK(rk_builder->Finish(&rk_array));

    /* Finish user column arrays */
    std::unordered_map<uint16_t, std::shared_ptr<arrow::Array>> col_arrays;
    for (uint16_t fid : field_order) {
        std::shared_ptr<arrow::Array> a;
        ARROW_CHECK_OK(builders[fid]->Finish(&a));
        col_arrays[fid] = a;
    }

    /* Finish _seq */
    std::shared_ptr<arrow::Array> seq_array;
    ARROW_CHECK_OK(seq_builder->Finish(&seq_array));

    /* _VALUE_KIND: 0=ADD for INSERT/UPDATE_AFTER, 1=DELETE for UPDATE_BEFORE/DELETE */
    bool has_pk = !schema.primary_keys.empty();
    std::shared_ptr<arrow::Array> vk_array;
    if (has_pk) {
        auto vk_b = std::make_shared<arrow::Int8Builder>(pool);
        ARROW_CHECK_OK(vk_b->Reserve(nrows));
        for (int64_t i = 0; i < nrows; i++) {
            auto k = row_kinds[perm[i]];
            ARROW_CHECK_OK(vk_b->Append(
                (int8_t)((k == PROW_UPDATE_BEFORE || k == PROW_DELETE) ? 1 : 0)));
        }
        ARROW_CHECK_OK(vk_b->Finish(&vk_array));
    }

    /* Assemble arrays in schema order (must match make_arrow_schema) */
    arrow::ArrayVector arrays;
    arrays.push_back(rk_array);                  /* 1. _row_kind */

    if (has_pk) {
        /* 2. _KEY_<pk> — same array as the pk user column */
        for (const auto& pk_name : schema.primary_keys) {
            for (const auto& f : schema.fields) {
                if (f.name == pk_name && f.field_id != 32767) {
                    arrays.push_back(col_arrays[f.field_id]);
                    break;
                }
            }
        }
        arrays.push_back(seq_array);              /* 3. _SEQUENCE_NUMBER */
        arrays.push_back(vk_array);               /* 4. _VALUE_KIND */
    }

    for (uint16_t fid : field_order)
        arrays.push_back(col_arrays[fid]);        /* 5. user columns */

    arrays.push_back(seq_array);                  /* 6. _seq */

    auto table = arrow::Table::Make(arrow_schema, arrays);

    fs::create_directories(file_dir);
    std::string fname = "data-" + make_uuid() + ".parquet";
    std::string fpath = file_dir + "/" + fname;

    std::shared_ptr<arrow::io::FileOutputStream> outfile =
        arrow::io::FileOutputStream::Open(fpath).ValueOrDie();

    auto props = parquet::WriterProperties::Builder()
                     .compression(parquet::Compression::SNAPPY)
                     ->enable_store_decimal_as_integer()
                     ->build();
    auto arrow_props = parquet::ArrowWriterProperties::Builder().build();

    std::unique_ptr<parquet::arrow::FileWriter> writer =
        parquet::arrow::FileWriter::Open(
            *arrow_schema, pool, outfile, props, arrow_props).ValueOrDie();

    ARROW_CHECK_OK(writer->WriteTable(*table, nrows));
    ARROW_CHECK_OK(writer->Close());
    ARROW_CHECK_OK(outfile->Close());

    int64_t fsize = (int64_t)fs::file_size(fpath);
    int64_t delete_count = 0;
    for (auto k : row_kinds)
        if (k == PROW_UPDATE_BEFORE || k == PROW_DELETE) ++delete_count;
    return {fpath, fname, fsize, nrows, delete_count};
}

} // namespace paimon
