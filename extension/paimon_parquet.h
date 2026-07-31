// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * Parquet row-group writer using Apache Arrow C++.
 *
 * Rows arrive as serialized column values from the wire protocol.
 * The writer accumulates them into Arrow builders and flushes when the
 * row-group limit is reached or on explicit close().
 *
 * Dependencies: libarrow, libparquet  (Apache Arrow C++ >= 12)
 */
#include <string>
#include <vector>
#include <cstdint>
#include <memory>

#include <arrow/api.h>
#include <parquet/arrow/writer.h>

#include "paimon_event.h"
#include "paimon_schema.h"

namespace paimon {

struct ColumnValues {
    uint16_t         attnum;
    PaimonTypeCode   type_code;
    uint32_t         typmod;
    /* One entry per row: length=-1 means NULL */
    std::vector<int32_t>     lengths;
    std::vector<std::string> values;
};

struct ParquetFileResult {
    std::string file_path;   /* absolute path written */
    std::string file_name;   /* basename */
    int64_t     file_size;
    int64_t     row_count;
    int64_t     delete_row_count = 0; /* rows with VALUE_KIND=1 */
};

/*
 * Writes one Parquet file from a batch of column data.
 *
 * Schema columns:
 *   _row_kind   INT8    (PaimonRowKind)
 *   <pk cols>   ...
 *   <data cols> ...
 *   _seq        INT64
 *
 * file_dir: directory where the Parquet file is created.
 * schema:   current Paimon schema (field order defines column order).
 * row_kinds / seqs: parallel to rows in col_data.
 */
ParquetFileResult write_parquet_file(
    const std::string&                 file_dir,
    const SchemaVersion&               schema,
    const std::vector<PaimonRowKind>&  row_kinds,
    const std::vector<uint64_t>&       seqs,
    const std::vector<ColumnValues>&   col_data);

/* Build an Arrow schema from a PaimonSchemaVersion. */
std::shared_ptr<arrow::Schema> make_arrow_schema(const SchemaVersion& sv);

} // namespace paimon
