// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * Arrow-dependent type mapping helpers.
 * Do NOT include this from files that also include PostgreSQL headers —
 * Arrow enum members clash with PG macros.  Use type_map_pg.h instead
 * for the PG-side (pg_oid_to_paimon_type, paimon_type_string).
 */
#include <memory>
#include <arrow/api.h>
#include "type_map_pg.h"

namespace paimon {

inline std::shared_ptr<arrow::DataType> arrow_type(PaimonTypeCode tc,
                                                    uint32_t typmod) {
    switch (tc) {
        case PTYPE_BOOLEAN:     return arrow::boolean();
        case PTYPE_SMALLINT:    return arrow::int16();
        case PTYPE_INT:         return arrow::int32();
        case PTYPE_BIGINT:      return arrow::int64();
        case PTYPE_FLOAT:       return arrow::float32();
        case PTYPE_DOUBLE:      return arrow::float64();
        case PTYPE_DECIMAL: {
            int prec  = (int)((typmod >> 16) & 0xFFFF);
            int scale = (int)(typmod & 0xFFFF);
            if (prec == 0) { prec = 38; scale = 18; }
            return arrow::decimal128(prec, scale);
        }
        case PTYPE_STRING:
        case PTYPE_TEXT:        return arrow::utf8();
        case PTYPE_BYTES:       return arrow::large_binary();
        case PTYPE_DATE:        return arrow::date32();
        case PTYPE_TIMESTAMP:   return arrow::timestamp(arrow::TimeUnit::MICRO);
        case PTYPE_TIMESTAMPTZ: return arrow::timestamp(arrow::TimeUnit::MICRO,
                                                         "UTC");
        default:                return arrow::utf8();
    }
}

} // namespace paimon
