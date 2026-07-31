// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * PG-safe type mapping helpers — no Arrow headers included.
 * Include this from files that also include PostgreSQL headers.
 * Arrow-dependent helpers live in type_map.h.
 */
#include <string>
#include "paimon_event.h"

namespace paimon {

inline std::string paimon_type_string(PaimonTypeCode tc, uint32_t typmod,
                                      bool not_null) {
    std::string base;
    switch (tc) {
        case PTYPE_BOOLEAN:    base = "BOOLEAN";  break;
        case PTYPE_SMALLINT:   base = "SMALLINT"; break;
        case PTYPE_INT:        base = "INT";       break;
        case PTYPE_BIGINT:     base = "BIGINT";    break;
        case PTYPE_FLOAT:      base = "FLOAT";     break;
        case PTYPE_DOUBLE:     base = "DOUBLE";    break;
        case PTYPE_DECIMAL: {
            int prec  = (int)((typmod >> 16) & 0xFFFF);
            int scale = (int)(typmod & 0xFFFF);
            if (prec == 0) { prec = 38; scale = 18; }
            base = "DECIMAL(" + std::to_string(prec) + ", " +
                                std::to_string(scale) + ")";
            break;
        }
        case PTYPE_STRING:
        case PTYPE_TEXT:       base = "STRING";    break;
        case PTYPE_BYTES:      base = "BYTES";     break;
        case PTYPE_DATE:       base = "DATE";       break;
        case PTYPE_TIMESTAMP:  base = "TIMESTAMP"; break;
        case PTYPE_TIMESTAMPTZ:base = "TIMESTAMP WITH LOCAL TIME ZONE"; break;
        default:               base = "STRING";    break;
    }
    return not_null ? base + " NOT NULL" : base;
}

inline PaimonTypeCode pg_oid_to_paimon_type(unsigned int typoid,
                                             uint32_t* typmod_out,
                                             uint32_t pg_typmod) {
    *typmod_out = 0;
    switch (typoid) {
        case 16:   return PTYPE_BOOLEAN;
        case 21:   return PTYPE_SMALLINT;
        case 23:   return PTYPE_INT;
        case 20:   return PTYPE_BIGINT;
        case 700:  return PTYPE_FLOAT;
        case 701:  return PTYPE_DOUBLE;
        case 1700: {
            if (pg_typmod > 4) {
                int raw   = (int)(pg_typmod - 4);
                int prec  = (raw >> 16) & 0xFFFF;
                int scale = raw & 0xFFFF;
                *typmod_out = ((uint32_t)prec << 16) | (uint32_t)scale;
            }
            return PTYPE_DECIMAL;
        }
        case 25:   return PTYPE_STRING;
        case 1043: return PTYPE_STRING;
        case 1042: return PTYPE_STRING;
        case 2950: return PTYPE_STRING;
        case 17:   return PTYPE_BYTES;
        case 1082: return PTYPE_DATE;
        case 1114: return PTYPE_TIMESTAMP;
        case 1184: return PTYPE_TIMESTAMPTZ;
        default:   return PTYPE_STRING;
    }
}

} // namespace paimon
