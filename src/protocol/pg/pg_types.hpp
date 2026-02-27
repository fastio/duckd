//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/pg/pg_types.hpp
//
// DuckDB to PostgreSQL type mapping
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/uuid.hpp"
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <unordered_map>
#include <string>
#include <vector>

namespace duckdb_server {
namespace pg {

//===----------------------------------------------------------------------===//
// PostgreSQL Type OIDs
//===----------------------------------------------------------------------===//
namespace TypeOid {
    constexpr int32_t BOOL = 16;
    constexpr int32_t BYTEA = 17;
    constexpr int32_t CHAR = 18;
    constexpr int32_t NAME = 19;
    constexpr int32_t INT8 = 20;
    constexpr int32_t INT2 = 21;
    constexpr int32_t INT2VECTOR = 22;
    constexpr int32_t INT4 = 23;
    constexpr int32_t REGPROC = 24;
    constexpr int32_t TEXT = 25;
    constexpr int32_t OID = 26;
    constexpr int32_t TID = 27;
    constexpr int32_t XID = 28;
    constexpr int32_t CID = 29;
    constexpr int32_t OIDVECTOR = 30;
    constexpr int32_t JSON = 114;
    constexpr int32_t XML = 142;
    constexpr int32_t POINT = 600;
    constexpr int32_t LSEG = 601;
    constexpr int32_t PATH = 602;
    constexpr int32_t BOX = 603;
    constexpr int32_t POLYGON = 604;
    constexpr int32_t LINE = 628;
    constexpr int32_t FLOAT4 = 700;
    constexpr int32_t FLOAT8 = 701;
    constexpr int32_t UNKNOWN = 705;
    constexpr int32_t CIRCLE = 718;
    constexpr int32_t MONEY = 790;
    constexpr int32_t MACADDR = 829;
    constexpr int32_t INET = 869;
    constexpr int32_t CIDR = 650;
    constexpr int32_t MACADDR8 = 774;
    constexpr int32_t BPCHAR = 1042;      // CHAR(n)
    constexpr int32_t VARCHAR = 1043;
    constexpr int32_t DATE = 1082;
    constexpr int32_t TIME = 1083;
    constexpr int32_t TIMESTAMP = 1114;
    constexpr int32_t TIMESTAMPTZ = 1184;
    constexpr int32_t INTERVAL = 1186;
    constexpr int32_t TIMETZ = 1266;
    constexpr int32_t BIT = 1560;
    constexpr int32_t VARBIT = 1562;
    constexpr int32_t NUMERIC = 1700;
    constexpr int32_t REFCURSOR = 1790;
    constexpr int32_t REGPROCEDURE = 2202;
    constexpr int32_t REGOPER = 2203;
    constexpr int32_t REGOPERATOR = 2204;
    constexpr int32_t REGCLASS = 2205;
    constexpr int32_t REGTYPE = 2206;
    constexpr int32_t UUID = 2950;
    constexpr int32_t JSONB = 3802;
    constexpr int32_t INT4RANGE = 3904;
    constexpr int32_t NUMRANGE = 3906;
    constexpr int32_t TSRANGE = 3908;
    constexpr int32_t TSTZRANGE = 3910;
    constexpr int32_t DATERANGE = 3912;
    constexpr int32_t INT8RANGE = 3926;

    // Array types
    constexpr int32_t BOOL_ARRAY = 1000;
    constexpr int32_t BYTEA_ARRAY = 1001;
    constexpr int32_t INT2_ARRAY = 1005;
    constexpr int32_t INT4_ARRAY = 1007;
    constexpr int32_t TEXT_ARRAY = 1009;
    constexpr int32_t INT8_ARRAY = 1016;
    constexpr int32_t FLOAT4_ARRAY = 1021;
    constexpr int32_t FLOAT8_ARRAY = 1022;
    constexpr int32_t VARCHAR_ARRAY = 1015;
}

//===----------------------------------------------------------------------===//
// DuckDB to PostgreSQL Type Mapping
//===----------------------------------------------------------------------===//
inline int32_t DuckDBTypeToOid(duckdb::LogicalTypeId type_id) {
    switch (type_id) {
        case duckdb::LogicalTypeId::BOOLEAN:
            return TypeOid::BOOL;
        case duckdb::LogicalTypeId::TINYINT:
        case duckdb::LogicalTypeId::SMALLINT:
            return TypeOid::INT2;
        case duckdb::LogicalTypeId::INTEGER:
            return TypeOid::INT4;
        case duckdb::LogicalTypeId::BIGINT:
            return TypeOid::INT8;
        case duckdb::LogicalTypeId::UTINYINT:
        case duckdb::LogicalTypeId::USMALLINT:
            return TypeOid::INT2;
        case duckdb::LogicalTypeId::UINTEGER:
        case duckdb::LogicalTypeId::UBIGINT:
            return TypeOid::NUMERIC;
        case duckdb::LogicalTypeId::HUGEINT:
        case duckdb::LogicalTypeId::UHUGEINT:
            return TypeOid::NUMERIC;
        case duckdb::LogicalTypeId::FLOAT:
            return TypeOid::FLOAT4;
        case duckdb::LogicalTypeId::DOUBLE:
            return TypeOid::FLOAT8;
        case duckdb::LogicalTypeId::DECIMAL:
            return TypeOid::NUMERIC;
        case duckdb::LogicalTypeId::VARCHAR:
            return TypeOid::VARCHAR;
        case duckdb::LogicalTypeId::BLOB:
            return TypeOid::BYTEA;
        case duckdb::LogicalTypeId::DATE:
            return TypeOid::DATE;
        case duckdb::LogicalTypeId::TIME:
            return TypeOid::TIME;
        case duckdb::LogicalTypeId::TIMESTAMP:
            return TypeOid::TIMESTAMP;
        case duckdb::LogicalTypeId::TIMESTAMP_TZ:
            return TypeOid::TIMESTAMPTZ;
        case duckdb::LogicalTypeId::INTERVAL:
            return TypeOid::INTERVAL;
        case duckdb::LogicalTypeId::UUID:
            return TypeOid::UUID;
        case duckdb::LogicalTypeId::LIST:
            return TypeOid::TEXT_ARRAY;  // Simplified
        case duckdb::LogicalTypeId::STRUCT:
        case duckdb::LogicalTypeId::MAP:
            return TypeOid::TEXT;  // Serialize as text
        default:
            return TypeOid::TEXT;  // Fallback to text
    }
}

inline int32_t DuckDBTypeToOid(const duckdb::LogicalType& type) {
    return DuckDBTypeToOid(type.id());
}

//===----------------------------------------------------------------------===//
// Type Size (-1 for variable length)
//===----------------------------------------------------------------------===//
inline int16_t GetTypeSize(int32_t oid) {
    switch (oid) {
        case TypeOid::BOOL:
            return 1;
        case TypeOid::INT2:
            return 2;
        case TypeOid::INT4:
        case TypeOid::FLOAT4:
        case TypeOid::OID:
        case TypeOid::DATE:
            return 4;
        case TypeOid::INT8:
        case TypeOid::FLOAT8:
        case TypeOid::TIMESTAMP:
        case TypeOid::TIMESTAMPTZ:
        case TypeOid::TIME:
        case TypeOid::TIMETZ:
        case TypeOid::MONEY:
            return 8;
        case TypeOid::UUID:
            return 16;
        default:
            return -1;  // Variable length
    }
}

//===----------------------------------------------------------------------===//
// Value Formatting (DuckDB Value -> PostgreSQL Text)
//===----------------------------------------------------------------------===//
inline std::string FormatValue(const duckdb::Value& value) {
    if (value.IsNull()) {
        return "";  // NULL handled separately
    }
    return value.ToString();
}

//===----------------------------------------------------------------------===//
// Value Formatting Into Existing Buffer (avoids allocation for common types)
//===----------------------------------------------------------------------===//
inline void FormatValueInto(const duckdb::Value& value, std::string& out) {
    if (value.IsNull()) {
        out.clear();
        return;
    }
    auto type_id = value.type().id();
    switch (type_id) {
        case duckdb::LogicalTypeId::BOOLEAN:
            out = value.GetValue<bool>() ? "t" : "f";
            return;
        case duckdb::LogicalTypeId::TINYINT: {
            char buf[5];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value.GetValue<int8_t>());
            out.assign(buf, ptr);
            return;
        }
        case duckdb::LogicalTypeId::SMALLINT: {
            char buf[7];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value.GetValue<int16_t>());
            out.assign(buf, ptr);
            return;
        }
        case duckdb::LogicalTypeId::INTEGER: {
            char buf[12];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value.GetValue<int32_t>());
            out.assign(buf, ptr);
            return;
        }
        case duckdb::LogicalTypeId::BIGINT: {
            char buf[21];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value.GetValue<int64_t>());
            out.assign(buf, ptr);
            return;
        }
        case duckdb::LogicalTypeId::FLOAT: {
            char buf[16];
            int n = snprintf(buf, sizeof(buf), "%.9g", value.GetValue<float>());
            out.assign(buf, n);
            return;
        }
        case duckdb::LogicalTypeId::DOUBLE: {
            char buf[32];
            int n = snprintf(buf, sizeof(buf), "%.17g", value.GetValue<double>());
            out.assign(buf, n);
            return;
        }
        default:
            out = value.ToString();
            return;
    }
}

//===----------------------------------------------------------------------===//
// Direct Cell Formatting (bypasses duckdb::Value allocation)
//===----------------------------------------------------------------------===//

// Helper: format a scaled integer as a decimal string (e.g. 12345 with scale=2 → "123.45")
// Works for any signed integer type that supports to_chars or Hugeint::ToString.
inline void FormatDecimalString(const std::string& int_str, uint8_t scale, std::string& out) {
    if (scale == 0) {
        out = int_str;
        return;
    }
    // Handle negative sign
    bool negative = (!int_str.empty() && int_str[0] == '-');
    const char* digits = int_str.data() + (negative ? 1 : 0);
    size_t num_digits = int_str.size() - (negative ? 1 : 0);

    out.clear();
    if (negative) out += '-';

    if (num_digits <= scale) {
        // Value is in range (-1, 1): need "0." prefix and zero-padding
        out += "0.";
        for (size_t i = 0; i < scale - num_digits; i++) out += '0';
        out.append(digits, num_digits);
    } else {
        // Integer part + decimal part
        size_t int_part_len = num_digits - scale;
        out.append(digits, int_part_len);
        out += '.';
        out.append(digits + int_part_len, scale);
    }
}

// Helper: write zero-padded integer into buffer, returns pointer past end
inline char* WritePadded(char* buf, int32_t val, int width) {
    // Write digits right-to-left
    char* end = buf + width;
    char* p = end;
    int32_t v = val < 0 ? -val : val;
    while (p > buf) {
        *(--p) = '0' + static_cast<char>(v % 10);
        v /= 10;
    }
    return end;
}

// Format a cell directly from raw column data into buffer.
// Returns false if NULL (out is not modified).
inline bool FormatCellDirect(const duckdb::UnifiedVectorFormat& col_data,
                             idx_t row_idx,
                             const duckdb::LogicalType& type,
                             std::string& out) {
    auto idx = col_data.sel->get_index(row_idx);
    if (!col_data.validity.RowIsValid(idx)) {
        return false;  // NULL
    }

    switch (type.id()) {
        case duckdb::LogicalTypeId::BOOLEAN: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<bool>(col_data)[idx];
            out = val ? "t" : "f";
            return true;
        }
        case duckdb::LogicalTypeId::TINYINT: {
            char buf[5];
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int8_t>(col_data)[idx];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
            out.assign(buf, ptr);
            return true;
        }
        case duckdb::LogicalTypeId::SMALLINT: {
            char buf[7];
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int16_t>(col_data)[idx];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
            out.assign(buf, ptr);
            return true;
        }
        case duckdb::LogicalTypeId::INTEGER: {
            char buf[12];
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int32_t>(col_data)[idx];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
            out.assign(buf, ptr);
            return true;
        }
        case duckdb::LogicalTypeId::BIGINT: {
            char buf[21];
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int64_t>(col_data)[idx];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
            out.assign(buf, ptr);
            return true;
        }
        case duckdb::LogicalTypeId::UTINYINT: {
            char buf[4];
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<uint8_t>(col_data)[idx];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
            out.assign(buf, ptr);
            return true;
        }
        case duckdb::LogicalTypeId::USMALLINT: {
            char buf[6];
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<uint16_t>(col_data)[idx];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
            out.assign(buf, ptr);
            return true;
        }
        case duckdb::LogicalTypeId::UINTEGER: {
            char buf[11];
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<uint32_t>(col_data)[idx];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
            out.assign(buf, ptr);
            return true;
        }
        case duckdb::LogicalTypeId::UBIGINT: {
            char buf[21];
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<uint64_t>(col_data)[idx];
            auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
            out.assign(buf, ptr);
            return true;
        }
        case duckdb::LogicalTypeId::FLOAT: {
            char buf[16];
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<float>(col_data)[idx];
            int n = snprintf(buf, sizeof(buf), "%.9g", static_cast<double>(val));
            out.assign(buf, n);
            return true;
        }
        case duckdb::LogicalTypeId::DOUBLE: {
            char buf[32];
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<double>(col_data)[idx];
            int n = snprintf(buf, sizeof(buf), "%.17g", val);
            out.assign(buf, n);
            return true;
        }
        case duckdb::LogicalTypeId::VARCHAR: {
            auto str = duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(col_data)[idx];
            out.assign(str.GetData(), str.GetSize());
            return true;
        }
        case duckdb::LogicalTypeId::DATE: {
            auto date = duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::date_t>(col_data)[idx];
            int32_t year, month, day;
            duckdb::Date::Convert(date, year, month, day);
            // "YYYY-MM-DD" = max 15 chars (negative years)
            char buf[16];
            char* p = buf;
            if (year < 0) {
                *p++ = '-';
                year = -year;
            }
            // Year: variable width
            auto [yp, yec] = std::to_chars(p, buf + sizeof(buf), year);
            // Pad year to at least 4 digits
            int ylen = static_cast<int>(yp - p);
            if (ylen < 4) {
                // Shift right and pad with zeros
                int pad = 4 - ylen;
                std::memmove(p + pad, p, ylen);
                std::memset(p, '0', pad);
                yp = p + 4;
            }
            p = yp;
            *p++ = '-';
            p = WritePadded(p, month, 2);
            *p++ = '-';
            p = WritePadded(p, day, 2);
            out.assign(buf, p);
            return true;
        }
        case duckdb::LogicalTypeId::TIME: {
            auto time = duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::dtime_t>(col_data)[idx];
            int32_t hour, min, sec, micros;
            duckdb::Time::Convert(time, hour, min, sec, micros);
            // "HH:MM:SS[.ffffff]"
            char buf[16];
            char* p = WritePadded(buf, hour, 2);
            *p++ = ':';
            p = WritePadded(p, min, 2);
            *p++ = ':';
            p = WritePadded(p, sec, 2);
            if (micros > 0) {
                *p++ = '.';
                p = WritePadded(p, micros, 6);
                // Trim trailing zeros
                while (p > buf && *(p - 1) == '0') --p;
            }
            out.assign(buf, p);
            return true;
        }
        case duckdb::LogicalTypeId::TIMESTAMP:
        case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
            auto ts = duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::timestamp_t>(col_data)[idx];
            duckdb::date_t date_part;
            duckdb::dtime_t time_part;
            duckdb::Timestamp::Convert(ts, date_part, time_part);

            int32_t year, month, day;
            duckdb::Date::Convert(date_part, year, month, day);
            int32_t hour, min, sec, micros;
            duckdb::Time::Convert(time_part, hour, min, sec, micros);

            // "YYYY-MM-DD HH:MM:SS[.ffffff]" = max ~32 chars
            char buf[36];
            char* p = buf;
            if (year < 0) {
                *p++ = '-';
                year = -year;
            }
            auto [yp, yec] = std::to_chars(p, buf + sizeof(buf), year);
            int ylen = static_cast<int>(yp - p);
            if (ylen < 4) {
                int pad = 4 - ylen;
                std::memmove(p + pad, p, ylen);
                std::memset(p, '0', pad);
                yp = p + 4;
            }
            p = yp;
            *p++ = '-';
            p = WritePadded(p, month, 2);
            *p++ = '-';
            p = WritePadded(p, day, 2);
            *p++ = ' ';
            p = WritePadded(p, hour, 2);
            *p++ = ':';
            p = WritePadded(p, min, 2);
            *p++ = ':';
            p = WritePadded(p, sec, 2);
            if (micros > 0) {
                *p++ = '.';
                p = WritePadded(p, micros, 6);
                while (p > buf && *(p - 1) == '0') --p;
            }
            out.assign(buf, p);
            return true;
        }
        case duckdb::LogicalTypeId::INTERVAL: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::interval_t>(col_data)[idx];
            out = duckdb::Interval::ToString(val);
            return true;
        }
        case duckdb::LogicalTypeId::HUGEINT: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::hugeint_t>(col_data)[idx];
            out = duckdb::Hugeint::ToString(val);
            return true;
        }
        case duckdb::LogicalTypeId::UHUGEINT: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::uhugeint_t>(col_data)[idx];
            out = duckdb::Uhugeint::ToString(val);
            return true;
        }
        case duckdb::LogicalTypeId::UUID: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::hugeint_t>(col_data)[idx];
            char buf[36];
            duckdb::UUID::ToString(val, buf);
            out.assign(buf, 36);
            return true;
        }
        case duckdb::LogicalTypeId::DECIMAL: {
            uint8_t width, scale;
            type.GetDecimalProperties(width, scale);
            switch (type.InternalType()) {
                case duckdb::PhysicalType::INT16: {
                    auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int16_t>(col_data)[idx];
                    char buf[7];
                    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
                    FormatDecimalString(std::string(buf, ptr), scale, out);
                    return true;
                }
                case duckdb::PhysicalType::INT32: {
                    auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int32_t>(col_data)[idx];
                    char buf[12];
                    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
                    FormatDecimalString(std::string(buf, ptr), scale, out);
                    return true;
                }
                case duckdb::PhysicalType::INT64: {
                    auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int64_t>(col_data)[idx];
                    char buf[21];
                    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), val);
                    FormatDecimalString(std::string(buf, ptr), scale, out);
                    return true;
                }
                case duckdb::PhysicalType::INT128: {
                    auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::hugeint_t>(col_data)[idx];
                    FormatDecimalString(duckdb::Hugeint::ToString(val), scale, out);
                    return true;
                }
                default:
                    return false;
            }
        }
        default:
            // Fallback: use Value::ToString() via chunk GetValue
            // Caller must handle this case by falling back to GetValue path
            return false;
    }
}

//===----------------------------------------------------------------------===//
// Binary Cell Formatting (PG binary wire format, big-endian)
//===----------------------------------------------------------------------===//

// Format a cell as PG binary into byte buffer. Returns false if NULL or unsupported type.
// Supported: BOOL, TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT,
//            FLOAT, DOUBLE
inline bool FormatCellBinary(const duckdb::UnifiedVectorFormat& col_data,
                             idx_t row_idx,
                             const duckdb::LogicalType& type,
                             std::vector<uint8_t>& out) {
    auto idx = col_data.sel->get_index(row_idx);
    if (!col_data.validity.RowIsValid(idx)) {
        return false;  // NULL
    }

    switch (type.id()) {
        case duckdb::LogicalTypeId::BOOLEAN: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<bool>(col_data)[idx];
            out.resize(1);
            out[0] = val ? 1 : 0;
            return true;
        }
        case duckdb::LogicalTypeId::TINYINT: {
            // PG INT2 = 2 bytes big-endian
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int8_t>(col_data)[idx];
            int16_t v16 = static_cast<int16_t>(val);
            int16_t net = htons(static_cast<uint16_t>(v16));
            out.resize(2);
            std::memcpy(out.data(), &net, 2);
            return true;
        }
        case duckdb::LogicalTypeId::SMALLINT: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int16_t>(col_data)[idx];
            int16_t net = htons(static_cast<uint16_t>(val));
            out.resize(2);
            std::memcpy(out.data(), &net, 2);
            return true;
        }
        case duckdb::LogicalTypeId::INTEGER: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int32_t>(col_data)[idx];
            int32_t net = htonl(static_cast<uint32_t>(val));
            out.resize(4);
            std::memcpy(out.data(), &net, 4);
            return true;
        }
        case duckdb::LogicalTypeId::BIGINT: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<int64_t>(col_data)[idx];
            uint64_t v = static_cast<uint64_t>(val);
            uint64_t net =
                ((v & 0x00000000000000FFULL) << 56) |
                ((v & 0x000000000000FF00ULL) << 40) |
                ((v & 0x0000000000FF0000ULL) << 24) |
                ((v & 0x00000000FF000000ULL) << 8)  |
                ((v & 0x000000FF00000000ULL) >> 8)  |
                ((v & 0x0000FF0000000000ULL) >> 24) |
                ((v & 0x00FF000000000000ULL) >> 40) |
                ((v & 0xFF00000000000000ULL) >> 56);
            out.resize(8);
            std::memcpy(out.data(), &net, 8);
            return true;
        }
        case duckdb::LogicalTypeId::UTINYINT: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<uint8_t>(col_data)[idx];
            int16_t v16 = static_cast<int16_t>(val);
            int16_t net = htons(static_cast<uint16_t>(v16));
            out.resize(2);
            std::memcpy(out.data(), &net, 2);
            return true;
        }
        case duckdb::LogicalTypeId::USMALLINT: {
            // Sent as INT2 (fits in signed range: 0..65535 needs INT4 but OID is INT2,
            // actually USMALLINT max 65535 > INT2 max 32767, so send as INT4)
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<uint16_t>(col_data)[idx];
            int32_t v32 = static_cast<int32_t>(val);
            int32_t net = htonl(static_cast<uint32_t>(v32));
            out.resize(4);
            std::memcpy(out.data(), &net, 4);
            return true;
        }
        // UINTEGER and UBIGINT are mapped to NUMERIC OID — no simple binary encoding,
        // fall through to text format.
        case duckdb::LogicalTypeId::FLOAT: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<float>(col_data)[idx];
            uint32_t bits;
            std::memcpy(&bits, &val, 4);
            uint32_t net = htonl(bits);
            out.resize(4);
            std::memcpy(out.data(), &net, 4);
            return true;
        }
        case duckdb::LogicalTypeId::DOUBLE: {
            auto val = duckdb::UnifiedVectorFormat::GetDataUnsafe<double>(col_data)[idx];
            uint64_t bits;
            std::memcpy(&bits, &val, 8);
            uint64_t net =
                ((bits & 0x00000000000000FFULL) << 56) |
                ((bits & 0x000000000000FF00ULL) << 40) |
                ((bits & 0x0000000000FF0000ULL) << 24) |
                ((bits & 0x00000000FF000000ULL) << 8)  |
                ((bits & 0x000000FF00000000ULL) >> 8)  |
                ((bits & 0x0000FF0000000000ULL) >> 24) |
                ((bits & 0x00FF000000000000ULL) >> 40) |
                ((bits & 0xFF00000000000000ULL) >> 56);
            out.resize(8);
            std::memcpy(out.data(), &net, 8);
            return true;
        }
        default:
            // Unsupported type for binary format — caller should fall back to text
            return false;
    }
}

} // namespace pg
} // namespace duckdb_server
