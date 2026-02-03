//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/pg/pg_types.hpp
//
// DuckDB to PostgreSQL type mapping
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include <cstdint>
#include <unordered_map>
#include <string>

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
            return TypeOid::INT4;
        case duckdb::LogicalTypeId::UBIGINT:
            return TypeOid::INT8;
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

} // namespace pg
} // namespace duckdb_server
