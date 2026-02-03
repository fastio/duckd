//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/pg/test_pg_types.cpp
//
// Unit tests for DuckDB to PostgreSQL type mapping
//===----------------------------------------------------------------------===//

#include "protocol/pg/pg_types.hpp"
#include <cassert>
#include <iostream>

using namespace duckdb_server::pg;

//===----------------------------------------------------------------------===//
// Type OID Tests
//===----------------------------------------------------------------------===//

void TestTypeOidConstants() {
    std::cout << "  Testing Type OID Constants..." << std::endl;

    // Basic types
    assert(TypeOid::BOOL == 16);
    assert(TypeOid::BYTEA == 17);
    assert(TypeOid::INT8 == 20);
    assert(TypeOid::INT2 == 21);
    assert(TypeOid::INT4 == 23);
    assert(TypeOid::TEXT == 25);
    assert(TypeOid::FLOAT4 == 700);
    assert(TypeOid::FLOAT8 == 701);

    // String types
    assert(TypeOid::VARCHAR == 1043);
    assert(TypeOid::BPCHAR == 1042);

    // Date/time types
    assert(TypeOid::DATE == 1082);
    assert(TypeOid::TIME == 1083);
    assert(TypeOid::TIMESTAMP == 1114);
    assert(TypeOid::TIMESTAMPTZ == 1184);
    assert(TypeOid::INTERVAL == 1186);

    // Special types
    assert(TypeOid::UUID == 2950);
    assert(TypeOid::NUMERIC == 1700);
    assert(TypeOid::JSON == 114);
    assert(TypeOid::JSONB == 3802);

    // Array types
    assert(TypeOid::INT4_ARRAY == 1007);
    assert(TypeOid::TEXT_ARRAY == 1009);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// DuckDB to PostgreSQL Type Mapping Tests
//===----------------------------------------------------------------------===//

void TestDuckDBTypeToOid() {
    std::cout << "  Testing DuckDBTypeToOid..." << std::endl;

    // Boolean
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::BOOLEAN) == TypeOid::BOOL);

    // Integer types
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::TINYINT) == TypeOid::INT2);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::SMALLINT) == TypeOid::INT2);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::INTEGER) == TypeOid::INT4);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::BIGINT) == TypeOid::INT8);

    // Unsigned integer types
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::UTINYINT) == TypeOid::INT2);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::USMALLINT) == TypeOid::INT2);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::UINTEGER) == TypeOid::INT4);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::UBIGINT) == TypeOid::INT8);

    // Huge integers
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::HUGEINT) == TypeOid::NUMERIC);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::UHUGEINT) == TypeOid::NUMERIC);

    // Floating point
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::FLOAT) == TypeOid::FLOAT4);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::DOUBLE) == TypeOid::FLOAT8);

    // Decimal
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::DECIMAL) == TypeOid::NUMERIC);

    // String
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::VARCHAR) == TypeOid::VARCHAR);

    // Binary
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::BLOB) == TypeOid::BYTEA);

    // Date/time
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::DATE) == TypeOid::DATE);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::TIME) == TypeOid::TIME);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::TIMESTAMP) == TypeOid::TIMESTAMP);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::TIMESTAMP_TZ) == TypeOid::TIMESTAMPTZ);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::INTERVAL) == TypeOid::INTERVAL);

    // UUID
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::UUID) == TypeOid::UUID);

    // Complex types (fallback to TEXT)
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::LIST) == TypeOid::TEXT_ARRAY);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::STRUCT) == TypeOid::TEXT);
    assert(DuckDBTypeToOid(duckdb::LogicalTypeId::MAP) == TypeOid::TEXT);

    std::cout << "    PASSED" << std::endl;
}

void TestDuckDBTypeToOidWithLogicalType() {
    std::cout << "  Testing DuckDBTypeToOid with LogicalType..." << std::endl;

    // Test with actual LogicalType objects
    assert(DuckDBTypeToOid(duckdb::LogicalType::INTEGER) == TypeOid::INT4);
    assert(DuckDBTypeToOid(duckdb::LogicalType::VARCHAR) == TypeOid::VARCHAR);
    assert(DuckDBTypeToOid(duckdb::LogicalType::DOUBLE) == TypeOid::FLOAT8);
    assert(DuckDBTypeToOid(duckdb::LogicalType::BOOLEAN) == TypeOid::BOOL);
    assert(DuckDBTypeToOid(duckdb::LogicalType::DATE) == TypeOid::DATE);
    assert(DuckDBTypeToOid(duckdb::LogicalType::TIMESTAMP) == TypeOid::TIMESTAMP);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Type Size Tests
//===----------------------------------------------------------------------===//

void TestGetTypeSize() {
    std::cout << "  Testing GetTypeSize..." << std::endl;

    // Fixed-size types
    assert(GetTypeSize(TypeOid::BOOL) == 1);
    assert(GetTypeSize(TypeOid::INT2) == 2);
    assert(GetTypeSize(TypeOid::INT4) == 4);
    assert(GetTypeSize(TypeOid::FLOAT4) == 4);
    assert(GetTypeSize(TypeOid::DATE) == 4);
    assert(GetTypeSize(TypeOid::INT8) == 8);
    assert(GetTypeSize(TypeOid::FLOAT8) == 8);
    assert(GetTypeSize(TypeOid::TIMESTAMP) == 8);
    assert(GetTypeSize(TypeOid::TIMESTAMPTZ) == 8);
    assert(GetTypeSize(TypeOid::TIME) == 8);
    assert(GetTypeSize(TypeOid::UUID) == 16);

    // Variable-length types
    assert(GetTypeSize(TypeOid::TEXT) == -1);
    assert(GetTypeSize(TypeOid::VARCHAR) == -1);
    assert(GetTypeSize(TypeOid::BYTEA) == -1);
    assert(GetTypeSize(TypeOid::NUMERIC) == -1);
    assert(GetTypeSize(TypeOid::JSON) == -1);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Value Formatting Tests
//===----------------------------------------------------------------------===//

void TestFormatValue() {
    std::cout << "  Testing FormatValue..." << std::endl;

    // NULL value
    duckdb::Value null_val;
    assert(FormatValue(null_val) == "");

    // Integer values
    assert(FormatValue(duckdb::Value::INTEGER(42)) == "42");
    assert(FormatValue(duckdb::Value::INTEGER(-123)) == "-123");
    assert(FormatValue(duckdb::Value::INTEGER(0)) == "0");

    // Floating point values
    assert(FormatValue(duckdb::Value::DOUBLE(3.14)) == "3.14");
    assert(FormatValue(duckdb::Value::FLOAT(2.5f)) == "2.5");

    // String values
    assert(FormatValue(duckdb::Value("hello")) == "hello");
    assert(FormatValue(duckdb::Value("")) == "");
    assert(FormatValue(duckdb::Value("hello world")) == "hello world");

    // Boolean values
    assert(FormatValue(duckdb::Value::BOOLEAN(true)) == "true");
    assert(FormatValue(duckdb::Value::BOOLEAN(false)) == "false");

    // BigInt
    assert(FormatValue(duckdb::Value::BIGINT(9223372036854775807LL)) == "9223372036854775807");

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== PostgreSQL Types Unit Tests ===" << std::endl;

    std::cout << "\n1. Type OID Constants:" << std::endl;
    TestTypeOidConstants();

    std::cout << "\n2. DuckDB to PostgreSQL Type Mapping:" << std::endl;
    TestDuckDBTypeToOid();
    TestDuckDBTypeToOidWithLogicalType();

    std::cout << "\n3. Type Size:" << std::endl;
    TestGetTypeSize();

    std::cout << "\n4. Value Formatting:" << std::endl;
    TestFormatValue();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}
