//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/pg/test_pg_message_writer.cpp
//
// Unit tests for PostgreSQL message writer
//===----------------------------------------------------------------------===//

#include "protocol/pg/pg_message_writer.hpp"
#include "protocol/pg/pg_protocol.hpp"
#include <cassert>
#include <iostream>
#include <cstring>

using namespace duckdb_server::pg;

// Helper to extract int32 from buffer in network byte order
int32_t ExtractInt32(const std::vector<uint8_t>& buf, size_t offset) {
    int32_t value;
    std::memcpy(&value, buf.data() + offset, 4);
    return NetworkToHost32(value);
}

// Helper to extract int16 from buffer in network byte order
int16_t ExtractInt16(const std::vector<uint8_t>& buf, size_t offset) {
    int16_t value;
    std::memcpy(&value, buf.data() + offset, 2);
    return NetworkToHost16(value);
}

//===----------------------------------------------------------------------===//
// Authentication Message Tests
//===----------------------------------------------------------------------===//

void TestWriteAuthenticationOk() {
    std::cout << "  Testing WriteAuthenticationOk..." << std::endl;

    PgMessageWriter writer;
    writer.WriteAuthenticationOk();

    const auto& buf = writer.GetBuffer();

    // Message: type(1) + length(4) + auth_type(4) = 9 bytes
    assert(buf.size() == 9);
    assert(buf[0] == 'R');  // Authentication message type

    int32_t length = ExtractInt32(buf, 1);
    assert(length == 8);  // length includes itself: 4 + 4

    int32_t auth_type = ExtractInt32(buf, 5);
    assert(auth_type == AuthType::Ok);

    std::cout << "    PASSED" << std::endl;
}

void TestWriteAuthenticationCleartextPassword() {
    std::cout << "  Testing WriteAuthenticationCleartextPassword..." << std::endl;

    PgMessageWriter writer;
    writer.WriteAuthenticationCleartextPassword();

    const auto& buf = writer.GetBuffer();

    assert(buf.size() == 9);
    assert(buf[0] == 'R');

    int32_t auth_type = ExtractInt32(buf, 5);
    assert(auth_type == AuthType::CleartextPassword);

    std::cout << "    PASSED" << std::endl;
}

void TestWriteAuthenticationMD5Password() {
    std::cout << "  Testing WriteAuthenticationMD5Password..." << std::endl;

    PgMessageWriter writer;
    uint8_t salt[4] = {0x12, 0x34, 0x56, 0x78};
    writer.WriteAuthenticationMD5Password(salt);

    const auto& buf = writer.GetBuffer();

    // Message: type(1) + length(4) + auth_type(4) + salt(4) = 13 bytes
    assert(buf.size() == 13);
    assert(buf[0] == 'R');

    int32_t length = ExtractInt32(buf, 1);
    assert(length == 12);  // 4 + 4 + 4

    int32_t auth_type = ExtractInt32(buf, 5);
    assert(auth_type == AuthType::MD5Password);

    // Check salt
    assert(buf[9] == 0x12);
    assert(buf[10] == 0x34);
    assert(buf[11] == 0x56);
    assert(buf[12] == 0x78);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Startup Message Tests
//===----------------------------------------------------------------------===//

void TestWriteParameterStatus() {
    std::cout << "  Testing WriteParameterStatus..." << std::endl;

    PgMessageWriter writer;
    writer.WriteParameterStatus("server_version", "15.0");

    const auto& buf = writer.GetBuffer();

    assert(buf[0] == 'S');  // ParameterStatus type

    // Check strings
    std::string name(reinterpret_cast<const char*>(buf.data() + 5));
    assert(name == "server_version");

    std::string value(reinterpret_cast<const char*>(buf.data() + 5 + name.size() + 1));
    assert(value == "15.0");

    std::cout << "    PASSED" << std::endl;
}

void TestWriteBackendKeyData() {
    std::cout << "  Testing WriteBackendKeyData..." << std::endl;

    PgMessageWriter writer;
    writer.WriteBackendKeyData(12345, 67890);

    const auto& buf = writer.GetBuffer();

    // Message: type(1) + length(4) + pid(4) + key(4) = 13 bytes
    assert(buf.size() == 13);
    assert(buf[0] == 'K');

    int32_t pid = ExtractInt32(buf, 5);
    assert(pid == 12345);

    int32_t key = ExtractInt32(buf, 9);
    assert(key == 67890);

    std::cout << "    PASSED" << std::endl;
}

void TestWriteReadyForQuery() {
    std::cout << "  Testing WriteReadyForQuery..." << std::endl;

    // Test Idle state
    {
        PgMessageWriter writer;
        writer.WriteReadyForQuery('I');

        const auto& buf = writer.GetBuffer();

        // Message: type(1) + length(4) + status(1) = 6 bytes
        assert(buf.size() == 6);
        assert(buf[0] == 'Z');
        assert(buf[5] == 'I');
    }

    // Test InTransaction state
    {
        PgMessageWriter writer;
        writer.WriteReadyForQuery('T');

        const auto& buf = writer.GetBuffer();
        assert(buf[5] == 'T');
    }

    // Test Failed state
    {
        PgMessageWriter writer;
        writer.WriteReadyForQuery('E');

        const auto& buf = writer.GetBuffer();
        assert(buf[5] == 'E');
    }

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Query Response Message Tests
//===----------------------------------------------------------------------===//

void TestWriteRowDescription() {
    std::cout << "  Testing WriteRowDescription..." << std::endl;

    PgMessageWriter writer;
    std::vector<std::string> names = {"id", "name", "value"};
    std::vector<duckdb::LogicalType> types = {
        duckdb::LogicalType::INTEGER,
        duckdb::LogicalType::VARCHAR,
        duckdb::LogicalType::DOUBLE
    };
    writer.WriteRowDescription(names, types);

    const auto& buf = writer.GetBuffer();

    assert(buf[0] == 'T');  // RowDescription type

    // Number of fields
    int16_t num_fields = ExtractInt16(buf, 5);
    assert(num_fields == 3);

    // First field: "id"
    size_t offset = 7;
    std::string field_name(reinterpret_cast<const char*>(buf.data() + offset));
    assert(field_name == "id");

    std::cout << "    PASSED" << std::endl;
}

void TestWriteDataRow() {
    std::cout << "  Testing WriteDataRow..." << std::endl;

    PgMessageWriter writer;
    std::vector<duckdb::Value> values = {
        duckdb::Value::INTEGER(42),
        duckdb::Value("hello"),
        duckdb::Value::DOUBLE(3.14)
    };
    writer.WriteDataRow(values);

    const auto& buf = writer.GetBuffer();

    assert(buf[0] == 'D');  // DataRow type

    // Number of columns
    int16_t num_cols = ExtractInt16(buf, 5);
    assert(num_cols == 3);

    // First column: "42"
    int32_t col_len = ExtractInt32(buf, 7);
    assert(col_len == 2);  // "42" is 2 bytes
    std::string col_val(reinterpret_cast<const char*>(buf.data() + 11), col_len);
    assert(col_val == "42");

    std::cout << "    PASSED" << std::endl;
}

void TestWriteDataRowWithNull() {
    std::cout << "  Testing WriteDataRow with NULL..." << std::endl;

    PgMessageWriter writer;
    std::vector<duckdb::Value> values = {
        duckdb::Value::INTEGER(1),
        duckdb::Value(),  // NULL
        duckdb::Value("test")
    };
    writer.WriteDataRow(values);

    const auto& buf = writer.GetBuffer();

    assert(buf[0] == 'D');

    // Number of columns
    int16_t num_cols = ExtractInt16(buf, 5);
    assert(num_cols == 3);

    // First column: "1"
    int32_t col1_len = ExtractInt32(buf, 7);
    assert(col1_len == 1);

    // Second column: NULL (length = -1)
    int32_t col2_len = ExtractInt32(buf, 7 + 4 + 1);
    assert(col2_len == -1);  // NULL indicator

    std::cout << "    PASSED" << std::endl;
}

void TestWriteCommandComplete() {
    std::cout << "  Testing WriteCommandComplete..." << std::endl;

    PgMessageWriter writer;
    writer.WriteCommandComplete("SELECT 5");

    const auto& buf = writer.GetBuffer();

    assert(buf[0] == 'C');  // CommandComplete type

    std::string tag(reinterpret_cast<const char*>(buf.data() + 5));
    assert(tag == "SELECT 5");

    std::cout << "    PASSED" << std::endl;
}

void TestWriteEmptyQueryResponse() {
    std::cout << "  Testing WriteEmptyQueryResponse..." << std::endl;

    PgMessageWriter writer;
    writer.WriteEmptyQueryResponse();

    const auto& buf = writer.GetBuffer();

    // Message: type(1) + length(4) = 5 bytes
    assert(buf.size() == 5);
    assert(buf[0] == 'I');

    int32_t length = ExtractInt32(buf, 1);
    assert(length == 4);  // Just the length field itself

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Extended Query Message Tests
//===----------------------------------------------------------------------===//

void TestWriteParseComplete() {
    std::cout << "  Testing WriteParseComplete..." << std::endl;

    PgMessageWriter writer;
    writer.WriteParseComplete();

    const auto& buf = writer.GetBuffer();

    assert(buf.size() == 5);
    assert(buf[0] == '1');

    std::cout << "    PASSED" << std::endl;
}

void TestWriteBindComplete() {
    std::cout << "  Testing WriteBindComplete..." << std::endl;

    PgMessageWriter writer;
    writer.WriteBindComplete();

    const auto& buf = writer.GetBuffer();

    assert(buf.size() == 5);
    assert(buf[0] == '2');

    std::cout << "    PASSED" << std::endl;
}

void TestWriteCloseComplete() {
    std::cout << "  Testing WriteCloseComplete..." << std::endl;

    PgMessageWriter writer;
    writer.WriteCloseComplete();

    const auto& buf = writer.GetBuffer();

    assert(buf.size() == 5);
    assert(buf[0] == '3');

    std::cout << "    PASSED" << std::endl;
}

void TestWriteNoData() {
    std::cout << "  Testing WriteNoData..." << std::endl;

    PgMessageWriter writer;
    writer.WriteNoData();

    const auto& buf = writer.GetBuffer();

    assert(buf.size() == 5);
    assert(buf[0] == 'n');

    std::cout << "    PASSED" << std::endl;
}

void TestWriteParameterDescription() {
    std::cout << "  Testing WriteParameterDescription..." << std::endl;

    PgMessageWriter writer;
    std::vector<int32_t> param_types = {TypeOid::INT4, TypeOid::VARCHAR, TypeOid::FLOAT8};
    writer.WriteParameterDescription(param_types);

    const auto& buf = writer.GetBuffer();

    assert(buf[0] == 't');  // ParameterDescription type

    int16_t num_params = ExtractInt16(buf, 5);
    assert(num_params == 3);

    int32_t param1 = ExtractInt32(buf, 7);
    assert(param1 == TypeOid::INT4);

    int32_t param2 = ExtractInt32(buf, 11);
    assert(param2 == TypeOid::VARCHAR);

    int32_t param3 = ExtractInt32(buf, 15);
    assert(param3 == TypeOid::FLOAT8);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Error Message Tests
//===----------------------------------------------------------------------===//

void TestWriteErrorResponse() {
    std::cout << "  Testing WriteErrorResponse..." << std::endl;

    PgMessageWriter writer;
    writer.WriteErrorResponse("ERROR", "42000", "Test error message", "Detail info");

    const auto& buf = writer.GetBuffer();

    assert(buf[0] == 'E');  // ErrorResponse type

    // Parse fields
    size_t offset = 5;
    bool found_severity = false;
    bool found_code = false;
    bool found_message = false;
    bool found_detail = false;

    while (offset < buf.size() && buf[offset] != 0) {
        char field_type = static_cast<char>(buf[offset++]);
        std::string value(reinterpret_cast<const char*>(buf.data() + offset));
        offset += value.size() + 1;

        if (field_type == 'S' || field_type == 'V') {
            assert(value == "ERROR");
            found_severity = true;
        } else if (field_type == 'C') {
            assert(value == "42000");
            found_code = true;
        } else if (field_type == 'M') {
            assert(value == "Test error message");
            found_message = true;
        } else if (field_type == 'D') {
            assert(value == "Detail info");
            found_detail = true;
        }
    }

    assert(found_severity);
    assert(found_code);
    assert(found_message);
    assert(found_detail);

    std::cout << "    PASSED" << std::endl;
}

void TestWriteNoticeResponse() {
    std::cout << "  Testing WriteNoticeResponse..." << std::endl;

    PgMessageWriter writer;
    writer.WriteNoticeResponse("WARNING", "01000", "Test warning");

    const auto& buf = writer.GetBuffer();

    assert(buf[0] == 'N');  // NoticeResponse type

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Buffer Management Tests
//===----------------------------------------------------------------------===//

void TestClearBuffer() {
    std::cout << "  Testing Clear..." << std::endl;

    PgMessageWriter writer;
    writer.WriteAuthenticationOk();
    assert(!writer.GetBuffer().empty());

    writer.Clear();
    assert(writer.GetBuffer().empty());

    std::cout << "    PASSED" << std::endl;
}

void TestMultipleMessages() {
    std::cout << "  Testing Multiple Messages..." << std::endl;

    PgMessageWriter writer;
    writer.WriteAuthenticationOk();
    writer.WriteParameterStatus("server_version", "15.0");
    writer.WriteReadyForQuery('I');

    const auto& buf = writer.GetBuffer();

    // Verify all messages are concatenated
    assert(buf[0] == 'R');  // First message

    // Find second message
    int32_t first_len = ExtractInt32(buf, 1);
    size_t second_start = 1 + first_len;
    assert(buf[second_start] == 'S');  // Second message

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== PostgreSQL Message Writer Unit Tests ===" << std::endl;

    std::cout << "\n1. Authentication Messages:" << std::endl;
    TestWriteAuthenticationOk();
    TestWriteAuthenticationCleartextPassword();
    TestWriteAuthenticationMD5Password();

    std::cout << "\n2. Startup Messages:" << std::endl;
    TestWriteParameterStatus();
    TestWriteBackendKeyData();
    TestWriteReadyForQuery();

    std::cout << "\n3. Query Response Messages:" << std::endl;
    TestWriteRowDescription();
    TestWriteDataRow();
    TestWriteDataRowWithNull();
    TestWriteCommandComplete();
    TestWriteEmptyQueryResponse();

    std::cout << "\n4. Extended Query Messages:" << std::endl;
    TestWriteParseComplete();
    TestWriteBindComplete();
    TestWriteCloseComplete();
    TestWriteNoData();
    TestWriteParameterDescription();

    std::cout << "\n5. Error Messages:" << std::endl;
    TestWriteErrorResponse();
    TestWriteNoticeResponse();

    std::cout << "\n6. Buffer Management:" << std::endl;
    TestClearBuffer();
    TestMultipleMessages();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}
