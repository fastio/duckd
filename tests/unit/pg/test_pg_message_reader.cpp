//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/pg/test_pg_message_reader.cpp
//
// Unit tests for PostgreSQL message reader
//===----------------------------------------------------------------------===//

#include "protocol/pg/pg_message_reader.hpp"
#include "protocol/pg/pg_protocol.hpp"
#include "protocol/pg/pg_types.hpp"
#include <cassert>
#include <iostream>
#include <cstring>

using namespace duckdb_server::pg;

// Helper to write int32 in network byte order
void WriteInt32(std::vector<uint8_t>& buf, int32_t value) {
    int32_t network = HostToNetwork32(value);
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&network);
    buf.insert(buf.end(), ptr, ptr + 4);
}

// Helper to write int16 in network byte order
void WriteInt16(std::vector<uint8_t>& buf, int16_t value) {
    int16_t network = HostToNetwork16(value);
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&network);
    buf.insert(buf.end(), ptr, ptr + 2);
}

// Helper to write null-terminated string
void WriteString(std::vector<uint8_t>& buf, const std::string& str) {
    buf.insert(buf.end(), str.begin(), str.end());
    buf.push_back(0);
}

//===----------------------------------------------------------------------===//
// Startup Message Tests
//===----------------------------------------------------------------------===//

void TestReadStartupMessage() {
    std::cout << "  Testing ReadStartupMessage..." << std::endl;

    std::vector<uint8_t> buf;

    // Build startup message
    // Length (will be filled at the end)
    size_t length_pos = buf.size();
    WriteInt32(buf, 0);  // Placeholder

    // Protocol version 3.0
    WriteInt32(buf, PROTOCOL_VERSION_3_0);

    // Parameters
    WriteString(buf, "user");
    WriteString(buf, "testuser");
    WriteString(buf, "database");
    WriteString(buf, "testdb");
    buf.push_back(0);  // Final null

    // Update length
    int32_t length = static_cast<int32_t>(buf.size());
    int32_t network_length = HostToNetwork32(length);
    std::memcpy(buf.data() + length_pos, &network_length, 4);

    // Parse
    PgMessageReader reader(buf.data(), buf.size());
    StartupMessage msg;
    assert(reader.ReadStartupMessage(msg));

    assert(msg.protocol_version == PROTOCOL_VERSION_3_0);
    assert(msg.GetUser() == "testuser");
    assert(msg.GetDatabase() == "testdb");

    std::cout << "    PASSED" << std::endl;
}

void TestReadStartupMessageSSLRequest() {
    std::cout << "  Testing ReadStartupMessage (SSL Request)..." << std::endl;

    std::vector<uint8_t> buf;
    WriteInt32(buf, 8);  // Length
    WriteInt32(buf, SSL_REQUEST_CODE);

    PgMessageReader reader(buf.data(), buf.size());
    StartupMessage msg;
    assert(reader.ReadStartupMessage(msg));
    assert(msg.protocol_version == SSL_REQUEST_CODE);

    std::cout << "    PASSED" << std::endl;
}

void TestReadStartupMessageCancelRequest() {
    std::cout << "  Testing ReadStartupMessage (Cancel Request)..." << std::endl;

    std::vector<uint8_t> buf;
    WriteInt32(buf, 16);  // Length
    WriteInt32(buf, CANCEL_REQUEST_CODE);
    WriteInt32(buf, 12345);  // PID
    WriteInt32(buf, 67890);  // Secret key

    PgMessageReader reader(buf.data(), buf.size());
    StartupMessage msg;
    assert(reader.ReadStartupMessage(msg));
    assert(msg.protocol_version == CANCEL_REQUEST_CODE);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Query Message Tests
//===----------------------------------------------------------------------===//

void TestReadQueryMessage() {
    std::cout << "  Testing ReadQueryMessage..." << std::endl;

    std::vector<uint8_t> buf;
    WriteString(buf, "SELECT * FROM users");

    PgMessageReader reader(buf.data(), buf.size());
    QueryMessage msg;
    assert(reader.ReadQueryMessage(msg));
    assert(msg.query == "SELECT * FROM users");

    std::cout << "    PASSED" << std::endl;
}

void TestReadQueryMessageEmpty() {
    std::cout << "  Testing ReadQueryMessage (empty)..." << std::endl;

    std::vector<uint8_t> buf;
    WriteString(buf, "");

    PgMessageReader reader(buf.data(), buf.size());
    QueryMessage msg;
    assert(reader.ReadQueryMessage(msg));
    assert(msg.query.empty());

    std::cout << "    PASSED" << std::endl;
}

void TestReadQueryMessageComplex() {
    std::cout << "  Testing ReadQueryMessage (complex SQL)..." << std::endl;

    std::string sql = "SELECT u.id, u.name, COUNT(o.id) "
                      "FROM users u "
                      "LEFT JOIN orders o ON u.id = o.user_id "
                      "WHERE u.active = true "
                      "GROUP BY u.id, u.name "
                      "HAVING COUNT(o.id) > 0 "
                      "ORDER BY u.name";

    std::vector<uint8_t> buf;
    WriteString(buf, sql);

    PgMessageReader reader(buf.data(), buf.size());
    QueryMessage msg;
    assert(reader.ReadQueryMessage(msg));
    assert(msg.query == sql);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Parse Message Tests
//===----------------------------------------------------------------------===//

void TestReadParseMessage() {
    std::cout << "  Testing ReadParseMessage..." << std::endl;

    std::vector<uint8_t> buf;
    WriteString(buf, "stmt1");  // Statement name
    WriteString(buf, "SELECT * FROM users WHERE id = $1");  // Query
    WriteInt16(buf, 1);  // Number of param types
    WriteInt32(buf, TypeOid::INT4);  // Param type

    PgMessageReader reader(buf.data(), buf.size());
    ParseMessage msg;
    assert(reader.ReadParseMessage(msg));
    assert(msg.statement_name == "stmt1");
    assert(msg.query == "SELECT * FROM users WHERE id = $1");
    assert(msg.param_types.size() == 1);
    assert(msg.param_types[0] == TypeOid::INT4);

    std::cout << "    PASSED" << std::endl;
}

void TestReadParseMessageUnnamed() {
    std::cout << "  Testing ReadParseMessage (unnamed)..." << std::endl;

    std::vector<uint8_t> buf;
    WriteString(buf, "");  // Empty statement name
    WriteString(buf, "SELECT 1");
    WriteInt16(buf, 0);  // No param types

    PgMessageReader reader(buf.data(), buf.size());
    ParseMessage msg;
    assert(reader.ReadParseMessage(msg));
    assert(msg.statement_name.empty());
    assert(msg.query == "SELECT 1");
    assert(msg.param_types.empty());

    std::cout << "    PASSED" << std::endl;
}

void TestReadParseMessageMultipleParams() {
    std::cout << "  Testing ReadParseMessage (multiple params)..." << std::endl;

    std::vector<uint8_t> buf;
    WriteString(buf, "insert_user");
    WriteString(buf, "INSERT INTO users (name, age, active) VALUES ($1, $2, $3)");
    WriteInt16(buf, 3);
    WriteInt32(buf, TypeOid::VARCHAR);
    WriteInt32(buf, TypeOid::INT4);
    WriteInt32(buf, TypeOid::BOOL);

    PgMessageReader reader(buf.data(), buf.size());
    ParseMessage msg;
    assert(reader.ReadParseMessage(msg));
    assert(msg.param_types.size() == 3);
    assert(msg.param_types[0] == TypeOid::VARCHAR);
    assert(msg.param_types[1] == TypeOid::INT4);
    assert(msg.param_types[2] == TypeOid::BOOL);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Bind Message Tests
//===----------------------------------------------------------------------===//

void TestReadBindMessage() {
    std::cout << "  Testing ReadBindMessage..." << std::endl;

    std::vector<uint8_t> buf;
    WriteString(buf, "portal1");  // Portal name
    WriteString(buf, "stmt1");    // Statement name

    // Parameter format codes
    WriteInt16(buf, 1);  // Number of format codes
    WriteInt16(buf, 0);  // Text format

    // Parameter values
    WriteInt16(buf, 1);  // Number of values
    std::string param = "42";
    WriteInt32(buf, static_cast<int32_t>(param.size()));
    buf.insert(buf.end(), param.begin(), param.end());

    // Result format codes
    WriteInt16(buf, 0);  // Use default (text)

    PgMessageReader reader(buf.data(), buf.size());
    BindMessage msg;
    bool result = reader.ReadBindMessage(msg);
    assert(result);
    assert(msg.portal_name == "portal1");
    assert(msg.statement_name == "stmt1");
    assert(msg.param_formats.size() == 1);
    assert(msg.param_formats[0] == 0);  // Text format
    assert(msg.param_values.size() == 1);
    assert(msg.param_values[0].has_value());

    const auto& val = msg.param_values[0].value();
    std::string value(val.begin(), val.end());
    assert(value == "42");

    std::cout << "    PASSED" << std::endl;
}

void TestReadBindMessageWithNull() {
    std::cout << "  Testing ReadBindMessage (with NULL)..." << std::endl;

    std::vector<uint8_t> buf;
    WriteString(buf, "");  // Unnamed portal
    WriteString(buf, "");  // Unnamed statement

    WriteInt16(buf, 0);  // No format codes

    // Two parameters, second is NULL
    WriteInt16(buf, 2);
    std::string param1 = "test";
    WriteInt32(buf, static_cast<int32_t>(param1.size()));
    buf.insert(buf.end(), param1.begin(), param1.end());
    WriteInt32(buf, -1);  // NULL

    WriteInt16(buf, 0);  // No result format codes

    PgMessageReader reader(buf.data(), buf.size());
    BindMessage msg;
    bool result = reader.ReadBindMessage(msg);
    assert(result);
    assert(msg.param_values.size() == 2);
    assert(msg.param_values[0].has_value());
    assert(!msg.param_values[1].has_value());  // NULL

    // Verify first parameter value
    const auto& val = msg.param_values[0].value();
    std::string value(val.begin(), val.end());
    assert(value == "test");

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Execute Message Tests
//===----------------------------------------------------------------------===//

void TestReadExecuteMessage() {
    std::cout << "  Testing ReadExecuteMessage..." << std::endl;

    std::vector<uint8_t> buf;
    WriteString(buf, "portal1");
    WriteInt32(buf, 100);  // Max rows

    PgMessageReader reader(buf.data(), buf.size());
    ExecuteMessage msg;
    assert(reader.ReadExecuteMessage(msg));
    assert(msg.portal_name == "portal1");
    assert(msg.max_rows == 100);

    std::cout << "    PASSED" << std::endl;
}

void TestReadExecuteMessageUnlimited() {
    std::cout << "  Testing ReadExecuteMessage (unlimited)..." << std::endl;

    std::vector<uint8_t> buf;
    WriteString(buf, "");
    WriteInt32(buf, 0);  // 0 = no limit

    PgMessageReader reader(buf.data(), buf.size());
    ExecuteMessage msg;
    assert(reader.ReadExecuteMessage(msg));
    assert(msg.max_rows == 0);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Describe Message Tests
//===----------------------------------------------------------------------===//

void TestReadDescribeMessage() {
    std::cout << "  Testing ReadDescribeMessage..." << std::endl;

    // Describe statement
    {
        std::vector<uint8_t> buf;
        buf.push_back('S');
        WriteString(buf, "stmt1");

        PgMessageReader reader(buf.data(), buf.size());
        DescribeMessage msg;
        assert(reader.ReadDescribeMessage(msg));
        assert(msg.describe_type == 'S');
        assert(msg.name == "stmt1");
    }

    // Describe portal
    {
        std::vector<uint8_t> buf;
        buf.push_back('P');
        WriteString(buf, "portal1");

        PgMessageReader reader(buf.data(), buf.size());
        DescribeMessage msg;
        assert(reader.ReadDescribeMessage(msg));
        assert(msg.describe_type == 'P');
        assert(msg.name == "portal1");
    }

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Close Message Tests
//===----------------------------------------------------------------------===//

void TestReadCloseMessage() {
    std::cout << "  Testing ReadCloseMessage..." << std::endl;

    // Close statement
    {
        std::vector<uint8_t> buf;
        buf.push_back('S');
        WriteString(buf, "stmt1");

        PgMessageReader reader(buf.data(), buf.size());
        CloseMessage msg;
        assert(reader.ReadCloseMessage(msg));
        assert(msg.close_type == 'S');
        assert(msg.name == "stmt1");
    }

    // Close portal
    {
        std::vector<uint8_t> buf;
        buf.push_back('P');
        WriteString(buf, "portal1");

        PgMessageReader reader(buf.data(), buf.size());
        CloseMessage msg;
        assert(reader.ReadCloseMessage(msg));
        assert(msg.close_type == 'P');
        assert(msg.name == "portal1");
    }

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Edge Cases and Error Handling
//===----------------------------------------------------------------------===//

void TestReaderHasRemaining() {
    std::cout << "  Testing HasRemaining..." << std::endl;

    std::vector<uint8_t> buf = {1, 2, 3, 4, 5};

    PgMessageReader reader(buf.data(), buf.size());
    assert(reader.HasRemaining(1));
    assert(reader.HasRemaining(5));
    assert(!reader.HasRemaining(6));
    assert(reader.Remaining() == 5);

    std::cout << "    PASSED" << std::endl;
}

void TestReaderEmptyBuffer() {
    std::cout << "  Testing empty buffer..." << std::endl;

    std::vector<uint8_t> buf;
    PgMessageReader reader(buf.data(), 0);

    assert(!reader.HasRemaining());
    assert(reader.Remaining() == 0);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== PostgreSQL Message Reader Unit Tests ===" << std::endl;

    std::cout << "\n1. Startup Message:" << std::endl;
    TestReadStartupMessage();
    TestReadStartupMessageSSLRequest();
    TestReadStartupMessageCancelRequest();

    std::cout << "\n2. Query Message:" << std::endl;
    TestReadQueryMessage();
    TestReadQueryMessageEmpty();
    TestReadQueryMessageComplex();

    std::cout << "\n3. Parse Message:" << std::endl;
    TestReadParseMessage();
    TestReadParseMessageUnnamed();
    TestReadParseMessageMultipleParams();

    std::cout << "\n4. Bind Message:" << std::endl;
    TestReadBindMessage();
    TestReadBindMessageWithNull();

    std::cout << "\n5. Execute Message:" << std::endl;
    TestReadExecuteMessage();
    TestReadExecuteMessageUnlimited();

    std::cout << "\n6. Describe Message:" << std::endl;
    TestReadDescribeMessage();

    std::cout << "\n7. Close Message:" << std::endl;
    TestReadCloseMessage();

    std::cout << "\n8. Edge Cases:" << std::endl;
    TestReaderHasRemaining();
    TestReaderEmptyBuffer();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}
