//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/pg/test_pg_protocol.cpp
//
// Unit tests for PostgreSQL protocol utilities
//===----------------------------------------------------------------------===//

#include "protocol/pg/pg_protocol.hpp"
#include <cassert>
#include <iostream>
#include <cstring>

using namespace duckdb_server::pg;

//===----------------------------------------------------------------------===//
// Byte Order Tests
//===----------------------------------------------------------------------===//

void TestHostToNetwork16() {
    std::cout << "  Testing HostToNetwork16..." << std::endl;

    // Test value 0x1234
    int16_t value = 0x1234;
    int16_t network = HostToNetwork16(value);

    // Network byte order is big-endian
    uint8_t* bytes = reinterpret_cast<uint8_t*>(&network);
    assert(bytes[0] == 0x12);
    assert(bytes[1] == 0x34);

    // Test round-trip
    assert(NetworkToHost16(network) == value);

    // Test zero
    assert(HostToNetwork16(0) == 0);
    assert(NetworkToHost16(0) == 0);

    // Test negative
    int16_t neg = -1;
    assert(NetworkToHost16(HostToNetwork16(neg)) == neg);

    std::cout << "    PASSED" << std::endl;
}

void TestHostToNetwork32() {
    std::cout << "  Testing HostToNetwork32..." << std::endl;

    // Test value 0x12345678
    int32_t value = 0x12345678;
    int32_t network = HostToNetwork32(value);

    // Network byte order is big-endian
    uint8_t* bytes = reinterpret_cast<uint8_t*>(&network);
    assert(bytes[0] == 0x12);
    assert(bytes[1] == 0x34);
    assert(bytes[2] == 0x56);
    assert(bytes[3] == 0x78);

    // Test round-trip
    assert(NetworkToHost32(network) == value);

    // Test zero
    assert(HostToNetwork32(0) == 0);
    assert(NetworkToHost32(0) == 0);

    // Test negative
    int32_t neg = -1;
    assert(NetworkToHost32(HostToNetwork32(neg)) == neg);

    // Test protocol version
    int32_t proto_version = PROTOCOL_VERSION_3_0;
    assert(proto_version == 196608);  // 3 << 16

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Protocol Constants Tests
//===----------------------------------------------------------------------===//

void TestProtocolConstants() {
    std::cout << "  Testing Protocol Constants..." << std::endl;

    // Protocol version
    assert(PROTOCOL_VERSION_3_0 == (3 << 16));  // Version 3.0

    // SSL request code
    assert(SSL_REQUEST_CODE == 80877103);

    // Cancel request code
    assert(CANCEL_REQUEST_CODE == 80877102);

    // Frontend message types
    assert(FrontendMessage::Query == 'Q');
    assert(FrontendMessage::Parse == 'P');
    assert(FrontendMessage::Bind == 'B');
    assert(FrontendMessage::Describe == 'D');
    assert(FrontendMessage::Execute == 'E');
    assert(FrontendMessage::Close == 'C');
    assert(FrontendMessage::Sync == 'S');
    assert(FrontendMessage::Flush == 'H');
    assert(FrontendMessage::Terminate == 'X');
    assert(FrontendMessage::PasswordMessage == 'p');

    // Backend message types
    assert(BackendMessage::Authentication == 'R');
    assert(BackendMessage::ParameterStatus == 'S');
    assert(BackendMessage::BackendKeyData == 'K');
    assert(BackendMessage::ReadyForQuery == 'Z');
    assert(BackendMessage::RowDescription == 'T');
    assert(BackendMessage::DataRow == 'D');
    assert(BackendMessage::CommandComplete == 'C');
    assert(BackendMessage::EmptyQueryResponse == 'I');
    assert(BackendMessage::ErrorResponse == 'E');
    assert(BackendMessage::NoticeResponse == 'N');
    assert(BackendMessage::ParseComplete == '1');
    assert(BackendMessage::BindComplete == '2');
    assert(BackendMessage::CloseComplete == '3');
    assert(BackendMessage::NoData == 'n');
    assert(BackendMessage::ParameterDescription == 't');

    // Authentication types
    assert(AuthType::Ok == 0);
    assert(AuthType::CleartextPassword == 3);
    assert(AuthType::MD5Password == 5);

    // Transaction status
    assert(TransactionStatus::Idle == 'I');
    assert(TransactionStatus::InTransaction == 'T');
    assert(TransactionStatus::Failed == 'E');

    // Format codes
    assert(FormatCode::Text == 0);
    assert(FormatCode::Binary == 1);

    std::cout << "    PASSED" << std::endl;
}

void TestErrorFieldTypes() {
    std::cout << "  Testing Error Field Types..." << std::endl;

    assert(ErrorField::Severity == 'S');
    assert(ErrorField::SeverityNonLocalized == 'V');
    assert(ErrorField::Code == 'C');
    assert(ErrorField::Message == 'M');
    assert(ErrorField::Detail == 'D');
    assert(ErrorField::Hint == 'H');
    assert(ErrorField::Position == 'P');
    assert(ErrorField::InternalPosition == 'p');
    assert(ErrorField::InternalQuery == 'q');
    assert(ErrorField::Where == 'W');
    assert(ErrorField::Schema == 's');
    assert(ErrorField::Table == 't');
    assert(ErrorField::Column == 'c');
    assert(ErrorField::DataType == 'd');
    assert(ErrorField::Constraint == 'n');
    assert(ErrorField::File == 'F');
    assert(ErrorField::Line == 'L');
    assert(ErrorField::Routine == 'R');

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== PostgreSQL Protocol Unit Tests ===" << std::endl;

    std::cout << "\n1. Byte Order Tests:" << std::endl;
    TestHostToNetwork16();
    TestHostToNetwork32();

    std::cout << "\n2. Protocol Constants Tests:" << std::endl;
    TestProtocolConstants();
    TestErrorFieldTypes();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}
