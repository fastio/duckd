//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/pg/pg_protocol.hpp
//
// PostgreSQL wire protocol constants and utilities
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <arpa/inet.h>

namespace duckdb_server {
namespace pg {

//===----------------------------------------------------------------------===//
// Protocol Version
//===----------------------------------------------------------------------===//
constexpr int32_t PROTOCOL_VERSION_3_0 = 196608;  // 3.0 = (3 << 16) | 0
constexpr int32_t SSL_REQUEST_CODE = 80877103;
constexpr int32_t CANCEL_REQUEST_CODE = 80877102;

//===----------------------------------------------------------------------===//
// Message Types (Backend -> Frontend)
//===----------------------------------------------------------------------===//
namespace BackendMessage {
    constexpr char Authentication = 'R';
    constexpr char BackendKeyData = 'K';
    constexpr char BindComplete = '2';
    constexpr char CloseComplete = '3';
    constexpr char CommandComplete = 'C';
    constexpr char CopyData = 'd';
    constexpr char CopyDone = 'c';
    constexpr char CopyInResponse = 'G';
    constexpr char CopyOutResponse = 'H';
    constexpr char DataRow = 'D';
    constexpr char EmptyQueryResponse = 'I';
    constexpr char ErrorResponse = 'E';
    constexpr char FunctionCallResponse = 'V';
    constexpr char NoData = 'n';
    constexpr char NoticeResponse = 'N';
    constexpr char NotificationResponse = 'A';
    constexpr char ParameterDescription = 't';
    constexpr char ParameterStatus = 'S';
    constexpr char ParseComplete = '1';
    constexpr char PortalSuspended = 's';
    constexpr char ReadyForQuery = 'Z';
    constexpr char RowDescription = 'T';
}

//===----------------------------------------------------------------------===//
// Message Types (Frontend -> Backend)
//===----------------------------------------------------------------------===//
namespace FrontendMessage {
    constexpr char Bind = 'B';
    constexpr char Close = 'C';
    constexpr char CopyData = 'd';
    constexpr char CopyDone = 'c';
    constexpr char CopyFail = 'f';
    constexpr char Describe = 'D';
    constexpr char Execute = 'E';
    constexpr char Flush = 'H';
    constexpr char FunctionCall = 'F';
    constexpr char Parse = 'P';
    constexpr char PasswordMessage = 'p';
    constexpr char Query = 'Q';
    constexpr char Sync = 'S';
    constexpr char Terminate = 'X';
}

//===----------------------------------------------------------------------===//
// Authentication Types
//===----------------------------------------------------------------------===//
namespace AuthType {
    constexpr int32_t Ok = 0;
    constexpr int32_t KerberosV5 = 2;
    constexpr int32_t CleartextPassword = 3;
    constexpr int32_t MD5Password = 5;
    constexpr int32_t SCMCredential = 6;
    constexpr int32_t GSS = 7;
    constexpr int32_t GSSContinue = 8;
    constexpr int32_t SSPI = 9;
    constexpr int32_t SASL = 10;
    constexpr int32_t SASLContinue = 11;
    constexpr int32_t SASLFinal = 12;
}

//===----------------------------------------------------------------------===//
// Transaction Status
//===----------------------------------------------------------------------===//
namespace TransactionStatus {
    constexpr char Idle = 'I';
    constexpr char InTransaction = 'T';
    constexpr char Failed = 'E';
}

//===----------------------------------------------------------------------===//
// Error/Notice Field Types
//===----------------------------------------------------------------------===//
namespace ErrorField {
    constexpr char Severity = 'S';
    constexpr char SeverityNonLocalized = 'V';
    constexpr char Code = 'C';
    constexpr char Message = 'M';
    constexpr char Detail = 'D';
    constexpr char Hint = 'H';
    constexpr char Position = 'P';
    constexpr char InternalPosition = 'p';
    constexpr char InternalQuery = 'q';
    constexpr char Where = 'W';
    constexpr char Schema = 's';
    constexpr char Table = 't';
    constexpr char Column = 'c';
    constexpr char DataType = 'd';
    constexpr char Constraint = 'n';
    constexpr char File = 'F';
    constexpr char Line = 'L';
    constexpr char Routine = 'R';
}

//===----------------------------------------------------------------------===//
// Format Codes
//===----------------------------------------------------------------------===//
namespace FormatCode {
    constexpr int16_t Text = 0;
    constexpr int16_t Binary = 1;
}

//===----------------------------------------------------------------------===//
// Byte Order Utilities
//===----------------------------------------------------------------------===//
inline int16_t NetworkToHost16(int16_t value) {
    return ntohs(value);
}

inline int32_t NetworkToHost32(int32_t value) {
    return ntohl(value);
}

inline int16_t HostToNetwork16(int16_t value) {
    return htons(value);
}

inline int32_t HostToNetwork32(int32_t value) {
    return htonl(value);
}

inline int64_t NetworkToHost64(int64_t value) {
    uint64_t v = static_cast<uint64_t>(value);
    return static_cast<int64_t>(
        ((v & 0x00000000000000FFULL) << 56) |
        ((v & 0x000000000000FF00ULL) << 40) |
        ((v & 0x0000000000FF0000ULL) << 24) |
        ((v & 0x00000000FF000000ULL) << 8) |
        ((v & 0x000000FF00000000ULL) >> 8) |
        ((v & 0x0000FF0000000000ULL) >> 24) |
        ((v & 0x00FF000000000000ULL) >> 40) |
        ((v & 0xFF00000000000000ULL) >> 56)
    );
}

inline int64_t HostToNetwork64(int64_t value) {
    return NetworkToHost64(value);  // Same operation
}

} // namespace pg
} // namespace duckdb_server
