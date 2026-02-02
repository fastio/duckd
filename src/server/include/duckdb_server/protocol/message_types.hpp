//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// duckdb_server/protocol/message_types.hpp
//
// Protocol message type definitions
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb_server {

//===----------------------------------------------------------------------===//
// Message Types
//===----------------------------------------------------------------------===//
enum class MessageType : uint8_t {
    // ===== Connection Management (0x01-0x0F) =====
    HELLO           = 0x01,  // Client handshake
    HELLO_RESPONSE  = 0x02,  // Server handshake response
    PING            = 0x03,  // Heartbeat request
    PONG            = 0x04,  // Heartbeat response
    CLOSE           = 0x05,  // Close connection
    
    // ===== Query Execution (0x10-0x1F) =====
    QUERY           = 0x10,  // SQL query request
    QUERY_RESULT    = 0x11,  // Query result (single return)
    QUERY_ERROR     = 0x12,  // Query error
    QUERY_PROGRESS  = 0x13,  // Query progress (optional)
    QUERY_CANCEL    = 0x14,  // Cancel query
    
    // ===== Prepared Statements (0x20-0x2F) =====
    PREPARE         = 0x20,  // Prepare request
    PREPARE_RESPONSE= 0x21,  // Prepare response
    EXECUTE         = 0x22,  // Execute request
    DEALLOCATE      = 0x23,  // Deallocate prepared statement
    
    // ===== Streaming Results (0x40-0x4F) =====
    RESULT_HEADER   = 0x40,  // Result header (Arrow Schema)
    RESULT_BATCH    = 0x41,  // Data batch (Arrow RecordBatch)
    RESULT_END      = 0x42,  // Result end + statistics
    
    // ===== Metadata (0x50-0x5F) =====
    GET_CATALOG     = 0x50,  // Get catalog info
    CATALOG_RESPONSE= 0x51,  // Catalog response
    
    // ===== Unknown =====
    UNKNOWN         = 0xFF
};

//===----------------------------------------------------------------------===//
// Message Flags
//===----------------------------------------------------------------------===//
namespace MessageFlags {
    constexpr uint8_t NONE              = 0x00;
    constexpr uint8_t COMPRESSED_LZ4    = 0x01;  // bit 0: LZ4 compression
    constexpr uint8_t COMPRESSED_ZSTD   = 0x02;  // bit 1: ZSTD compression
    constexpr uint8_t ENCRYPTED         = 0x04;  // bit 2: encrypted
    // bits 3-7: reserved
}

//===----------------------------------------------------------------------===//
// Error Codes
//===----------------------------------------------------------------------===//
enum class ErrorCode : uint32_t {
    // ===== 0x0000xxxx: Success =====
    OK                      = 0x00000000,
    
    // ===== 0x0001xxxx: Client Errors =====
    SYNTAX_ERROR            = 0x00010001,  // SQL syntax error
    TABLE_NOT_FOUND         = 0x00010002,  // Table not found
    COLUMN_NOT_FOUND        = 0x00010003,  // Column not found
    TYPE_MISMATCH           = 0x00010004,  // Type mismatch
    CONSTRAINT_VIOLATION    = 0x00010005,  // Constraint violation
    PERMISSION_DENIED       = 0x00010006,  // Permission denied
    INVALID_PARAMETER       = 0x00010007,  // Invalid parameter
    INVALID_SQL             = 0x00010008,  // Invalid SQL
    
    // ===== 0x0002xxxx: Server Errors =====
    INTERNAL_ERROR          = 0x00020001,  // Internal error
    OUT_OF_MEMORY           = 0x00020002,  // Out of memory
    DISK_FULL               = 0x00020003,  // Disk full
    QUERY_CANCELLED         = 0x00020004,  // Query cancelled
    TIMEOUT                 = 0x00020005,  // Timeout
    
    // ===== 0x0003xxxx: Connection Errors =====
    CONNECTION_CLOSED       = 0x00030001,  // Connection closed
    AUTHENTICATION_FAILED   = 0x00030002,  // Authentication failed
    SESSION_NOT_FOUND       = 0x00030003,  // Session not found
    PROTOCOL_ERROR          = 0x00030004,  // Protocol error
    VERSION_MISMATCH        = 0x00030005,  // Version mismatch
    MAX_CONNECTIONS         = 0x00030006,  // Max connections reached
};

//===----------------------------------------------------------------------===//
// Client Types
//===----------------------------------------------------------------------===//
enum class ClientType : uint8_t {
    UNKNOWN     = 0x00,
    CPP         = 0x01,
    PYTHON      = 0x02,
    JAVA        = 0x03,
    GO          = 0x04,
    RUST        = 0x05,
    JAVASCRIPT  = 0x06,
    CSHARP      = 0x07,
};

//===----------------------------------------------------------------------===//
// Utility Functions
//===----------------------------------------------------------------------===//
inline const char* MessageTypeToString(MessageType type) {
    switch (type) {
        case MessageType::HELLO:            return "HELLO";
        case MessageType::HELLO_RESPONSE:   return "HELLO_RESPONSE";
        case MessageType::PING:             return "PING";
        case MessageType::PONG:             return "PONG";
        case MessageType::CLOSE:            return "CLOSE";
        case MessageType::QUERY:            return "QUERY";
        case MessageType::QUERY_RESULT:     return "QUERY_RESULT";
        case MessageType::QUERY_ERROR:      return "QUERY_ERROR";
        case MessageType::QUERY_PROGRESS:   return "QUERY_PROGRESS";
        case MessageType::QUERY_CANCEL:     return "QUERY_CANCEL";
        case MessageType::PREPARE:          return "PREPARE";
        case MessageType::PREPARE_RESPONSE: return "PREPARE_RESPONSE";
        case MessageType::EXECUTE:          return "EXECUTE";
        case MessageType::DEALLOCATE:       return "DEALLOCATE";
        case MessageType::RESULT_HEADER:    return "RESULT_HEADER";
        case MessageType::RESULT_BATCH:     return "RESULT_BATCH";
        case MessageType::RESULT_END:       return "RESULT_END";
        case MessageType::GET_CATALOG:      return "GET_CATALOG";
        case MessageType::CATALOG_RESPONSE: return "CATALOG_RESPONSE";
        default:                            return "UNKNOWN";
    }
}

inline const char* ErrorCodeToString(ErrorCode code) {
    switch (code) {
        case ErrorCode::OK:                     return "OK";
        case ErrorCode::SYNTAX_ERROR:           return "SYNTAX_ERROR";
        case ErrorCode::TABLE_NOT_FOUND:        return "TABLE_NOT_FOUND";
        case ErrorCode::COLUMN_NOT_FOUND:       return "COLUMN_NOT_FOUND";
        case ErrorCode::TYPE_MISMATCH:          return "TYPE_MISMATCH";
        case ErrorCode::CONSTRAINT_VIOLATION:   return "CONSTRAINT_VIOLATION";
        case ErrorCode::PERMISSION_DENIED:      return "PERMISSION_DENIED";
        case ErrorCode::INVALID_PARAMETER:      return "INVALID_PARAMETER";
        case ErrorCode::INVALID_SQL:            return "INVALID_SQL";
        case ErrorCode::INTERNAL_ERROR:         return "INTERNAL_ERROR";
        case ErrorCode::OUT_OF_MEMORY:          return "OUT_OF_MEMORY";
        case ErrorCode::DISK_FULL:              return "DISK_FULL";
        case ErrorCode::QUERY_CANCELLED:        return "QUERY_CANCELLED";
        case ErrorCode::TIMEOUT:                return "TIMEOUT";
        case ErrorCode::CONNECTION_CLOSED:      return "CONNECTION_CLOSED";
        case ErrorCode::AUTHENTICATION_FAILED:  return "AUTHENTICATION_FAILED";
        case ErrorCode::SESSION_NOT_FOUND:      return "SESSION_NOT_FOUND";
        case ErrorCode::PROTOCOL_ERROR:         return "PROTOCOL_ERROR";
        case ErrorCode::VERSION_MISMATCH:       return "VERSION_MISMATCH";
        case ErrorCode::MAX_CONNECTIONS:        return "MAX_CONNECTIONS";
        default:                                return "UNKNOWN_ERROR";
    }
}

// Map ErrorCode to SQLSTATE
inline const char* ErrorCodeToSqlState(ErrorCode code) {
    switch (code) {
        case ErrorCode::OK:                     return "00000";  // Success
        case ErrorCode::SYNTAX_ERROR:           return "42000";  // Syntax error
        case ErrorCode::TABLE_NOT_FOUND:        return "42S02";  // Table not found
        case ErrorCode::COLUMN_NOT_FOUND:       return "42S22";  // Column not found
        case ErrorCode::TYPE_MISMATCH:          return "42804";  // Type mismatch
        case ErrorCode::CONSTRAINT_VIOLATION:   return "23000";  // Constraint violation
        case ErrorCode::PERMISSION_DENIED:      return "42501";  // Permission denied
        case ErrorCode::CONNECTION_CLOSED:      return "08000";  // Connection exception
        case ErrorCode::AUTHENTICATION_FAILED:  return "28000";  // Invalid authorization
        default:                                return "HY000";  // General error
    }
}

} // namespace duckdb_server
