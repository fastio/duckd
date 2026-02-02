//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// protocol/message.cpp
//
// Protocol message implementation
//===----------------------------------------------------------------------===//

#include "protocol/message.hpp"
#include <cstring>
#include <stdexcept>

namespace duckdb_server {

//===----------------------------------------------------------------------===//
// HelloPayload
//===----------------------------------------------------------------------===//
std::vector<uint8_t> HelloPayload::Serialize() const {
    std::vector<uint8_t> buffer;
    
    // Calculate total size
    size_t size = 1 + 1 + 2 + 4;  // Fixed fields
    size += 4 + username.size();
    size += 4 + auth_data.size();
    
    buffer.reserve(size);
    
    // Protocol version
    buffer.push_back(protocol_version);
    
    // Client type
    buffer.push_back(client_type);
    
    // Client version (little-endian)
    buffer.push_back(client_version & 0xFF);
    buffer.push_back((client_version >> 8) & 0xFF);
    
    // Capabilities (little-endian)
    buffer.push_back(capabilities & 0xFF);
    buffer.push_back((capabilities >> 8) & 0xFF);
    buffer.push_back((capabilities >> 16) & 0xFF);
    buffer.push_back((capabilities >> 24) & 0xFF);
    
    // Username length and data
    uint32_t username_len = static_cast<uint32_t>(username.size());
    buffer.push_back(username_len & 0xFF);
    buffer.push_back((username_len >> 8) & 0xFF);
    buffer.push_back((username_len >> 16) & 0xFF);
    buffer.push_back((username_len >> 24) & 0xFF);
    buffer.insert(buffer.end(), username.begin(), username.end());
    
    // Auth data length and data
    uint32_t auth_len = static_cast<uint32_t>(auth_data.size());
    buffer.push_back(auth_len & 0xFF);
    buffer.push_back((auth_len >> 8) & 0xFF);
    buffer.push_back((auth_len >> 16) & 0xFF);
    buffer.push_back((auth_len >> 24) & 0xFF);
    buffer.insert(buffer.end(), auth_data.begin(), auth_data.end());
    
    return buffer;
}

HelloPayload HelloPayload::Deserialize(const std::vector<uint8_t>& data) {
    HelloPayload payload;
    
    if (data.size() < 8) {
        throw std::runtime_error("HelloPayload too short");
    }
    
    size_t offset = 0;
    
    // Protocol version
    payload.protocol_version = data[offset++];
    
    // Client type
    payload.client_type = data[offset++];
    
    // Client version
    payload.client_version = data[offset] | (data[offset + 1] << 8);
    offset += 2;
    
    // Capabilities
    payload.capabilities = data[offset] | (data[offset + 1] << 8) |
                          (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    // Username
    if (offset + 4 > data.size()) {
        throw std::runtime_error("HelloPayload truncated at username length");
    }
    uint32_t username_len = data[offset] | (data[offset + 1] << 8) |
                           (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    if (offset + username_len > data.size()) {
        throw std::runtime_error("HelloPayload truncated at username");
    }
    payload.username = std::string(data.begin() + offset, data.begin() + offset + username_len);
    offset += username_len;
    
    // Auth data
    if (offset + 4 > data.size()) {
        throw std::runtime_error("HelloPayload truncated at auth length");
    }
    uint32_t auth_len = data[offset] | (data[offset + 1] << 8) |
                       (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    if (offset + auth_len > data.size()) {
        throw std::runtime_error("HelloPayload truncated at auth data");
    }
    payload.auth_data = std::vector<uint8_t>(data.begin() + offset, data.begin() + offset + auth_len);
    
    return payload;
}

//===----------------------------------------------------------------------===//
// HelloResponsePayload
//===----------------------------------------------------------------------===//
std::vector<uint8_t> HelloResponsePayload::Serialize() const {
    std::vector<uint8_t> buffer;
    
    // Status
    buffer.push_back(status);
    
    // Session ID (little-endian, 8 bytes)
    for (int i = 0; i < 8; ++i) {
        buffer.push_back((session_id >> (i * 8)) & 0xFF);
    }
    
    // Protocol version
    buffer.push_back(protocol_version);
    
    // Server capabilities
    buffer.push_back(server_capabilities & 0xFF);
    buffer.push_back((server_capabilities >> 8) & 0xFF);
    buffer.push_back((server_capabilities >> 16) & 0xFF);
    buffer.push_back((server_capabilities >> 24) & 0xFF);
    
    // Server info length and data
    uint32_t info_len = static_cast<uint32_t>(server_info.size());
    buffer.push_back(info_len & 0xFF);
    buffer.push_back((info_len >> 8) & 0xFF);
    buffer.push_back((info_len >> 16) & 0xFF);
    buffer.push_back((info_len >> 24) & 0xFF);
    buffer.insert(buffer.end(), server_info.begin(), server_info.end());
    
    return buffer;
}

HelloResponsePayload HelloResponsePayload::Deserialize(const std::vector<uint8_t>& data) {
    HelloResponsePayload payload;
    
    if (data.size() < 14) {
        throw std::runtime_error("HelloResponsePayload too short");
    }
    
    size_t offset = 0;
    
    // Status
    payload.status = data[offset++];
    
    // Session ID
    payload.session_id = 0;
    for (int i = 0; i < 8; ++i) {
        payload.session_id |= static_cast<uint64_t>(data[offset++]) << (i * 8);
    }
    
    // Protocol version
    payload.protocol_version = data[offset++];
    
    // Server capabilities
    payload.server_capabilities = data[offset] | (data[offset + 1] << 8) |
                                 (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    // Server info
    if (offset + 4 > data.size()) {
        throw std::runtime_error("HelloResponsePayload truncated");
    }
    uint32_t info_len = data[offset] | (data[offset + 1] << 8) |
                       (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    if (offset + info_len > data.size()) {
        throw std::runtime_error("HelloResponsePayload truncated at info");
    }
    payload.server_info = std::string(data.begin() + offset, data.begin() + offset + info_len);
    
    return payload;
}

//===----------------------------------------------------------------------===//
// QueryPayload
//===----------------------------------------------------------------------===//
std::vector<uint8_t> QueryPayload::Serialize() const {
    std::vector<uint8_t> buffer;
    
    // Query ID (8 bytes)
    for (int i = 0; i < 8; ++i) {
        buffer.push_back((query_id >> (i * 8)) & 0xFF);
    }
    
    // Flags
    buffer.push_back(flags & 0xFF);
    buffer.push_back((flags >> 8) & 0xFF);
    buffer.push_back((flags >> 16) & 0xFF);
    buffer.push_back((flags >> 24) & 0xFF);
    
    // Timeout
    buffer.push_back(timeout_ms & 0xFF);
    buffer.push_back((timeout_ms >> 8) & 0xFF);
    buffer.push_back((timeout_ms >> 16) & 0xFF);
    buffer.push_back((timeout_ms >> 24) & 0xFF);
    
    // Max rows
    buffer.push_back(max_rows & 0xFF);
    buffer.push_back((max_rows >> 8) & 0xFF);
    buffer.push_back((max_rows >> 16) & 0xFF);
    buffer.push_back((max_rows >> 24) & 0xFF);
    
    // SQL length and data
    uint32_t sql_len = static_cast<uint32_t>(sql.size());
    buffer.push_back(sql_len & 0xFF);
    buffer.push_back((sql_len >> 8) & 0xFF);
    buffer.push_back((sql_len >> 16) & 0xFF);
    buffer.push_back((sql_len >> 24) & 0xFF);
    buffer.insert(buffer.end(), sql.begin(), sql.end());
    
    return buffer;
}

QueryPayload QueryPayload::Deserialize(const std::vector<uint8_t>& data) {
    QueryPayload payload;
    
    if (data.size() < 24) {
        throw std::runtime_error("QueryPayload too short");
    }
    
    size_t offset = 0;
    
    // Query ID
    payload.query_id = 0;
    for (int i = 0; i < 8; ++i) {
        payload.query_id |= static_cast<uint64_t>(data[offset++]) << (i * 8);
    }
    
    // Flags
    payload.flags = data[offset] | (data[offset + 1] << 8) |
                   (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    // Timeout
    payload.timeout_ms = data[offset] | (data[offset + 1] << 8) |
                        (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    // Max rows
    payload.max_rows = data[offset] | (data[offset + 1] << 8) |
                      (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    // SQL
    if (offset + 4 > data.size()) {
        throw std::runtime_error("QueryPayload truncated");
    }
    uint32_t sql_len = data[offset] | (data[offset + 1] << 8) |
                      (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    if (offset + sql_len > data.size()) {
        throw std::runtime_error("QueryPayload truncated at sql");
    }
    payload.sql = std::string(data.begin() + offset, data.begin() + offset + sql_len);
    
    return payload;
}

//===----------------------------------------------------------------------===//
// QueryErrorPayload
//===----------------------------------------------------------------------===//
std::vector<uint8_t> QueryErrorPayload::Serialize() const {
    std::vector<uint8_t> buffer;
    
    // Error code
    buffer.push_back(error_code & 0xFF);
    buffer.push_back((error_code >> 8) & 0xFF);
    buffer.push_back((error_code >> 16) & 0xFF);
    buffer.push_back((error_code >> 24) & 0xFF);
    
    // SQL state (5 bytes)
    buffer.insert(buffer.end(), sql_state, sql_state + 5);
    
    // Message length and data
    uint32_t msg_len = static_cast<uint32_t>(message.size());
    buffer.push_back(msg_len & 0xFF);
    buffer.push_back((msg_len >> 8) & 0xFF);
    buffer.push_back((msg_len >> 16) & 0xFF);
    buffer.push_back((msg_len >> 24) & 0xFF);
    buffer.insert(buffer.end(), message.begin(), message.end());
    
    return buffer;
}

QueryErrorPayload QueryErrorPayload::Deserialize(const std::vector<uint8_t>& data) {
    QueryErrorPayload payload;
    
    if (data.size() < 13) {
        throw std::runtime_error("QueryErrorPayload too short");
    }
    
    size_t offset = 0;
    
    // Error code
    payload.error_code = data[offset] | (data[offset + 1] << 8) |
                        (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    // SQL state
    std::memcpy(payload.sql_state, data.data() + offset, 5);
    payload.sql_state[5] = '\0';
    offset += 5;
    
    // Message
    uint32_t msg_len = data[offset] | (data[offset + 1] << 8) |
                      (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    if (offset + msg_len > data.size()) {
        throw std::runtime_error("QueryErrorPayload truncated");
    }
    payload.message = std::string(data.begin() + offset, data.begin() + offset + msg_len);
    
    return payload;
}

//===----------------------------------------------------------------------===//
// ResultEndPayload
//===----------------------------------------------------------------------===//
std::vector<uint8_t> ResultEndPayload::Serialize() const {
    std::vector<uint8_t> buffer(24);
    
    // Total rows
    for (int i = 0; i < 8; ++i) {
        buffer[i] = (total_rows >> (i * 8)) & 0xFF;
    }
    
    // Execution time
    for (int i = 0; i < 8; ++i) {
        buffer[8 + i] = (execution_time_us >> (i * 8)) & 0xFF;
    }
    
    // Bytes scanned
    for (int i = 0; i < 8; ++i) {
        buffer[16 + i] = (bytes_scanned >> (i * 8)) & 0xFF;
    }
    
    return buffer;
}

ResultEndPayload ResultEndPayload::Deserialize(const std::vector<uint8_t>& data) {
    ResultEndPayload payload;
    
    if (data.size() < 24) {
        throw std::runtime_error("ResultEndPayload too short");
    }
    
    // Total rows
    payload.total_rows = 0;
    for (int i = 0; i < 8; ++i) {
        payload.total_rows |= static_cast<uint64_t>(data[i]) << (i * 8);
    }
    
    // Execution time
    payload.execution_time_us = 0;
    for (int i = 0; i < 8; ++i) {
        payload.execution_time_us |= static_cast<uint64_t>(data[8 + i]) << (i * 8);
    }
    
    // Bytes scanned
    payload.bytes_scanned = 0;
    for (int i = 0; i < 8; ++i) {
        payload.bytes_scanned |= static_cast<uint64_t>(data[16 + i]) << (i * 8);
    }
    
    return payload;
}

//===----------------------------------------------------------------------===//
// PreparePayload
//===----------------------------------------------------------------------===//
std::vector<uint8_t> PreparePayload::Serialize() const {
    std::vector<uint8_t> buffer;
    
    // Statement name length and data
    uint32_t name_len = static_cast<uint32_t>(statement_name.size());
    buffer.push_back(name_len & 0xFF);
    buffer.push_back((name_len >> 8) & 0xFF);
    buffer.push_back((name_len >> 16) & 0xFF);
    buffer.push_back((name_len >> 24) & 0xFF);
    buffer.insert(buffer.end(), statement_name.begin(), statement_name.end());
    
    // SQL length and data
    uint32_t sql_len = static_cast<uint32_t>(sql.size());
    buffer.push_back(sql_len & 0xFF);
    buffer.push_back((sql_len >> 8) & 0xFF);
    buffer.push_back((sql_len >> 16) & 0xFF);
    buffer.push_back((sql_len >> 24) & 0xFF);
    buffer.insert(buffer.end(), sql.begin(), sql.end());
    
    return buffer;
}

PreparePayload PreparePayload::Deserialize(const std::vector<uint8_t>& data) {
    PreparePayload payload;
    
    if (data.size() < 8) {
        throw std::runtime_error("PreparePayload too short");
    }
    
    size_t offset = 0;
    
    // Statement name
    uint32_t name_len = data[offset] | (data[offset + 1] << 8) |
                       (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    if (offset + name_len > data.size()) {
        throw std::runtime_error("PreparePayload truncated at name");
    }
    payload.statement_name = std::string(data.begin() + offset, data.begin() + offset + name_len);
    offset += name_len;
    
    // SQL
    if (offset + 4 > data.size()) {
        throw std::runtime_error("PreparePayload truncated at sql length");
    }
    uint32_t sql_len = data[offset] | (data[offset + 1] << 8) |
                      (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    if (offset + sql_len > data.size()) {
        throw std::runtime_error("PreparePayload truncated at sql");
    }
    payload.sql = std::string(data.begin() + offset, data.begin() + offset + sql_len);
    
    return payload;
}

//===----------------------------------------------------------------------===//
// ExecutePayload
//===----------------------------------------------------------------------===//
std::vector<uint8_t> ExecutePayload::Serialize() const {
    std::vector<uint8_t> buffer;
    
    // Query ID
    for (int i = 0; i < 8; ++i) {
        buffer.push_back((query_id >> (i * 8)) & 0xFF);
    }
    
    // Statement name length and data
    uint32_t name_len = static_cast<uint32_t>(statement_name.size());
    buffer.push_back(name_len & 0xFF);
    buffer.push_back((name_len >> 8) & 0xFF);
    buffer.push_back((name_len >> 16) & 0xFF);
    buffer.push_back((name_len >> 24) & 0xFF);
    buffer.insert(buffer.end(), statement_name.begin(), statement_name.end());
    
    // Parameter count
    uint32_t param_count = static_cast<uint32_t>(parameters.size());
    buffer.push_back(param_count & 0xFF);
    buffer.push_back((param_count >> 8) & 0xFF);
    buffer.push_back((param_count >> 16) & 0xFF);
    buffer.push_back((param_count >> 24) & 0xFF);
    
    // Parameters
    for (const auto& param : parameters) {
        uint32_t param_len = static_cast<uint32_t>(param.size());
        buffer.push_back(param_len & 0xFF);
        buffer.push_back((param_len >> 8) & 0xFF);
        buffer.push_back((param_len >> 16) & 0xFF);
        buffer.push_back((param_len >> 24) & 0xFF);
        buffer.insert(buffer.end(), param.begin(), param.end());
    }
    
    return buffer;
}

ExecutePayload ExecutePayload::Deserialize(const std::vector<uint8_t>& data) {
    ExecutePayload payload;
    
    if (data.size() < 16) {
        throw std::runtime_error("ExecutePayload too short");
    }
    
    size_t offset = 0;
    
    // Query ID
    payload.query_id = 0;
    for (int i = 0; i < 8; ++i) {
        payload.query_id |= static_cast<uint64_t>(data[offset++]) << (i * 8);
    }
    
    // Statement name
    uint32_t name_len = data[offset] | (data[offset + 1] << 8) |
                       (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    if (offset + name_len > data.size()) {
        throw std::runtime_error("ExecutePayload truncated at name");
    }
    payload.statement_name = std::string(data.begin() + offset, data.begin() + offset + name_len);
    offset += name_len;
    
    // Parameter count
    if (offset + 4 > data.size()) {
        throw std::runtime_error("ExecutePayload truncated at param count");
    }
    uint32_t param_count = data[offset] | (data[offset + 1] << 8) |
                          (data[offset + 2] << 16) | (data[offset + 3] << 24);
    offset += 4;
    
    // Parameters
    for (uint32_t i = 0; i < param_count; ++i) {
        if (offset + 4 > data.size()) {
            throw std::runtime_error("ExecutePayload truncated at param length");
        }
        uint32_t param_len = data[offset] | (data[offset + 1] << 8) |
                            (data[offset + 2] << 16) | (data[offset + 3] << 24);
        offset += 4;
        
        if (offset + param_len > data.size()) {
            throw std::runtime_error("ExecutePayload truncated at param data");
        }
        payload.parameters.push_back(
            std::vector<uint8_t>(data.begin() + offset, data.begin() + offset + param_len)
        );
        offset += param_len;
    }
    
    return payload;
}

//===----------------------------------------------------------------------===//
// QueryCancelPayload
//===----------------------------------------------------------------------===//
std::vector<uint8_t> QueryCancelPayload::Serialize() const {
    std::vector<uint8_t> buffer(8);
    for (int i = 0; i < 8; ++i) {
        buffer[i] = (query_id >> (i * 8)) & 0xFF;
    }
    return buffer;
}

QueryCancelPayload QueryCancelPayload::Deserialize(const std::vector<uint8_t>& data) {
    QueryCancelPayload payload;
    
    if (data.size() < 8) {
        throw std::runtime_error("QueryCancelPayload too short");
    }
    
    payload.query_id = 0;
    for (int i = 0; i < 8; ++i) {
        payload.query_id |= static_cast<uint64_t>(data[i]) << (i * 8);
    }
    
    return payload;
}

} // namespace duckdb_server
