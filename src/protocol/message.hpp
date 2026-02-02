//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// protocol/message.hpp
//
// Protocol message definitions
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "protocol/message_types.hpp"
#include <cstring>

namespace duckdb_server {

//===----------------------------------------------------------------------===//
// Message Header
//===----------------------------------------------------------------------===//
#pragma pack(push, 1)
struct MessageHeader {
    uint32_t magic;      // Protocol magic: "DUCK" = 0x4B435544
    uint8_t  version;    // Protocol version
    uint8_t  type;       // Message type
    uint8_t  flags;      // Compression/encryption flags
    uint8_t  reserved;   // Reserved for future use
    uint32_t length;     // Payload length
    
    static constexpr size_t SIZE = 12;
    
    MessageHeader() 
        : magic(PROTOCOL_MAGIC)
        , version(PROTOCOL_VERSION)
        , type(static_cast<uint8_t>(MessageType::UNKNOWN))
        , flags(MessageFlags::NONE)
        , reserved(0)
        , length(0) {}
    
    MessageHeader(MessageType msg_type, uint32_t payload_length, uint8_t msg_flags = MessageFlags::NONE)
        : magic(PROTOCOL_MAGIC)
        , version(PROTOCOL_VERSION)
        , type(static_cast<uint8_t>(msg_type))
        , flags(msg_flags)
        , reserved(0)
        , length(payload_length) {}
    
    bool IsValid() const {
        return magic == PROTOCOL_MAGIC && version == PROTOCOL_VERSION;
    }
    
    MessageType GetType() const {
        return static_cast<MessageType>(type);
    }
    
    void SetType(MessageType msg_type) {
        type = static_cast<uint8_t>(msg_type);
    }
};
#pragma pack(pop)

static_assert(sizeof(MessageHeader) == MessageHeader::SIZE, "MessageHeader size mismatch");

//===----------------------------------------------------------------------===//
// Message Class
//===----------------------------------------------------------------------===//
class Message {
public:
    Message() = default;
    
    Message(MessageType type)
        : header_(type, 0) {}
    
    Message(MessageType type, const std::vector<uint8_t>& payload)
        : header_(type, static_cast<uint32_t>(payload.size()))
        , payload_(payload) {}
    
    Message(MessageType type, std::vector<uint8_t>&& payload)
        : header_(type, static_cast<uint32_t>(payload.size()))
        , payload_(std::move(payload)) {}
    
    // Getters
    const MessageHeader& GetHeader() const { return header_; }
    MessageHeader& GetHeader() { return header_; }
    MessageType GetType() const { return header_.GetType(); }
    uint8_t GetFlags() const { return header_.flags; }
    uint32_t GetPayloadLength() const { return header_.length; }
    const std::vector<uint8_t>& GetPayload() const { return payload_; }
    std::vector<uint8_t>& GetPayload() { return payload_; }
    
    // Setters
    void SetType(MessageType type) { header_.SetType(type); }
    void SetFlags(uint8_t flags) { header_.flags = flags; }
    void SetPayload(const std::vector<uint8_t>& payload) {
        payload_ = payload;
        header_.length = static_cast<uint32_t>(payload_.size());
    }
    void SetPayload(std::vector<uint8_t>&& payload) {
        payload_ = std::move(payload);
        header_.length = static_cast<uint32_t>(payload_.size());
    }
    
    // Validation
    bool IsValid() const { return header_.IsValid(); }
    
    // Serialization
    std::vector<uint8_t> Serialize() const {
        std::vector<uint8_t> buffer(MessageHeader::SIZE + payload_.size());
        std::memcpy(buffer.data(), &header_, MessageHeader::SIZE);
        if (!payload_.empty()) {
            std::memcpy(buffer.data() + MessageHeader::SIZE, payload_.data(), payload_.size());
        }
        return buffer;
    }
    
    // Total size
    size_t TotalSize() const {
        return MessageHeader::SIZE + payload_.size();
    }

private:
    MessageHeader header_;
    std::vector<uint8_t> payload_;
};

//===----------------------------------------------------------------------===//
// Hello Payload
//===----------------------------------------------------------------------===//
struct HelloPayload {
    uint8_t  protocol_version;
    uint8_t  client_type;
    uint16_t client_version;
    uint32_t capabilities;
    std::string username;
    std::vector<uint8_t> auth_data;
    
    std::vector<uint8_t> Serialize() const;
    static HelloPayload Deserialize(const std::vector<uint8_t>& data);
};

//===----------------------------------------------------------------------===//
// Hello Response Payload
//===----------------------------------------------------------------------===//
struct HelloResponsePayload {
    uint8_t  status;  // 0 = success
    uint64_t session_id;
    uint8_t  protocol_version;
    uint32_t server_capabilities;
    std::string server_info;
    
    std::vector<uint8_t> Serialize() const;
    static HelloResponsePayload Deserialize(const std::vector<uint8_t>& data);
};

//===----------------------------------------------------------------------===//
// Query Payload
//===----------------------------------------------------------------------===//
struct QueryPayload {
    uint64_t query_id;
    uint32_t flags;
    uint32_t timeout_ms;
    uint32_t max_rows;
    std::string sql;
    
    std::vector<uint8_t> Serialize() const;
    static QueryPayload Deserialize(const std::vector<uint8_t>& data);
};

//===----------------------------------------------------------------------===//
// Query Error Payload
//===----------------------------------------------------------------------===//
struct QueryErrorPayload {
    uint32_t error_code;
    char sql_state[6];  // 5 chars + null terminator
    std::string message;
    
    QueryErrorPayload()
        : error_code(0) {
        std::memset(sql_state, 0, sizeof(sql_state));
    }
    
    QueryErrorPayload(ErrorCode code, const std::string& msg)
        : error_code(static_cast<uint32_t>(code))
        , message(msg) {
        std::strncpy(sql_state, ErrorCodeToSqlState(code), 5);
        sql_state[5] = '\0';
    }
    
    std::vector<uint8_t> Serialize() const;
    static QueryErrorPayload Deserialize(const std::vector<uint8_t>& data);
};

//===----------------------------------------------------------------------===//
// Result End Payload
//===----------------------------------------------------------------------===//
struct ResultEndPayload {
    uint64_t total_rows;
    uint64_t execution_time_us;
    uint64_t bytes_scanned;
    
    ResultEndPayload()
        : total_rows(0)
        , execution_time_us(0)
        , bytes_scanned(0) {}
    
    std::vector<uint8_t> Serialize() const;
    static ResultEndPayload Deserialize(const std::vector<uint8_t>& data);
};

//===----------------------------------------------------------------------===//
// Prepare Payload
//===----------------------------------------------------------------------===//
struct PreparePayload {
    std::string statement_name;
    std::string sql;
    
    std::vector<uint8_t> Serialize() const;
    static PreparePayload Deserialize(const std::vector<uint8_t>& data);
};

//===----------------------------------------------------------------------===//
// Execute Payload
//===----------------------------------------------------------------------===//
struct ExecutePayload {
    uint64_t query_id;
    std::string statement_name;
    std::vector<std::vector<uint8_t>> parameters;  // Arrow-encoded parameters
    
    std::vector<uint8_t> Serialize() const;
    static ExecutePayload Deserialize(const std::vector<uint8_t>& data);
};

//===----------------------------------------------------------------------===//
// Query Cancel Payload
//===----------------------------------------------------------------------===//
struct QueryCancelPayload {
    uint64_t query_id;
    
    std::vector<uint8_t> Serialize() const;
    static QueryCancelPayload Deserialize(const std::vector<uint8_t>& data);
};

} // namespace duckdb_server
