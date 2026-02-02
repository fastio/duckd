//===----------------------------------------------------------------------===//
//                         DuckDB Remote Extension
//
// remote_client.hpp
//
// Client for connecting to DuckDB Server using the DUCK protocol
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Protocol Constants (matching duckdb_server protocol)
//===----------------------------------------------------------------------===//
namespace remote_protocol {
    constexpr uint32_t PROTOCOL_MAGIC = 0x4B435544;  // "DUCK"
    constexpr uint8_t PROTOCOL_VERSION = 0x01;
    constexpr size_t HEADER_SIZE = 12;

    // Message types
    enum class MessageType : uint8_t {
        HELLO           = 0x01,
        HELLO_RESPONSE  = 0x02,
        PING            = 0x03,
        PONG            = 0x04,
        CLOSE           = 0x05,
        QUERY           = 0x10,
        QUERY_RESULT    = 0x11,
        QUERY_ERROR     = 0x12,
        RESULT_HEADER   = 0x40,
        RESULT_BATCH    = 0x41,
        RESULT_END      = 0x42,
    };

    // Client types
    enum class ClientType : uint8_t {
        CPP = 0x01,
    };

#pragma pack(push, 1)
    struct MessageHeader {
        uint32_t magic;
        uint8_t version;
        uint8_t type;
        uint8_t flags;
        uint8_t reserved;
        uint32_t length;

        bool IsValid() const {
            return magic == PROTOCOL_MAGIC && version == PROTOCOL_VERSION;
        }
    };
#pragma pack(pop)

    static_assert(sizeof(MessageHeader) == HEADER_SIZE, "Header size mismatch");
} // namespace remote_protocol

//===----------------------------------------------------------------------===//
// Column Info
//===----------------------------------------------------------------------===//
struct RemoteColumnInfo {
    string name;
    string type_str;
    LogicalType type;
    bool nullable;
};

//===----------------------------------------------------------------------===//
// Query Result
//===----------------------------------------------------------------------===//
class RemoteQueryResult {
public:
    vector<RemoteColumnInfo> columns;
    vector<vector<Value>> rows;
    idx_t total_rows = 0;
    uint64_t execution_time_us = 0;
    uint64_t bytes_scanned = 0;
    string error_message;
    bool has_error = false;
};

//===----------------------------------------------------------------------===//
// Remote Client
//===----------------------------------------------------------------------===//
class RemoteClient {
public:
    RemoteClient();
    ~RemoteClient();

    // Connection management
    void Connect(const string &host, uint16_t port, const string &username = "");
    void Disconnect();
    bool IsConnected() const;

    // Query execution
    unique_ptr<RemoteQueryResult> Query(const string &sql, uint32_t timeout_ms = 0);

    // Connection info
    uint64_t GetSessionId() const { return session_id_; }
    const string& GetHost() const { return host_; }
    uint16_t GetPort() const { return port_; }

private:
    // Network operations
    void SendMessage(remote_protocol::MessageType type, const vector<uint8_t> &payload);
    bool ReceiveMessage(remote_protocol::MessageHeader &header, vector<uint8_t> &payload);
    void ReceiveExact(uint8_t *buffer, size_t size);

    // Payload builders
    vector<uint8_t> BuildHelloPayload(const string &username);
    vector<uint8_t> BuildQueryPayload(uint64_t query_id, const string &sql, uint32_t timeout_ms);

    // Response parsers
    void ParseHelloResponse(const vector<uint8_t> &payload);
    void ParseSchema(const vector<uint8_t> &payload, RemoteQueryResult &result);
    void ParseBatch(const vector<uint8_t> &payload, RemoteQueryResult &result);
    void ParseResultEnd(const vector<uint8_t> &payload, RemoteQueryResult &result);
    void ParseError(const vector<uint8_t> &payload, RemoteQueryResult &result);

    // Type conversion
    LogicalType ParseArrowTypeFormat(const string &format);
    vector<Value> DecodeColumn(const vector<vector<uint8_t>> &buffers, const string &type_str,
                                uint64_t length, uint64_t null_count);

private:
    // Socket management (using pimpl to hide asio details)
    class SocketImpl;
    unique_ptr<SocketImpl> socket_impl_;

    // Connection state
    string host_;
    uint16_t port_ = 0;
    uint64_t session_id_ = 0;
    atomic<uint64_t> query_id_counter_{0};
    atomic<bool> connected_{false};
    mutex send_mutex_;
    mutex recv_mutex_;
};

} // namespace duckdb
