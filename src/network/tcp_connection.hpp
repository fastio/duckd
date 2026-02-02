//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// network/tcp_connection.hpp
//
// TCP connection handling
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "protocol/message.hpp"
#include <asio.hpp>
#include <queue>

namespace duckdb_server {

class TcpServer;
class ProtocolHandler;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    using Ptr = std::shared_ptr<TcpConnection>;
    
    TcpConnection(asio::io_context& io_context, 
                  TcpServer& server,
                  std::shared_ptr<ProtocolHandler> handler);
    ~TcpConnection();
    
    // Non-copyable
    TcpConnection(const TcpConnection&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;
    
    // Get the socket
    asio::ip::tcp::socket& GetSocket() { return socket_; }
    
    // Start reading
    void Start();
    
    // Close connection
    void Close();
    
    // Send message
    void Send(const Message& message);
    void Send(std::vector<uint8_t> data);
    
    // Get remote endpoint info
    std::string GetRemoteAddress() const;
    uint16_t GetRemotePort() const;
    
    // Get connection ID
    uint64_t GetConnectionId() const { return connection_id_; }
    
    // Check if connected
    bool IsConnected() const { return connected_; }
    
    // Get associated session ID
    uint64_t GetSessionId() const { return session_id_; }
    void SetSessionId(uint64_t id) { session_id_ = id; }

private:
    // Read header
    void DoReadHeader();
    
    // Read payload
    void DoReadPayload();
    
    // Process complete message
    void ProcessMessage();
    
    // Write next message from queue
    void DoWrite();
    
    // Handle errors
    void HandleError(const std::string& operation, const asio::error_code& ec);

private:
    // Socket
    asio::ip::tcp::socket socket_;
    
    // Server reference
    TcpServer& server_;
    
    // Protocol handler
    std::shared_ptr<ProtocolHandler> handler_;
    
    // Connection ID
    uint64_t connection_id_;
    
    // Session ID (set after HELLO)
    uint64_t session_id_;
    
    // Connected flag
    std::atomic<bool> connected_;
    
    // Read buffer for header
    std::array<uint8_t, MessageHeader::SIZE> header_buffer_;
    
    // Current message being read
    Message current_message_;
    
    // Write queue
    std::queue<std::vector<uint8_t>> write_queue_;
    std::mutex write_mutex_;
    std::atomic<bool> writing_;
    
    // Statistics
    std::atomic<uint64_t> bytes_received_;
    std::atomic<uint64_t> bytes_sent_;
    std::atomic<uint64_t> messages_received_;
    std::atomic<uint64_t> messages_sent_;
    
    // Static connection ID generator
    static std::atomic<uint64_t> next_connection_id_;
};

} // namespace duckdb_server
