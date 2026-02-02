//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// network/tcp_connection.cpp
//
// TCP connection implementation
//===----------------------------------------------------------------------===//

#include "network/tcp_connection.hpp"
#include "network/tcp_server.hpp"
#include "protocol/protocol_handler.hpp"
#include "utils/logger.hpp"

namespace duckdb_server {

std::atomic<uint64_t> TcpConnection::next_connection_id_{1};

TcpConnection::TcpConnection(asio::io_context& io_context,
                             TcpServer& server,
                             std::shared_ptr<ProtocolHandler> handler)
    : socket_(io_context)
    , server_(server)
    , handler_(handler)
    , connection_id_(next_connection_id_.fetch_add(1))
    , session_id_(0)
    , connected_(false)
    , writing_(false)
    , bytes_received_(0)
    , bytes_sent_(0)
    , messages_received_(0)
    , messages_sent_(0) {
}

TcpConnection::~TcpConnection() {
    Close();
}

void TcpConnection::Start() {
    connected_ = true;
    
    LOG_DEBUG("connection", "Connection " + std::to_string(connection_id_) + 
              " started from " + GetRemoteAddress() + ":" + std::to_string(GetRemotePort()));
    
    DoReadHeader();
}

void TcpConnection::Close() {
    if (!connected_.exchange(false)) {
        return;
    }
    
    LOG_DEBUG("connection", "Connection " + std::to_string(connection_id_) + " closing");
    
    asio::error_code ec;
    socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    socket_.close(ec);
    
    // Remove from server
    server_.RemoveConnection(shared_from_this());
}

void TcpConnection::Send(const Message& message) {
    Send(message.Serialize());
}

void TcpConnection::Send(std::vector<uint8_t> data) {
    if (!connected_) {
        return;
    }
    
    {
        std::lock_guard<std::mutex> lock(write_mutex_);
        write_queue_.push(std::move(data));
    }
    
    // Start writing if not already
    if (!writing_.exchange(true)) {
        DoWrite();
    }
}

std::string TcpConnection::GetRemoteAddress() const {
    try {
        return socket_.remote_endpoint().address().to_string();
    } catch (...) {
        return "unknown";
    }
}

uint16_t TcpConnection::GetRemotePort() const {
    try {
        return socket_.remote_endpoint().port();
    } catch (...) {
        return 0;
    }
}

void TcpConnection::DoReadHeader() {
    if (!connected_) {
        return;
    }
    
    auto self = shared_from_this();
    
    asio::async_read(socket_,
        asio::buffer(header_buffer_),
        [this, self](const asio::error_code& ec, size_t bytes_transferred) {
            if (ec) {
                if (ec != asio::error::operation_aborted && ec != asio::error::eof) {
                    HandleError("read_header", ec);
                }
                Close();
                return;
            }
            
            bytes_received_ += bytes_transferred;
            server_.AddBytesReceived(bytes_transferred);
            
            // Parse header
            std::memcpy(&current_message_.GetHeader(), header_buffer_.data(), MessageHeader::SIZE);
            
            // Validate header
            if (!current_message_.IsValid()) {
                LOG_WARN("connection", "Invalid message header from connection " + 
                         std::to_string(connection_id_));
                Close();
                return;
            }
            
            // Read payload if any
            if (current_message_.GetPayloadLength() > 0) {
                current_message_.GetPayload().resize(current_message_.GetPayloadLength());
                DoReadPayload();
            } else {
                ProcessMessage();
            }
        });
}

void TcpConnection::DoReadPayload() {
    if (!connected_) {
        return;
    }
    
    auto self = shared_from_this();
    
    asio::async_read(socket_,
        asio::buffer(current_message_.GetPayload()),
        [this, self](const asio::error_code& ec, size_t bytes_transferred) {
            if (ec) {
                if (ec != asio::error::operation_aborted && ec != asio::error::eof) {
                    HandleError("read_payload", ec);
                }
                Close();
                return;
            }
            
            bytes_received_ += bytes_transferred;
            server_.AddBytesReceived(bytes_transferred);
            
            ProcessMessage();
        });
}

void TcpConnection::ProcessMessage() {
    messages_received_++;
    
    LOG_TRACE("connection", "Received message type " + 
              std::string(MessageTypeToString(current_message_.GetType())) +
              " on connection " + std::to_string(connection_id_));
    
    // Handle message
    if (handler_) {
        handler_->HandleMessage(current_message_, shared_from_this());
    }
    
    // Clear current message and read next
    current_message_ = Message();
    DoReadHeader();
}

void TcpConnection::DoWrite() {
    if (!connected_) {
        writing_ = false;
        return;
    }
    
    std::vector<uint8_t> data;
    {
        std::lock_guard<std::mutex> lock(write_mutex_);
        if (write_queue_.empty()) {
            writing_ = false;
            return;
        }
        data = std::move(write_queue_.front());
        write_queue_.pop();
    }
    
    auto self = shared_from_this();
    
    asio::async_write(socket_,
        asio::buffer(data),
        [this, self, data_size = data.size()](const asio::error_code& ec, size_t bytes_transferred) {
            if (ec) {
                if (ec != asio::error::operation_aborted) {
                    HandleError("write", ec);
                }
                Close();
                return;
            }
            
            bytes_sent_ += bytes_transferred;
            server_.AddBytesSent(bytes_transferred);
            messages_sent_++;
            
            // Write next message if any
            DoWrite();
        });
}

void TcpConnection::HandleError(const std::string& operation, const asio::error_code& ec) {
    LOG_WARN("connection", "Error on connection " + std::to_string(connection_id_) +
             " during " + operation + ": " + ec.message());
}

} // namespace duckdb_server
