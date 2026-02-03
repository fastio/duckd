//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// network/pg_connection.cpp
//
// PostgreSQL protocol connection implementation
//===----------------------------------------------------------------------===//

#include "network/pg_connection.hpp"
#include "network/tcp_server.hpp"
#include "session/session.hpp"
#include "utils/logger.hpp"

namespace duckdb_server {

PgConnection::Ptr PgConnection::Create(asio::ip::tcp::socket socket,
                                        TcpServer* server,
                                        std::shared_ptr<Session> session) {
    return Ptr(new PgConnection(std::move(socket), server, std::move(session)));
}

PgConnection::PgConnection(asio::ip::tcp::socket socket,
                           TcpServer* server,
                           std::shared_ptr<Session> session)
    : socket_(std::move(socket))
    , server_(server)
    , session_(std::move(session)) {
}

PgConnection::~PgConnection() {
    Close();
}

void PgConnection::Start() {
    auto self = shared_from_this();

    // Create the PG handler with send callback
    handler_ = std::make_unique<pg::PgHandler>(session_, [this, self](const std::vector<uint8_t>& data) {
        Send(data);
    });

    LOG_INFO("pg_conn", "New PostgreSQL connection from " + GetRemoteAddress() + ":" + std::to_string(GetRemotePort()));

    DoRead();
}

void PgConnection::Close() {
    if (closed_) return;
    closed_ = true;

    asio::error_code ec;
    socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    socket_.close(ec);

    LOG_DEBUG("pg_conn", "Connection closed");
}

std::string PgConnection::GetRemoteAddress() const {
    try {
        return socket_.remote_endpoint().address().to_string();
    } catch (...) {
        return "unknown";
    }
}

uint16_t PgConnection::GetRemotePort() const {
    try {
        return socket_.remote_endpoint().port();
    } catch (...) {
        return 0;
    }
}

void PgConnection::DoRead() {
    if (closed_) return;

    auto self = shared_from_this();
    socket_.async_read_some(
        asio::buffer(read_buffer_),
        [this, self](std::error_code ec, std::size_t bytes_read) {
            if (ec) {
                if (ec != asio::error::operation_aborted &&
                    ec != asio::error::eof) {
                    LOG_DEBUG("pg_conn", "Read error: " + ec.message());
                }
                Close();
                return;
            }

            if (server_) {
                server_->AddBytesReceived(bytes_read);
            }

            // Process data through PG handler
            bool should_continue = handler_->ProcessData(read_buffer_.data(), bytes_read);

            if (!should_continue || handler_->GetState() == pg::PgConnectionState::Closed) {
                Close();
                return;
            }

            DoRead();
        });
}

void PgConnection::Send(const std::vector<uint8_t>& data) {
    if (closed_ || data.empty()) return;

    bool should_write = false;
    {
        std::lock_guard<std::mutex> lock(write_mutex_);
        write_queue_.push_back(data);

        if (!writing_) {
            writing_ = true;
            should_write = true;
        }
    }

    if (should_write) {
        DoWrite();
    }
}

void PgConnection::DoWrite() {
    if (closed_) return;

    auto self = shared_from_this();

    std::vector<uint8_t> data;
    {
        std::lock_guard<std::mutex> lock(write_mutex_);
        if (write_queue_.empty()) {
            writing_ = false;
            return;
        }
        data = std::move(write_queue_.front());
        write_queue_.pop_front();
    }

    asio::async_write(
        socket_,
        asio::buffer(data),
        [this, self](std::error_code ec, std::size_t bytes_written) {
            if (ec) {
                if (ec != asio::error::operation_aborted) {
                    LOG_DEBUG("pg_conn", "Write error: " + ec.message());
                }
                Close();
                return;
            }

            if (server_) {
                server_->AddBytesSent(bytes_written);
            }

            DoWrite();
        });
}

} // namespace duckdb_server
