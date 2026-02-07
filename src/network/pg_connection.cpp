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
#include "session/session_manager.hpp"
#include "executor/executor_pool.hpp"
#include "logging/logger.hpp"

namespace duckdb_server {

PgConnection::Ptr PgConnection::Create(asio::ip::tcp::socket socket,
                                        TcpServer* server,
                                        std::shared_ptr<Session> session,
                                        std::shared_ptr<ExecutorPool> executor_pool) {
    return Ptr(new PgConnection(std::move(socket), server, std::move(session), std::move(executor_pool)));
}

PgConnection::PgConnection(asio::ip::tcp::socket socket,
                           TcpServer* server,
                           std::shared_ptr<Session> session,
                           std::shared_ptr<ExecutorPool> executor_pool)
    : socket_(std::move(socket))
    , server_(server)
    , session_(std::move(session))
    , executor_pool_(std::move(executor_pool)) {
}

PgConnection::~PgConnection() {
    LOG_DEBUG("pg_conn", "PgConnection destructor called");
    Close();
}

void PgConnection::Start() {
    auto self = shared_from_this();

    // Create resume callback that posts back to the IO thread
    auto resume_callback = [this, self]() {
        asio::post(socket_.get_executor(), [this, self]() {
            if (closed_ || !handler_) return;
            ResumeAfterAsync();
        });
    };

    // Create the PG handler with send callback, executor pool, and resume callback
    handler_ = std::make_unique<pg::PgHandler>(
        session_,
        [this, self](const std::vector<uint8_t>& data) {
            Send(data);
        },
        executor_pool_,
        std::move(resume_callback)
    );

    LOG_INFO("pg_conn", "New PostgreSQL connection from " + GetRemoteAddress() + ":" + std::to_string(GetRemotePort()));

    DoRead();
}

void PgConnection::Close() {
    if (closed_) return;

    LOG_DEBUG("pg_conn", "Close() called, cleaning up resources");
    closed_ = true;

    if (server_) {
        server_->DecrementConnections();
    }

    // IMPORTANT: Remove session from SessionManager first.
    // The SessionManager holds a shared_ptr<Session> in its sessions_ map.
    // Without removing it, the Session and PooledConnection would be kept alive
    // until the cleanup thread removes expired sessions (default: 30 minutes).
    if (session_ && server_) {
        uint64_t session_id = session_->GetSessionId();
        if (auto session_manager = server_->GetSessionManager()) {
            session_manager->RemoveSession(session_id);
            LOG_DEBUG("pg_conn", "Removed session " + std::to_string(session_id) + " from SessionManager");
        }
    }

    // Reset handler_ to break circular reference.
    // The send_callback_ lambda in handler_ captures shared_ptr<PgConnection>,
    // creating a cycle: PgConnection -> handler_ -> send_callback_ -> self.
    handler_.reset();

    // Release our session reference
    session_.reset();

    // Release executor pool reference
    executor_pool_.reset();

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

            // Check if connection was closed while async operation was pending
            if (closed_ || !handler_) {
                return;
            }

            if (server_) {
                server_->AddBytesReceived(bytes_read);
            }

            // Process data through PG handler
            auto result = handler_->ProcessData(read_buffer_.data(), bytes_read);

            switch (result) {
                case pg::ProcessResult::Continue:
                    DoRead();
                    break;
                case pg::ProcessResult::AsyncPending:
                    // Don't call DoRead() — wait for ResumeAfterAsync
                    break;
                case pg::ProcessResult::Close:
                    Close();
                    break;
            }
        });
}

void PgConnection::ResumeAfterAsync() {
    if (closed_ || !handler_) return;

    auto result = handler_->ProcessPendingBuffer();

    switch (result) {
        case pg::ProcessResult::Continue:
            DoRead();
            break;
        case pg::ProcessResult::AsyncPending:
            // Another async op was triggered, wait again
            break;
        case pg::ProcessResult::Close:
            Close();
            break;
    }
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
        // Ensure DoWrite runs on the IO thread (may be called from executor thread)
        auto self = shared_from_this();
        asio::post(socket_.get_executor(), [this, self]() {
            DoWrite();
        });
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
