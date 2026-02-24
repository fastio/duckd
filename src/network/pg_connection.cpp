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

PgConnection::Ptr PgConnection::Create(asio::ip::tcp::socket socket_p,
                                        TcpServer* server_p,
                                        std::shared_ptr<Session> session_p,
                                        std::shared_ptr<ExecutorPool> executor_pool_p) {
    return Ptr(new PgConnection(std::move(socket_p), server_p, std::move(session_p), std::move(executor_pool_p)));
}

PgConnection::PgConnection(asio::ip::tcp::socket socket_p,
                           TcpServer* server_p,
                           std::shared_ptr<Session> session_p,
                           std::shared_ptr<ExecutorPool> executor_pool_p)
    : socket(std::move(socket_p))
    , server(server_p)
    , session(std::move(session_p))
    , executor_pool(std::move(executor_pool_p)) {
}

PgConnection::~PgConnection() {
    LOG_DEBUG("pg_conn", "PgConnection destructor called");
    Close();
}

void PgConnection::Start() {
    auto self = shared_from_this();

    // Create resume callback that posts back to the IO thread
    // The state_update closure runs on the IO thread before resuming message processing,
    // ensuring all handler member modifications happen on the IO thread (no data race).
    auto resume_callback = [this, self](pg::PgHandler::StateUpdate state_update) {
        asio::post(socket.get_executor(), [this, self, state_update = std::move(state_update)]() {
            if (closed || !handler) return;
            if (state_update) {
                state_update();
            }
            ResumeAfterAsync();
        });
    };

    // Create cancel callback
    pg::PgHandler::CancelCallback cancel_callback;
    if (server) {
        cancel_callback = [this](int32_t pid, int32_t key) -> bool {
            if (auto sm = server->GetSessionManager()) {
                return sm->CancelQuery(pid, key);
            }
            return false;
        };
    }

    // Create the PG handler with send callback, executor pool, and resume callback
    handler = std::make_unique<pg::PgHandler>(
        session,
        [this, self](std::vector<uint8_t> data) {
            Send(std::move(data));
        },
        executor_pool,
        std::move(resume_callback),
        std::move(cancel_callback)
    );

    LOG_INFO("pg_conn", "New PostgreSQL connection from " + GetRemoteAddress() + ":" + std::to_string(GetRemotePort()));

    DoRead();
}

void PgConnection::Close() {
    if (closed) return;

    LOG_DEBUG("pg_conn", "Close() called, cleaning up resources");
    closed = true;

    if (server) {
        server->DecrementConnections();
    }

    // IMPORTANT: Remove session from SessionManager first.
    // The SessionManager holds a shared_ptr<Session> in its sessions_ map.
    // Without removing it, the Session and PooledConnection would be kept alive
    // until the cleanup thread removes expired sessions (default: 30 minutes).
    if (session && server) {
        uint64_t session_id = session->GetSessionId();
        if (auto session_manager = server->GetSessionManager()) {
            session_manager->RemoveSession(session_id);
            LOG_DEBUG("pg_conn", "Removed session " + std::to_string(session_id) + " from SessionManager");
        }
    }

    // Reset handler to break circular reference.
    // The send_callback_ lambda in handler captures shared_ptr<PgConnection>,
    // creating a cycle: PgConnection -> handler -> send_callback_ -> self.
    handler.reset();

    // Release our session reference
    session.reset();

    // Release executor pool reference
    executor_pool.reset();

    asio::error_code ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    socket.close(ec);

    LOG_DEBUG("pg_conn", "Connection closed");
}

std::string PgConnection::GetRemoteAddress() const {
    try {
        return socket.remote_endpoint().address().to_string();
    } catch (...) {
        return "unknown";
    }
}

uint16_t PgConnection::GetRemotePort() const {
    try {
        return socket.remote_endpoint().port();
    } catch (...) {
        return 0;
    }
}

void PgConnection::DoRead() {
    if (closed) return;

    auto self = shared_from_this();
    socket.async_read_some(
        asio::buffer(read_buffer),
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
            if (closed || !handler) {
                return;
            }

            if (server) {
                server->AddBytesReceived(bytes_read);
            }

            // Process data through PG handler
            auto result = handler->ProcessData(read_buffer.data(), bytes_read);

            switch (result) {
                case pg::ProcessResult::CONTINUE:
                    DoRead();
                    break;
                case pg::ProcessResult::ASYNC_PENDING:
                    // Don't call DoRead() — wait for ResumeAfterAsync
                    break;
                case pg::ProcessResult::CLOSE:
                    Close();
                    break;
            }
        });
}

void PgConnection::ResumeAfterAsync() {
    if (closed || !handler) return;

    auto result = handler->ProcessPendingBuffer();

    switch (result) {
        case pg::ProcessResult::CONTINUE:
            DoRead();
            break;
        case pg::ProcessResult::ASYNC_PENDING:
            // Another async op was triggered, wait again
            break;
        case pg::ProcessResult::CLOSE:
            Close();
            break;
    }
}

void PgConnection::Send(std::vector<uint8_t> data) {
    if (closed || data.empty()) return;

    bool should_write = false;
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        write_queue.push_back(std::move(data));

        if (!writing) {
            writing = true;
            should_write = true;
        }
    }

    if (should_write) {
        // dispatch() runs inline when already on the IO thread (sync handlers),
        // and posts when called from an executor pool thread (streaming sends).
        // This avoids a wasted event-loop round-trip for BindComplete, RFQ, etc.
        auto self = shared_from_this();
        asio::dispatch(socket.get_executor(), [this, self]() {
            DoWrite();
        });
    }
}

void PgConnection::DoWrite() {
    if (closed) return;

    auto self = shared_from_this();

    // Drain all pending buffers into a batch for scatter-gather write
    {
        std::lock_guard<std::mutex> lock(write_mutex);
        if (write_queue.empty()) {
            writing = false;
            return;
        }
        write_batch.clear();
        std::swap(write_batch, write_queue);
    }

    // Build scatter-gather buffer sequence
    std::vector<asio::const_buffer> buffers;
    buffers.reserve(write_batch.size());
    for (const auto& buf : write_batch) {
        buffers.emplace_back(asio::buffer(buf));
    }

    asio::async_write(
        socket,
        buffers,
        [this, self](std::error_code ec, std::size_t bytes_written) {
            write_batch.clear();

            if (ec) {
                if (ec != asio::error::operation_aborted) {
                    LOG_DEBUG("pg_conn", "Write error: " + ec.message());
                }
                Close();
                return;
            }

            if (server) {
                server->AddBytesSent(bytes_written);
            }

            DoWrite();
        });
}

} // namespace duckdb_server
