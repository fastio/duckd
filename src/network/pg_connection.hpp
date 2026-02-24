//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// network/pg_connection.hpp
//
// PostgreSQL protocol connection
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "protocol/pg/pg_handler.hpp"
#include <asio.hpp>
#include <memory>
#include <array>
#include <vector>

namespace duckdb_server {

class TcpServer;
class Session;
class ExecutorPool;

class PgConnection : public std::enable_shared_from_this<PgConnection> {
public:
    using Ptr = std::shared_ptr<PgConnection>;

    static Ptr Create(asio::ip::tcp::socket socket_p,
                      TcpServer* server_p,
                      std::shared_ptr<Session> session_p,
                      std::shared_ptr<ExecutorPool> executor_pool_p);

    ~PgConnection();

    // Start the connection
    void Start();

    // Close the connection
    void Close();

    // Get remote endpoint info
    std::string GetRemoteAddress() const;
    uint16_t GetRemotePort() const;

private:
    PgConnection(asio::ip::tcp::socket socket_p,
                 TcpServer* server_p,
                 std::shared_ptr<Session> session_p,
                 std::shared_ptr<ExecutorPool> executor_pool_p);

    void DoRead();
    void DoWrite();
    void Send(std::vector<uint8_t> data);

    // Resume message processing after async operation completes
    void ResumeAfterAsync();

private:
    asio::ip::tcp::socket socket;
    TcpServer* server;
    std::shared_ptr<Session> session;
    std::shared_ptr<ExecutorPool> executor_pool;
    std::unique_ptr<pg::PgHandler> handler;

    std::array<uint8_t, 32768> read_buffer;
    std::vector<std::vector<uint8_t>> write_queue;
    std::vector<std::vector<uint8_t>> write_batch;  // Owned by DoWrite, holds data during async_write
    std::mutex write_mutex;
    bool writing = false;
    bool closed = false;
};

} // namespace duckdb_server
