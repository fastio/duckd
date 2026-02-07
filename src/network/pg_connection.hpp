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
#include <deque>

namespace duckdb_server {

class TcpServer;
class Session;
class ExecutorPool;

class PgConnection : public std::enable_shared_from_this<PgConnection> {
public:
    using Ptr = std::shared_ptr<PgConnection>;

    static Ptr Create(asio::ip::tcp::socket socket,
                      TcpServer* server,
                      std::shared_ptr<Session> session,
                      std::shared_ptr<ExecutorPool> executor_pool);

    ~PgConnection();

    // Start the connection
    void Start();

    // Close the connection
    void Close();

    // Get remote endpoint info
    std::string GetRemoteAddress() const;
    uint16_t GetRemotePort() const;

private:
    PgConnection(asio::ip::tcp::socket socket,
                 TcpServer* server,
                 std::shared_ptr<Session> session,
                 std::shared_ptr<ExecutorPool> executor_pool);

    void DoRead();
    void DoWrite();
    void Send(const std::vector<uint8_t>& data);

    // Resume message processing after async operation completes
    void ResumeAfterAsync();

private:
    asio::ip::tcp::socket socket_;
    TcpServer* server_;
    std::shared_ptr<Session> session_;
    std::shared_ptr<ExecutorPool> executor_pool_;
    std::unique_ptr<pg::PgHandler> handler_;

    std::array<uint8_t, 8192> read_buffer_;
    std::deque<std::vector<uint8_t>> write_queue_;
    std::mutex write_mutex_;
    bool writing_ = false;
    bool closed_ = false;
};

} // namespace duckdb_server
