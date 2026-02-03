//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// http/http_server.hpp
//
// Simple HTTP server for health checks and metrics
//===----------------------------------------------------------------------===//

#pragma once

#include <asio.hpp>
#include <string>
#include <memory>
#include <functional>
#include <thread>
#include <atomic>

namespace duckdb_server {

class TcpServer;  // Forward declaration

class HttpServer : public std::enable_shared_from_this<HttpServer> {
public:
    using MetricsCallback = std::function<std::string()>;

    HttpServer(uint16_t port, TcpServer* server);
    ~HttpServer();

    void Start();
    void Stop();

    void SetMetricsCallback(MetricsCallback callback) { metrics_callback_ = std::move(callback); }

private:
    void DoAccept();
    void HandleConnection(asio::ip::tcp::socket socket);
    std::string HandleRequest(const std::string& request);
    std::string BuildResponse(int status_code, const std::string& status_text,
                              const std::string& content_type, const std::string& body);

    std::string GetHealthResponse();
    std::string GetMetricsResponse();

private:
    uint16_t port_;
    TcpServer* server_;
    asio::io_context io_context_;
    asio::ip::tcp::acceptor acceptor_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    MetricsCallback metrics_callback_;
};

} // namespace duckdb_server
