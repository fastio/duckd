//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// http/http_server.cpp
//
// Simple HTTP server for health checks and metrics
//===----------------------------------------------------------------------===//

#include "http/http_server.hpp"
#include "network/tcp_server.hpp"
#include "logging/logger.hpp"
#include <sstream>

namespace duckdb_server {

HttpServer::HttpServer(uint16_t port, TcpServer* server)
    : port_(port)
    , server_(server)
    , acceptor_(io_context_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)) {
    acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
}

HttpServer::~HttpServer() {
    Stop();
}

void HttpServer::Start() {
    if (running_) return;
    running_ = true;

    DoAccept();

    thread_ = std::thread([this]() {
        io_context_.run();
    });

    LOG_INFO("http", "HTTP server started on port " + std::to_string(port_));
}

void HttpServer::Stop() {
    if (!running_) return;
    running_ = false;

    asio::error_code ec;
    acceptor_.close(ec);
    io_context_.stop();
    if (thread_.joinable()) {
        thread_.join();
    }

    LOG_INFO("http", "HTTP server stopped");
}

void HttpServer::DoAccept() {
    acceptor_.async_accept([this](std::error_code ec, asio::ip::tcp::socket socket) {
        if (!ec && running_) {
            HandleConnection(std::move(socket));
        }
        if (running_ && acceptor_.is_open()) {
            DoAccept();
        }
    });
}

void HttpServer::HandleConnection(asio::ip::tcp::socket socket) {
    // Use async I/O on the existing io_context — no threads spawned per request.
    auto sock = std::make_shared<asio::ip::tcp::socket>(std::move(socket));
    auto buf = std::make_shared<std::array<char, 4096>>();

    sock->async_read_some(asio::buffer(*buf),
        [this, sock, buf](std::error_code ec, size_t bytes_read) {
            if (ec) return;

            std::string request(buf->data(), bytes_read);
            std::string response = HandleRequest(request);

            auto resp = std::make_shared<std::string>(std::move(response));
            asio::async_write(*sock, asio::buffer(*resp),
                [sock, resp](std::error_code ec, size_t /*bytes_written*/) {
                    asio::error_code shutdown_ec;
                    sock->shutdown(asio::ip::tcp::socket::shutdown_both, shutdown_ec);
                    sock->close(shutdown_ec);
                });
        });
}

std::string HttpServer::HandleRequest(const std::string& request) {
    // Parse request line
    std::istringstream iss(request);
    std::string method, path, version;
    iss >> method >> path >> version;

    if (method != "GET") {
        return BuildResponse(405, "Method Not Allowed", "text/plain", "Method Not Allowed");
    }

    if (path == "/health" || path == "/healthz" || path == "/ready") {
        return GetHealthResponse();
    } else if (path == "/metrics") {
        return GetMetricsResponse();
    } else if (path == "/") {
        return BuildResponse(200, "OK", "text/plain", "DuckD Server");
    }

    return BuildResponse(404, "Not Found", "text/plain", "Not Found");
}

std::string HttpServer::BuildResponse(int status_code, const std::string& status_text,
                                       const std::string& content_type, const std::string& body) {
    std::ostringstream oss;
    oss << "HTTP/1.1 " << status_code << " " << status_text << "\r\n"
        << "Content-Type: " << content_type << "\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: close\r\n"
        << "\r\n"
        << body;
    return oss.str();
}

std::string HttpServer::GetHealthResponse() {
    std::ostringstream json;
    json << "{\n"
         << "  \"status\": \"healthy\",\n"
         << "  \"connections\": " << (server_ ? server_->GetConnectionCount() : 0) << "\n"
         << "}";
    return BuildResponse(200, "OK", "application/json", json.str());
}

std::string HttpServer::GetMetricsResponse() {
    std::ostringstream metrics;

    // Prometheus format
    if (server_) {
        metrics << "# HELP duckd_connections_total Total number of connections\n"
                << "# TYPE duckd_connections_total counter\n"
                << "duckd_connections_total " << server_->GetTotalConnections() << "\n"
                << "\n"
                << "# HELP duckd_connections_active Current active connections\n"
                << "# TYPE duckd_connections_active gauge\n"
                << "duckd_connections_active " << server_->GetConnectionCount() << "\n"
                << "\n"
                << "# HELP duckd_bytes_received_total Total bytes received\n"
                << "# TYPE duckd_bytes_received_total counter\n"
                << "duckd_bytes_received_total " << server_->GetTotalBytesReceived() << "\n"
                << "\n"
                << "# HELP duckd_bytes_sent_total Total bytes sent\n"
                << "# TYPE duckd_bytes_sent_total counter\n"
                << "duckd_bytes_sent_total " << server_->GetTotalBytesSent() << "\n";
    }

    if (metrics_callback_) {
        metrics << metrics_callback_();
    }

    return BuildResponse(200, "OK", "text/plain; version=0.0.4", metrics.str());
}

} // namespace duckdb_server
