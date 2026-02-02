//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// config/server_config.hpp
//
// Server configuration
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <thread>
#include <iostream>
#include <cstdlib>

namespace duckdb_server {

struct ServerConfig {
    std::string host = "0.0.0.0";
    uint16_t port = 9999;
    std::string database_path = ":memory:";
    std::string log_file;
    std::string log_level = "info";

    uint32_t io_threads = 0;  // 0 = auto (based on hardware)
    uint32_t executor_threads = 0;  // 0 = auto
    uint32_t max_connections = 100;
    uint32_t session_timeout_minutes = 30;

    uint32_t GetIoThreadCount() const {
        if (io_threads == 0) {
            return std::max(1u, std::thread::hardware_concurrency() / 2);
        }
        return io_threads;
    }

    uint32_t GetExecutorThreadCount() const {
        if (executor_threads == 0) {
            return std::max(1u, std::thread::hardware_concurrency());
        }
        return executor_threads;
    }

    bool Validate(std::string& error) const {
        if (port == 0) {
            error = "Invalid port number";
            return false;
        }
        if (max_connections == 0) {
            error = "Max connections must be greater than 0";
            return false;
        }
        return true;
    }
};

inline void PrintUsage(const char* program) {
    std::cout << "Usage: " << program << " [options]\n"
              << "Options:\n"
              << "  -h, --host <host>       Host to bind (default: 0.0.0.0)\n"
              << "  -p, --port <port>       Port to bind (default: 9999)\n"
              << "  -d, --database <path>   Database path (default: :memory:)\n"
              << "  --log-file <path>       Log file path\n"
              << "  --log-level <level>     Log level (debug, info, warn, error)\n"
              << "  --io-threads <n>        IO thread count (default: auto)\n"
              << "  --executor-threads <n>  Executor thread count (default: auto)\n"
              << "  --max-connections <n>   Max connections (default: 100)\n"
              << "  --help                  Show this help\n";
}

inline ServerConfig ParseCommandLine(int argc, char* argv[]) {
    ServerConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help") {
            PrintUsage(argv[0]);
            std::exit(0);
        } else if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            config.host = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            config.port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if ((arg == "-d" || arg == "--database") && i + 1 < argc) {
            config.database_path = argv[++i];
        } else if (arg == "--log-file" && i + 1 < argc) {
            config.log_file = argv[++i];
        } else if (arg == "--log-level" && i + 1 < argc) {
            config.log_level = argv[++i];
        } else if (arg == "--io-threads" && i + 1 < argc) {
            config.io_threads = static_cast<uint32_t>(std::stoi(argv[++i]));
        } else if (arg == "--executor-threads" && i + 1 < argc) {
            config.executor_threads = static_cast<uint32_t>(std::stoi(argv[++i]));
        } else if (arg == "--max-connections" && i + 1 < argc) {
            config.max_connections = static_cast<uint32_t>(std::stoi(argv[++i]));
        }
    }

    return config;
}

} // namespace duckdb_server
