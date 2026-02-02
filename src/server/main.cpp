//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// main.cpp
//
// Server main entry point
//===----------------------------------------------------------------------===//

#include "duckdb_server/common.hpp"
#include "duckdb_server/config/server_config.hpp"
#include "duckdb_server/network/tcp_server.hpp"
#include "duckdb_server/session/session_manager.hpp"
#include "duckdb_server/executor/executor_pool.hpp"
#include "duckdb_server/utils/logger.hpp"

#include <csignal>
#include <iostream>

using namespace duckdb_server;

// Global server pointer for signal handling
static std::shared_ptr<TcpServer> g_server;
static std::atomic<bool> g_shutdown_requested{false};

void SignalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        LOG_INFO("main", "Shutdown signal received");
        g_shutdown_requested = true;
        if (g_server) {
            g_server->Stop();
        }
    }
}

int main(int argc, char* argv[]) {
    try {
        // Parse command line arguments
        ServerConfig config = ParseCommandLine(argc, argv);
        
        // Validate configuration
        std::string error;
        if (!config.Validate(error)) {
            std::cerr << "Configuration error: " << error << std::endl;
            return 1;
        }
        
        // Initialize logger
        Logger::Instance().SetLevel(config.log_level);
        if (!config.log_file.empty()) {
            Logger::Instance().SetOutput(config.log_file);
        }
        
        LOG_INFO("main", "Starting DuckDB Server...");
        LOG_INFO("main", "Configuration:");
        LOG_INFO("main", "  Host: " + config.host);
        LOG_INFO("main", "  Port: " + std::to_string(config.port));
        LOG_INFO("main", "  Database: " + config.database_path);
        LOG_INFO("main", "  IO Threads: " + std::to_string(config.GetIoThreadCount()));
        LOG_INFO("main", "  Executor Threads: " + std::to_string(config.GetExecutorThreadCount()));
        LOG_INFO("main", "  Max Connections: " + std::to_string(config.max_connections));
        
        // Create DuckDB instance
        LOG_INFO("main", "Opening database: " + config.database_path);
        auto db = std::make_shared<duckdb::DuckDB>(config.database_path);
        
        // Create session manager
        auto session_manager = std::make_shared<SessionManager>(
            db,
            config.max_connections,
            std::chrono::minutes(config.session_timeout_minutes)
        );
        
        // Create executor pool
        auto executor_pool = std::make_shared<ExecutorPool>(config.GetExecutorThreadCount());
        executor_pool->Start();
        
        // Create and start server
        g_server = std::make_shared<TcpServer>(config, session_manager, executor_pool);
        
        // Set up signal handlers
        std::signal(SIGINT, SignalHandler);
        std::signal(SIGTERM, SignalHandler);
#ifndef _WIN32
        std::signal(SIGPIPE, SIG_IGN);
#endif
        
        // Start server
        g_server->Start();
        
        LOG_INFO("main", "DuckDB Server is ready to accept connections");
        
        // Wait for shutdown
        while (!g_shutdown_requested) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        // Cleanup
        LOG_INFO("main", "Shutting down...");
        
        if (g_server) {
            g_server->Stop();
            g_server.reset();
        }
        
        executor_pool->Stop();
        
        LOG_INFO("main", "DuckDB Server stopped");
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
