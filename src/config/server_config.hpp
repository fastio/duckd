//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// config/server_config.hpp
//
// Server configuration
//===----------------------------------------------------------------------===//

#pragma once

#include "config/config_file.hpp"
#include "config/yaml_config.hpp"
#include <string>
#include <thread>
#include <iostream>
#include <cstdlib>
#include <algorithm>

namespace duckdb_server {

struct ServerConfig {
    // Network
    std::string host = "0.0.0.0";
    uint16_t port = 5432;  // Default to PostgreSQL port
    uint16_t http_port = 0;  // 0 = disabled, for health/metrics
    uint16_t flight_port = 0;  // 0 = disabled, Arrow Flight SQL

    // Database
    std::string database_path = ":memory:";

    // Logging
    std::string log_file;
    std::string log_level = "info";

    // Process
    std::string pid_file;
    std::string config_file;
    std::string user;  // User to run as (privilege dropping)
    bool daemon = false;

    // Threading
    uint32_t io_threads = 0;  // 0 = auto
    uint32_t executor_threads = 0;  // 0 = auto
    uint32_t max_connections = 100;
    uint32_t session_timeout_minutes = 30;

    // Resource limits
    uint64_t max_memory = 0;  // 0 = unlimited (bytes)
    uint32_t max_open_files = 0;  // 0 = system default
    uint32_t query_timeout_ms = 300000;  // 5 minutes

    // Connection pool settings
    uint32_t pool_min_connections = 5;
    uint32_t pool_max_connections = 50;
    uint32_t pool_idle_timeout_seconds = 300;
    uint32_t pool_acquire_timeout_ms = 5000;
    bool pool_validate_on_acquire = true;

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

    // Load from config file (auto-detects format by extension)
    bool LoadFromFile(const std::string& path, std::string& error) {
        // Detect file format by extension
        std::string ext;
        auto dot_pos = path.rfind('.');
        if (dot_pos != std::string::npos) {
            ext = path.substr(dot_pos);
            std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
        }

        if (ext == ".yaml" || ext == ".yml") {
            return LoadFromYaml(path, error);
        } else {
            return LoadFromIni(path, error);
        }
    }

    // Load from INI-style config file (legacy format)
    bool LoadFromIni(const std::string& path, std::string& error) {
        ConfigFile cfg;
        if (!cfg.Load(path)) {
            error = cfg.GetError();
            return false;
        }

        if (cfg.Has("host")) host = cfg.GetString("host");
        if (cfg.Has("port")) port = static_cast<uint16_t>(cfg.GetInt("port"));
        if (cfg.Has("http_port")) http_port = static_cast<uint16_t>(cfg.GetInt("http_port"));
        if (cfg.Has("flight_port")) flight_port = static_cast<uint16_t>(cfg.GetInt("flight_port"));
        if (cfg.Has("database")) database_path = cfg.GetString("database");
        if (cfg.Has("log_file")) log_file = cfg.GetString("log_file");
        if (cfg.Has("log_level")) log_level = cfg.GetString("log_level");
        if (cfg.Has("pid_file")) pid_file = cfg.GetString("pid_file");
        if (cfg.Has("user")) user = cfg.GetString("user");
        if (cfg.Has("daemon")) daemon = cfg.GetBool("daemon");
        if (cfg.Has("io_threads")) io_threads = static_cast<uint32_t>(cfg.GetInt("io_threads"));
        if (cfg.Has("executor_threads")) executor_threads = static_cast<uint32_t>(cfg.GetInt("executor_threads"));
        if (cfg.Has("max_connections")) max_connections = static_cast<uint32_t>(cfg.GetInt("max_connections"));
        if (cfg.Has("session_timeout_minutes")) session_timeout_minutes = static_cast<uint32_t>(cfg.GetInt("session_timeout_minutes"));
        if (cfg.Has("max_memory")) max_memory = static_cast<uint64_t>(cfg.GetInt("max_memory"));
        if (cfg.Has("max_open_files")) max_open_files = static_cast<uint32_t>(cfg.GetInt("max_open_files"));
        if (cfg.Has("query_timeout_ms")) query_timeout_ms = static_cast<uint32_t>(cfg.GetInt("query_timeout_ms"));
        if (cfg.Has("pool_min_connections")) pool_min_connections = static_cast<uint32_t>(cfg.GetInt("pool_min_connections"));
        if (cfg.Has("pool_max_connections")) pool_max_connections = static_cast<uint32_t>(cfg.GetInt("pool_max_connections"));
        if (cfg.Has("pool_idle_timeout")) pool_idle_timeout_seconds = static_cast<uint32_t>(cfg.GetInt("pool_idle_timeout"));
        if (cfg.Has("pool_acquire_timeout")) pool_acquire_timeout_ms = static_cast<uint32_t>(cfg.GetInt("pool_acquire_timeout"));
        if (cfg.Has("pool_validate_on_acquire")) pool_validate_on_acquire = cfg.GetBool("pool_validate_on_acquire");

        return true;
    }

    // Load from YAML config file
    bool LoadFromYaml(const std::string& path, std::string& error) {
        YamlConfig cfg;
        if (!cfg.Load(path)) {
            error = cfg.GetError();
            return false;
        }

        // Server section
        if (cfg.Has("server.host")) host = cfg.GetString("server.host");
        if (cfg.Has("server.port")) port = static_cast<uint16_t>(cfg.GetInt("server.port"));
        if (cfg.Has("server.http_port")) http_port = static_cast<uint16_t>(cfg.GetInt("server.http_port"));
        if (cfg.Has("server.flight_port")) flight_port = static_cast<uint16_t>(cfg.GetInt("server.flight_port"));

        // Database section
        if (cfg.Has("database.path")) database_path = cfg.GetString("database.path");

        // Logging section
        if (cfg.Has("logging.file")) log_file = cfg.GetString("logging.file");
        if (cfg.Has("logging.level")) log_level = cfg.GetString("logging.level");

        // Process section
        if (cfg.Has("process.daemon")) daemon = cfg.GetBool("process.daemon");
        if (cfg.Has("process.pid_file")) pid_file = cfg.GetString("process.pid_file");
        if (cfg.Has("process.user")) user = cfg.GetString("process.user");

        // Threads section
        if (cfg.Has("threads.io")) io_threads = static_cast<uint32_t>(cfg.GetInt("threads.io"));
        if (cfg.Has("threads.executor")) executor_threads = static_cast<uint32_t>(cfg.GetInt("threads.executor"));

        // Limits section
        if (cfg.Has("limits.max_connections")) max_connections = static_cast<uint32_t>(cfg.GetInt("limits.max_connections"));
        if (cfg.Has("limits.max_memory")) max_memory = static_cast<uint64_t>(cfg.GetInt64("limits.max_memory"));
        if (cfg.Has("limits.max_open_files")) max_open_files = static_cast<uint32_t>(cfg.GetInt("limits.max_open_files"));
        if (cfg.Has("limits.query_timeout_ms")) query_timeout_ms = static_cast<uint32_t>(cfg.GetInt("limits.query_timeout_ms"));
        if (cfg.Has("limits.session_timeout_minutes")) session_timeout_minutes = static_cast<uint32_t>(cfg.GetInt("limits.session_timeout_minutes"));

        // Pool section
        if (cfg.Has("pool.min")) pool_min_connections = static_cast<uint32_t>(cfg.GetInt("pool.min"));
        if (cfg.Has("pool.max")) pool_max_connections = static_cast<uint32_t>(cfg.GetInt("pool.max"));
        if (cfg.Has("pool.idle_timeout_seconds")) pool_idle_timeout_seconds = static_cast<uint32_t>(cfg.GetInt("pool.idle_timeout_seconds"));
        if (cfg.Has("pool.acquire_timeout_ms")) pool_acquire_timeout_ms = static_cast<uint32_t>(cfg.GetInt("pool.acquire_timeout_ms"));
        if (cfg.Has("pool.validate_on_acquire")) pool_validate_on_acquire = cfg.GetBool("pool.validate_on_acquire");

        return true;
    }
};

inline void PrintUsage(const char* program) {
    std::cout << "Usage: " << program << " [options]\n"
              << "\nOptions:\n"
              << "  -c, --config <path>     Config file path\n"
              << "  -h, --host <host>       Host to bind (default: 0.0.0.0)\n"
              << "  -p, --port <port>       Port to bind (default: 5432)\n"
              << "  -d, --database <path>   Database path (default: :memory:)\n"
              << "  --daemon                Run as daemon (background)\n"
              << "  --pid-file <path>       PID file path\n"
              << "  --user <name>           User to run as (drops privileges)\n"
              << "  --log-file <path>       Log file path\n"
              << "  --log-level <level>     Log level (debug, info, warn, error)\n"
              << "  --io-threads <n>        IO thread count (default: auto)\n"
              << "  --executor-threads <n>  Executor thread count (default: auto)\n"
              << "  --max-connections <n>   Max connections (default: 100)\n"
              << "  --http-port <port>      HTTP port for health/metrics (default: disabled)\n"
              << "  --flight-port <port>    Arrow Flight SQL port (default: disabled)\n"
              << "  --max-memory <bytes>    Max memory limit (default: unlimited)\n"
              << "  --max-open-files <n>    Max open file descriptors\n"
              << "  --query-timeout <ms>    Query timeout in milliseconds (default: 300000)\n"
              << "  --pool-min <n>          Pool minimum connections (default: 5)\n"
              << "  --pool-max <n>          Pool maximum connections (default: 50)\n"
              << "  --pool-idle-timeout <s> Pool idle connection timeout in seconds (default: 300)\n"
              << "  --version               Show version info\n"
              << "  --help                  Show this help\n";
}

inline ServerConfig ParseCommandLine(int argc, char* argv[], bool& show_version) {
    ServerConfig config;
    show_version = false;
    std::string config_file_path;

    // First pass: look for config file
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "-c" || arg == "--config") && i + 1 < argc) {
            config_file_path = argv[++i];
        }
    }

    // Load config file if specified
    if (!config_file_path.empty()) {
        std::string error;
        if (!config.LoadFromFile(config_file_path, error)) {
            std::cerr << "Error loading config file: " << error << std::endl;
            std::exit(1);
        }
        config.config_file = config_file_path;
    }

    // Second pass: command line overrides config file
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help") {
            PrintUsage(argv[0]);
            std::exit(0);
        } else if (arg == "--version") {
            show_version = true;
        } else if ((arg == "-c" || arg == "--config") && i + 1 < argc) {
            ++i;  // Already processed
        } else if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            config.host = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            config.port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if ((arg == "-d" || arg == "--database") && i + 1 < argc) {
            config.database_path = argv[++i];
        } else if (arg == "--daemon") {
            config.daemon = true;
        } else if (arg == "--pid-file" && i + 1 < argc) {
            config.pid_file = argv[++i];
        } else if (arg == "--user" && i + 1 < argc) {
            config.user = argv[++i];
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
        } else if (arg == "--http-port" && i + 1 < argc) {
            config.http_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--flight-port" && i + 1 < argc) {
            config.flight_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--max-memory" && i + 1 < argc) {
            config.max_memory = static_cast<uint64_t>(std::stoll(argv[++i]));
        } else if (arg == "--max-open-files" && i + 1 < argc) {
            config.max_open_files = static_cast<uint32_t>(std::stoi(argv[++i]));
        } else if (arg == "--query-timeout" && i + 1 < argc) {
            config.query_timeout_ms = static_cast<uint32_t>(std::stoi(argv[++i]));
        } else if (arg == "--pool-min" && i + 1 < argc) {
            config.pool_min_connections = static_cast<uint32_t>(std::stoi(argv[++i]));
        } else if (arg == "--pool-max" && i + 1 < argc) {
            config.pool_max_connections = static_cast<uint32_t>(std::stoi(argv[++i]));
        } else if (arg == "--pool-idle-timeout" && i + 1 < argc) {
            config.pool_idle_timeout_seconds = static_cast<uint32_t>(std::stoi(argv[++i]));
        }
    }

    return config;
}

// Backward compatibility
inline ServerConfig ParseCommandLine(int argc, char* argv[]) {
    bool show_version;
    return ParseCommandLine(argc, argv, show_version);
}

} // namespace duckdb_server
