//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// main.cpp
//
// Server main entry point
//===----------------------------------------------------------------------===//

#include "common.hpp"
#include "config/server_config.hpp"
#include "network/tcp_server.hpp"
#include "session/session_manager.hpp"
#include "executor/executor_pool.hpp"
#include "http/http_server.hpp"
#include "logging/logger.hpp"
#include "version.hpp"

#include <csignal>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <pwd.h>
#include <grp.h>
#include <execinfo.h>
#include <cxxabi.h>

#ifdef WITH_SYSTEMD
#include <systemd/sd-daemon.h>
#endif

#ifdef DUCKD_WITH_FLIGHT_SQL
#include "protocol/flight/flight_sql_server.hpp"
#endif

using namespace duckdb_server;

// Global state
static std::shared_ptr<TcpServer> g_server;
static std::shared_ptr<HttpServer> g_http_server;
#ifdef DUCKD_WITH_FLIGHT_SQL
static std::unique_ptr<DuckDBFlightSqlServer> g_flight_server;
#endif
static std::atomic<bool> g_shutdown_requested{false};
static std::string g_pid_file;
static ServerConfig g_config;

//===----------------------------------------------------------------------===//
// Version Info
//===----------------------------------------------------------------------===//
void PrintVersion() {
    std::cout << "DuckD Server " << DUCKD_VERSION << "\n"
              << "Git commit: " << DUCKD_GIT_COMMIT << "\n"
              << "Build type: " << DUCKD_BUILD_TYPE << "\n"
              << "Build time: " << DUCKD_BUILD_TIME << "\n";
}

//===----------------------------------------------------------------------===//
// Daemon Mode
//===----------------------------------------------------------------------===//
bool Daemonize() {
    // First fork
    pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "Failed to fork: " << strerror(errno) << std::endl;
        return false;
    }
    if (pid > 0) {
        _exit(0);
    }

    // Create new session
    if (setsid() < 0) {
        std::cerr << "Failed to create new session: " << strerror(errno) << std::endl;
        return false;
    }

    // Second fork
    pid = fork();
    if (pid < 0) {
        std::cerr << "Failed to fork (second): " << strerror(errno) << std::endl;
        return false;
    }
    if (pid > 0) {
        _exit(0);
    }

    umask(0);

    if (chdir("/") < 0) {
        // Non-fatal
    }

    // Redirect standard file descriptors
    int null_fd = open("/dev/null", O_RDWR);
    if (null_fd >= 0) {
        dup2(null_fd, STDIN_FILENO);
        dup2(null_fd, STDOUT_FILENO);
        dup2(null_fd, STDERR_FILENO);
        if (null_fd > STDERR_FILENO) {
            close(null_fd);
        }
    }

    return true;
}

//===----------------------------------------------------------------------===//
// PID File
//===----------------------------------------------------------------------===//
bool WritePidFile(const std::string& path) {
    std::ofstream file(path);
    if (!file) {
        std::cerr << "Failed to open PID file: " << path << std::endl;
        return false;
    }
    file << getpid();
    file.close();
    return true;
}

void RemovePidFile(const std::string& path) {
    if (!path.empty()) {
        std::remove(path.c_str());
    }
}

//===----------------------------------------------------------------------===//
// Privilege Dropping
//===----------------------------------------------------------------------===//
bool DropPrivileges(const std::string& username) {
    if (username.empty()) return true;
    if (getuid() != 0) {
        LOG_WARN("main", "Not running as root, cannot drop privileges");
        return true;
    }

    struct passwd* pw = getpwnam(username.c_str());
    if (!pw) {
        std::cerr << "User not found: " << username << std::endl;
        return false;
    }

    // Set supplementary groups
    if (initgroups(username.c_str(), pw->pw_gid) < 0) {
        std::cerr << "Failed to set supplementary groups: " << strerror(errno) << std::endl;
        return false;
    }

    // Set GID
    if (setgid(pw->pw_gid) < 0) {
        std::cerr << "Failed to set GID: " << strerror(errno) << std::endl;
        return false;
    }

    // Set UID
    if (setuid(pw->pw_uid) < 0) {
        std::cerr << "Failed to set UID: " << strerror(errno) << std::endl;
        return false;
    }

    LOG_INFO("main", "Dropped privileges to user: " + username);
    return true;
}

//===----------------------------------------------------------------------===//
// Resource Limits
//===----------------------------------------------------------------------===//
bool SetResourceLimits(const ServerConfig& config) {
    // Set max open files
    if (config.max_open_files > 0) {
        struct rlimit rlim;
        rlim.rlim_cur = config.max_open_files;
        rlim.rlim_max = config.max_open_files;
        if (setrlimit(RLIMIT_NOFILE, &rlim) < 0) {
            LOG_WARN("main", "Failed to set RLIMIT_NOFILE: " + std::string(strerror(errno)));
        } else {
            LOG_INFO("main", "Set max open files to " + std::to_string(config.max_open_files));
        }
    }

#ifdef __linux__
    // Set max memory (Linux only, macOS doesn't support RLIMIT_AS well)
    if (config.max_memory > 0) {
        struct rlimit rlim;
        rlim.rlim_cur = config.max_memory;
        rlim.rlim_max = config.max_memory;
        if (setrlimit(RLIMIT_AS, &rlim) < 0) {
            LOG_WARN("main", "Failed to set RLIMIT_AS: " + std::string(strerror(errno)));
        } else {
            LOG_INFO("main", "Set max memory to " + std::to_string(config.max_memory) + " bytes");
        }
    }
#endif

    return true;
}

//===----------------------------------------------------------------------===//
// Crash Handler
//===----------------------------------------------------------------------===//
void PrintStackTrace() {
    void* array[50];
    int size = backtrace(array, 50);
    char** symbols = backtrace_symbols(array, size);

    std::cerr << "\n=== Stack Trace ===\n";
    for (int i = 0; i < size; i++) {
        // Try to demangle C++ symbols
        std::string symbol(symbols[i]);
        size_t start = symbol.find('_');
        size_t end = symbol.find('+');

        if (start != std::string::npos && end != std::string::npos && end > start) {
            std::string mangled = symbol.substr(start, end - start);
            int status;
            char* demangled = abi::__cxa_demangle(mangled.c_str(), nullptr, nullptr, &status);
            if (status == 0 && demangled) {
                std::cerr << "  " << i << ": " << demangled << "\n";
                free(demangled);
                continue;
            }
        }
        std::cerr << "  " << i << ": " << symbols[i] << "\n";
    }
    std::cerr << "===================\n";

    free(symbols);
}

void CrashHandler(int signal) {
    const char* signal_name = signal == SIGSEGV ? "SIGSEGV" :
                              signal == SIGABRT ? "SIGABRT" :
                              signal == SIGFPE ? "SIGFPE" :
                              signal == SIGBUS ? "SIGBUS" : "UNKNOWN";

    std::cerr << "\n!!! CRASH: Received signal " << signal_name << " (" << signal << ") !!!\n";

    PrintStackTrace();

    // Remove PID file
    RemovePidFile(g_pid_file);

    // Re-raise signal with default handler
    std::signal(signal, SIG_DFL);
    raise(signal);
}


void ReloadConfig() {
    if (g_config.config_file.empty()) {
        LOG_WARN("main", "No config file specified, cannot reload");
        return;
    }

    LOG_INFO("main", "Reloading configuration from: " + g_config.config_file);

    ServerConfig new_config;
    std::string error;
    if (!new_config.LoadFromFile(g_config.config_file, error)) {
        LOG_ERROR("main", "Failed to reload config: " + error);
        return;
    }

    // Apply dynamic settings (log level can be changed at runtime)
    if (new_config.log_level != g_config.log_level) {
        duckd::Logger::SetLevel(new_config.log_level);
        LOG_INFO("main", "Log level changed to: " + new_config.log_level);
    }

    // Note: Most settings require restart to take effect
    LOG_INFO("main", "Configuration reloaded (some settings require restart)");
}

//===----------------------------------------------------------------------===//
// Systemd Integration
//===----------------------------------------------------------------------===//
void NotifySystemd(const char* state) {
#ifdef WITH_SYSTEMD
    sd_notify(0, state);
#else
    (void)state;
#endif
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//
int main(int argc, char* argv[]) {
    try {
        // Parse command line
        bool show_version;
        g_config = ParseCommandLine(argc, argv, show_version);

        if (show_version) {
            PrintVersion();
            return 0;
        }

        // Validate configuration
        std::string error;
        if (!g_config.Validate(error)) {
            std::cerr << "Configuration error: " << error << std::endl;
            return 1;
        }

        // Daemonize if requested
        if (g_config.daemon) {
            if (!Daemonize()) {
                return 1;
            }
        }

        // Initialize logger
        duckd::Logger::Initialize(g_config.log_file, g_config.log_level);

        // Write PID file
        if (!g_config.pid_file.empty()) {
            g_pid_file = g_config.pid_file;
            if (!WritePidFile(g_pid_file)) {
                return 1;
            }
        }

        // Set resource limits (before dropping privileges)
        SetResourceLimits(g_config);

        // Drop privileges
        if (!DropPrivileges(g_config.user)) {
            return 1;
        }

        // Block SIGINT/SIGTERM/SIGHUP so they are handled synchronously via
        // sigwait() in the main loop. All threads created after this inherit
        // the mask, which ensures signals are only delivered to the main thread.
        sigset_t shutdown_mask;
        sigemptyset(&shutdown_mask);
        sigaddset(&shutdown_mask, SIGINT);
        sigaddset(&shutdown_mask, SIGTERM);
        sigaddset(&shutdown_mask, SIGHUP);
        pthread_sigmask(SIG_BLOCK, &shutdown_mask, nullptr);

        std::signal(SIGPIPE, SIG_IGN);

        // Set up crash handlers
        std::signal(SIGSEGV, CrashHandler);
        std::signal(SIGABRT, CrashHandler);
        std::signal(SIGFPE, CrashHandler);
        std::signal(SIGBUS, CrashHandler);

        LOG_INFO("main", "Starting DuckD Server " + std::string(DUCKD_VERSION));
        LOG_INFO("main", "Configuration:");
        LOG_INFO("main", "  Host: " + g_config.host);
        LOG_INFO("main", "  Port: " + std::to_string(g_config.port));
        LOG_INFO("main", "  Database: " + g_config.database_path);
        LOG_INFO("main", "  IO Threads: " + std::to_string(g_config.GetIoThreadCount()));
        LOG_INFO("main", "  Executor Threads: " + std::to_string(g_config.GetExecutorThreadCount()));
        LOG_INFO("main", "  Max Connections: " + std::to_string(g_config.max_connections));
        if (g_config.http_port > 0) {
            LOG_INFO("main", "  HTTP Port: " + std::to_string(g_config.http_port));
        }
#ifdef DUCKD_WITH_FLIGHT_SQL
        if (g_config.flight_port > 0) {
            LOG_INFO("main", "  Flight SQL Port: " + std::to_string(g_config.flight_port));
        }
#endif

        // Create DuckDB instance
        LOG_INFO("main", "Opening database: " + g_config.database_path);
        auto db = std::make_shared<duckdb::DuckDB>(g_config.database_path);

        // Create session manager with connection pool
        SessionManager::Config sm_config;
        sm_config.max_sessions = g_config.max_connections;
        sm_config.session_timeout = std::chrono::minutes(g_config.session_timeout_minutes);
        sm_config.pool_min_connections = g_config.pool_min_connections;
        sm_config.pool_max_connections = g_config.pool_max_connections;
        sm_config.pool_idle_timeout = std::chrono::seconds(g_config.pool_idle_timeout_seconds);
        sm_config.pool_acquire_timeout = std::chrono::milliseconds(g_config.pool_acquire_timeout_ms);
        sm_config.pool_validate_on_acquire = g_config.pool_validate_on_acquire;

        auto session_manager = std::make_shared<SessionManager>(
            db, sm_config, std::chrono::milliseconds(g_config.query_timeout_ms));

        LOG_INFO("main", "  Connection Pool: min=" + std::to_string(g_config.pool_min_connections) +
                 ", max=" + std::to_string(g_config.pool_max_connections));

        // Create executor pool
        auto executor_pool = std::make_shared<ExecutorPool>(g_config.GetExecutorThreadCount());
        executor_pool->Start();

        // Create and start server
        g_server = std::make_shared<TcpServer>(g_config, session_manager, executor_pool);
        g_server->Start();

        // Start HTTP server for health/metrics
        if (g_config.http_port > 0) {
            g_http_server = std::make_shared<HttpServer>(g_config.http_port, g_server.get());

            // Add connection pool metrics
            g_http_server->SetMetricsCallback([session_manager]() -> std::string {
                auto stats = session_manager->GetPoolStats();
                std::ostringstream metrics;
                metrics << "\n"
                        << "# HELP duckd_pool_connections_total Total connections created by pool\n"
                        << "# TYPE duckd_pool_connections_total counter\n"
                        << "duckd_pool_connections_total " << stats.total_created << "\n"
                        << "\n"
                        << "# HELP duckd_pool_connections_destroyed Total connections destroyed by pool\n"
                        << "# TYPE duckd_pool_connections_destroyed counter\n"
                        << "duckd_pool_connections_destroyed " << stats.total_destroyed << "\n"
                        << "\n"
                        << "# HELP duckd_pool_connections_current Current pool size\n"
                        << "# TYPE duckd_pool_connections_current gauge\n"
                        << "duckd_pool_connections_current " << stats.current_size << "\n"
                        << "\n"
                        << "# HELP duckd_pool_connections_available Available connections in pool\n"
                        << "# TYPE duckd_pool_connections_available gauge\n"
                        << "duckd_pool_connections_available " << stats.available << "\n"
                        << "\n"
                        << "# HELP duckd_pool_connections_in_use Connections currently in use\n"
                        << "# TYPE duckd_pool_connections_in_use gauge\n"
                        << "duckd_pool_connections_in_use " << stats.in_use << "\n"
                        << "\n"
                        << "# HELP duckd_pool_acquire_total Total connection acquire requests\n"
                        << "# TYPE duckd_pool_acquire_total counter\n"
                        << "duckd_pool_acquire_total " << stats.acquire_count << "\n"
                        << "\n"
                        << "# HELP duckd_pool_acquire_timeout_total Acquire requests that timed out\n"
                        << "# TYPE duckd_pool_acquire_timeout_total counter\n"
                        << "duckd_pool_acquire_timeout_total " << stats.acquire_timeout_count << "\n"
                        << "\n"
                        << "# HELP duckd_pool_validation_failures_total Connection validation failures\n"
                        << "# TYPE duckd_pool_validation_failures_total counter\n"
                        << "duckd_pool_validation_failures_total " << stats.validation_failure_count << "\n"
                        << "\n"
                        << "# HELP duckd_sessions_active Current active sessions\n"
                        << "# TYPE duckd_sessions_active gauge\n"
                        << "duckd_sessions_active " << session_manager->GetActiveSessionCount() << "\n"
                        << "\n"
                        << "# HELP duckd_sessions_total Total sessions created\n"
                        << "# TYPE duckd_sessions_total counter\n"
                        << "duckd_sessions_total " << session_manager->GetTotalSessionsCreated() << "\n";
                return metrics.str();
            });

            g_http_server->Start();
        }

#ifdef DUCKD_WITH_FLIGHT_SQL
        // Start Arrow Flight SQL server
        if (g_config.flight_port > 0) {
            g_flight_server = std::make_unique<DuckDBFlightSqlServer>(
                session_manager.get(), executor_pool.get(),
                g_config.host, g_config.flight_port);

            auto flight_status = g_flight_server->Init();
            if (!flight_status.ok()) {
                LOG_ERROR("main", "Failed to init Flight SQL server: " + flight_status.ToString());
                return 1;
            }

            auto serve_status = g_flight_server->ServeAsync();
            if (!serve_status.ok()) {
                LOG_ERROR("main", "Failed to start Flight SQL server: " + serve_status.ToString());
                return 1;
            }

            LOG_INFO("main", "Flight SQL server started on " + g_config.host + ":" + std::to_string(g_config.flight_port));
        }
#endif

        LOG_INFO("main", "DuckD Server is ready to accept connections");

        // Notify systemd
        NotifySystemd("READY=1");

        // Main loop: wait for signals synchronously (zero CPU overhead)
        {
            int sig;
            while (sigwait(&shutdown_mask, &sig) == 0) {
                if (sig == SIGHUP) {
                    LOG_INFO("main", "Reload signal received");
                    ReloadConfig();
                } else {
                    // SIGINT or SIGTERM
                    LOG_INFO("main", "Shutdown signal received");
                    g_shutdown_requested = true;
#ifdef DUCKD_WITH_FLIGHT_SQL
                    if (g_flight_server) {
                        g_flight_server->Shutdown();
                    }
#endif
                    if (g_server) {
                        g_server->Stop();
                    }
                    break;
                }
            }
        }

        // Notify systemd we're stopping
        NotifySystemd("STOPPING=1");

        // Cleanup
        LOG_INFO("main", "Shutting down...");

#ifdef DUCKD_WITH_FLIGHT_SQL
        if (g_flight_server) {
            g_flight_server->Shutdown();
            g_flight_server.reset();
        }
#endif

        if (g_http_server) {
            g_http_server->Stop();
            g_http_server.reset();
        }

        if (g_server) {
            g_server->Stop();
            g_server.reset();
        }

        executor_pool->Stop();

        RemovePidFile(g_pid_file);

        LOG_INFO("main", "DuckD Server stopped");

        // Shutdown logger
        duckd::Logger::Shutdown();

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        RemovePidFile(g_pid_file);
        return 1;
    }
}
