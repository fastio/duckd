//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// common.hpp
//
// Common definitions and includes for DuckDB Server
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <thread>
#include <unordered_map>

// DuckDB includes
#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"

namespace duckdb_server {

// Type aliases
using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

// Forward declarations
class TcpServer;
class Session;
class SessionManager;
class ExecutorPool;
struct ServerConfig;
class Logger;

// Shared pointer types
using SessionPtr = std::shared_ptr<Session>;

// Constants
constexpr size_t DEFAULT_MAX_CONNECTIONS = 10000;
constexpr size_t DEFAULT_IO_THREADS = 0;  // 0 = auto (CPU cores)
constexpr size_t DEFAULT_EXECUTOR_THREADS = 0;  // 0 = auto (CPU cores * 2)
constexpr size_t DEFAULT_READ_BUFFER_SIZE = 65536;  // 64KB
constexpr size_t DEFAULT_WRITE_BUFFER_SIZE = 65536;  // 64KB
constexpr uint32_t DEFAULT_SESSION_TIMEOUT_MINUTES = 30;
constexpr uint32_t DEFAULT_QUERY_TIMEOUT_MS = 300000;  // 5 minutes

} // namespace duckdb_server
