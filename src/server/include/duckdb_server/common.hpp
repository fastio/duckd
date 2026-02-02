//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// duckdb_server/common.hpp
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
class TcpConnection;
class Session;
class SessionManager;
class QueryExecutor;
class ExecutorPool;
class ProtocolHandler;
class ArrowSerializer;
class ServerConfig;
class Logger;

// Shared pointer types
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
using SessionPtr = std::shared_ptr<Session>;

// Callback types
using SendCallback = std::function<void(const std::vector<uint8_t>&)>;
using ErrorCallback = std::function<void(const std::string&)>;

// Constants
constexpr uint32_t PROTOCOL_MAGIC = 0x4B435544;  // "DUCK" in little-endian
constexpr uint8_t PROTOCOL_VERSION = 0x01;
constexpr uint16_t DEFAULT_PORT = 9999;
constexpr size_t DEFAULT_MAX_CONNECTIONS = 10000;
constexpr size_t DEFAULT_IO_THREADS = 0;  // 0 = auto (CPU cores)
constexpr size_t DEFAULT_EXECUTOR_THREADS = 0;  // 0 = auto (CPU cores * 2)
constexpr size_t DEFAULT_READ_BUFFER_SIZE = 65536;  // 64KB
constexpr size_t DEFAULT_WRITE_BUFFER_SIZE = 65536;  // 64KB
constexpr size_t DEFAULT_ARROW_BATCH_SIZE = 1048576;  // 1MB
constexpr uint32_t DEFAULT_SESSION_TIMEOUT_MINUTES = 30;
constexpr uint32_t DEFAULT_QUERY_TIMEOUT_MS = 300000;  // 5 minutes

} // namespace duckdb_server
