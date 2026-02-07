//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/pg/pg_handler.hpp
//
// PostgreSQL protocol handler
//===----------------------------------------------------------------------===//

#pragma once

#include "protocol/pg/pg_protocol.hpp"
#include "protocol/pg/pg_types.hpp"
#include "protocol/pg/pg_message_reader.hpp"
#include "protocol/pg/pg_message_writer.hpp"
#include "duckdb.hpp"
#include <parallel_hashmap/phmap.h>
#include <memory>
#include <string>
#include <functional>

namespace duckdb_server {

class Session;
class ExecutorPool;

namespace pg {

//===----------------------------------------------------------------------===//
// Process Result - controls message loop and read scheduling
//===----------------------------------------------------------------------===//
enum class ProcessResult {
    Continue,      // Continue reading from socket
    AsyncPending,  // Async operation in flight, stop reading until resume
    Close          // Close the connection
};

//===----------------------------------------------------------------------===//
// Prepared Statement Info
//===----------------------------------------------------------------------===//
struct PreparedStatementInfo {
    std::string query;
    duckdb::shared_ptr<duckdb::PreparedStatement> statement;
    std::vector<duckdb::LogicalType> param_types;
    std::vector<std::string> column_names;
    std::vector<duckdb::LogicalType> column_types;
};

//===----------------------------------------------------------------------===//
// Portal Info (bound prepared statement)
//===----------------------------------------------------------------------===//
struct PortalInfo {
    std::string statement_name;
    std::vector<duckdb::Value> parameters;
    std::vector<int16_t> result_formats;
    std::unique_ptr<duckdb::QueryResult> result;
    bool executed = false;
};

//===----------------------------------------------------------------------===//
// PostgreSQL Connection State
//===----------------------------------------------------------------------===//
enum class PgConnectionState {
    Initial,        // Waiting for startup message
    Authenticating, // Sent auth request, waiting for password
    Ready,          // Ready for queries
    InQuery,        // Processing simple query
    InExtended,     // Processing extended query protocol
    Failed,         // Connection failed
    Closed          // Connection closed
};

//===----------------------------------------------------------------------===//
// PostgreSQL Protocol Handler
//===----------------------------------------------------------------------===//
class PgHandler {
public:
    using SendCallback = std::function<void(const std::vector<uint8_t>&)>;
    using ResumeCallback = std::function<void()>;

    PgHandler(std::shared_ptr<Session> session, SendCallback send_callback,
              std::shared_ptr<ExecutorPool> executor_pool, ResumeCallback resume_callback);
    ~PgHandler();

    // Process incoming data
    ProcessResult ProcessData(const uint8_t* data, size_t len);

    // Process remaining buffered messages after async completion
    ProcessResult ProcessPendingBuffer();

    // Get current state
    PgConnectionState GetState() const { return state_; }

    // Check if in transaction
    bool InTransaction() const { return in_transaction_; }

    // Check if connection can be released (Step 2: lazy connection)
    bool CanReleaseConnection() const;

private:
    // Internal buffer processing loop (shared by ProcessData and ProcessPendingBuffer)
    ProcessResult ProcessBufferLoop();

    // State handlers
    bool HandleStartup(const uint8_t* data, size_t len);
    bool HandleAuthentication(const uint8_t* data, size_t len);
    bool HandleMessage(char type, const uint8_t* data, size_t len);

    // Message handlers
    void HandleQuery(const uint8_t* data, size_t len);
    void HandleParse(const uint8_t* data, size_t len);
    void HandleBind(const uint8_t* data, size_t len);
    void HandleDescribe(const uint8_t* data, size_t len);
    void HandleExecute(const uint8_t* data, size_t len);
    void HandleClose(const uint8_t* data, size_t len);
    void HandleSync(const uint8_t* data, size_t len);
    void HandleFlush(const uint8_t* data, size_t len);
    void HandleTerminate(const uint8_t* data, size_t len);

    // Query execution helpers (used in async lambdas with local writer)
    static void WriteQueryResult(PgMessageWriter& writer, duckdb::QueryResult& result,
                                 const std::string& command, const std::string& sql,
                                 uint64_t& rows_affected);
    static std::string GetCommandTag(const std::string& sql, uint64_t rows_affected);

    // Response helpers
    void SendStartupResponse();
    void SendError(const std::string& severity, const std::string& code,
                   const std::string& message, const std::string& detail = "");
    void SendReadyForQuery();

    // Transaction state helpers
    void UpdateTransactionState(const std::string& sql);

    // Utility
    char GetTransactionStatus() const;

    // Revalidate prepared statements after connection switch (Step 2)
    void RevalidatePreparedStatements(duckdb::Connection& conn);

private:
    std::shared_ptr<Session> session_;
    SendCallback send_callback_;
    std::shared_ptr<ExecutorPool> executor_pool_;
    ResumeCallback resume_callback_;
    PgMessageWriter writer_;  // Only used for synchronous operations on IO thread

    PgConnectionState state_ = PgConnectionState::Initial;
    bool in_transaction_ = false;
    bool transaction_failed_ = false;
    bool async_pending_ = false;

    // Connection info
    std::string username_;
    std::string database_;
    int32_t process_id_;
    int32_t secret_key_;

    // Prepared statements and portals
    // Using phmap::flat_hash_map for better performance than std::unordered_map
    phmap::flat_hash_map<std::string, PreparedStatementInfo> prepared_statements_;
    phmap::flat_hash_map<std::string, PortalInfo> portals_;

    // Track last connection pointer for revalidation (Step 2)
    duckdb::Connection* last_connection_ = nullptr;

    // Buffer for incomplete messages
    std::vector<uint8_t> buffer_;
};

} // namespace pg
} // namespace duckdb_server
