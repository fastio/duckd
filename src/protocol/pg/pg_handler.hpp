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
    CONTINUE,       // Continue reading from socket
    ASYNC_PENDING,  // Async operation in flight, stop reading until resume
    CLOSE           // Close the connection
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
};

//===----------------------------------------------------------------------===//
// PostgreSQL Connection State
//===----------------------------------------------------------------------===//
enum class PgConnectionState {
    INITIAL,        // Waiting for startup message
    AUTHENTICATING, // Sent auth request, waiting for password
    READY,          // Ready for queries
    IN_QUERY,       // Processing simple query
    IN_EXTENDED,    // Processing extended query protocol
    FAILED,         // Connection failed
    CLOSED          // Connection closed
};

//===----------------------------------------------------------------------===//
// PostgreSQL Protocol Handler
//===----------------------------------------------------------------------===//
class PgHandler {
public:
    using SendCallback = std::function<void(std::vector<uint8_t>)>;
    using StateUpdate = std::function<void()>;
    using ResumeCallback = std::function<void(StateUpdate)>;
    using CancelCallback = std::function<bool(int32_t, int32_t)>;

    PgHandler(std::shared_ptr<Session> session_p, SendCallback send_callback_p,
              std::shared_ptr<ExecutorPool> executor_pool_p, ResumeCallback resume_callback_p,
              CancelCallback cancel_callback_p = nullptr);
    ~PgHandler();

    // Process incoming data
    ProcessResult ProcessData(const uint8_t* data, size_t len);

    // Process remaining buffered messages after async completion
    ProcessResult ProcessPendingBuffer();

    // Get current state
    PgConnectionState GetState() const { return state; }

    // Check if in transaction
    bool InTransaction() const { return in_transaction; }

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
                                 uint64_t& rows_affected, const SendCallback& send_cb);
    static std::string GetCommandTag(const std::string& sql, uint64_t rows_affected);

    // Response helpers
    void SendStartupResponse();
    void SendError(const std::string& severity, const std::string& code,
                   const std::string& message, const std::string& detail = "");
    void SendReadyForQuery();

    // Utility
    char GetTransactionStatus() const;

    // Revalidate prepared statements after connection switch (Step 2)
    void RevalidatePreparedStatements(duckdb::Connection& conn);

private:
    std::shared_ptr<Session> session;
    SendCallback send_callback;
    std::shared_ptr<ExecutorPool> executor_pool;
    ResumeCallback resume_callback;
    CancelCallback cancel_callback;
    PgMessageWriter writer;  // Only used for synchronous operations on IO thread

    PgConnectionState state = PgConnectionState::INITIAL;
    bool in_transaction = false;
    bool transaction_failed = false;
    bool async_pending = false;

    // Connection info
    std::string username;
    std::string database;
    int32_t process_id;
    int32_t secret_key;

    // Prepared statements and portals
    // Using phmap::flat_hash_map for better performance than std::unordered_map
    phmap::flat_hash_map<std::string, PreparedStatementInfo> prepared_statements;
    phmap::flat_hash_map<std::string, PortalInfo> portals;

    // Track last connection pointer for revalidation (Step 2)
    duckdb::Connection* last_connection = nullptr;

    // Buffer for incomplete messages
    std::vector<uint8_t> buffer;
    size_t buffer_offset = 0;
};

} // namespace pg
} // namespace duckdb_server
