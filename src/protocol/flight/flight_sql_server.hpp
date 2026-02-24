//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/flight/flight_sql_server.hpp
//
// Arrow Flight SQL server implementation
//===----------------------------------------------------------------------===//

#pragma once

#include "session/session_manager.hpp"
#include "executor/executor_pool.hpp"

#include <arrow/flight/sql/server.h>

#include <atomic>
#include <mutex>
#include <thread>

namespace duckdb_server {

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

class DuckDBFlightSqlServer : public flightsql::FlightSqlServerBase {
public:
    DuckDBFlightSqlServer(SessionManager* session_manager,
                          ExecutorPool* executor_pool,
                          const std::string& host,
                          uint16_t port);

    ~DuckDBFlightSqlServer() override;

    // Lifecycle
    arrow::Status Init();
    arrow::Status Serve();
    arrow::Status ServeAsync();
    void Shutdown();
    bool IsRunning() const { return running_; }

    //===------------------------------------------------------------------===//
    // Statement execution
    //===------------------------------------------------------------------===//

    arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoStatement(
        const flight::ServerCallContext& context,
        const flightsql::StatementQuery& command,
        const flight::FlightDescriptor& descriptor) override;

    arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetStatement(
        const flight::ServerCallContext& context,
        const flightsql::StatementQueryTicket& command) override;

    //===------------------------------------------------------------------===//
    // Prepared statements
    //===------------------------------------------------------------------===//

    arrow::Result<flightsql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
        const flight::ServerCallContext& context,
        const flightsql::ActionCreatePreparedStatementRequest& request) override;

    arrow::Status ClosePreparedStatement(
        const flight::ServerCallContext& context,
        const flightsql::ActionClosePreparedStatementRequest& request) override;

    arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoPreparedStatement(
        const flight::ServerCallContext& context,
        const flightsql::PreparedStatementQuery& command,
        const flight::FlightDescriptor& descriptor) override;

    arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetPreparedStatement(
        const flight::ServerCallContext& context,
        const flightsql::PreparedStatementQuery& command) override;

    arrow::Status DoPutPreparedStatementQuery(
        const flight::ServerCallContext& context,
        const flightsql::PreparedStatementQuery& command,
        flight::FlightMessageReader* reader,
        flight::FlightMetadataWriter* writer) override;

    //===------------------------------------------------------------------===//
    // Catalog / metadata
    //===------------------------------------------------------------------===//

    arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoCatalogs(
        const flight::ServerCallContext& context,
        const flight::FlightDescriptor& descriptor) override;

    arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetCatalogs(
        const flight::ServerCallContext& context) override;

    arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoSchemas(
        const flight::ServerCallContext& context,
        const flightsql::GetDbSchemas& command,
        const flight::FlightDescriptor& descriptor) override;

    arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetDbSchemas(
        const flight::ServerCallContext& context,
        const flightsql::GetDbSchemas& command) override;

    arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTables(
        const flight::ServerCallContext& context,
        const flightsql::GetTables& command,
        const flight::FlightDescriptor& descriptor) override;

    arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetTables(
        const flight::ServerCallContext& context,
        const flightsql::GetTables& command) override;

    arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTableTypes(
        const flight::ServerCallContext& context,
        const flight::FlightDescriptor& descriptor) override;

    arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetTableTypes(
        const flight::ServerCallContext& context) override;

    arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoSqlInfo(
        const flight::ServerCallContext& context,
        const flightsql::GetSqlInfo& command,
        const flight::FlightDescriptor& descriptor) override;

    arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetSqlInfo(
        const flight::ServerCallContext& context,
        const flightsql::GetSqlInfo& command) override;

private:
    // Execute a query and return results as a RecordBatchReader
    arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ExecuteQuery(
        const std::string& query);

    // Execute a parameterized query and return results as a RecordBatchReader
    arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> ExecuteParameterizedQuery(
        const std::string& query,
        duckdb::vector<duckdb::Value>& params);

    // Build FlightInfo for a schema
    arrow::Result<std::unique_ptr<flight::FlightInfo>> MakeFlightInfo(
        const std::shared_ptr<arrow::Schema>& schema,
        const flight::FlightDescriptor& descriptor,
        const std::string& handle);

    // Prepared statement storage
    // NOTE: connection must be declared BEFORE statement so it is destroyed AFTER
    struct PreparedStatementEntry {
        std::unique_ptr<duckdb::Connection> connection;
        std::unique_ptr<duckdb::PreparedStatement> statement;
        std::string query;
        std::shared_ptr<arrow::Schema> result_schema;
        std::shared_ptr<arrow::Schema> parameter_schema;
        duckdb::vector<duckdb::Value> bound_parameters;
    };

    // Transient query storage (for GetFlightInfo → DoGet flow)
    struct TransientQuery {
        std::string query;                  // SQL query, or prepared statement handle if is_prepared
        std::shared_ptr<arrow::Schema> schema;
        bool is_prepared = false;           // true if query holds a prepared statement handle
    };

    std::string GenerateHandle();

    SessionManager* session_manager_;
    ExecutorPool* executor_pool_;
    std::string host_;
    uint16_t port_;
    std::atomic<bool> running_{false};
    std::thread serve_thread_;

    // Prepared statements by handle
    std::mutex prepared_mutex_;
    std::unordered_map<std::string, PreparedStatementEntry> prepared_statements_;

    // Transient queries by handle (GetFlightInfoStatement → DoGetStatement)
    std::mutex query_mutex_;
    std::unordered_map<std::string, TransientQuery> transient_queries_;

    // Handle counter
    std::atomic<uint64_t> handle_counter_{0};
};

} // namespace duckdb_server
