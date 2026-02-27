//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/flight/flight_sql_server.cpp
//
// Arrow Flight SQL server implementation
//===----------------------------------------------------------------------===//

#include "protocol/flight/flight_sql_server.hpp"
#include "protocol/flight/flight_arrow_bridge.hpp"
#include "logging/logger.hpp"

#include <arrow/flight/sql/server.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/ipc/writer.h>
#include <arrow/io/memory.h>
#include <arrow/buffer.h>

namespace duckdb_server {

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

//===----------------------------------------------------------------------===//
// Constructor / Destructor
//===----------------------------------------------------------------------===//

DuckDBFlightSqlServer::DuckDBFlightSqlServer(
    SessionManager* session_manager,
    ExecutorPool* executor_pool,
    const std::string& host,
    uint16_t port)
    : session_manager_(session_manager)
    , host_(host)
    , port_(port) {
    (void)executor_pool;
}

DuckDBFlightSqlServer::~DuckDBFlightSqlServer() {
    Shutdown();
}

//===----------------------------------------------------------------------===//
// Lifecycle
//===----------------------------------------------------------------------===//

arrow::Status DuckDBFlightSqlServer::Init() {
    auto location_result = flight::Location::ForGrpcTcp(host_, port_);
    if (!location_result.ok()) {
        return location_result.status();
    }

    flight::FlightServerOptions options(location_result.MoveValueUnsafe());
    return FlightSqlServerBase::Init(std::move(options));
}

arrow::Status DuckDBFlightSqlServer::Serve() {
    running_ = true;
    LOG_INFO("flight", "Flight SQL server listening on " + host_ + ":" + std::to_string(port_));
    auto status = FlightServerBase::Serve();
    running_ = false;
    return status;
}

arrow::Status DuckDBFlightSqlServer::ServeAsync() {
    serve_thread_ = std::thread([this]() {
        auto status = Serve();
        if (!status.ok()) {
            LOG_ERROR("flight", "Flight SQL server error: " + status.ToString());
        }
    });
    return arrow::Status::OK();
}

void DuckDBFlightSqlServer::Shutdown() {
    if (running_) {
        LOG_INFO("flight", "Shutting down Flight SQL server");
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
        (void)FlightServerBase::Shutdown(&deadline);
        running_ = false;
    }
    if (serve_thread_.joinable()) {
        serve_thread_.join();
    }
}

//===----------------------------------------------------------------------===//
// Helpers
//===----------------------------------------------------------------------===//

std::string DuckDBFlightSqlServer::GenerateHandle() {
    return "duckd-stmt-" + std::to_string(handle_counter_++);
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>>
DuckDBFlightSqlServer::ExecuteQuery(const std::string& query) {
    auto conn = std::make_unique<duckdb::Connection>(session_manager_->GetDatabase());

    auto result = conn->Query(query);
    if (result->HasError()) {
        return arrow::Status::ExecutionError(result->GetError());
    }

    auto& types = result->types;
    auto& names = result->names;

    auto props = GetClientProps(*conn);
    ARROW_ASSIGN_OR_RAISE(auto schema, MakeArrowSchema(types, names, props));

    // Pass conn to reader to keep connection alive for streaming
    auto reader = std::make_shared<DuckDBRecordBatchReader>(
        std::move(result), schema, std::move(conn));
    return reader;
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>>
DuckDBFlightSqlServer::ExecuteParameterizedQuery(
    const std::string& query,
    duckdb::vector<duckdb::Value>& params) {
    auto conn = std::make_unique<duckdb::Connection>(session_manager_->GetDatabase());

    auto prepared = conn->Prepare(query);
    if (prepared->HasError()) {
        return arrow::Status::ExecutionError(prepared->GetError());
    }

    auto result = prepared->Execute(params);
    if (result->HasError()) {
        return arrow::Status::ExecutionError(result->GetError());
    }

    auto& types = result->types;
    auto& names = result->names;

    auto props = GetClientProps(*conn);
    ARROW_ASSIGN_OR_RAISE(auto schema, MakeArrowSchema(types, names, props));

    auto reader = std::make_shared<DuckDBRecordBatchReader>(
        std::move(result), schema, std::move(conn));
    return reader;
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::MakeFlightInfo(
    const std::shared_ptr<arrow::Schema>& schema,
    const flight::FlightDescriptor& descriptor,
    const std::string& handle) {

    // Pack the handle into a proper Flight SQL protobuf ticket
    ARROW_ASSIGN_OR_RAISE(auto ticket_string,
        flightsql::CreateStatementQueryTicket(handle));

    flight::FlightEndpoint endpoint;
    endpoint.ticket.ticket = std::move(ticket_string);

    std::vector<flight::FlightEndpoint> endpoints = {endpoint};
    ARROW_ASSIGN_OR_RAISE(auto info,
        flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1));
    return std::make_unique<flight::FlightInfo>(std::move(info));
}

// For catalog/metadata methods, the ticket should be the descriptor's command
// bytes so the base class DoGet can route to the correct DoGet* method.
static arrow::Result<std::unique_ptr<flight::FlightInfo>> MakeCatalogFlightInfo(
    const std::shared_ptr<arrow::Schema>& schema,
    const flight::FlightDescriptor& descriptor) {

    flight::FlightEndpoint endpoint;
    endpoint.ticket.ticket = std::string(descriptor.cmd);

    std::vector<flight::FlightEndpoint> endpoints = {endpoint};
    ARROW_ASSIGN_OR_RAISE(auto info,
        flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1));
    return std::make_unique<flight::FlightInfo>(std::move(info));
}

//===----------------------------------------------------------------------===//
// Transactions
//===----------------------------------------------------------------------===//

arrow::Result<flightsql::ActionBeginTransactionResult>
DuckDBFlightSqlServer::BeginTransaction(
    const flight::ServerCallContext& /*context*/,
    const flightsql::ActionBeginTransactionRequest& /*request*/) {

    auto handle = "duckd-txn-" + std::to_string(handle_counter_++);

    // Open a dedicated connection and begin a DuckDB transaction
    auto conn = std::make_shared<duckdb::Connection>(session_manager_->GetDatabase());
    auto begin_result = conn->Query("BEGIN TRANSACTION");
    if (begin_result->HasError()) {
        return arrow::Status::ExecutionError(
            "BEGIN TRANSACTION failed: " + begin_result->GetError());
    }

    {
        std::lock_guard<std::mutex> lock(txn_mutex_);
        open_transactions_[handle] = TransactionEntry{std::move(conn)};
    }

    flightsql::ActionBeginTransactionResult result;
    result.transaction_id = handle;
    return result;
}

arrow::Status DuckDBFlightSqlServer::EndTransaction(
    const flight::ServerCallContext& /*context*/,
    const flightsql::ActionEndTransactionRequest& request) {

    std::shared_ptr<duckdb::Connection> conn;
    {
        std::lock_guard<std::mutex> lock(txn_mutex_);
        auto it = open_transactions_.find(request.transaction_id);
        if (it == open_transactions_.end()) {
            return arrow::Status::KeyError(
                "Unknown transaction ID: " + request.transaction_id);
        }
        conn = it->second.connection;
        open_transactions_.erase(it);
    }

    std::string sql = (request.action == flightsql::ActionEndTransactionRequest::kCommit)
                      ? "COMMIT"
                      : "ROLLBACK";
    auto result = conn->Query(sql);
    if (result->HasError()) {
        return arrow::Status::ExecutionError(sql + " failed: " + result->GetError());
    }
    return arrow::Status::OK();
}

// Helper: retrieve the connection for a transaction ID, or nullptr for auto-commit
std::shared_ptr<duckdb::Connection>
DuckDBFlightSqlServer::GetTransactionConnection(const std::string& transaction_id) {
    if (transaction_id.empty()) {
        return nullptr;
    }
    std::lock_guard<std::mutex> lock(txn_mutex_);
    auto it = open_transactions_.find(transaction_id);
    if (it != open_transactions_.end()) {
        return it->second.connection;
    }
    return nullptr;
}

//===----------------------------------------------------------------------===//
// Statement execution
//===----------------------------------------------------------------------===//

arrow::Result<int64_t> DuckDBFlightSqlServer::DoPutCommandStatementUpdate(
    const flight::ServerCallContext& /*context*/,
    const flightsql::StatementUpdate& command) {

    // Use transaction connection if one is active; otherwise create a fresh one
    std::shared_ptr<duckdb::Connection> txn_conn =
        GetTransactionConnection(command.transaction_id);

    duckdb::Connection *conn_ptr = nullptr;
    std::unique_ptr<duckdb::Connection> fresh_conn;
    if (txn_conn) {
        conn_ptr = txn_conn.get();
    } else {
        fresh_conn = std::make_unique<duckdb::Connection>(session_manager_->GetDatabase());
        conn_ptr   = fresh_conn.get();
    }

    auto *conn   = conn_ptr;
    auto result = conn->Query(command.query);

    if (result->HasError()) {
        return arrow::Status::ExecutionError(result->GetError());
    }

    // Try to read an affected-row count if the result contains one
    if (result->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        auto &mat = static_cast<duckdb::MaterializedQueryResult &>(*result);
        if (mat.RowCount() > 0 && mat.ColumnCount() > 0) {
            try {
                return mat.GetValue(0, 0).GetValue<int64_t>();
            } catch (...) {}
        }
    }
    return 0;
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoStatement(
    const flight::ServerCallContext& context,
    const flightsql::StatementQuery& command,
    const flight::FlightDescriptor& descriptor) {

    const auto& query = command.query;

    // Use transaction connection if active; otherwise a fresh connection
    std::shared_ptr<duckdb::Connection> txn_conn =
        GetTransactionConnection(command.transaction_id);
    std::unique_ptr<duckdb::Connection> fresh_conn;
    duckdb::Connection *conn_ptr = nullptr;
    if (txn_conn) {
        conn_ptr = txn_conn.get();
    } else {
        fresh_conn = std::make_unique<duckdb::Connection>(session_manager_->GetDatabase());
        conn_ptr   = fresh_conn.get();
    }

    // Prepare the query to get schema without executing
    auto prepared = conn_ptr->Prepare(query);
    if (prepared->HasError()) {
        return arrow::Status::ExecutionError(prepared->GetError());
    }

    auto& types = prepared->GetTypes();
    auto& names = prepared->GetNames();
    auto props = GetClientProps(*conn_ptr);
    ARROW_ASSIGN_OR_RAISE(auto schema, MakeArrowSchema(types, names, props));

    auto handle = GenerateHandle();

    // Store the query text + transaction ID so DoGetStatement can execute it
    {
        std::lock_guard<std::mutex> lock(query_mutex_);
        TransientQuery tq;
        tq.query          = query;
        tq.schema         = schema;
        tq.transaction_id = command.transaction_id;
        transient_queries_[handle] = std::move(tq);
    }

    return MakeFlightInfo(schema, descriptor, handle);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetStatement(
    const flight::ServerCallContext& context,
    const flightsql::StatementQueryTicket& command) {

    const auto& handle = command.statement_handle;

    // Look up the transient query stored by GetFlightInfoStatement or
    // GetFlightInfoPreparedStatement
    std::string query;
    std::shared_ptr<arrow::Schema> schema;
    bool is_prepared = false;
    std::string transaction_id;
    {
        std::lock_guard<std::mutex> lock(query_mutex_);
        auto it = transient_queries_.find(handle);
        if (it == transient_queries_.end()) {
            return arrow::Status::KeyError("Statement handle not found: ", handle);
        }
        query          = it->second.query;
        schema         = it->second.schema;
        is_prepared    = it->second.is_prepared;
        transaction_id = it->second.transaction_id;
        transient_queries_.erase(it);
    }

    // If this is a prepared statement execution, delegate to DoGetPreparedStatement logic
    if (is_prepared) {
        flightsql::PreparedStatementQuery ps_command;
        ps_command.prepared_statement_handle = query;  // query holds the prepared stmt handle
        return DoGetPreparedStatement(context, ps_command);
    }

    // Use transaction connection if active; otherwise create a fresh connection
    std::shared_ptr<duckdb::Connection> txn_conn =
        GetTransactionConnection(transaction_id);

    if (txn_conn) {
        // Execute on the transaction connection (shared ownership keeps it alive)
        auto result = txn_conn->Query(query);
        if (result->HasError()) {
            return arrow::Status::ExecutionError(result->GetError());
        }
        auto reader = std::make_shared<DuckDBRecordBatchReader>(
            std::move(result), schema, txn_conn);
        return std::make_unique<flight::RecordBatchStream>(reader);
    }

    // Execute the query using a fresh connection
    auto conn = std::make_unique<duckdb::Connection>(session_manager_->GetDatabase());
    auto result = conn->Query(query);

    if (result->HasError()) {
        return arrow::Status::ExecutionError(result->GetError());
    }

    auto reader = std::make_shared<DuckDBRecordBatchReader>(
        std::move(result), schema, std::move(conn));
    return std::make_unique<flight::RecordBatchStream>(reader);
}

//===----------------------------------------------------------------------===//
// Prepared statements
//===----------------------------------------------------------------------===//

arrow::Result<flightsql::ActionCreatePreparedStatementResult>
DuckDBFlightSqlServer::CreatePreparedStatement(
    const flight::ServerCallContext& context,
    const flightsql::ActionCreatePreparedStatementRequest& request) {

    const auto& query = request.query;

    auto conn = std::make_unique<duckdb::Connection>(session_manager_->GetDatabase());

    auto prepared = conn->Prepare(query);
    if (prepared->HasError()) {
        return arrow::Status::ExecutionError(prepared->GetError());
    }

    auto& types = prepared->GetTypes();
    auto& names = prepared->GetNames();
    auto props = GetClientProps(*conn);
    ARROW_ASSIGN_OR_RAISE(auto result_schema, MakeArrowSchema(types, names, props));

    // Build parameter schema from expected parameter types
    auto expected_params = prepared->GetExpectedParameterTypes();
    duckdb::vector<duckdb::string> param_names;
    duckdb::vector<duckdb::LogicalType> p_types;
    for (auto& kv : expected_params) {
        param_names.push_back(kv.first);
        p_types.push_back(kv.second);
    }

    std::shared_ptr<arrow::Schema> parameter_schema;
    if (!p_types.empty()) {
        ARROW_ASSIGN_OR_RAISE(parameter_schema, MakeArrowSchema(p_types, param_names, props));
    } else {
        parameter_schema = arrow::schema({});
    }

    auto handle = GenerateHandle();

    {
        std::lock_guard<std::mutex> lock(prepared_mutex_);
        PreparedStatementEntry entry;
        entry.connection = std::move(conn);  // keeps connection alive for statement lifetime
        entry.statement = std::move(prepared);
        entry.query = query;
        entry.result_schema = result_schema;
        entry.parameter_schema = parameter_schema;
        prepared_statements_[handle] = std::move(entry);
    }

    flightsql::ActionCreatePreparedStatementResult result;
    result.prepared_statement_handle = handle;
    result.dataset_schema = result_schema;
    result.parameter_schema = parameter_schema;

    return result;
}

arrow::Status DuckDBFlightSqlServer::ClosePreparedStatement(
    const flight::ServerCallContext& context,
    const flightsql::ActionClosePreparedStatementRequest& request) {

    const auto& handle = request.prepared_statement_handle;

    std::lock_guard<std::mutex> lock(prepared_mutex_);
    auto it = prepared_statements_.find(handle);
    if (it != prepared_statements_.end()) {
        prepared_statements_.erase(it);
    }

    return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoPreparedStatement(
    const flight::ServerCallContext& context,
    const flightsql::PreparedStatementQuery& command,
    const flight::FlightDescriptor& descriptor) {

    const auto& handle = command.prepared_statement_handle;

    std::shared_ptr<arrow::Schema> schema;
    {
        std::lock_guard<std::mutex> lock(prepared_mutex_);
        auto it = prepared_statements_.find(handle);
        if (it == prepared_statements_.end()) {
            return arrow::Status::KeyError("Prepared statement not found: ", handle);
        }
        schema = it->second.result_schema;
    }

    // Generate a unique handle for this execution and register it as a
    // transient query so DoGetStatement can find it via the prepared_statement_handle
    auto exec_handle = GenerateHandle();
    {
        std::lock_guard<std::mutex> lock(query_mutex_);
        TransientQuery tq;
        tq.query = handle;  // Store the prepared statement handle as the "query"
        tq.schema = schema;
        tq.is_prepared = true;
        transient_queries_[exec_handle] = std::move(tq);
    }

    return MakeFlightInfo(schema, descriptor, exec_handle);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetPreparedStatement(
    const flight::ServerCallContext& context,
    const flightsql::PreparedStatementQuery& command) {

    const auto& handle = command.prepared_statement_handle;

    std::shared_ptr<arrow::Schema> schema;
    duckdb::PreparedStatement* stmt = nullptr;
    duckdb::vector<duckdb::Value> bound_params;
    duckdb::ClientProperties props;

    {
        std::lock_guard<std::mutex> lock(prepared_mutex_);
        auto it = prepared_statements_.find(handle);
        if (it == prepared_statements_.end()) {
            return arrow::Status::KeyError("Prepared statement not found: ", handle);
        }
        schema = it->second.result_schema;
        stmt = it->second.statement.get();
        // For SELECT (DoGet), use the first parameter row only.
        if (!it->second.bound_parameter_rows.empty()) {
            bound_params = std::move(it->second.bound_parameter_rows[0]);
        }
        it->second.bound_parameter_rows.clear();
        props = GetClientProps(*it->second.connection);
    }

    if (!stmt) {
        return arrow::Status::Invalid("Prepared statement has no associated statement");
    }

    std::unique_ptr<duckdb::QueryResult> result;
    if (!bound_params.empty()) {
        result = stmt->Execute(bound_params);
    } else {
        duckdb::vector<duckdb::Value> empty;
        result = stmt->Execute(empty);
    }

    if (result->HasError()) {
        return arrow::Status::ExecutionError(result->GetError());
    }

    auto reader = std::make_shared<DuckDBRecordBatchReader>(
        std::move(result), schema, std::move(props));
    return std::make_unique<flight::RecordBatchStream>(reader);
}

arrow::Status DuckDBFlightSqlServer::DoPutPreparedStatementQuery(
    const flight::ServerCallContext& context,
    const flightsql::PreparedStatementQuery& command,
    flight::FlightMessageReader* reader,
    flight::FlightMetadataWriter* writer) {

    // Parameter binding: read parameter batches from client
    // For now, we support simple scalar parameters
    const auto& handle = command.prepared_statement_handle;

    // Step 1: Quick check that the handle exists (under lock)
    {
        std::lock_guard<std::mutex> lock(prepared_mutex_);
        if (prepared_statements_.find(handle) == prepared_statements_.end()) {
            return arrow::Status::KeyError("Prepared statement not found: ", handle);
        }
    }

    // Step 2: Read parameter data from client WITHOUT holding the lock.
    // reader->ToTable() performs network IO and can block if the client is slow;
    // holding prepared_mutex_ during this call would block all other prepared
    // statement operations (Create/Close/Execute/GetFlightInfo).
    auto table_result = reader->ToTable();
    if (!table_result.ok()) {
        return table_result.status();
    }

    auto table = table_result.MoveValueUnsafe();
    if (table->num_rows() == 0) {
        return arrow::Status::OK();
    }

    // Convert all parameter rows from Arrow to DuckDB Values (still lock-free).
    // Each row in the table is a separate set of bound parameters.
    int64_t num_rows = table->num_rows();
    int num_cols = table->num_columns();
    duckdb::vector<duckdb::vector<duckdb::Value>> param_rows;
    param_rows.reserve(num_rows);

    for (int64_t row = 0; row < num_rows; row++) {
        duckdb::vector<duckdb::Value> params;
        params.reserve(num_cols);
        for (int col = 0; col < num_cols; col++) {
            // Locate the chunk and local index for this row within the ChunkedArray
            auto chunked = table->column(col);
            int64_t offset = row;
            std::shared_ptr<arrow::Array> chunk;
            for (int c = 0; c < chunked->num_chunks(); c++) {
                chunk = chunked->chunk(c);
                if (offset < chunk->length()) break;
                offset -= chunk->length();
            }
            if (chunk->IsNull(offset)) {
                params.push_back(duckdb::Value());
            } else {
                params.push_back(ArrowScalarToDuckDBValue(chunk, offset));
            }
        }
        param_rows.push_back(std::move(params));
    }

    // Step 3: Re-acquire lock and write bound parameters.
    // Double-check: the statement may have been closed while we were reading.
    {
        std::lock_guard<std::mutex> lock(prepared_mutex_);
        auto it = prepared_statements_.find(handle);
        if (it == prepared_statements_.end()) {
            return arrow::Status::KeyError("Prepared statement closed during parameter binding: ", handle);
        }
        it->second.bound_parameter_rows = std::move(param_rows);
    }

    return arrow::Status::OK();
}

//===----------------------------------------------------------------------===//
// Catalog / metadata
//===----------------------------------------------------------------------===//

arrow::Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoCatalogs(
    const flight::ServerCallContext& context,
    const flight::FlightDescriptor& descriptor) {

    auto schema = flightsql::SqlSchema::GetCatalogsSchema();
    return MakeCatalogFlightInfo(schema, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetCatalogs(
    const flight::ServerCallContext& context) {

    ARROW_ASSIGN_OR_RAISE(auto reader,
        ExecuteQuery("SELECT DISTINCT catalog_name FROM information_schema.schemata ORDER BY catalog_name"));

    // Map to the standard catalog schema
    auto catalog_schema = flightsql::SqlSchema::GetCatalogsSchema();

    // Read all results and rebuild with the correct schema
    arrow::StringBuilder catalog_builder;
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
        if (!batch) break;

        auto col = batch->column(0);
        auto string_array = std::static_pointer_cast<arrow::StringArray>(col);
        for (int64_t i = 0; i < string_array->length(); i++) {
            ARROW_RETURN_NOT_OK(catalog_builder.Append(string_array->GetString(i)));
        }
    }

    std::shared_ptr<arrow::Array> catalog_array;
    ARROW_RETURN_NOT_OK(catalog_builder.Finish(&catalog_array));

    auto result_batch = arrow::RecordBatch::Make(catalog_schema, catalog_array->length(),
                                                  {catalog_array});
    auto batch_reader = arrow::RecordBatchReader::Make({result_batch}, catalog_schema);
    if (!batch_reader.ok()) {
        return batch_reader.status();
    }
    return std::make_unique<flight::RecordBatchStream>(batch_reader.MoveValueUnsafe());
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoSchemas(
    const flight::ServerCallContext& context,
    const flightsql::GetDbSchemas& command,
    const flight::FlightDescriptor& descriptor) {

    auto schema = flightsql::SqlSchema::GetDbSchemasSchema();
    return MakeCatalogFlightInfo(schema, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetDbSchemas(
    const flight::ServerCallContext& context,
    const flightsql::GetDbSchemas& command) {

    std::string query = "SELECT catalog_name, schema_name FROM information_schema.schemata";
    std::vector<std::string> conditions;
    duckdb::vector<duckdb::Value> params;
    int param_idx = 1;

    if (command.catalog.has_value() && !command.catalog.value().empty()) {
        conditions.push_back("catalog_name = $" + std::to_string(param_idx++));
        params.push_back(duckdb::Value(command.catalog.value()));
    }
    if (command.db_schema_filter_pattern.has_value() && !command.db_schema_filter_pattern.value().empty()) {
        conditions.push_back("schema_name LIKE $" + std::to_string(param_idx++));
        params.push_back(duckdb::Value(command.db_schema_filter_pattern.value()));
    }

    if (!conditions.empty()) {
        query += " WHERE ";
        for (size_t i = 0; i < conditions.size(); i++) {
            if (i > 0) query += " AND ";
            query += conditions[i];
        }
    }
    query += " ORDER BY catalog_name, schema_name";

    std::shared_ptr<arrow::RecordBatchReader> reader;
    if (params.empty()) {
        ARROW_ASSIGN_OR_RAISE(reader, ExecuteQuery(query));
    } else {
        ARROW_ASSIGN_OR_RAISE(reader, ExecuteParameterizedQuery(query, params));
    }

    auto db_schema = flightsql::SqlSchema::GetDbSchemasSchema();

    arrow::StringBuilder catalog_builder;
    arrow::StringBuilder schema_builder;

    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
        if (!batch) break;

        auto cat_col = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
        auto sch_col = std::static_pointer_cast<arrow::StringArray>(batch->column(1));

        for (int64_t i = 0; i < batch->num_rows(); i++) {
            ARROW_RETURN_NOT_OK(catalog_builder.Append(cat_col->GetString(i)));
            ARROW_RETURN_NOT_OK(schema_builder.Append(sch_col->GetString(i)));
        }
    }

    std::shared_ptr<arrow::Array> cat_array, sch_array;
    ARROW_RETURN_NOT_OK(catalog_builder.Finish(&cat_array));
    ARROW_RETURN_NOT_OK(schema_builder.Finish(&sch_array));

    auto result_batch = arrow::RecordBatch::Make(db_schema, cat_array->length(),
                                                  {cat_array, sch_array});
    auto batch_reader = arrow::RecordBatchReader::Make({result_batch}, db_schema);
    if (!batch_reader.ok()) {
        return batch_reader.status();
    }
    return std::make_unique<flight::RecordBatchStream>(batch_reader.MoveValueUnsafe());
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoTables(
    const flight::ServerCallContext& context,
    const flightsql::GetTables& command,
    const flight::FlightDescriptor& descriptor) {

    auto schema = command.include_schema
        ? flightsql::SqlSchema::GetTablesSchemaWithIncludedSchema()
        : flightsql::SqlSchema::GetTablesSchema();
    return MakeCatalogFlightInfo(schema, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetTables(
    const flight::ServerCallContext& context,
    const flightsql::GetTables& command) {

    std::string query =
        "SELECT table_catalog, table_schema, table_name, table_type "
        "FROM information_schema.tables";

    std::vector<std::string> conditions;
    duckdb::vector<duckdb::Value> params;
    int param_idx = 1;

    if (command.catalog.has_value() && !command.catalog.value().empty()) {
        conditions.push_back("table_catalog = $" + std::to_string(param_idx++));
        params.push_back(duckdb::Value(command.catalog.value()));
    }
    if (command.db_schema_filter_pattern.has_value() && !command.db_schema_filter_pattern.value().empty()) {
        conditions.push_back("table_schema LIKE $" + std::to_string(param_idx++));
        params.push_back(duckdb::Value(command.db_schema_filter_pattern.value()));
    }
    if (command.table_name_filter_pattern.has_value() && !command.table_name_filter_pattern.value().empty()) {
        conditions.push_back("table_name LIKE $" + std::to_string(param_idx++));
        params.push_back(duckdb::Value(command.table_name_filter_pattern.value()));
    }
    if (!command.table_types.empty()) {
        std::string placeholders;
        for (size_t i = 0; i < command.table_types.size(); i++) {
            if (i > 0) placeholders += ", ";
            placeholders += "$" + std::to_string(param_idx++);
            params.push_back(duckdb::Value(command.table_types[i]));
        }
        conditions.push_back("table_type IN (" + placeholders + ")");
    }

    if (!conditions.empty()) {
        query += " WHERE ";
        for (size_t i = 0; i < conditions.size(); i++) {
            if (i > 0) query += " AND ";
            query += conditions[i];
        }
    }
    query += " ORDER BY table_catalog, table_schema, table_name";

    std::shared_ptr<arrow::RecordBatchReader> reader;
    if (params.empty()) {
        ARROW_ASSIGN_OR_RAISE(reader, ExecuteQuery(query));
    } else {
        ARROW_ASSIGN_OR_RAISE(reader, ExecuteParameterizedQuery(query, params));
    }

    // Build the result with the standard schema
    auto table_schema = command.include_schema
        ? flightsql::SqlSchema::GetTablesSchemaWithIncludedSchema()
        : flightsql::SqlSchema::GetTablesSchema();

    arrow::StringBuilder catalog_builder, schema_builder, name_builder, type_builder;

    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
        if (!batch) break;

        auto cat_col = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
        auto sch_col = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
        auto name_col = std::static_pointer_cast<arrow::StringArray>(batch->column(2));
        auto type_col = std::static_pointer_cast<arrow::StringArray>(batch->column(3));

        for (int64_t i = 0; i < batch->num_rows(); i++) {
            ARROW_RETURN_NOT_OK(catalog_builder.Append(cat_col->GetString(i)));
            ARROW_RETURN_NOT_OK(schema_builder.Append(sch_col->GetString(i)));
            ARROW_RETURN_NOT_OK(name_builder.Append(name_col->GetString(i)));
            ARROW_RETURN_NOT_OK(type_builder.Append(type_col->GetString(i)));
        }
    }

    std::shared_ptr<arrow::Array> cat_array, sch_array, name_array, type_array;
    ARROW_RETURN_NOT_OK(catalog_builder.Finish(&cat_array));
    ARROW_RETURN_NOT_OK(schema_builder.Finish(&sch_array));
    ARROW_RETURN_NOT_OK(name_builder.Finish(&name_array));
    ARROW_RETURN_NOT_OK(type_builder.Finish(&type_array));

    std::vector<std::shared_ptr<arrow::Array>> columns = {
        cat_array, sch_array, name_array, type_array
    };

    if (command.include_schema) {
        // For each table, Prepare "SELECT * FROM schema.table" to get column
        // types, then serialize the Arrow schema as IPC bytes.
        arrow::BinaryBuilder schema_bytes_builder;

        auto cat_str = std::static_pointer_cast<arrow::StringArray>(cat_array);
        auto sch_str = std::static_pointer_cast<arrow::StringArray>(sch_array);
        auto name_str = std::static_pointer_cast<arrow::StringArray>(name_array);

        auto conn = std::make_unique<duckdb::Connection>(session_manager_->GetDatabase());
        auto props = GetClientProps(*conn);

        for (int64_t i = 0; i < cat_array->length(); i++) {
            std::string probe_sql = "SELECT * FROM \"" +
                sch_str->GetString(i) + "\".\"" + name_str->GetString(i) + "\" LIMIT 0";

            auto prepared = conn->Prepare(probe_sql);
            if (prepared->HasError()) {
                // Cannot resolve schema — append NULL
                ARROW_RETURN_NOT_OK(schema_bytes_builder.AppendNull());
                continue;
            }

            auto& types = prepared->GetTypes();
            auto& names = prepared->GetNames();
            ARROW_ASSIGN_OR_RAISE(auto tbl_schema, MakeArrowSchema(types, names, props));

            // Serialize schema to IPC format
            ARROW_ASSIGN_OR_RAISE(auto serialized, arrow::ipc::SerializeSchema(*tbl_schema));
            ARROW_RETURN_NOT_OK(schema_bytes_builder.Append(
                serialized->data(), static_cast<int32_t>(serialized->size())));
        }

        std::shared_ptr<arrow::Array> schema_bytes_array;
        ARROW_RETURN_NOT_OK(schema_bytes_builder.Finish(&schema_bytes_array));
        columns.push_back(schema_bytes_array);
    }

    auto result_batch = arrow::RecordBatch::Make(
        table_schema, cat_array->length(), columns);
    auto batch_reader = arrow::RecordBatchReader::Make({result_batch}, table_schema);
    if (!batch_reader.ok()) {
        return batch_reader.status();
    }
    return std::make_unique<flight::RecordBatchStream>(batch_reader.MoveValueUnsafe());
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoTableTypes(
    const flight::ServerCallContext& context,
    const flight::FlightDescriptor& descriptor) {

    auto schema = flightsql::SqlSchema::GetTableTypesSchema();
    return MakeCatalogFlightInfo(schema, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetTableTypes(
    const flight::ServerCallContext& context) {

    auto schema = flightsql::SqlSchema::GetTableTypesSchema();

    arrow::StringBuilder type_builder;
    ARROW_RETURN_NOT_OK(type_builder.Append("BASE TABLE"));
    ARROW_RETURN_NOT_OK(type_builder.Append("LOCAL TEMPORARY"));
    ARROW_RETURN_NOT_OK(type_builder.Append("VIEW"));

    std::shared_ptr<arrow::Array> type_array;
    ARROW_RETURN_NOT_OK(type_builder.Finish(&type_array));

    auto result_batch = arrow::RecordBatch::Make(schema, 3, {type_array});
    auto batch_reader = arrow::RecordBatchReader::Make({result_batch}, schema);
    if (!batch_reader.ok()) {
        return batch_reader.status();
    }
    return std::make_unique<flight::RecordBatchStream>(batch_reader.MoveValueUnsafe());
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoSqlInfo(
    const flight::ServerCallContext& context,
    const flightsql::GetSqlInfo& command,
    const flight::FlightDescriptor& descriptor) {

    // Use base class to handle SqlInfo
    return FlightSqlServerBase::GetFlightInfoSqlInfo(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetSqlInfo(
    const flight::ServerCallContext& context,
    const flightsql::GetSqlInfo& command) {

    // Use base class to handle SqlInfo
    return FlightSqlServerBase::DoGetSqlInfo(context, command);
}

} // namespace duckdb_server
