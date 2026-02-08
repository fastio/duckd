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
    , executor_pool_(executor_pool)
    , host_(host)
    , port_(port) {
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
    auto& pool = session_manager_->GetConnectionPool();
    auto conn = pool.Acquire();
    if (!conn) {
        return arrow::Status::ExecutionError("Failed to acquire connection from pool");
    }

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
    auto& pool = session_manager_->GetConnectionPool();
    auto conn = pool.Acquire();
    if (!conn) {
        return arrow::Status::ExecutionError("Failed to acquire connection from pool");
    }

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
// Statement execution
//===----------------------------------------------------------------------===//

arrow::Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoStatement(
    const flight::ServerCallContext& context,
    const flightsql::StatementQuery& command,
    const flight::FlightDescriptor& descriptor) {

    const auto& query = command.query;

    // Prepare the query to get schema without executing
    auto& pool = session_manager_->GetConnectionPool();
    auto conn = pool.Acquire();
    if (!conn) {
        return arrow::Status::ExecutionError("Failed to acquire connection from pool");
    }
    auto prepared = conn->Prepare(query);
    if (prepared->HasError()) {
        return arrow::Status::ExecutionError(prepared->GetError());
    }

    auto& types = prepared->GetTypes();
    auto& names = prepared->GetNames();
    auto props = GetClientProps(*conn);
    ARROW_ASSIGN_OR_RAISE(auto schema, MakeArrowSchema(types, names, props));

    auto handle = GenerateHandle();

    // Store the query text so DoGetStatement can re-execute it
    {
        std::lock_guard<std::mutex> lock(query_mutex_);
        TransientQuery tq;
        tq.query = query;
        tq.schema = schema;
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
    {
        std::lock_guard<std::mutex> lock(query_mutex_);
        auto it = transient_queries_.find(handle);
        if (it == transient_queries_.end()) {
            return arrow::Status::KeyError("Statement handle not found: ", handle);
        }
        query = it->second.query;
        schema = it->second.schema;
        is_prepared = it->second.is_prepared;
        transient_queries_.erase(it);
    }

    // If this is a prepared statement execution, delegate to DoGetPreparedStatement logic
    if (is_prepared) {
        flightsql::PreparedStatementQuery ps_command;
        ps_command.prepared_statement_handle = query;  // query holds the prepared stmt handle
        return DoGetPreparedStatement(context, ps_command);
    }

    // Execute the query using a pooled connection
    auto& pool = session_manager_->GetConnectionPool();
    auto conn = pool.Acquire();
    if (!conn) {
        return arrow::Status::ExecutionError("Failed to acquire connection from pool");
    }
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

    auto& pool = session_manager_->GetConnectionPool();
    auto conn = pool.Acquire();
    if (!conn) {
        return arrow::Status::ExecutionError("Failed to acquire connection from pool");
    }

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
        bound_params = std::move(it->second.bound_parameters);
        it->second.bound_parameters.clear();
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

    std::lock_guard<std::mutex> lock(prepared_mutex_);
    auto it = prepared_statements_.find(handle);
    if (it == prepared_statements_.end()) {
        return arrow::Status::KeyError("Prepared statement not found: ", handle);
    }

    // Read parameter record batches from the client
    auto table_result = reader->ToTable();
    if (!table_result.ok()) {
        return table_result.status();
    }

    auto table = table_result.MoveValueUnsafe();
    if (table->num_rows() == 0) {
        return arrow::Status::OK();
    }

    // Convert Arrow columns at row 0 to DuckDB Values
    duckdb::vector<duckdb::Value> params;
    for (int col = 0; col < table->num_columns(); col++) {
        auto chunk = table->column(col)->chunk(0);
        if (chunk->IsNull(0)) {
            params.push_back(duckdb::Value());
        } else {
            params.push_back(ArrowScalarToDuckDBValue(chunk, 0));
        }
    }

    it->second.bound_parameters = std::move(params);
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
        // Add empty binary column for table_schema (schema bytes)
        arrow::BinaryBuilder schema_bytes_builder;
        for (int64_t i = 0; i < cat_array->length(); i++) {
            ARROW_RETURN_NOT_OK(schema_bytes_builder.AppendNull());
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
