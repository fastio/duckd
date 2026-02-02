//===----------------------------------------------------------------------===//
//                         DuckDB Remote Extension
//
// duckdb_remote_extension.cpp
//
// Extension entry point - registers remote_query table function
//===----------------------------------------------------------------------===//

#define DUCKDB_EXTENSION_MAIN

#include "duckdb_remote.hpp"
#include "remote_client.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// remote_query Table Function
//===----------------------------------------------------------------------===//

struct RemoteQueryBindData : public TableFunctionData {
    string host;
    uint16_t port;
    string sql;
    string username;
    uint32_t timeout_ms;

    // Result data (populated during bind)
    vector<RemoteColumnInfo> columns;
};

struct RemoteQueryGlobalState : public GlobalTableFunctionState {
    unique_ptr<RemoteClient> client;
    unique_ptr<RemoteQueryResult> result;
    idx_t current_row = 0;
    bool finished = false;

    idx_t MaxThreads() const override {
        return 1;  // Single-threaded for now
    }
};

static unique_ptr<FunctionData> RemoteQueryBind(ClientContext &context,
                                                 TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types,
                                                 vector<string> &names) {
    auto bind_data = make_uniq<RemoteQueryBindData>();

    // Parse arguments: remote_query(host, port, sql, [username], [timeout_ms])
    if (input.inputs.size() < 3) {
        throw InvalidInputException("remote_query requires at least 3 arguments: host, port, sql");
    }

    bind_data->host = input.inputs[0].GetValue<string>();
    bind_data->port = static_cast<uint16_t>(input.inputs[1].GetValue<int32_t>());
    bind_data->sql = input.inputs[2].GetValue<string>();

    if (input.inputs.size() > 3) {
        bind_data->username = input.inputs[3].GetValue<string>();
    }
    if (input.inputs.size() > 4) {
        bind_data->timeout_ms = static_cast<uint32_t>(input.inputs[4].GetValue<int32_t>());
    }

    // Connect and get schema
    auto client = make_uniq<RemoteClient>();
    client->Connect(bind_data->host, bind_data->port, bind_data->username);

    // Execute query to get schema (we'll re-execute in the scan function)
    auto result = client->Query(bind_data->sql, bind_data->timeout_ms);

    if (result->has_error) {
        throw IOException("Remote query error: " + result->error_message);
    }

    // Extract column info
    bind_data->columns = std::move(result->columns);

    for (const auto &col : bind_data->columns) {
        return_types.push_back(col.type);
        names.push_back(col.name);
    }

    client->Disconnect();

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> RemoteQueryInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<RemoteQueryBindData>();

    auto state = make_uniq<RemoteQueryGlobalState>();

    // Connect to server
    state->client = make_uniq<RemoteClient>();
    state->client->Connect(bind_data.host, bind_data.port, bind_data.username);

    // Execute query
    state->result = state->client->Query(bind_data.sql, bind_data.timeout_ms);

    if (state->result->has_error) {
        throw IOException("Remote query error: " + state->result->error_message);
    }

    return std::move(state);
}

static void RemoteQueryFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<RemoteQueryGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.current_row < state.result->rows.size() && count < max_count) {
        const auto &row = state.result->rows[state.current_row];

        for (idx_t col = 0; col < row.size(); col++) {
            output.data[col].SetValue(count, row[col]);
        }

        state.current_row++;
        count++;
    }

    output.SetCardinality(count);

    if (state.current_row >= state.result->rows.size()) {
        state.finished = true;
    }
}

//===----------------------------------------------------------------------===//
// duckdb_remote_ping Scalar Function (connection test)
//===----------------------------------------------------------------------===//

static void DuckDBRemotePingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &host_vector = args.data[0];
    auto &port_vector = args.data[1];

    BinaryExecutor::Execute<string_t, int32_t, bool>(
        host_vector, port_vector, result, args.size(),
        [&](string_t host, int32_t port) {
            try {
                RemoteClient client;
                client.Connect(host.GetString(), static_cast<uint16_t>(port));
                client.Disconnect();
                return true;
            } catch (...) {
                return false;
            }
        });
}

//===----------------------------------------------------------------------===//
// Extension Load
//===----------------------------------------------------------------------===//

void DuckDBRemoteExtension::Load(ExtensionLoader &loader) {
    // Register remote_query table function
    TableFunction remote_query_func("remote_query",
                                     {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR},
                                     RemoteQueryFunction,
                                     RemoteQueryBind,
                                     RemoteQueryInitGlobal);

    // Add optional parameters
    remote_query_func.named_parameters["username"] = LogicalType::VARCHAR;
    remote_query_func.named_parameters["timeout_ms"] = LogicalType::INTEGER;

    loader.RegisterFunction(remote_query_func);

    // Register duckdb_remote_ping scalar function
    ScalarFunction ping_func("duckdb_remote_ping",
                              {LogicalType::VARCHAR, LogicalType::INTEGER},
                              LogicalType::BOOLEAN,
                              DuckDBRemotePingFunction);

    loader.RegisterFunction(ping_func);
}

std::string DuckDBRemoteExtension::Name() {
    return "duckdb_remote";
}

std::string DuckDBRemoteExtension::Version() const {
    return "1.0.0";
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
// Extension Entry Point
//===----------------------------------------------------------------------===//

extern "C" {

DUCKDB_EXTENSION_API void duckdb_remote_init(duckdb::DatabaseInstance &db) {
    duckdb::ExtensionLoader loader(db, "duckdb_remote");
    duckdb::DuckDBRemoteExtension extension;
    extension.Load(loader);
}

DUCKDB_EXTENSION_API const char *duckdb_remote_version() {
    return "1.0.0";
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN should be defined
#endif
