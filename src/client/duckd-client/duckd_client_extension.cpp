//===----------------------------------------------------------------------===//
//                         DuckD Client Extension
//
// src/client/duckd-client/duckd_client_extension.cpp
//
// Extension entry point: registers the duckd storage type and helper functions
//
// Usage:
//   LOAD duckd_client;
//   ATTACH 'grpc://host:8815' AS remote (TYPE duckd);
//   SELECT * FROM remote.main.my_table;
//   SELECT duckd_exec('grpc://host:8815', 'INSERT INTO ...');
//   SELECT * FROM duckd_query('grpc://host:8815', 'SELECT ...');
//===----------------------------------------------------------------------===//

#define DUCKDB_EXTENSION_MAIN

#include "duckd_catalog.hpp"
#include "flight_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

#include <arrow/api.h>
#include <stdexcept>

namespace duckdb {

//===----------------------------------------------------------------------===//
// StorageExtension callbacks
//===----------------------------------------------------------------------===//

static unique_ptr<Catalog> DuckdAttach(optional_ptr<StorageExtensionInfo> /*storage_info*/,
                                        ClientContext & /*context*/,
                                        AttachedDatabase &db,
                                        const string & /*name*/,
                                        AttachInfo &info,
                                        AttachOptions & /*options*/) {
    const auto &url = info.path;
    return make_uniq<DuckdCatalog>(db, url);
}

static unique_ptr<TransactionManager>
DuckdCreateTransactionManager(optional_ptr<StorageExtensionInfo> /*storage_info*/,
                               AttachedDatabase &db, Catalog & /*catalog*/) {
    return make_uniq<DuckdTransactionManager>(db);
}

//===----------------------------------------------------------------------===//
// duckd_exec(url, sql) → BIGINT
//   Execute arbitrary SQL on a remote duckd server (DML, DDL).
//   Returns the number of affected rows (0 for DDL).
//===----------------------------------------------------------------------===//

static void DuckdExecFunction(DataChunk &args, ExpressionState & /*state*/,
                               Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, int64_t>(
        args.data[0], args.data[1], result, args.size(),
        [](string_t url_sv, string_t sql_sv) -> int64_t {
            try {
                // Reuse a cached connection via the registry instead of
                // opening a new gRPC channel for every call.
                auto res = duckdb_client::DuckdClientRegistry::Instance()
                               .WithReconnect(url_sv.GetString(),
                                   [&](duckdb_client::DuckdFlightClient &c) {
                                       return c.ExecuteUpdate(sql_sv.GetString());
                                   });
                if (!res.ok()) {
                    throw IOException("duckd_exec: " + res.status().ToString());
                }
                return res.ValueUnsafe();
            } catch (const IOException &) {
                throw;
            } catch (const std::exception &e) {
                throw IOException(string("duckd_exec: ") + e.what());
            }
        });
}

//===----------------------------------------------------------------------===//
// duckd_query(url, sql) → TABLE
//   Execute an arbitrary SELECT query on a remote duckd server and return
//   the results as a table.
//===----------------------------------------------------------------------===//

struct DuckdQueryTableBindData : public TableFunctionData {
    string url;
    string sql;
    // Populated during bind
    vector<string>      column_names;
    vector<LogicalType> column_types;
    std::shared_ptr<duckdb_client::DuckdFlightClient> client;
};

struct DuckdQueryTableGlobalState : public GlobalTableFunctionState {
    duckdb_client::DuckdQueryResult result;
    idx_t                           batch_idx    = 0;
    idx_t                           row_in_batch = 0;
    bool                            finished     = false;

    idx_t MaxThreads() const override { return 1; }
};

// Helper: convert Arrow schema to DuckDB types/names
static void ExtractSchemaFromArrow(
    const arrow::Schema &schema,
    vector<string> &names,
    vector<LogicalType> &types) {

    for (int i = 0; i < schema.num_fields(); i++) {
        auto &f = schema.field(i);
        names.push_back(f->name());

        // Basic Arrow → DuckDB type mapping
        switch (f->type()->id()) {
            case arrow::Type::BOOL:           types.push_back(LogicalType::BOOLEAN); break;
            case arrow::Type::INT8:           types.push_back(LogicalType::TINYINT); break;
            case arrow::Type::INT16:          types.push_back(LogicalType::SMALLINT); break;
            case arrow::Type::INT32:          types.push_back(LogicalType::INTEGER); break;
            case arrow::Type::INT64:          types.push_back(LogicalType::BIGINT); break;
            case arrow::Type::UINT8:          types.push_back(LogicalType::UTINYINT); break;
            case arrow::Type::UINT16:         types.push_back(LogicalType::USMALLINT); break;
            case arrow::Type::UINT32:         types.push_back(LogicalType::UINTEGER); break;
            case arrow::Type::UINT64:         types.push_back(LogicalType::UBIGINT); break;
            case arrow::Type::FLOAT:          types.push_back(LogicalType::FLOAT); break;
            case arrow::Type::DOUBLE:         types.push_back(LogicalType::DOUBLE); break;
            case arrow::Type::DATE32:
            case arrow::Type::DATE64:         types.push_back(LogicalType::DATE); break;
            case arrow::Type::TIME32:
            case arrow::Type::TIME64:         types.push_back(LogicalType::TIME); break;
            case arrow::Type::TIMESTAMP:      types.push_back(LogicalType::TIMESTAMP); break;
            case arrow::Type::BINARY:
            case arrow::Type::LARGE_BINARY:   types.push_back(LogicalType::BLOB); break;
            default:                          types.push_back(LogicalType::VARCHAR); break;
        }
    }
}

static unique_ptr<FunctionData> DuckdQueryTableBind(
    ClientContext &ctx, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) {

    if (input.inputs.size() < 2) {
        throw InvalidInputException("duckd_query(url, sql) requires 2 arguments");
    }

    auto bind    = make_uniq<DuckdQueryTableBindData>();
    bind->url    = input.inputs[0].GetValue<string>();
    bind->sql    = input.inputs[1].GetValue<string>();
    bind->client = duckdb_client::DuckdClientRegistry::Instance().GetOrCreate(bind->url);

    // Probe schema without fetching all data
    auto schema_res = bind->client->GetQuerySchema(bind->sql);
    if (!schema_res.ok()) {
        // GetQuerySchema appends LIMIT 0; try executing directly if it fails
        // (some queries don't support LIMIT, e.g. CALL)
        auto res = bind->client->ExecuteQuery(bind->sql);
        if (!res.ok()) {
            throw IOException("duckd_query: " + res.status().ToString());
        }
        if (res.ValueUnsafe().schema) {
            ExtractSchemaFromArrow(*res.ValueUnsafe().schema, names, return_types);
        } else {
            // Fallback: single INTEGER column for affected rows
            names.push_back("rows_affected");
            return_types.push_back(LogicalType::BIGINT);
        }
    } else {
        ExtractSchemaFromArrow(*schema_res.ValueUnsafe(), names, return_types);
    }

    bind->column_names = names;
    bind->column_types = return_types;
    return std::move(bind);
}

static unique_ptr<GlobalTableFunctionState> DuckdQueryTableInitGlobal(
    ClientContext &ctx, TableFunctionInitInput &input) {

    auto &bd    = input.bind_data->Cast<DuckdQueryTableBindData>();
    auto  state = make_uniq<DuckdQueryTableGlobalState>();

    auto result = bd.client->ExecuteQuery(bd.sql);
    if (!result.ok()) {
        throw IOException("duckd_query: " + result.status().ToString());
    }
    state->result   = result.MoveValueUnsafe();
    state->finished = state->result.batches.empty();
    return std::move(state);
}

// Arrow scalar → DuckDB Value (duplicated from duckd_catalog.cpp for independence)
static Value DuckdArrowToValue(const arrow::Array &arr, int64_t idx) {
    if (arr.IsNull(idx)) return Value();
    switch (arr.type_id()) {
        case arrow::Type::BOOL:    return Value::BOOLEAN(static_cast<const arrow::BooleanArray&>(arr).Value(idx));
        case arrow::Type::INT8:    return Value::TINYINT(static_cast<const arrow::Int8Array&>(arr).Value(idx));
        case arrow::Type::INT16:   return Value::SMALLINT(static_cast<const arrow::Int16Array&>(arr).Value(idx));
        case arrow::Type::INT32:   return Value::INTEGER(static_cast<const arrow::Int32Array&>(arr).Value(idx));
        case arrow::Type::INT64:   return Value::BIGINT(static_cast<const arrow::Int64Array&>(arr).Value(idx));
        case arrow::Type::UINT8:   return Value::UTINYINT(static_cast<const arrow::UInt8Array&>(arr).Value(idx));
        case arrow::Type::UINT16:  return Value::USMALLINT(static_cast<const arrow::UInt16Array&>(arr).Value(idx));
        case arrow::Type::UINT32:  return Value::UINTEGER(static_cast<const arrow::UInt32Array&>(arr).Value(idx));
        case arrow::Type::UINT64:  return Value::UBIGINT(static_cast<const arrow::UInt64Array&>(arr).Value(idx));
        case arrow::Type::FLOAT:   return Value::FLOAT(static_cast<const arrow::FloatArray&>(arr).Value(idx));
        case arrow::Type::DOUBLE:  return Value::DOUBLE(static_cast<const arrow::DoubleArray&>(arr).Value(idx));
        case arrow::Type::STRING:  return Value(static_cast<const arrow::StringArray&>(arr).GetString(idx));
        case arrow::Type::LARGE_STRING: return Value(static_cast<const arrow::LargeStringArray&>(arr).GetString(idx));
        default: {
            auto sc = arr.GetScalar(idx);
            if (sc.ok()) return Value(sc.ValueUnsafe()->ToString());
            return Value();
        }
    }
}

static void DuckdQueryTableFunction(ClientContext &ctx, TableFunctionInput &input,
                                     DataChunk &output) {
    auto &state = input.global_state->Cast<DuckdQueryTableGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    while (state.batch_idx < state.result.batches.size()) {
        auto &batch = state.result.batches[state.batch_idx];
        auto  avail = (idx_t)(batch->num_rows() - state.row_in_batch);

        if (avail == 0) {
            state.batch_idx++;
            state.row_in_batch = 0;
            continue;
        }

        idx_t to_fill = MinValue<idx_t>(avail, (idx_t)STANDARD_VECTOR_SIZE);

        idx_t ncols = MinValue<idx_t>((idx_t)batch->num_columns(),
                                       (idx_t)output.data.size());
        for (idx_t col = 0; col < ncols; col++) {
            auto &arrow_col = *batch->column((int)col);
            for (idx_t row = 0; row < to_fill; row++) {
                output.data[col].SetValue(
                    row, DuckdArrowToValue(arrow_col, (int64_t)(state.row_in_batch + row)));
            }
        }

        output.SetCardinality(to_fill);
        state.row_in_batch += to_fill;
        if ((idx_t)batch->num_rows() <= state.row_in_batch) {
            state.batch_idx++;
            state.row_in_batch = 0;
        }
        return;
    }

    state.finished = true;
    output.SetCardinality(0);
}

//===----------------------------------------------------------------------===//
// Extension Load / Init
//===----------------------------------------------------------------------===//

class DuckdClientExtension {
public:
    static void Load(ExtensionLoader &loader) {
        // Register 'duckd' storage type for ATTACH
        auto storage_ext                    = make_shared_ptr<StorageExtension>();
        storage_ext->attach                 = DuckdAttach;
        storage_ext->create_transaction_manager = DuckdCreateTransactionManager;
        StorageExtension::Register(loader.GetDatabaseInstance().config,
                                   "duckd", storage_ext);

        // duckd_exec(url VARCHAR, sql VARCHAR) → BIGINT
        ScalarFunction exec_fn(
            "duckd_exec",
            {LogicalType::VARCHAR, LogicalType::VARCHAR},
            LogicalType::BIGINT,
            DuckdExecFunction);
        loader.RegisterFunction(exec_fn);

        // duckd_query(url VARCHAR, sql VARCHAR) → TABLE
        TableFunction query_fn(
            "duckd_query",
            {LogicalType::VARCHAR, LogicalType::VARCHAR},
            DuckdQueryTableFunction,
            DuckdQueryTableBind,
            DuckdQueryTableInitGlobal);
        loader.RegisterFunction(query_fn);
    }

    static string Name() { return "duckd_client"; }
    static const char *Version() { return "1.0.0"; }
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
// C Extension Entry Points
//===----------------------------------------------------------------------===//

extern "C" {

DUCKDB_EXTENSION_API void duckd_client_init(duckdb::DatabaseInstance &db) {
    duckdb::ExtensionLoader loader(db, "duckd_client");
    duckdb::DuckdClientExtension::Load(loader);
}

DUCKDB_EXTENSION_API const char *duckd_client_version() {
    return duckdb::DuckdClientExtension::Version();
}

} // extern "C"

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN should be defined
#endif
