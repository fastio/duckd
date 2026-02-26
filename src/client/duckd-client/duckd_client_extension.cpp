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
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
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
                               AttachedDatabase &db, Catalog &catalog_ref) {
    auto &duck_catalog = catalog_ref.Cast<DuckdCatalog>();
    return make_uniq<DuckdTransactionManager>(db, duck_catalog.GetSharedClient());
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
    vector<string>      column_names;
    vector<LogicalType> column_types;
    std::shared_ptr<duckdb_client::DuckdFlightClient> client;
};

// Streaming global state — mirrors DuckdScanGlobalState but without
// projection/filter pushdown (duckd_query runs the SQL as-is on the server).
struct DuckdQueryTableGlobalState : public GlobalTableFunctionState {
    std::unique_ptr<duckdb_client::DuckdQueryStream> stream;
    std::shared_ptr<arrow::RecordBatch>               current_batch;
    idx_t                                             row_in_batch = 0;
    bool                                              finished     = false;

    idx_t MaxThreads() const override { return 1; }
};

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

    // Probe schema via LIMIT 0 (no data transfer).
    auto schema_res = bind->client->GetQuerySchema(bind->sql);
    if (!schema_res.ok()) {
        // GetQuerySchema wraps the query in LIMIT 0; fall back to a full
        // execute for queries that don't support LIMIT (e.g. CALL).
        auto res = bind->client->ExecuteQuery(bind->sql);
        if (!res.ok()) {
            throw IOException("duckd_query: " + res.status().ToString());
        }
        auto &r = res.ValueUnsafe();
        if (r.schema) {
            for (int i = 0; i < r.schema->num_fields(); i++) {
                auto &f = r.schema->field(i);
                names.push_back(f->name());
                return_types.push_back(ArrowToDuckDBType(*f->type()));
            }
        } else {
            names.push_back("rows_affected");
            return_types.push_back(LogicalType::BIGINT);
        }
    } else {
        auto &arrow_schema = *schema_res.ValueUnsafe();
        for (int i = 0; i < arrow_schema.num_fields(); i++) {
            auto &f = arrow_schema.field(i);
            names.push_back(f->name());
            return_types.push_back(ArrowToDuckDBType(*f->type()));
        }
    }

    bind->column_names = names;
    bind->column_types = return_types;
    return std::move(bind);
}

static unique_ptr<GlobalTableFunctionState> DuckdQueryTableInitGlobal(
    ClientContext &ctx, TableFunctionInitInput &input) {

    auto &bd    = input.bind_data->Cast<DuckdQueryTableBindData>();
    auto  state = make_uniq<DuckdQueryTableGlobalState>();

    auto stream_res = bd.client->ExecuteQueryStream(bd.sql);
    if (!stream_res.ok()) {
        throw IOException("duckd_query: " + stream_res.status().ToString());
    }
    state->stream = stream_res.MoveValueUnsafe();

    // Pull the first batch eagerly so we detect an empty result immediately.
    auto first = state->stream->Next();
    if (!first.ok()) {
        throw IOException("duckd_query: " + first.status().ToString());
    }
    if (*first) {
        state->current_batch = std::move(*first);
    } else {
        state->finished = true;
    }
    return std::move(state);
}

static void DuckdQueryTableFunction(ClientContext &ctx, TableFunctionInput &input,
                                     DataChunk &output) {
    auto &state = input.global_state->Cast<DuckdQueryTableGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    // Advance to the next batch if the current one is exhausted.
    while (state.current_batch &&
           state.row_in_batch >= (idx_t)state.current_batch->num_rows()) {
        auto next = state.stream->Next();
        if (!next.ok()) {
            throw IOException("duckd_query: " + next.status().ToString());
        }
        if (!*next) {
            state.finished      = true;
            state.current_batch = nullptr;
            output.SetCardinality(0);
            return;
        }
        state.current_batch = std::move(*next);
        state.row_in_batch  = 0;
    }

    if (!state.current_batch) {
        state.finished = true;
        output.SetCardinality(0);
        return;
    }

    auto  &batch   = *state.current_batch;
    idx_t  avail   = (idx_t)batch.num_rows() - state.row_in_batch;
    idx_t  to_fill = MinValue<idx_t>(avail, (idx_t)STANDARD_VECTOR_SIZE);

    // Zero-copy columnar fill — same path as the catalog scan.
    idx_t ncols = MinValue<idx_t>((idx_t)batch.num_columns(), (idx_t)output.data.size());
    for (idx_t col = 0; col < ncols; col++) {
        FillColumnFromArrow(output.data[col], *batch.column((int)col),
                            (int64_t)state.row_in_batch, to_fill);
    }

    output.SetCardinality(to_fill);
    state.row_in_batch += to_fill;
}

//===----------------------------------------------------------------------===//
// LIMIT pushdown optimizer extension
//
// Walks the logical plan and, when a LogicalLimit with a constant limit (and
// no offset) directly wraps a duckd_scan LogicalGet — possibly via one
// LogicalProjection after the LimitPushdown optimizer has run — stores the
// limit value in DuckdScanBindData.  DuckdScanInitGlobal then appends
// "LIMIT N" to the SQL sent to the remote server.
//
// Pattern 1 (no DuckDB LimitPushdown yet, or limit >= 8192):
//   LIMIT(N) → GET(duckd_scan)
//   LIMIT(N) → PROJECTION → GET(duckd_scan)
//
// Pattern 2 (after DuckDB LimitPushdown, limit < 8192):
//   PROJECTION → LIMIT(N) → GET(duckd_scan)
//===----------------------------------------------------------------------===//

// Returns the LogicalGet if it is a duckd_scan, looking through one optional
// LogicalProjection.
static LogicalGet *FindDuckdScan(LogicalOperator &op) {
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        auto &get = op.Cast<LogicalGet>();
        if (get.function.name == "duckd_scan") {
            return &get;
        }
        return nullptr;
    }
    if (op.type == LogicalOperatorType::LOGICAL_PROJECTION &&
        !op.children.empty()) {
        return FindDuckdScan(*op.children[0]);
    }
    return nullptr;
}

static void DuckdLimitPushdownWalk(LogicalOperator &op) {
    if (op.type == LogicalOperatorType::LOGICAL_LIMIT && !op.children.empty()) {
        auto &limit = op.Cast<LogicalLimit>();
        // Only push a constant, non-percentage LIMIT with no offset (or zero
        // constant offset — e.g. OFFSET 0 is legal but rare).
        bool offset_zero =
            limit.offset_val.Type() == LimitNodeType::UNSET ||
            (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE &&
             limit.offset_val.GetConstantValue() == 0);

        if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE &&
            offset_zero) {
            idx_t n   = limit.limit_val.GetConstantValue();
            auto *get = FindDuckdScan(*op.children[0]);
            if (get && get->bind_data) {
                auto &bd = get->bind_data->Cast<DuckdScanBindData>();
                // Keep the tightest limit if multiple LIMIT nodes nest.
                if (bd.limit_val == 0 || n < bd.limit_val) {
                    bd.limit_val = n;
                }
            }
        }
    }

    for (auto &child : op.children) {
        DuckdLimitPushdownWalk(*child);
    }
}

static void DuckdLimitPushdownOptimize(OptimizerExtensionInput & /*input*/,
                                        unique_ptr<LogicalOperator> &plan) {
    if (plan) {
        DuckdLimitPushdownWalk(*plan);
    }
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

        // Register LIMIT pushdown optimizer for duckd_scan table functions.
        OptimizerExtension limit_ext;
        limit_ext.optimize_function = DuckdLimitPushdownOptimize;
        OptimizerExtension::Register(loader.GetDatabaseInstance().config,
                                     std::move(limit_ext));

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
