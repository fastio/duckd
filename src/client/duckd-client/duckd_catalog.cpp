//===----------------------------------------------------------------------===//
//                         DuckD Client Extension
//
// src/client/duckd-client/duckd_catalog.cpp
//
// Implementation of the remote duckd catalog, schema, and table entries
//===----------------------------------------------------------------------===//

#include "duckd_catalog.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/storage/database_size.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <arrow/api.h>
#include <arrow/type.h>

#include <cstring>
#include <stdexcept>
#include <string>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Utilities: Arrow → DuckDB type conversion
//===----------------------------------------------------------------------===//

static LogicalType ArrowToDuckDBType(const arrow::DataType &type) {
    switch (type.id()) {
        case arrow::Type::BOOL:           return LogicalType::BOOLEAN;
        case arrow::Type::INT8:           return LogicalType::TINYINT;
        case arrow::Type::INT16:          return LogicalType::SMALLINT;
        case arrow::Type::INT32:          return LogicalType::INTEGER;
        case arrow::Type::INT64:          return LogicalType::BIGINT;
        case arrow::Type::UINT8:          return LogicalType::UTINYINT;
        case arrow::Type::UINT16:         return LogicalType::USMALLINT;
        case arrow::Type::UINT32:         return LogicalType::UINTEGER;
        case arrow::Type::UINT64:         return LogicalType::UBIGINT;
        case arrow::Type::FLOAT:          return LogicalType::FLOAT;
        case arrow::Type::DOUBLE:         return LogicalType::DOUBLE;
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:   return LogicalType::VARCHAR;
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:   return LogicalType::BLOB;
        case arrow::Type::DATE32:
        case arrow::Type::DATE64:         return LogicalType::DATE;
        case arrow::Type::TIME32:
        case arrow::Type::TIME64:         return LogicalType::TIME;
        case arrow::Type::TIMESTAMP:      return LogicalType::TIMESTAMP;
        case arrow::Type::INTERVAL_MONTHS:
        case arrow::Type::INTERVAL_DAY_TIME:
        case arrow::Type::DURATION:       return LogicalType::INTERVAL;
        case arrow::Type::DECIMAL128: {
            auto &d = static_cast<const arrow::Decimal128Type &>(type);
            return LogicalType::DECIMAL(d.precision(), d.scale());
        }
        case arrow::Type::LIST:
        case arrow::Type::LARGE_LIST: {
            auto &lt = static_cast<const arrow::BaseListType &>(type);
            return LogicalType::LIST(ArrowToDuckDBType(*lt.value_type()));
        }
        case arrow::Type::STRUCT: {
            auto &st = static_cast<const arrow::StructType &>(type);
            child_list_t<LogicalType> children;
            for (int i = 0; i < st.num_fields(); i++) {
                auto &f = st.field(i);
                children.push_back({f->name(), ArrowToDuckDBType(*f->type())});
            }
            return LogicalType::STRUCT(std::move(children));
        }
        default:
            return LogicalType::VARCHAR; // safe fallback
    }
}

//===----------------------------------------------------------------------===//
// Utilities: Arrow scalar → DuckDB Value
//===----------------------------------------------------------------------===//

static Value ArrowScalarToValue(const arrow::Array &arr, int64_t idx) {
    if (arr.IsNull(idx)) {
        return Value();
    }

    switch (arr.type_id()) {
        case arrow::Type::BOOL:
            return Value::BOOLEAN(
                static_cast<const arrow::BooleanArray &>(arr).Value(idx));
        case arrow::Type::INT8:
            return Value::TINYINT(
                static_cast<const arrow::Int8Array &>(arr).Value(idx));
        case arrow::Type::INT16:
            return Value::SMALLINT(
                static_cast<const arrow::Int16Array &>(arr).Value(idx));
        case arrow::Type::INT32:
            return Value::INTEGER(
                static_cast<const arrow::Int32Array &>(arr).Value(idx));
        case arrow::Type::INT64:
            return Value::BIGINT(
                static_cast<const arrow::Int64Array &>(arr).Value(idx));
        case arrow::Type::UINT8:
            return Value::UTINYINT(
                static_cast<const arrow::UInt8Array &>(arr).Value(idx));
        case arrow::Type::UINT16:
            return Value::USMALLINT(
                static_cast<const arrow::UInt16Array &>(arr).Value(idx));
        case arrow::Type::UINT32:
            return Value::UINTEGER(
                static_cast<const arrow::UInt32Array &>(arr).Value(idx));
        case arrow::Type::UINT64:
            return Value::UBIGINT(
                static_cast<const arrow::UInt64Array &>(arr).Value(idx));
        case arrow::Type::FLOAT:
            return Value::FLOAT(
                static_cast<const arrow::FloatArray &>(arr).Value(idx));
        case arrow::Type::DOUBLE:
            return Value::DOUBLE(
                static_cast<const arrow::DoubleArray &>(arr).Value(idx));
        case arrow::Type::STRING:
            return Value(
                static_cast<const arrow::StringArray &>(arr).GetString(idx));
        case arrow::Type::LARGE_STRING:
            return Value(
                static_cast<const arrow::LargeStringArray &>(arr).GetString(idx));
        case arrow::Type::BINARY:
            return Value::BLOB(
                static_cast<const arrow::BinaryArray &>(arr).GetString(idx));
        case arrow::Type::LARGE_BINARY:
            return Value::BLOB(
                static_cast<const arrow::LargeBinaryArray &>(arr).GetString(idx));
        case arrow::Type::DATE32:
            return Value::DATE(date_t(
                static_cast<const arrow::Date32Array &>(arr).Value(idx)));
        case arrow::Type::TIME64: {
            auto v = static_cast<const arrow::Time64Array &>(arr).Value(idx);
            return Value::TIME(dtime_t(v));
        }
        case arrow::Type::TIMESTAMP: {
            auto v = static_cast<const arrow::TimestampArray &>(arr).Value(idx);
            return Value::TIMESTAMP(timestamp_t(v));
        }
        default: {
            // Fallback: convert via Arrow scalar ToString
            auto sc = arr.GetScalar(idx);
            if (sc.ok()) {
                return Value(sc.ValueUnsafe()->ToString());
            }
            return Value();
        }
    }
}

//===----------------------------------------------------------------------===//
// Utilities: Arrow column → DuckDB Vector (zero-copy for fixed-width types)
//
// Fills `count` values starting at `src_offset` in the Arrow array `src`
// into the DuckDB output Vector `dst` (positions 0..count-1).
//
// For fixed-width numeric types the data buffer is memcpy'd directly.
// For variable-width types (strings, blobs) we iterate and use
// StringVector::AddString to intern each value into the DuckDB string heap.
// Boolean is bitpacked in Arrow; we unpack it per-element.
// All other types fall back to the row-by-row ArrowScalarToValue path.
//===----------------------------------------------------------------------===//

static void FillColumnFromArrow(Vector &dst, const arrow::Array &src,
                                 int64_t src_offset, idx_t count) {
    auto &validity = FlatVector::Validity(dst);

    // Mark null positions (validity mask starts all-valid on a fresh vector)
    if (src.null_count() > 0) {
        for (idx_t i = 0; i < count; i++) {
            if (src.IsNull(src_offset + i)) {
                validity.SetInvalid(i);
            }
        }
    }

    switch (src.type_id()) {

    case arrow::Type::BOOL: {
        auto &arr  = static_cast<const arrow::BooleanArray &>(src);
        auto *data = FlatVector::GetData<uint8_t>(dst);
        for (idx_t i = 0; i < count; i++) {
            data[i] = arr.Value(src_offset + i) ? 1 : 0;
        }
        break;
    }

    // Fixed-width integers — direct memcpy from Arrow data buffer
#define DUCKD_FILL_FIXED(ArrowTy, CppTy, DuckTy)                                   \
    case ArrowTy: {                                                                 \
        const auto *sptr = src.data()->GetValues<CppTy>(1) + src_offset;          \
        auto       *dptr = FlatVector::GetData<DuckTy>(dst);                       \
        std::memcpy(dptr, sptr, count * sizeof(CppTy));                             \
        break;                                                                     \
    }

    DUCKD_FILL_FIXED(arrow::Type::INT8,   int8_t,   int8_t)
    DUCKD_FILL_FIXED(arrow::Type::INT16,  int16_t,  int16_t)
    DUCKD_FILL_FIXED(arrow::Type::INT32,  int32_t,  int32_t)
    DUCKD_FILL_FIXED(arrow::Type::INT64,  int64_t,  int64_t)
    DUCKD_FILL_FIXED(arrow::Type::UINT8,  uint8_t,  uint8_t)
    DUCKD_FILL_FIXED(arrow::Type::UINT16, uint16_t, uint16_t)
    DUCKD_FILL_FIXED(arrow::Type::UINT32, uint32_t, uint32_t)
    DUCKD_FILL_FIXED(arrow::Type::UINT64, uint64_t, uint64_t)
    DUCKD_FILL_FIXED(arrow::Type::FLOAT,  float,    float)
    DUCKD_FILL_FIXED(arrow::Type::DOUBLE, double,   double)
#undef DUCKD_FILL_FIXED

    // DATE32: Arrow = int32 days since Unix epoch; DuckDB date_t = int32 days
    case arrow::Type::DATE32: {
        static_assert(sizeof(date_t) == sizeof(int32_t), "date_t size mismatch");
        const auto *sptr = src.data()->GetValues<int32_t>(1) + src_offset;
        auto       *dptr = FlatVector::GetData<date_t>(dst);
        std::memcpy(dptr, sptr, count * sizeof(int32_t));
        break;
    }

    // TIME64(us): Arrow = int64 µs; DuckDB dtime_t = int64 µs
    case arrow::Type::TIME64: {
        static_assert(sizeof(dtime_t) == sizeof(int64_t), "dtime_t size mismatch");
        const auto *sptr = src.data()->GetValues<int64_t>(1) + src_offset;
        auto       *dptr = FlatVector::GetData<dtime_t>(dst);
        std::memcpy(dptr, sptr, count * sizeof(int64_t));
        break;
    }

    // TIMESTAMP(us): Arrow = int64 µs since epoch; DuckDB timestamp_t = int64 µs
    case arrow::Type::TIMESTAMP: {
        static_assert(sizeof(timestamp_t) == sizeof(int64_t), "timestamp_t size mismatch");
        const auto *sptr = src.data()->GetValues<int64_t>(1) + src_offset;
        auto       *dptr = FlatVector::GetData<timestamp_t>(dst);
        std::memcpy(dptr, sptr, count * sizeof(int64_t));
        break;
    }

    // STRING: iterate and intern into DuckDB's string heap
    case arrow::Type::STRING: {
        auto &arr  = static_cast<const arrow::StringArray &>(src);
        auto *dptr = FlatVector::GetData<string_t>(dst);
        for (idx_t i = 0; i < count; i++) {
            if (!src.IsNull(src_offset + i)) {
                auto sv  = arr.GetView(src_offset + i);
                dptr[i]  = StringVector::AddString(dst, sv.data(), sv.size());
            }
        }
        break;
    }

    case arrow::Type::LARGE_STRING: {
        auto &arr  = static_cast<const arrow::LargeStringArray &>(src);
        auto *dptr = FlatVector::GetData<string_t>(dst);
        for (idx_t i = 0; i < count; i++) {
            if (!src.IsNull(src_offset + i)) {
                auto sv  = arr.GetView(src_offset + i);
                dptr[i]  = StringVector::AddString(dst, sv.data(), sv.size());
            }
        }
        break;
    }

    // BINARY / LARGE_BINARY → BLOB
    case arrow::Type::BINARY: {
        auto &arr  = static_cast<const arrow::BinaryArray &>(src);
        auto *dptr = FlatVector::GetData<string_t>(dst);
        for (idx_t i = 0; i < count; i++) {
            if (!src.IsNull(src_offset + i)) {
                auto sv  = arr.GetView(src_offset + i);
                dptr[i]  = StringVector::AddStringOrBlob(dst, sv.data(), sv.size());
            }
        }
        break;
    }

    case arrow::Type::LARGE_BINARY: {
        auto &arr  = static_cast<const arrow::LargeBinaryArray &>(src);
        auto *dptr = FlatVector::GetData<string_t>(dst);
        for (idx_t i = 0; i < count; i++) {
            if (!src.IsNull(src_offset + i)) {
                auto sv  = arr.GetView(src_offset + i);
                dptr[i]  = StringVector::AddStringOrBlob(dst, sv.data(), sv.size());
            }
        }
        break;
    }

    default: {
        // Fallback: row-by-row scalar boxing (covers DECIMAL128, nested types, …)
        for (idx_t i = 0; i < count; i++) {
            if (!src.IsNull(src_offset + i)) {
                dst.SetValue(i, ArrowScalarToValue(src, src_offset + i));
            }
        }
        break;
    }

    } // switch
}

//===----------------------------------------------------------------------===//
// Utilities: TableFilter → SQL WHERE fragment
//===----------------------------------------------------------------------===//

static string ValueToSQLLiteral(const Value &val) {
    if (val.IsNull()) {
        return "NULL";
    }
    switch (val.type().id()) {
        case LogicalTypeId::VARCHAR:
            return "'" + StringUtil::Replace(val.ToString(), "'", "''") + "'";
        case LogicalTypeId::BOOLEAN:
            return val.GetValue<bool>() ? "true" : "false";
        default:
            return val.ToString();
    }
}

static string FilterToSQL(const TableFilter &filter, const string &col_name) {
    switch (filter.filter_type) {
        case TableFilterType::CONSTANT_COMPARISON: {
            auto &cf = filter.Cast<ConstantFilter>();
            string op;
            switch (cf.comparison_type) {
                case ExpressionType::COMPARE_EQUAL:              op = "=";  break;
                case ExpressionType::COMPARE_NOTEQUAL:           op = "<>"; break;
                case ExpressionType::COMPARE_LESSTHAN:           op = "<";  break;
                case ExpressionType::COMPARE_GREATERTHAN:        op = ">";  break;
                case ExpressionType::COMPARE_LESSTHANOREQUALTO:  op = "<="; break;
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO: op = ">="; break;
                default: return "";
            }
            return col_name + " " + op + " " + ValueToSQLLiteral(cf.constant);
        }
        case TableFilterType::IS_NULL:
            return col_name + " IS NULL";
        case TableFilterType::IS_NOT_NULL:
            return col_name + " IS NOT NULL";
        case TableFilterType::CONJUNCTION_AND: {
            auto &caf = filter.Cast<ConjunctionAndFilter>();
            vector<string> parts;
            for (auto &child : caf.child_filters) {
                string part = FilterToSQL(*child, col_name);
                if (!part.empty()) {
                    parts.push_back("(" + part + ")");
                }
            }
            return StringUtil::Join(parts, " AND ");
        }
        case TableFilterType::CONJUNCTION_OR: {
            auto &cof = filter.Cast<ConjunctionOrFilter>();
            vector<string> parts;
            for (auto &child : cof.child_filters) {
                string part = FilterToSQL(*child, col_name);
                if (!part.empty()) {
                    parts.push_back("(" + part + ")");
                }
            }
            return StringUtil::Join(parts, " OR ");
        }
        default:
            return ""; // unsupported — skip this filter
    }
}

//===----------------------------------------------------------------------===//
// DuckdTransactionManager
//===----------------------------------------------------------------------===//

Transaction &DuckdTransactionManager::StartTransaction(ClientContext &context) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto txn = make_uniq<DuckdTransaction>(*this, context);
    auto *ptr = txn.get();
    active_transactions_.push_back(std::move(txn));
    return *ptr;
}

ErrorData DuckdTransactionManager::CommitTransaction(ClientContext &context,
                                                      Transaction &transaction) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = active_transactions_.begin(); it != active_transactions_.end(); ++it) {
        if (it->get() == &transaction) {
            active_transactions_.erase(it);
            break;
        }
    }
    return ErrorData();
}

void DuckdTransactionManager::RollbackTransaction(Transaction &transaction) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = active_transactions_.begin(); it != active_transactions_.end(); ++it) {
        if (it->get() == &transaction) {
            active_transactions_.erase(it);
            break;
        }
    }
}

//===----------------------------------------------------------------------===//
// DuckdScan – global state (streaming: holds an open FlightStreamReader)
//===----------------------------------------------------------------------===//

struct DuckdScanGlobalState : public GlobalTableFunctionState {
    // Streaming reader; yields one RecordBatch per Next() call
    std::unique_ptr<duckdb_client::DuckdQueryStream> stream;

    // Current batch being drained into DuckDB DataChunks
    std::shared_ptr<arrow::RecordBatch> current_batch;
    idx_t                               row_in_batch = 0;
    bool                                finished     = false;

    // output column i → Arrow batch column index (-1 = synthetic rowid)
    vector<int> output_to_arrow;

    idx_t MaxThreads() const override { return 1; }
};

//===----------------------------------------------------------------------===//
// DuckdScan – function callbacks
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState>
DuckdScanInitGlobal(ClientContext &ctx, TableFunctionInitInput &input) {
    auto &bd    = input.bind_data->Cast<DuckdScanBindData>();
    auto  state = make_uniq<DuckdScanGlobalState>();

    // Build projected column list
    vector<string> selected_cols;
    vector<int>    output_to_arrow;
    int            arrow_col_idx = 0;

    for (auto col_id : input.column_ids) {
        if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
            output_to_arrow.push_back(-1); // synthetic rowid
        } else {
            selected_cols.push_back(bd.column_names[col_id]);
            output_to_arrow.push_back(arrow_col_idx++);
        }
    }
    state->output_to_arrow = std::move(output_to_arrow);

    // Build WHERE clause from pushed-down filters
    vector<string> conditions;
    if (input.filters) {
        for (auto &[filter_idx, filter] : input.filters->filters) {
            if (filter_idx < input.column_ids.size()) {
                auto col_id = input.column_ids[filter_idx];
                if (col_id != COLUMN_IDENTIFIER_ROW_ID &&
                    col_id < (idx_t)bd.column_names.size()) {
                    string cond = FilterToSQL(*filter, bd.column_names[col_id]);
                    if (!cond.empty()) {
                        conditions.push_back(cond);
                    }
                }
            }
        }
    }

    // Construct SQL
    string select_list = selected_cols.empty()
                             ? "1"
                             : StringUtil::Join(selected_cols, ", ");
    string sql = "SELECT " + select_list +
                 " FROM " + bd.schema_name + "." + bd.table_name;
    if (!conditions.empty()) {
        sql += " WHERE " + StringUtil::Join(conditions, " AND ");
    }

    // Open a streaming scan — data is pulled batch-by-batch in DuckdScanFunction
    auto stream_res = bd.client->ExecuteQueryStream(sql);
    if (!stream_res.ok()) {
        throw IOException("duckd: scan failed: " + stream_res.status().ToString());
    }
    state->stream = stream_res.MoveValueUnsafe();

    // Pull the first batch so we know immediately if the result is empty
    auto first = state->stream->Next();
    if (!first.ok()) {
        throw IOException("duckd: scan read failed: " + first.status().ToString());
    }
    if (*first) {
        state->current_batch = std::move(*first);
    } else {
        state->finished = true;
    }

    return std::move(state);
}

static void DuckdScanFunction(ClientContext &ctx, TableFunctionInput &input,
                               DataChunk &output) {
    auto &state = input.global_state->Cast<DuckdScanGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    // Advance to the next batch if the current one is exhausted
    while (state.current_batch &&
           state.row_in_batch >= (idx_t)state.current_batch->num_rows()) {
        auto next = state.stream->Next();
        if (!next.ok()) {
            throw IOException("duckd: scan read failed: " + next.status().ToString());
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

    for (idx_t out_col = 0; out_col < output.data.size(); out_col++) {
        int arrow_col = state.output_to_arrow[out_col];
        if (arrow_col < 0) {
            // Synthetic rowid: fill sequentially
            auto *data = FlatVector::GetData<int64_t>(output.data[out_col]);
            for (idx_t i = 0; i < to_fill; i++) {
                data[i] = (int64_t)(state.row_in_batch + i);
            }
        } else {
            // Zero-copy columnar fill from Arrow buffer
            FillColumnFromArrow(output.data[out_col],
                                *batch.column(arrow_col),
                                (int64_t)state.row_in_batch,
                                to_fill);
        }
    }

    output.SetCardinality(to_fill);
    state.row_in_batch += to_fill;
}

//===----------------------------------------------------------------------===//
// DuckdTableEntry
//===----------------------------------------------------------------------===//

DuckdTableEntry::DuckdTableEntry(
    Catalog &catalog, SchemaCatalogEntry &schema,
    CreateTableInfo &info, string url, string schema_name,
    std::shared_ptr<duckdb_client::DuckdFlightClient> client)
    : TableCatalogEntry(catalog, schema, info)
    , url_(std::move(url))
    , schema_name_(std::move(schema_name))
    , client_(std::move(client)) {
}

DataTable &DuckdTableEntry::GetStorage() {
    throw NotImplementedException(
        "DuckD remote tables do not support local storage access. "
        "Use duckd_exec(url, sql) for DML operations.");
}

unique_ptr<BaseStatistics> DuckdTableEntry::GetStatistics(ClientContext &,
                                                           column_t) {
    return nullptr;
}

TableFunction DuckdTableEntry::GetScanFunction(ClientContext &context,
                                                unique_ptr<FunctionData> &bind_data) {
    // Populate bind data
    auto bd          = make_uniq<DuckdScanBindData>();
    bd->url          = url_;
    bd->schema_name  = schema_name_;
    bd->table_name   = name;
    bd->client       = client_;

    for (auto &col : columns.Physical()) {
        bd->column_names.push_back(col.Name());
        bd->column_types.push_back(col.Type());
    }
    bind_data = std::move(bd);

    // Build and return the scan TableFunction
    TableFunction fn("duckd_scan", {}, DuckdScanFunction, nullptr, DuckdScanInitGlobal);
    fn.projection_pushdown = true;
    fn.filter_pushdown     = true;
    return fn;
}

TableStorageInfo DuckdTableEntry::GetStorageInfo(ClientContext &) {
    TableStorageInfo info;
    info.cardinality = optional_idx(); // unknown
    return info;
}

unique_ptr<CatalogEntry> DuckdTableEntry::Copy(ClientContext &ctx) const {
    CreateTableInfo info;
    info.catalog = catalog.GetName();
    info.schema  = schema_name_;
    info.table   = name;
    for (auto &col : columns.Physical()) {
        info.columns.AddColumn(col.Copy());
    }
    for (auto &c : constraints) {
        info.constraints.push_back(c->Copy());
    }
    return make_uniq<DuckdTableEntry>(catalog, schema, info,
                                      url_, schema_name_, client_);
}

//===----------------------------------------------------------------------===//
// DuckdSchemaEntry helpers
//===----------------------------------------------------------------------===//

DuckdSchemaEntry::DuckdSchemaEntry(
    Catalog &catalog, CreateSchemaInfo &info,
    string url,
    std::shared_ptr<duckdb_client::DuckdFlightClient> client)
    : SchemaCatalogEntry(catalog, info)
    , url_(std::move(url))
    , client_(std::move(client)) {
}

// Internal helper: create a DuckdTableEntry from a remote Arrow schema.
// NOTE: does NOT acquire entry_mutex_; callers must hold it if needed.
static unique_ptr<DuckdTableEntry> FetchTableEntry(
    Catalog &catalog, SchemaCatalogEntry &schema,
    const string &schema_name, const string &table_name,
    const string &url,
    const std::shared_ptr<duckdb_client::DuckdFlightClient> &client) {

    // GetQuerySchema already appends " LIMIT 0" internally — do NOT add it here.
    string sql       = "SELECT * FROM " + schema_name + "." + table_name;
    auto schema_res  = client->GetQuerySchema(sql);
    if (!schema_res.ok()) {
        return nullptr;
    }
    auto &arrow_schema = schema_res.ValueUnsafe();

    CreateTableInfo info;
    info.catalog = catalog.GetName();
    info.schema  = schema_name;
    info.table   = table_name;
    for (int i = 0; i < arrow_schema->num_fields(); i++) {
        auto &field = arrow_schema->field(i);
        info.columns.AddColumn(
            ColumnDefinition(field->name(), ArrowToDuckDBType(*field->type())));
    }
    return make_uniq<DuckdTableEntry>(catalog, schema, info, url, schema_name, client);
}

optional_ptr<DuckdTableEntry> DuckdSchemaEntry::GetOrFetchTable(
    const string &table_name) {

    std::lock_guard<std::mutex> lock(entry_mutex_);

    auto it = table_cache_.find(table_name);
    if (it != table_cache_.end()) {
        return *it->second;
    }

    // Fetch from remote using the shared helper (does not re-acquire the lock)
    auto entry = FetchTableEntry(catalog, *this, name, table_name, url_, client_);
    if (!entry) {
        return nullptr;
    }
    auto *ptr = entry.get();
    table_cache_[table_name] = std::move(entry);
    return *ptr;
}

void DuckdSchemaEntry::PopulateTableCache() {
    // NOTE: entry_mutex_ is already held by the caller (Scan / GetOrFetchTable).
    if (cache_populated_) {
        return;
    }
    cache_populated_ = true;

    // Query the remote's information_schema for tables in this schema
    string sql = "SELECT table_name FROM information_schema.tables "
                 "WHERE table_schema = '" +
                 StringUtil::Replace(name, "'", "''") + "' ORDER BY table_name";

    auto result = client_->ExecuteQuery(sql);
    if (!result.ok()) {
        return;
    }

    for (auto &batch : result.ValueUnsafe().batches) {
        if (batch->num_columns() < 1) continue;
        auto &col = *batch->column(0);
        for (int64_t row = 0; row < batch->num_rows(); row++) {
            if (!col.IsNull(row)) {
                string tname = static_cast<const arrow::StringArray &>(col).GetString(row);
                if (table_cache_.find(tname) == table_cache_.end()) {
                    // Fetch directly without re-acquiring the lock
                    auto entry = FetchTableEntry(catalog, *this, name, tname, url_, client_);
                    if (entry) {
                        table_cache_[tname] = std::move(entry);
                    }
                }
            }
        }
    }
}

//===----------------------------------------------------------------------===//
// DuckdSchemaEntry – interface implementation
//===----------------------------------------------------------------------===//

optional_ptr<CatalogEntry> DuckdSchemaEntry::LookupEntry(
    CatalogTransaction transaction, const EntryLookupInfo &lookup_info) {

    if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY &&
        lookup_info.GetCatalogType() != CatalogType::VIEW_ENTRY) {
        return nullptr;
    }

    auto entry = GetOrFetchTable(lookup_info.GetEntryName());
    return optional_ptr<CatalogEntry>(entry.get());
}

void DuckdSchemaEntry::Scan(ClientContext &context, CatalogType type,
                             const std::function<void(CatalogEntry &)> &callback) {
    if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) {
        return;
    }
    std::lock_guard<std::mutex> lock(entry_mutex_);
    PopulateTableCache();
    for (auto &[name, entry] : table_cache_) {
        callback(*entry);
    }
}

void DuckdSchemaEntry::Scan(CatalogType type,
                             const std::function<void(CatalogEntry &)> &callback) {
    // Non-context version: enumerate cached entries (may be incomplete)
    if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) {
        return;
    }
    std::lock_guard<std::mutex> lock(entry_mutex_);
    for (auto &[name, entry] : table_cache_) {
        callback(*entry);
    }
}

optional_ptr<CatalogEntry> DuckdSchemaEntry::CreateTable(
    CatalogTransaction transaction, BoundCreateTableInfo &info) {

    // Forward CREATE TABLE to the remote server
    auto &create_info = info.Base();
    string sql        = create_info.ToString();
    auto   result     = client_->ExecuteUpdate(sql);
    if (!result.ok()) {
        throw IOException("duckd: CREATE TABLE failed: " + result.status().ToString());
    }

    // Invalidate cache so the new table shows up on next lookup
    {
        std::lock_guard<std::mutex> lock(entry_mutex_);
        cache_populated_ = false;
    }
    return nullptr;
}

void DuckdSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
    string sql;
    switch (info.type) {
        case CatalogType::TABLE_ENTRY:
            sql = "DROP TABLE";
            break;
        case CatalogType::VIEW_ENTRY:
            sql = "DROP VIEW";
            break;
        default:
            throw NotImplementedException(
                "duckd: DROP of this object type is not supported");
    }
    if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
        sql += " IF EXISTS";
    }
    sql += " " + name + "." + info.name;

    auto result = client_->ExecuteUpdate(sql);
    if (!result.ok()) {
        throw IOException("duckd: DROP failed: " + result.status().ToString());
    }
    std::lock_guard<std::mutex> lock(entry_mutex_);
    table_cache_.erase(info.name);
    cache_populated_ = false;
}

void DuckdSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
    throw NotImplementedException(
        "duckd: ALTER is not supported via the remote catalog. "
        "Use duckd_exec(url, sql) to run ALTER statements directly.");
}

// Unsupported object types
optional_ptr<CatalogEntry> DuckdSchemaEntry::CreateFunction(CatalogTransaction, CreateFunctionInfo &) {
    throw NotImplementedException("duckd: remote functions are not supported");
}
optional_ptr<CatalogEntry> DuckdSchemaEntry::CreateIndex(CatalogTransaction, CreateIndexInfo &, TableCatalogEntry &) {
    throw NotImplementedException("duckd: remote index creation is not supported");
}
optional_ptr<CatalogEntry> DuckdSchemaEntry::CreateView(CatalogTransaction, CreateViewInfo &info) {
    string sql    = info.ToString();
    auto   result = client_->ExecuteUpdate(sql);
    if (!result.ok()) {
        throw IOException("duckd: CREATE VIEW failed: " + result.status().ToString());
    }
    return nullptr;
}
optional_ptr<CatalogEntry> DuckdSchemaEntry::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
    throw NotImplementedException("duckd: remote sequences are not supported");
}
optional_ptr<CatalogEntry> DuckdSchemaEntry::CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) {
    throw NotImplementedException("duckd: remote table functions are not supported");
}
optional_ptr<CatalogEntry> DuckdSchemaEntry::CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) {
    throw NotImplementedException("duckd: remote copy functions are not supported");
}
optional_ptr<CatalogEntry> DuckdSchemaEntry::CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) {
    throw NotImplementedException("duckd: remote pragma functions are not supported");
}
optional_ptr<CatalogEntry> DuckdSchemaEntry::CreateCollation(CatalogTransaction, CreateCollationInfo &) {
    throw NotImplementedException("duckd: remote collations are not supported");
}
optional_ptr<CatalogEntry> DuckdSchemaEntry::CreateType(CatalogTransaction, CreateTypeInfo &) {
    throw NotImplementedException("duckd: remote types are not supported");
}

//===----------------------------------------------------------------------===//
// DuckdCatalog
//===----------------------------------------------------------------------===//

DuckdCatalog::DuckdCatalog(AttachedDatabase &db, string url)
    : Catalog(db)
    , url_(std::move(url)) {
    // Reuse a shared connection from the registry so that duckd_exec() and
    // the catalog target the same gRPC channel when the URL matches.
    client_ = duckdb_client::DuckdClientRegistry::Instance().GetOrCreate(url_);
}

void DuckdCatalog::Initialize(bool load_builtin) {
    // Nothing to initialize locally; remote catalog is the source of truth
}

optional_ptr<DuckdSchemaEntry> DuckdCatalog::GetOrCreateSchema(
    const string &schema_name) {

    std::lock_guard<std::mutex> lock(schema_mutex_);

    auto it = schema_cache_.find(schema_name);
    if (it != schema_cache_.end()) {
        return *it->second;
    }

    CreateSchemaInfo info;
    info.catalog = GetName();
    info.schema  = schema_name;
    auto entry   = make_uniq<DuckdSchemaEntry>(*this, info, url_, client_);
    auto *ptr    = entry.get();
    schema_cache_[schema_name] = std::move(entry);
    return *ptr;
}

optional_ptr<CatalogEntry> DuckdCatalog::CreateSchema(
    CatalogTransaction transaction, CreateSchemaInfo &info) {

    string sql    = "CREATE SCHEMA " + info.schema;
    auto   result = client_->ExecuteUpdate(sql);
    if (!result.ok()) {
        throw IOException("duckd: CREATE SCHEMA failed: " + result.status().ToString());
    }
    auto schema = GetOrCreateSchema(info.schema);
    return optional_ptr<CatalogEntry>(schema.get());
}

void DuckdCatalog::ScanSchemas(ClientContext &context,
                                std::function<void(SchemaCatalogEntry &)> callback) {
    // Query remote for available schemas
    auto result = client_->ExecuteQuery(
        "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name");
    if (!result.ok()) {
        return;
    }

    for (auto &batch : result.ValueUnsafe().batches) {
        if (batch->num_columns() < 1) continue;
        auto &col = *batch->column(0);
        for (int64_t row = 0; row < batch->num_rows(); row++) {
            if (!col.IsNull(row)) {
                string sname = static_cast<const arrow::StringArray &>(col).GetString(row);
                auto   entry = GetOrCreateSchema(sname);
                if (entry) {
                    callback(*entry);
                }
            }
        }
    }
}

optional_ptr<SchemaCatalogEntry> DuckdCatalog::LookupSchema(
    CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
    OnEntryNotFound if_not_found) {

    const auto &schema_name = schema_lookup.GetEntryName();

    // Check cache first
    {
        std::lock_guard<std::mutex> lock(schema_mutex_);
        auto it = schema_cache_.find(schema_name);
        if (it != schema_cache_.end()) {
            return *it->second;
        }
    }

    // Verify schema exists on remote
    string sql    = "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name = '" +
                    StringUtil::Replace(schema_name, "'", "''") + "' LIMIT 1";
    auto   result = client_->ExecuteQuery(sql);

    if (!result.ok() || result.ValueUnsafe().total_rows == 0) {
        if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
            throw CatalogException("Schema \"%s\" not found in remote catalog at %s",
                                   schema_name, url_);
        }
        return nullptr;
    }

    auto schema_entry = GetOrCreateSchema(schema_name);
    return optional_ptr<SchemaCatalogEntry>(schema_entry.get());
}

void DuckdCatalog::DropSchema(ClientContext &context, DropInfo &info) {
    string sql = "DROP SCHEMA ";
    if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
        sql += "IF EXISTS ";
    }
    sql       += info.name;
    auto result = client_->ExecuteUpdate(sql);
    if (!result.ok()) {
        throw IOException("duckd: DROP SCHEMA failed: " + result.status().ToString());
    }
    std::lock_guard<std::mutex> lock(schema_mutex_);
    schema_cache_.erase(info.name);
}

PhysicalOperator &DuckdCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &,
                                                   LogicalCreateTable &, PhysicalOperator &) {
    throw NotImplementedException(
        "duckd: CREATE TABLE AS SELECT is not supported via the remote catalog. "
        "Use duckd_exec(url, 'CREATE TABLE AS SELECT ...') instead.");
}

PhysicalOperator &DuckdCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &,
                                            LogicalInsert &, optional_ptr<PhysicalOperator>) {
    throw NotImplementedException(
        "duckd: INSERT via the remote catalog is not supported. "
        "Use duckd_exec(url, 'INSERT INTO ...') for DML operations.");
}

PhysicalOperator &DuckdCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &,
                                            LogicalDelete &, PhysicalOperator &) {
    throw NotImplementedException(
        "duckd: DELETE via the remote catalog is not supported. "
        "Use duckd_exec(url, 'DELETE FROM ...') for DML operations.");
}

PhysicalOperator &DuckdCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &,
                                            LogicalUpdate &, PhysicalOperator &) {
    throw NotImplementedException(
        "duckd: UPDATE via the remote catalog is not supported. "
        "Use duckd_exec(url, 'UPDATE ...') for DML operations.");
}

DatabaseSize DuckdCatalog::GetDatabaseSize(ClientContext &) {
    DatabaseSize size;
    size.total_blocks      = 0;
    size.block_size        = 0;
    size.free_blocks       = 0;
    size.used_blocks       = 0;
    size.bytes             = 0;
    size.wal_size          = 0;
    return size;
}

} // namespace duckdb
