//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/flight/flight_arrow_bridge.hpp
//
// DuckDB DataChunk → Arrow C++ RecordBatch bridge (zero-copy)
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/client_context.hpp"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

#include <cstring>

namespace duckdb_server {

// Get ClientProperties from a DuckDB Connection
inline duckdb::ClientProperties GetClientProps(duckdb::Connection& conn) {
    return conn.context->GetClientProperties();
}

// Convert DuckDB types + names to Arrow C++ Schema via Arrow C Data Interface
inline arrow::Result<std::shared_ptr<arrow::Schema>> MakeArrowSchema(
    const duckdb::vector<duckdb::LogicalType>& types,
    const duckdb::vector<duckdb::string>& names,
    duckdb::ClientProperties& props) {

    ArrowSchema c_schema;
    c_schema.Init();

    duckdb::ArrowConverter::ToArrowSchema(&c_schema, types, names, props);

    auto result = arrow::ImportSchema(&c_schema);
    return result;
}

// Convert a DuckDB DataChunk to Arrow RecordBatch (zero-copy via C Data Interface)
inline arrow::Result<std::shared_ptr<arrow::RecordBatch>> ChunkToRecordBatch(
    duckdb::DataChunk& chunk,
    const std::shared_ptr<arrow::Schema>& schema,
    duckdb::ClientProperties& props) {

    ArrowArray c_array;
    c_array.Init();

    std::unordered_map<duckdb::idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>> ext_types;
    duckdb::ArrowConverter::ToArrowArray(chunk, &c_array, props, ext_types);

    return arrow::ImportRecordBatch(&c_array, schema);
}

// Convert an Arrow array element to a DuckDB Value
inline duckdb::Value ArrowScalarToDuckDBValue(
    const std::shared_ptr<arrow::Array>& array, int64_t index) {
    if (array->IsNull(index)) {
        return duckdb::Value();
    }

    switch (array->type_id()) {
        case arrow::Type::BOOL:
            return duckdb::Value::BOOLEAN(
                std::static_pointer_cast<arrow::BooleanArray>(array)->Value(index));
        case arrow::Type::INT8:
            return duckdb::Value::TINYINT(
                std::static_pointer_cast<arrow::Int8Array>(array)->Value(index));
        case arrow::Type::INT16:
            return duckdb::Value::SMALLINT(
                std::static_pointer_cast<arrow::Int16Array>(array)->Value(index));
        case arrow::Type::INT32:
            return duckdb::Value::INTEGER(
                std::static_pointer_cast<arrow::Int32Array>(array)->Value(index));
        case arrow::Type::INT64:
            return duckdb::Value::BIGINT(
                std::static_pointer_cast<arrow::Int64Array>(array)->Value(index));
        case arrow::Type::UINT8:
            return duckdb::Value::UTINYINT(
                std::static_pointer_cast<arrow::UInt8Array>(array)->Value(index));
        case arrow::Type::UINT16:
            return duckdb::Value::USMALLINT(
                std::static_pointer_cast<arrow::UInt16Array>(array)->Value(index));
        case arrow::Type::UINT32:
            return duckdb::Value::UINTEGER(
                std::static_pointer_cast<arrow::UInt32Array>(array)->Value(index));
        case arrow::Type::UINT64:
            return duckdb::Value::UBIGINT(
                std::static_pointer_cast<arrow::UInt64Array>(array)->Value(index));
        case arrow::Type::FLOAT:
            return duckdb::Value::FLOAT(
                std::static_pointer_cast<arrow::FloatArray>(array)->Value(index));
        case arrow::Type::DOUBLE:
            return duckdb::Value::DOUBLE(
                std::static_pointer_cast<arrow::DoubleArray>(array)->Value(index));
        case arrow::Type::STRING:
            return duckdb::Value(
                std::static_pointer_cast<arrow::StringArray>(array)->GetString(index));
        case arrow::Type::LARGE_STRING:
            return duckdb::Value(
                std::static_pointer_cast<arrow::LargeStringArray>(array)->GetString(index));
        case arrow::Type::BINARY:
            return duckdb::Value::BLOB(
                std::static_pointer_cast<arrow::BinaryArray>(array)->GetString(index));
        case arrow::Type::LARGE_BINARY:
            return duckdb::Value::BLOB(
                std::static_pointer_cast<arrow::LargeBinaryArray>(array)->GetString(index));
        case arrow::Type::DATE32:
            return duckdb::Value::DATE(duckdb::date_t(
                std::static_pointer_cast<arrow::Date32Array>(array)->Value(index)));
        case arrow::Type::DATE64: {
            auto ms = std::static_pointer_cast<arrow::Date64Array>(array)->Value(index);
            return duckdb::Value::DATE(duckdb::date_t(static_cast<int32_t>(ms / 86400000LL)));
        }
        case arrow::Type::TIME32: {
            auto typed = std::static_pointer_cast<arrow::Time32Array>(array);
            auto unit = static_cast<const arrow::Time32Type&>(*array->type()).unit();
            int64_t factor = (unit == arrow::TimeUnit::SECOND) ? 1'000'000LL : 1'000LL;
            return duckdb::Value::TIME(
                duckdb::dtime_t(static_cast<int64_t>(typed->Value(index)) * factor));
        }
        case arrow::Type::TIME64: {
            auto v = std::static_pointer_cast<arrow::Time64Array>(array)->Value(index);
            auto unit = static_cast<const arrow::Time64Type&>(*array->type()).unit();
            if (unit == arrow::TimeUnit::NANO) {
                v /= 1000; // nanoseconds → microseconds
            }
            return duckdb::Value::TIME(duckdb::dtime_t(v));
        }
        case arrow::Type::TIMESTAMP: {
            auto v = std::static_pointer_cast<arrow::TimestampArray>(array)->Value(index);
            auto unit = static_cast<const arrow::TimestampType&>(*array->type()).unit();
            switch (unit) {
                case arrow::TimeUnit::SECOND: v *= 1'000'000LL; break;
                case arrow::TimeUnit::MILLI:  v *= 1'000LL;     break;
                case arrow::TimeUnit::MICRO:                     break;
                case arrow::TimeUnit::NANO:   v /= 1'000LL;     break;
            }
            return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(v));
        }
        case arrow::Type::DECIMAL128: {
            auto typed = std::static_pointer_cast<arrow::Decimal128Array>(array);
            auto& dec_type = static_cast<const arrow::Decimal128Type&>(*array->type());
            const uint8_t* raw = typed->Value(index);
            uint64_t low_bits;
            int64_t  high_bits;
            std::memcpy(&low_bits,  raw,     8);
            std::memcpy(&high_bits, raw + 8, 8);
            duckdb::hugeint_t hv;
            hv.lower = low_bits;
            hv.upper = high_bits;
            return duckdb::Value::DECIMAL(hv, dec_type.precision(), dec_type.scale());
        }
        default: {
            // Fallback: use Arrow's scalar ToString conversion
            auto scalar_result = array->GetScalar(index);
            if (scalar_result.ok()) {
                return duckdb::Value(scalar_result.ValueUnsafe()->ToString());
            }
            return duckdb::Value();
        }
    }
}

// RecordBatchReader that wraps a DuckDB QueryResult for streaming.
// Optionally holds a PooledConnection to keep it alive for streaming results.
class DuckDBRecordBatchReader : public arrow::RecordBatchReader {
public:
    // Constructor with externally-provided ClientProperties
    DuckDBRecordBatchReader(
        std::unique_ptr<duckdb::QueryResult> result,
        std::shared_ptr<arrow::Schema> schema,
        duckdb::ClientProperties props)
        : result_(std::move(result))
        , schema_(std::move(schema))
        , props_(std::move(props)) {}

    // Constructor that also holds a connection alive for streaming results
    DuckDBRecordBatchReader(
        std::unique_ptr<duckdb::QueryResult> result,
        std::shared_ptr<arrow::Schema> schema,
        std::unique_ptr<duckdb::Connection> connection)
        : result_(std::move(result))
        , schema_(std::move(schema))
        , connection_(std::move(connection))
        , props_(GetClientProps(*connection_)) {}

    // Constructor that borrows a shared transaction connection
    DuckDBRecordBatchReader(
        std::unique_ptr<duckdb::QueryResult> result,
        std::shared_ptr<arrow::Schema> schema,
        std::shared_ptr<duckdb::Connection> shared_connection)
        : result_(std::move(result))
        , schema_(std::move(schema))
        , shared_conn_(std::move(shared_connection))
        , props_(GetClientProps(*shared_conn_)) {}

    std::shared_ptr<arrow::Schema> schema() const override {
        return schema_;
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        auto chunk = result_->Fetch();
        if (!chunk || chunk->size() == 0) {
            *out = nullptr;
            return arrow::Status::OK();
        }

        auto batch_result = ChunkToRecordBatch(*chunk, schema_, props_);
        if (!batch_result.ok()) {
            return batch_result.status();
        }
        *out = batch_result.MoveValueUnsafe();
        return arrow::Status::OK();
    }

private:
    std::unique_ptr<duckdb::QueryResult> result_;
    std::shared_ptr<arrow::Schema> schema_;
    std::unique_ptr<duckdb::Connection> connection_;        // owned connection (non-transaction)
    std::shared_ptr<duckdb::Connection> shared_conn_;       // shared transaction connection
    duckdb::ClientProperties props_;
};

} // namespace duckdb_server
