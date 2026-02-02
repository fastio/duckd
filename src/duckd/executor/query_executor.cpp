//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// executor/query_executor.cpp
//
// Query executor implementation
//===----------------------------------------------------------------------===//

#include "executor/query_executor.hpp"
#include "serialization/arrow_serializer.hpp"
#include "utils/logger.hpp"

namespace duckdb_server {

QueryExecutor::QueryExecutor(std::shared_ptr<ArrowSerializer> serializer)
    : serializer_(serializer) {
}

void QueryExecutor::Execute(QueryContext context) {
    auto start_time = Clock::now();
    
    try {
        // Execute query using Query() for materialized results
        // (SendQuery returns streaming results that can have thread-safety issues)
        auto result = context.session->GetConnection().Query(context.sql);
        
        if (result->HasError()) {
            SendError(ErrorCode::INVALID_SQL, result->GetError(), context);
            return;
        }
        
        // Send result
        SendResult(*result, context);
        
        auto end_time = Clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        
        LOG_DEBUG("query_executor", "Query " + std::to_string(context.query_id) + 
                  " completed in " + std::to_string(duration_ms) + "ms");
        
    } catch (const std::exception& e) {
        LOG_ERROR("query_executor", "Query " + std::to_string(context.query_id) + 
                  " failed: " + e.what());
        SendError(ErrorCode::INTERNAL_ERROR, e.what(), context);
    }
}

void QueryExecutor::ExecutePrepared(QueryContext context,
                                   const std::string& statement_name,
                                   duckdb::vector<duckdb::Value> parameters) {
    auto start_time = Clock::now();
    
    try {
        // Get prepared statement
        auto* stmt = context.session->GetPreparedStatement(statement_name);
        if (!stmt) {
            SendError(ErrorCode::INVALID_PARAMETER, 
                     "Prepared statement not found: " + statement_name, 
                     context);
            return;
        }
        
        // Execute with parameters
        auto result = stmt->Execute(parameters);
        
        if (result->HasError()) {
            SendError(ErrorCode::INVALID_SQL, result->GetError(), context);
            return;
        }
        
        // Send result
        SendResult(*result, context);
        
        auto end_time = Clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        
        LOG_DEBUG("query_executor", "Prepared query " + std::to_string(context.query_id) + 
                  " completed in " + std::to_string(duration_ms) + "ms");
        
    } catch (const std::exception& e) {
        LOG_ERROR("query_executor", "Prepared query " + std::to_string(context.query_id) + 
                  " failed: " + e.what());
        SendError(ErrorCode::INTERNAL_ERROR, e.what(), context);
    }
}

void QueryExecutor::SendResult(duckdb::QueryResult& result, QueryContext& context) {
    auto start_time = Clock::now();
    uint64_t total_rows = 0;
    
    // Send header (schema)
    auto header_data = serializer_->SerializeResultHeader(result);
    Message header_msg(MessageType::RESULT_HEADER, std::move(header_data));
    context.send_callback(header_msg.Serialize());
    
    // Send data batches
    std::unique_ptr<duckdb::DataChunk> chunk;
    while ((chunk = result.Fetch()) != nullptr) {
        // Check for cancellation
        if (IsCancelled(context)) {
            SendError(ErrorCode::QUERY_CANCELLED, "Query cancelled", context);
            return;
        }
        
        // Check max rows
        if (context.max_rows > 0 && total_rows >= context.max_rows) {
            break;
        }
        
        // Serialize and send batch
        auto batch_data = serializer_->SerializeResultBatch(*chunk, result);
        Message batch_msg(MessageType::RESULT_BATCH, std::move(batch_data));
        context.send_callback(batch_msg.Serialize());
        
        total_rows += chunk->size();
    }
    
    // Send end
    auto end_time = Clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time).count();
    
    ResultEndPayload end_payload;
    end_payload.total_rows = total_rows;
    end_payload.execution_time_us = duration_us;
    end_payload.bytes_scanned = 0;  // TODO: Get from result
    
    Message end_msg(MessageType::RESULT_END, end_payload.Serialize());
    context.send_callback(end_msg.Serialize());
}

void QueryExecutor::SendError(ErrorCode code,
                             const std::string& message,
                             QueryContext& context) {
    QueryErrorPayload error(code, message);
    Message error_msg(MessageType::QUERY_ERROR, error.Serialize());
    context.send_callback(error_msg.Serialize());
}

bool QueryExecutor::IsCancelled(QueryContext& context) {
    if (context.cancel_flag && *context.cancel_flag) {
        return true;
    }
    if (context.session && context.session->IsCancelRequested()) {
        return true;
    }
    return false;
}

} // namespace duckdb_server
