//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// executor/query_executor.hpp
//
// Query executor
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "session/session.hpp"
#include "protocol/message.hpp"
#include "duckdb.hpp"

namespace duckdb_server {

class ArrowSerializer;

struct QueryContext {
    uint64_t query_id;
    std::string sql;
    uint32_t timeout_ms;
    uint32_t max_rows;
    SessionPtr session;
    SendCallback send_callback;
    
    // Cancellation
    std::atomic<bool>* cancel_flag;
};

class QueryExecutor {
public:
    explicit QueryExecutor(std::shared_ptr<ArrowSerializer> serializer);
    ~QueryExecutor() = default;
    
    // Execute a query
    void Execute(QueryContext context);
    
    // Execute a prepared statement
    void ExecutePrepared(QueryContext context,
                        const std::string& statement_name,
                        duckdb::vector<duckdb::Value> parameters);

private:
    // Send query result
    void SendResult(duckdb::QueryResult& result, 
                   QueryContext& context);
    
    // Send error
    void SendError(ErrorCode code, 
                  const std::string& message,
                  QueryContext& context);
    
    // Check if cancelled
    bool IsCancelled(QueryContext& context);

private:
    std::shared_ptr<ArrowSerializer> serializer_;
};

} // namespace duckdb_server
