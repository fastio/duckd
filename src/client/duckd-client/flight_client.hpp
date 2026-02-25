//===----------------------------------------------------------------------===//
//                         DuckD Client Extension
//
// src/client/duckd-client/flight_client.hpp
//
// Arrow Flight SQL client wrapper for connecting to duckd servers
//===----------------------------------------------------------------------===//

#pragma once

#include <arrow/flight/sql/client.h>
#include <arrow/flight/client.h>
#include <arrow/api.h>

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb_client {

namespace flight    = arrow::flight;
namespace flightsql = arrow::flight::sql;

//===----------------------------------------------------------------------===//
// Query result container
//===----------------------------------------------------------------------===//

struct DuckdQueryResult {
    std::shared_ptr<arrow::Schema>                    schema;
    std::vector<std::shared_ptr<arrow::RecordBatch>>  batches;
    int64_t                                           total_rows = 0;
};

//===----------------------------------------------------------------------===//
// DuckdFlightClient - thread-safe Arrow Flight SQL connection
//===----------------------------------------------------------------------===//

class DuckdFlightClient {
public:
    // Connect to a duckd server.  url must be grpc://host:port.
    static std::unique_ptr<DuckdFlightClient> Connect(const std::string& url);

    // Execute a SELECT query; returns all result batches.
    arrow::Result<DuckdQueryResult> ExecuteQuery(const std::string& sql);

    // Execute a SELECT query, calling callback once per batch (streaming).
    arrow::Result<int64_t> ExecuteQueryStreaming(
        const std::string& sql,
        std::function<arrow::Status(std::shared_ptr<arrow::RecordBatch>)> callback);

    // Execute a DML / DDL statement.  Returns the number of affected rows.
    arrow::Result<int64_t> ExecuteUpdate(const std::string& sql);

    // Get the Arrow schema for a query without fetching any data.
    arrow::Result<std::shared_ptr<arrow::Schema>> GetQuerySchema(const std::string& sql);

private:
    DuckdFlightClient() = default;

    std::unique_ptr<flightsql::FlightSqlClient> client_;
    flight::FlightCallOptions                   call_options_;
};

//===----------------------------------------------------------------------===//
// DuckdClientRegistry - per-URL connection cache
//
// Maintains one shared DuckdFlightClient per URL.  All callers (ATTACH catalog
// and duckd_exec scalar function) that target the same URL reuse the same
// underlying gRPC channel, avoiding repeated connection setup overhead.
//
// Thread-safety: all public methods are protected by an internal mutex.
//===----------------------------------------------------------------------===//

class DuckdClientRegistry {
public:
    // Global singleton.
    static DuckdClientRegistry &Instance();

    // Return an existing client for 'url', or create a new one.
    std::shared_ptr<DuckdFlightClient> GetOrCreate(const std::string &url);

    // Evict a client (e.g. after a connection error).  Next call to
    // GetOrCreate will open a fresh connection.
    void Evict(const std::string &url);

    // Execute fn(client) for the given url, retrying once after evicting
    // a stale connection if an IOException is thrown.
    template <typename F>
    auto WithReconnect(const std::string &url, F &&fn)
        -> decltype(fn(std::declval<DuckdFlightClient &>())) {
        try {
            return fn(*GetOrCreate(url));
        } catch (const std::exception &) {
            // Evict the potentially broken connection and retry once.
            Evict(url);
            return fn(*GetOrCreate(url));
        }
    }

private:
    DuckdClientRegistry() = default;

    std::mutex                                                  mutex_;
    std::unordered_map<std::string, std::shared_ptr<DuckdFlightClient>> clients_;
};

} // namespace duckdb_client
