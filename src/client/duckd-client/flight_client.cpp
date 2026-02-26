//===----------------------------------------------------------------------===//
//                         DuckD Client Extension
//
// src/client/duckd-client/flight_client.cpp
//
// Arrow Flight SQL client implementation
//===----------------------------------------------------------------------===//

#include "flight_client.hpp"

#include <stdexcept>

namespace duckdb_client {

//===----------------------------------------------------------------------===//
// Connect
//===----------------------------------------------------------------------===//

std::unique_ptr<DuckdFlightClient> DuckdFlightClient::Connect(const std::string& url) {
    auto loc = flight::Location::Parse(url);
    if (!loc.ok()) {
        throw std::runtime_error("Invalid duckd URL '" + url + "': " + loc.status().ToString());
    }

    auto fc = flight::FlightClient::Connect(loc.MoveValueUnsafe());
    if (!fc.ok()) {
        throw std::runtime_error(
            "Cannot connect to duckd at '" + url + "': " + fc.status().ToString());
    }

    auto inst       = std::unique_ptr<DuckdFlightClient>(new DuckdFlightClient());
    inst->client_   = std::make_unique<flightsql::FlightSqlClient>(fc.MoveValueUnsafe());
    return inst;
}

//===----------------------------------------------------------------------===//
// ExecuteQuery
//===----------------------------------------------------------------------===//

arrow::Result<DuckdQueryResult> DuckdFlightClient::ExecuteQuery(const std::string& sql) {
    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(call_options_, sql));

    DuckdQueryResult result;
    for (const auto& ep : info->endpoints()) {
        ARROW_ASSIGN_OR_RAISE(auto stream, client_->DoGet(call_options_, ep.ticket));

        if (!result.schema) {
            ARROW_ASSIGN_OR_RAISE(result.schema, stream->GetSchema());
        }

        while (true) {
            ARROW_ASSIGN_OR_RAISE(auto chunk, stream->Next());
            if (!chunk.data) break;
            result.total_rows += chunk.data->num_rows();
            result.batches.push_back(std::move(chunk.data));
        }
    }
    return result;
}

//===----------------------------------------------------------------------===//
// DuckdQueryStream::Next
//===----------------------------------------------------------------------===//

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DuckdQueryStream::Next() {
    while (true) {
        if (current_stream_) {
            ARROW_ASSIGN_OR_RAISE(auto chunk, current_stream_->Next());
            if (chunk.data) {
                return chunk.data;
            }
            // Current endpoint exhausted; move to next
            current_stream_.reset();
        }

        if (ep_idx_ >= endpoints_.size()) {
            return nullptr; // all endpoints consumed
        }

        ARROW_ASSIGN_OR_RAISE(current_stream_,
            sql_client_->DoGet(call_options_, endpoints_[ep_idx_++].ticket));

        // Populate schema from the first endpoint if not yet set
        if (!schema_) {
            ARROW_ASSIGN_OR_RAISE(schema_, current_stream_->GetSchema());
        }
    }
}

//===----------------------------------------------------------------------===//
// ExecuteQueryStream
//===----------------------------------------------------------------------===//

arrow::Result<std::unique_ptr<DuckdQueryStream>>
DuckdFlightClient::ExecuteQueryStream(const std::string& sql) {
    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(call_options_, sql));

    auto stream = std::unique_ptr<DuckdQueryStream>(new DuckdQueryStream());
    stream->client_owner_ = shared_from_this(); // keep *this alive for stream lifetime
    stream->endpoints_   = info->endpoints();
    stream->sql_client_  = client_.get();
    stream->call_options_ = call_options_;

    // Open the first endpoint immediately so the schema is available before
    // the caller starts pulling batches.
    if (!stream->endpoints_.empty()) {
        ARROW_ASSIGN_OR_RAISE(stream->current_stream_,
            client_->DoGet(call_options_, stream->endpoints_[0].ticket));
        stream->ep_idx_ = 1;
        ARROW_ASSIGN_OR_RAISE(stream->schema_, stream->current_stream_->GetSchema());
    }

    return std::move(stream);
}

//===----------------------------------------------------------------------===//
// ExecuteQueryStreaming
//===----------------------------------------------------------------------===//

arrow::Result<int64_t> DuckdFlightClient::ExecuteQueryStreaming(
    const std::string& sql,
    std::function<arrow::Status(std::shared_ptr<arrow::RecordBatch>)> callback) {

    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(call_options_, sql));

    int64_t total = 0;
    for (const auto& ep : info->endpoints()) {
        ARROW_ASSIGN_OR_RAISE(auto stream, client_->DoGet(call_options_, ep.ticket));

        while (true) {
            ARROW_ASSIGN_OR_RAISE(auto chunk, stream->Next());
            if (!chunk.data) break;
            total += chunk.data->num_rows();
            ARROW_RETURN_NOT_OK(callback(std::move(chunk.data)));
        }
    }
    return total;
}

//===----------------------------------------------------------------------===//
// ExecuteUpdate
//===----------------------------------------------------------------------===//

arrow::Result<int64_t> DuckdFlightClient::ExecuteUpdate(const std::string& sql) {
    return client_->ExecuteUpdate(call_options_, sql);
}

//===----------------------------------------------------------------------===//
// GetQuerySchema
//===----------------------------------------------------------------------===//

arrow::Result<std::shared_ptr<arrow::Schema>> DuckdFlightClient::GetQuerySchema(
    const std::string& sql) {

    // Wrap in a subquery so that any trailing line comment in `sql` cannot
    // accidentally consume the LIMIT 0 token (e.g. "SELECT ... -- comment").
    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(call_options_,
        "SELECT * FROM (\n" + sql + "\n) AS _schema_probe LIMIT 0"));
    if (info->endpoints().empty()) {
        return arrow::Status::IOError("No endpoints returned for schema query: " + sql);
    }

    ARROW_ASSIGN_OR_RAISE(auto stream,
        client_->DoGet(call_options_, info->endpoints()[0].ticket));
    return stream->GetSchema();
}

//===----------------------------------------------------------------------===//
// Transaction management
//===----------------------------------------------------------------------===//

arrow::Result<flightsql::Transaction> DuckdFlightClient::BeginTransaction() {
    return client_->BeginTransaction(call_options_);
}

arrow::Status DuckdFlightClient::CommitTransaction(const flightsql::Transaction& txn) {
    return client_->Commit(call_options_, txn);
}

arrow::Status DuckdFlightClient::RollbackTransaction(const flightsql::Transaction& txn) {
    return client_->Rollback(call_options_, txn);
}

arrow::Result<std::unique_ptr<DuckdQueryStream>>
DuckdFlightClient::ExecuteQueryStream(const std::string& sql,
                                      const flightsql::Transaction& txn) {
    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(call_options_, sql, txn));

    auto stream = std::unique_ptr<DuckdQueryStream>(new DuckdQueryStream());
    stream->client_owner_ = shared_from_this(); // keep *this alive for stream lifetime
    stream->endpoints_    = info->endpoints();
    stream->sql_client_   = client_.get();
    stream->call_options_ = call_options_;

    if (!stream->endpoints_.empty()) {
        ARROW_ASSIGN_OR_RAISE(stream->current_stream_,
            client_->DoGet(call_options_, stream->endpoints_[0].ticket));
        stream->ep_idx_ = 1;
        ARROW_ASSIGN_OR_RAISE(stream->schema_, stream->current_stream_->GetSchema());
    }

    return std::move(stream);
}

arrow::Result<int64_t> DuckdFlightClient::ExecuteUpdate(const std::string& sql,
                                                         const flightsql::Transaction& txn) {
    return client_->ExecuteUpdate(call_options_, sql, txn);
}

//===----------------------------------------------------------------------===//
// DuckdClientRegistry
//===----------------------------------------------------------------------===//

DuckdClientRegistry &DuckdClientRegistry::Instance() {
    static DuckdClientRegistry instance;
    return instance;
}

std::shared_ptr<DuckdFlightClient> DuckdClientRegistry::GetOrCreate(
    const std::string &url) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = clients_.find(url);
    if (it != clients_.end()) {
        return it->second;
    }
    // Connect() throws on failure – let exception propagate to caller.
    auto client = DuckdFlightClient::Connect(url);
    auto shared = std::shared_ptr<DuckdFlightClient>(std::move(client));
    clients_[url] = shared;
    return shared;
}

void DuckdClientRegistry::Evict(const std::string &url) {
    std::lock_guard<std::mutex> lock(mutex_);
    clients_.erase(url);
}

} // namespace duckdb_client
