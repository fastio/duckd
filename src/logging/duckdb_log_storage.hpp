//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// logging/duckdb_log_storage.hpp
//
// Custom LogStorage to bridge DuckDB logs to spdlog
//===----------------------------------------------------------------------===//

#pragma once

#include <duckdb/logging/log_storage.hpp>
#include <spdlog/spdlog.h>

namespace duckd {

// Custom LogStorage that forwards DuckDB internal logs to spdlog
class SpdlogLogStorage : public duckdb::LogStorage {
public:
    explicit SpdlogLogStorage(std::shared_ptr<spdlog::logger> logger)
        : logger_(std::move(logger)) {}

    const std::string GetStorageName() override {
        return "spdlog";
    }

    void WriteLogEntry(duckdb::timestamp_t timestamp,
                       duckdb::LogLevel level,
                       const std::string& log_type,
                       const std::string& log_message,
                       const duckdb::RegisteredLoggingContext& context) override {
        if (!logger_) {
            return;
        }

        auto spdlog_level = ConvertLevel(level);
        logger_->log(spdlog_level, "[DuckDB:{}] {}", log_type, log_message);
    }

    void WriteLogEntries(duckdb::DataChunk& chunk,
                         const duckdb::RegisteredLoggingContext& context) override {
        // Batch write not implemented - individual entries are sufficient
    }

    void Flush(duckdb::LoggingTargetTable table) override {
        if (logger_) {
            logger_->flush();
        }
    }

    void FlushAll() override {
        if (logger_) {
            logger_->flush();
        }
    }

    bool IsEnabled(duckdb::LoggingTargetTable table) override {
        return table == duckdb::LoggingTargetTable::ALL_LOGS;
    }

private:
    static spdlog::level::level_enum ConvertLevel(duckdb::LogLevel level) {
        switch (level) {
            case duckdb::LogLevel::LOG_TRACE:   return spdlog::level::trace;
            case duckdb::LogLevel::LOG_DEBUG:   return spdlog::level::debug;
            case duckdb::LogLevel::LOG_INFO:    return spdlog::level::info;
            case duckdb::LogLevel::LOG_WARNING: return spdlog::level::warn;
            case duckdb::LogLevel::LOG_ERROR:   return spdlog::level::err;
            case duckdb::LogLevel::LOG_FATAL:   return spdlog::level::critical;
            default:                            return spdlog::level::info;
        }
    }

    std::shared_ptr<spdlog::logger> logger_;
};

// Helper function to register SpdlogLogStorage with DuckDB
inline bool RegisterSpdlogStorage(duckdb::DatabaseInstance& db,
                                   std::shared_ptr<spdlog::logger> logger) {
    auto storage = duckdb::make_shared_ptr<SpdlogLogStorage>(std::move(logger));
    duckdb::shared_ptr<duckdb::LogStorage> storage_ptr = storage;

    // Register the storage
    bool success = db.GetLogManager().RegisterLogStorage("spdlog", storage_ptr);
    if (!success) {
        return false;
    }

    // Set it as active storage
    db.GetLogManager().SetLogStorage(db, "spdlog");

    // Enable logging
    db.GetLogManager().SetEnableLogging(true);

    return true;
}

} // namespace duckd
