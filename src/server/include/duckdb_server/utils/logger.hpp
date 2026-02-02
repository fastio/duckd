//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// duckdb_server/utils/logger.hpp
//
// Logging utilities
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_server/common.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <ctime>

namespace duckdb_server {

enum class LogLevel {
    TRACE = 0,
    DEBUG = 1,
    INFO = 2,
    WARN = 3,
    ERROR = 4,
    FATAL = 5
};

class Logger {
public:
    static Logger& Instance();
    
    // Set log level
    void SetLevel(LogLevel level);
    void SetLevel(const std::string& level);
    
    // Set output file (empty = stdout)
    void SetOutput(const std::string& file);
    
    // Log methods
    void Trace(const std::string& component, const std::string& message);
    void Debug(const std::string& component, const std::string& message);
    void Info(const std::string& component, const std::string& message);
    void Warn(const std::string& component, const std::string& message);
    void Error(const std::string& component, const std::string& message);
    void Fatal(const std::string& component, const std::string& message);
    
    // Generic log
    void Log(LogLevel level, const std::string& component, const std::string& message);
    
    // Get current level
    LogLevel GetLevel() const { return level_; }

private:
    Logger();
    ~Logger();
    
    // Non-copyable
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    
    // Format timestamp
    std::string FormatTimestamp();
    
    // Level to string
    const char* LevelToString(LogLevel level);

private:
    LogLevel level_;
    std::mutex mutex_;
    std::ofstream file_;
    bool use_file_;
};

// Convenience macros
#define LOG_TRACE(component, message) \
    duckdb_server::Logger::Instance().Trace(component, message)
#define LOG_DEBUG(component, message) \
    duckdb_server::Logger::Instance().Debug(component, message)
#define LOG_INFO(component, message) \
    duckdb_server::Logger::Instance().Info(component, message)
#define LOG_WARN(component, message) \
    duckdb_server::Logger::Instance().Warn(component, message)
#define LOG_ERROR(component, message) \
    duckdb_server::Logger::Instance().Error(component, message)
#define LOG_FATAL(component, message) \
    duckdb_server::Logger::Instance().Fatal(component, message)

} // namespace duckdb_server
