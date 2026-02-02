//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// utils/logger.cpp
//
// Logger implementation
//===----------------------------------------------------------------------===//

#include "utils/logger.hpp"
#include <chrono>

namespace duckdb_server {

Logger& Logger::Instance() {
    static Logger instance;
    return instance;
}

Logger::Logger()
    : level_(LogLevel::INFO)
    , use_file_(false) {
}

Logger::~Logger() {
    if (file_.is_open()) {
        file_.close();
    }
}

void Logger::SetLevel(LogLevel level) {
    level_ = level;
}

void Logger::SetLevel(const std::string& level) {
    std::string lower = level;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    
    if (lower == "trace") level_ = LogLevel::TRACE;
    else if (lower == "debug") level_ = LogLevel::DEBUG;
    else if (lower == "info") level_ = LogLevel::INFO;
    else if (lower == "warn" || lower == "warning") level_ = LogLevel::WARN;
    else if (lower == "error") level_ = LogLevel::ERROR;
    else if (lower == "fatal") level_ = LogLevel::FATAL;
}

void Logger::SetOutput(const std::string& file) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (file_.is_open()) {
        file_.close();
    }
    
    if (file.empty()) {
        use_file_ = false;
    } else {
        file_.open(file, std::ios::app);
        if (file_.is_open()) {
            use_file_ = true;
        } else {
            use_file_ = false;
            std::cerr << "Failed to open log file: " << file << std::endl;
        }
    }
}

void Logger::Trace(const std::string& component, const std::string& message) {
    Log(LogLevel::TRACE, component, message);
}

void Logger::Debug(const std::string& component, const std::string& message) {
    Log(LogLevel::DEBUG, component, message);
}

void Logger::Info(const std::string& component, const std::string& message) {
    Log(LogLevel::INFO, component, message);
}

void Logger::Warn(const std::string& component, const std::string& message) {
    Log(LogLevel::WARN, component, message);
}

void Logger::Error(const std::string& component, const std::string& message) {
    Log(LogLevel::ERROR, component, message);
}

void Logger::Fatal(const std::string& component, const std::string& message) {
    Log(LogLevel::FATAL, component, message);
}

void Logger::Log(LogLevel level, const std::string& component, const std::string& message) {
    if (level < level_) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::ostringstream oss;
    oss << "[" << FormatTimestamp() << "] "
        << "[" << LevelToString(level) << "] "
        << "[" << component << "] "
        << message;
    
    std::string line = oss.str();
    
    if (use_file_ && file_.is_open()) {
        file_ << line << std::endl;
        file_.flush();
    } else {
        if (level >= LogLevel::WARN) {
            std::cerr << line << std::endl;
        } else {
            std::cout << line << std::endl;
        }
    }
}

std::string Logger::FormatTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::tm tm;
#ifdef _WIN32
    localtime_s(&tm, &time);
#else
    localtime_r(&time, &tm);
#endif
    
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S")
        << "." << std::setfill('0') << std::setw(3) << ms.count();
    
    return oss.str();
}

const char* Logger::LevelToString(LogLevel level) {
    switch (level) {
        case LogLevel::TRACE: return "TRACE";
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO ";
        case LogLevel::WARN:  return "WARN ";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
        default: return "?????";
    }
}

} // namespace duckdb_server
