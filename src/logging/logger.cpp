//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// logging/logger.cpp
//
// Logger implementation using spdlog
//===----------------------------------------------------------------------===//

#include "logging/logger.hpp"
#include <algorithm>
#include <vector>

namespace duckd {

std::shared_ptr<spdlog::logger> Logger::logger_;
bool Logger::initialized_ = false;

void Logger::Initialize(const std::string& log_file, const std::string& log_level) {
    if (initialized_) {
        return;
    }

    std::vector<spdlog::sink_ptr> sinks;

    // Always add console sink with colors
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");
    sinks.push_back(console_sink);

    // Add file sink if specified
    if (!log_file.empty()) {
        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            log_file,
            100 * 1024 * 1024,  // 100 MB max file size
            3                    // Keep 3 rotated files
        );
        file_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
        sinks.push_back(file_sink);
    }

    // Create multi-sink logger
    logger_ = std::make_shared<spdlog::logger>("duckd", sinks.begin(), sinks.end());

    // Set log level
    logger_->set_level(ToSpdlogLevel(log_level));

    // Flush on warn and above
    logger_->flush_on(spdlog::level::warn);

    // Register as default logger
    spdlog::set_default_logger(logger_);

    initialized_ = true;
}

void Logger::Shutdown() {
    if (logger_) {
        logger_->flush();
    }
    spdlog::shutdown();
    initialized_ = false;
}

std::shared_ptr<spdlog::logger>& Logger::Get() {
    if (!initialized_) {
        // Auto-initialize with defaults if not initialized
        Initialize();
    }
    return logger_;
}

void Logger::SetLevel(LogLevel level) {
    if (logger_) {
        logger_->set_level(ToSpdlogLevel(level));
    }
}

void Logger::SetLevel(const std::string& level) {
    if (logger_) {
        logger_->set_level(ToSpdlogLevel(level));
    }
}

void Logger::SetOutput(const std::string& file) {
    // Re-initialize with new file output
    bool was_initialized = initialized_;
    auto current_level = logger_ ? logger_->level() : spdlog::level::info;

    if (was_initialized) {
        Shutdown();
    }

    Initialize(file, spdlog::level::to_string_view(current_level).data());
}

void Logger::Flush() {
    if (logger_) {
        logger_->flush();
    }
}

spdlog::level::level_enum Logger::ToSpdlogLevel(LogLevel level) {
    switch (level) {
        case LogLevel::TRACE: return spdlog::level::trace;
        case LogLevel::DEBUG: return spdlog::level::debug;
        case LogLevel::INFO:  return spdlog::level::info;
        case LogLevel::WARN:  return spdlog::level::warn;
        case LogLevel::ERROR: return spdlog::level::err;
        case LogLevel::FATAL: return spdlog::level::critical;
        default:              return spdlog::level::info;
    }
}

spdlog::level::level_enum Logger::ToSpdlogLevel(const std::string& level) {
    std::string lower = level;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    if (lower == "trace") return spdlog::level::trace;
    if (lower == "debug") return spdlog::level::debug;
    if (lower == "info")  return spdlog::level::info;
    if (lower == "warn" || lower == "warning") return spdlog::level::warn;
    if (lower == "error") return spdlog::level::err;
    if (lower == "fatal" || lower == "critical") return spdlog::level::critical;

    return spdlog::level::info;
}

} // namespace duckd
