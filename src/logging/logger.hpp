//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// logging/logger.hpp
//
// Logging utilities based on spdlog
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace duckd {

// Log levels (compatible with old interface)
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
    // Initialize logging system
    static void Initialize(const std::string& log_file = "",
                          const std::string& log_level = "info");

    // Shutdown logging system
    static void Shutdown();

    // Get the main logger instance
    static std::shared_ptr<spdlog::logger>& Get();

    // Set log level
    static void SetLevel(LogLevel level);
    static void SetLevel(const std::string& level);

    // Set output file (empty = console only)
    static void SetOutput(const std::string& file);

    // Flush all logs
    static void Flush();

    // Convert between log levels
    static spdlog::level::level_enum ToSpdlogLevel(LogLevel level);
    static spdlog::level::level_enum ToSpdlogLevel(const std::string& level);

private:
    static std::shared_ptr<spdlog::logger> logger_;
    static bool initialized_;
};

} // namespace duckd

// Convenience macros - compatible with old interface
// Old format: LOG_INFO("component", "message " + std::to_string(x))
// New format: LOG_INFO("component", "message {}", x)

#define LOG_TRACE(component, message) \
    do { \
        if (duckd::Logger::Get()->should_log(spdlog::level::trace)) \
            duckd::Logger::Get()->trace("[{}] {}", component, message); \
    } while(0)

#define LOG_DEBUG(component, message) \
    do { \
        if (duckd::Logger::Get()->should_log(spdlog::level::debug)) \
            duckd::Logger::Get()->debug("[{}] {}", component, message); \
    } while(0)

#define LOG_INFO(component, message) \
    do { \
        if (duckd::Logger::Get()->should_log(spdlog::level::info)) \
            duckd::Logger::Get()->info("[{}] {}", component, message); \
    } while(0)

#define LOG_WARN(component, message) \
    do { \
        if (duckd::Logger::Get()->should_log(spdlog::level::warn)) \
            duckd::Logger::Get()->warn("[{}] {}", component, message); \
    } while(0)

#define LOG_ERROR(component, message) \
    do { \
        if (duckd::Logger::Get()->should_log(spdlog::level::err)) \
            duckd::Logger::Get()->error("[{}] {}", component, message); \
    } while(0)

#define LOG_FATAL(component, message) \
    do { \
        if (duckd::Logger::Get()->should_log(spdlog::level::critical)) \
            duckd::Logger::Get()->critical("[{}] {}", component, message); \
    } while(0)

// New style macros with fmt format support
#define DLOG_TRACE(component, fmt, ...) \
    duckd::Logger::Get()->trace("[{}] " fmt, component, ##__VA_ARGS__)
#define DLOG_DEBUG(component, fmt, ...) \
    duckd::Logger::Get()->debug("[{}] " fmt, component, ##__VA_ARGS__)
#define DLOG_INFO(component, fmt, ...) \
    duckd::Logger::Get()->info("[{}] " fmt, component, ##__VA_ARGS__)
#define DLOG_WARN(component, fmt, ...) \
    duckd::Logger::Get()->warn("[{}] " fmt, component, ##__VA_ARGS__)
#define DLOG_ERROR(component, fmt, ...) \
    duckd::Logger::Get()->error("[{}] " fmt, component, ##__VA_ARGS__)
#define DLOG_FATAL(component, fmt, ...) \
    duckd::Logger::Get()->critical("[{}] " fmt, component, ##__VA_ARGS__)
