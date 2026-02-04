//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/logging/test_logger.cpp
//
// Unit tests for Logger and DuckDB log bridging
//===----------------------------------------------------------------------===//

#include "logging/logger.hpp"

// DuckDB includes required for log storage testing
#include <duckdb/main/database.hpp>
#include <duckdb/logging/log_manager.hpp>
#include <duckdb/logging/log_storage.hpp>

#include "logging/duckdb_log_storage.hpp"
#include <cassert>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <thread>
#include <chrono>

using namespace duckd;

//===----------------------------------------------------------------------===//
// Log Level Conversion Tests
//===----------------------------------------------------------------------===//

void TestLogLevelEnumConversion() {
    std::cout << "  Testing LogLevel enum to spdlog conversion..." << std::endl;

    assert(Logger::ToSpdlogLevel(LogLevel::TRACE) == spdlog::level::trace);
    assert(Logger::ToSpdlogLevel(LogLevel::DEBUG) == spdlog::level::debug);
    assert(Logger::ToSpdlogLevel(LogLevel::INFO) == spdlog::level::info);
    assert(Logger::ToSpdlogLevel(LogLevel::WARN) == spdlog::level::warn);
    assert(Logger::ToSpdlogLevel(LogLevel::ERROR) == spdlog::level::err);
    assert(Logger::ToSpdlogLevel(LogLevel::FATAL) == spdlog::level::critical);

    std::cout << "    PASSED" << std::endl;
}

void TestLogLevelStringConversion() {
    std::cout << "  Testing string to spdlog level conversion..." << std::endl;

    // Standard level names
    assert(Logger::ToSpdlogLevel("trace") == spdlog::level::trace);
    assert(Logger::ToSpdlogLevel("debug") == spdlog::level::debug);
    assert(Logger::ToSpdlogLevel("info") == spdlog::level::info);
    assert(Logger::ToSpdlogLevel("warn") == spdlog::level::warn);
    assert(Logger::ToSpdlogLevel("error") == spdlog::level::err);
    assert(Logger::ToSpdlogLevel("fatal") == spdlog::level::critical);

    // Case insensitive
    assert(Logger::ToSpdlogLevel("TRACE") == spdlog::level::trace);
    assert(Logger::ToSpdlogLevel("DEBUG") == spdlog::level::debug);
    assert(Logger::ToSpdlogLevel("INFO") == spdlog::level::info);
    assert(Logger::ToSpdlogLevel("WARN") == spdlog::level::warn);
    assert(Logger::ToSpdlogLevel("ERROR") == spdlog::level::err);
    assert(Logger::ToSpdlogLevel("FATAL") == spdlog::level::critical);

    // Mixed case
    assert(Logger::ToSpdlogLevel("Trace") == spdlog::level::trace);
    assert(Logger::ToSpdlogLevel("Debug") == spdlog::level::debug);

    // Alternative names
    assert(Logger::ToSpdlogLevel("warning") == spdlog::level::warn);
    assert(Logger::ToSpdlogLevel("WARNING") == spdlog::level::warn);
    assert(Logger::ToSpdlogLevel("critical") == spdlog::level::critical);
    assert(Logger::ToSpdlogLevel("CRITICAL") == spdlog::level::critical);

    // Unknown defaults to info
    assert(Logger::ToSpdlogLevel("unknown") == spdlog::level::info);
    assert(Logger::ToSpdlogLevel("") == spdlog::level::info);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Logger Initialization Tests
//===----------------------------------------------------------------------===//

void TestLoggerAutoInitialize() {
    std::cout << "  Testing auto-initialization..." << std::endl;

    // Shutdown any existing logger first
    Logger::Shutdown();

    // Get() should auto-initialize
    auto& logger = Logger::Get();
    assert(logger != nullptr);

    // Default level should be info
    assert(logger->level() == spdlog::level::info);

    Logger::Shutdown();
    std::cout << "    PASSED" << std::endl;
}

void TestLoggerExplicitInitialize() {
    std::cout << "  Testing explicit initialization..." << std::endl;

    Logger::Shutdown();

    // Initialize with custom level
    Logger::Initialize("", "debug");

    auto& logger = Logger::Get();
    assert(logger != nullptr);
    assert(logger->level() == spdlog::level::debug);

    Logger::Shutdown();
    std::cout << "    PASSED" << std::endl;
}

void TestLoggerDoubleInitialize() {
    std::cout << "  Testing double initialization (should be no-op)..." << std::endl;

    Logger::Shutdown();

    // First initialize
    Logger::Initialize("", "debug");
    assert(Logger::Get()->level() == spdlog::level::debug);

    // Second initialize should be ignored
    Logger::Initialize("", "error");
    assert(Logger::Get()->level() == spdlog::level::debug);  // Still debug

    Logger::Shutdown();
    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Set Level Tests
//===----------------------------------------------------------------------===//

void TestSetLevelByEnum() {
    std::cout << "  Testing SetLevel by enum..." << std::endl;

    Logger::Shutdown();
    Logger::Initialize("", "info");

    Logger::SetLevel(LogLevel::TRACE);
    assert(Logger::Get()->level() == spdlog::level::trace);

    Logger::SetLevel(LogLevel::DEBUG);
    assert(Logger::Get()->level() == spdlog::level::debug);

    Logger::SetLevel(LogLevel::WARN);
    assert(Logger::Get()->level() == spdlog::level::warn);

    Logger::SetLevel(LogLevel::ERROR);
    assert(Logger::Get()->level() == spdlog::level::err);

    Logger::Shutdown();
    std::cout << "    PASSED" << std::endl;
}

void TestSetLevelByString() {
    std::cout << "  Testing SetLevel by string..." << std::endl;

    Logger::Shutdown();
    Logger::Initialize("", "info");

    Logger::SetLevel("trace");
    assert(Logger::Get()->level() == spdlog::level::trace);

    Logger::SetLevel("DEBUG");
    assert(Logger::Get()->level() == spdlog::level::debug);

    Logger::SetLevel("Warning");
    assert(Logger::Get()->level() == spdlog::level::warn);

    Logger::Shutdown();
    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// File Output Tests
//===----------------------------------------------------------------------===//

void TestLoggerFileOutput() {
    std::cout << "  Testing file output..." << std::endl;

    std::string test_log_file = "/tmp/duckd_test_logger.log";

    // Clean up any existing file
    std::filesystem::remove(test_log_file);

    Logger::Shutdown();
    Logger::Initialize(test_log_file, "info");

    // Log some messages
    LOG_INFO("test", "Test message 1");
    LOG_WARN("test", "Test message 2");

    // Flush to ensure messages are written
    Logger::Flush();

    // Give some time for file I/O
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    Logger::Shutdown();

    // Verify file exists and has content
    assert(std::filesystem::exists(test_log_file));

    std::ifstream file(test_log_file);
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());

    assert(content.find("Test message 1") != std::string::npos);
    assert(content.find("Test message 2") != std::string::npos);

    // Cleanup
    std::filesystem::remove(test_log_file);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Log Macros Tests
//===----------------------------------------------------------------------===//

void TestLogMacrosFiltering() {
    std::cout << "  Testing log macros filtering..." << std::endl;

    Logger::Shutdown();
    Logger::Initialize("", "warn");

    // These should NOT log (level too low)
    LOG_TRACE("test", "trace message");
    LOG_DEBUG("test", "debug message");
    LOG_INFO("test", "info message");

    // These SHOULD log
    LOG_WARN("test", "warn message");
    LOG_ERROR("test", "error message");
    LOG_FATAL("test", "fatal message");

    // Verify logger is at correct level
    assert(Logger::Get()->level() == spdlog::level::warn);
    assert(!Logger::Get()->should_log(spdlog::level::info));
    assert(Logger::Get()->should_log(spdlog::level::warn));
    assert(Logger::Get()->should_log(spdlog::level::err));

    Logger::Shutdown();
    std::cout << "    PASSED" << std::endl;
}

void TestDlogMacros() {
    std::cout << "  Testing DLOG_* macros with fmt..." << std::endl;

    Logger::Shutdown();
    Logger::Initialize("", "debug");

    // Test fmt-style formatting (should not crash)
    int value = 42;
    std::string name = "test";

    DLOG_DEBUG("test", "value={}", value);
    DLOG_INFO("test", "name={} value={}", name, value);
    DLOG_WARN("test", "multiple: {} {} {}", 1, 2, 3);

    Logger::Shutdown();
    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// SpdlogLogStorage Tests
//===----------------------------------------------------------------------===//

void TestSpdlogLogStorageName() {
    std::cout << "  Testing SpdlogLogStorage name..." << std::endl;

    auto logger = spdlog::default_logger();
    SpdlogLogStorage storage(logger);

    assert(storage.GetStorageName() == "spdlog");

    std::cout << "    PASSED" << std::endl;
}

void TestSpdlogLogStorageNullLogger() {
    std::cout << "  Testing SpdlogLogStorage with null logger..." << std::endl;

    // Should not crash with null logger
    SpdlogLogStorage storage(nullptr);

    // Flush should handle null gracefully
    storage.Flush(duckdb::LoggingTargetTable::ALL_LOGS);
    storage.FlushAll();

    std::cout << "    PASSED" << std::endl;
}

void TestSpdlogLogStorageIsEnabled() {
    std::cout << "  Testing SpdlogLogStorage IsEnabled..." << std::endl;

    auto logger = spdlog::default_logger();
    SpdlogLogStorage storage(logger);

    // Only ALL_LOGS should be enabled
    assert(storage.IsEnabled(duckdb::LoggingTargetTable::ALL_LOGS) == true);
    assert(storage.IsEnabled(duckdb::LoggingTargetTable::QUERY_LOG) == false);
    assert(storage.IsEnabled(duckdb::LoggingTargetTable::QUERY_LOG_OPTIMIZED) == false);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// DuckDB Integration Tests
//===----------------------------------------------------------------------===//

void TestRegisterSpdlogStorage() {
    std::cout << "  Testing RegisterSpdlogStorage..." << std::endl;

    Logger::Shutdown();
    Logger::Initialize("", "debug");

    // Create an in-memory DuckDB database
    duckdb::DuckDB db(nullptr);
    auto& instance = *db.instance;

    // Register spdlog storage
    bool success = RegisterSpdlogStorage(instance, Logger::Get());
    assert(success);

    // Verify the storage is registered
    auto& log_manager = instance.GetLogManager();
    assert(log_manager.LoggingEnabled() == true);

    Logger::Shutdown();
    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== Logger Unit Tests ===" << std::endl;

    std::cout << "\n1. Log Level Conversion Tests:" << std::endl;
    TestLogLevelEnumConversion();
    TestLogLevelStringConversion();

    std::cout << "\n2. Logger Initialization Tests:" << std::endl;
    TestLoggerAutoInitialize();
    TestLoggerExplicitInitialize();
    TestLoggerDoubleInitialize();

    std::cout << "\n3. Set Level Tests:" << std::endl;
    TestSetLevelByEnum();
    TestSetLevelByString();

    std::cout << "\n4. File Output Tests:" << std::endl;
    TestLoggerFileOutput();

    std::cout << "\n5. Log Macro Tests:" << std::endl;
    TestLogMacrosFiltering();
    TestDlogMacros();

    std::cout << "\n6. SpdlogLogStorage Tests:" << std::endl;
    TestSpdlogLogStorageName();
    TestSpdlogLogStorageNullLogger();
    TestSpdlogLogStorageIsEnabled();

    std::cout << "\n7. DuckDB Integration Tests:" << std::endl;
    TestRegisterSpdlogStorage();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}
