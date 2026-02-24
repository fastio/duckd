//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/config/test_server_config.cpp
//
// Unit tests for ServerConfig
//===----------------------------------------------------------------------===//

#include "config/server_config.hpp"
#include <cassert>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <thread>

using namespace duckdb_server;

//===----------------------------------------------------------------------===//
// Helper: write temp config file
//===----------------------------------------------------------------------===//

static std::string WriteTempFile(const std::string& content, const std::string& suffix = ".conf") {
    std::string path = "/tmp/duckd_test_config" + suffix;
    std::ofstream f(path);
    f << content;
    f.close();
    return path;
}

static void CleanupFile(const std::string& path) {
    std::remove(path.c_str());
}

//===----------------------------------------------------------------------===//
// Default Values Tests
//===----------------------------------------------------------------------===//

void TestDefaultValues() {
    std::cout << "  Testing default values..." << std::endl;

    ServerConfig config;

    assert(config.host == "0.0.0.0");
    assert(config.port == 5432);
    assert(config.http_port == 0);
    assert(config.flight_port == 0);
    assert(config.database_path == ":memory:");
    assert(config.log_level == "info");
    assert(config.log_file.empty());
    assert(config.pid_file.empty());
    assert(config.user.empty());
    assert(config.daemon == false);
    assert(config.io_threads == 0);
    assert(config.executor_threads == 0);
    assert(config.max_connections == 100);
    assert(config.session_timeout_minutes == 30);
    assert(config.max_memory == 0);
    assert(config.max_open_files == 0);
    assert(config.query_timeout_ms == 300000);
    assert(config.pool_min_connections == 5);
    assert(config.pool_max_connections == 50);
    assert(config.pool_idle_timeout_seconds == 300);
    assert(config.pool_acquire_timeout_ms == 5000);
    assert(config.pool_validate_on_acquire == false);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Validation Tests
//===----------------------------------------------------------------------===//

void TestValidation() {
    std::cout << "  Testing Validate..." << std::endl;

    std::string error;

    // Valid config
    ServerConfig config;
    assert(config.Validate(error));

    // Invalid port
    config.port = 0;
    assert(!config.Validate(error));
    assert(error == "Invalid port number");

    // Fix port, break max_connections
    config.port = 5432;
    config.max_connections = 0;
    assert(!config.Validate(error));
    assert(error == "Max connections must be greater than 0");

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Auto Thread Count Tests
//===----------------------------------------------------------------------===//

void TestAutoThreadCounts() {
    std::cout << "  Testing auto thread counts..." << std::endl;

    ServerConfig config;

    // With 0 (auto), should use hardware_concurrency
    uint32_t hw = std::thread::hardware_concurrency();

    // IO threads: max(1, hw/2)
    uint32_t expected_io = std::max(1u, hw / 2);
    assert(config.GetIoThreadCount() == expected_io);

    // Executor threads: max(1, hw)
    uint32_t expected_exec = std::max(1u, hw);
    assert(config.GetExecutorThreadCount() == expected_exec);

    // Explicit values
    config.io_threads = 4;
    config.executor_threads = 8;
    assert(config.GetIoThreadCount() == 4);
    assert(config.GetExecutorThreadCount() == 8);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// INI Config File Tests
//===----------------------------------------------------------------------===//

void TestLoadFromIni() {
    std::cout << "  Testing LoadFromIni..." << std::endl;

    std::string content =
        "# DuckD config\n"
        "host = 127.0.0.1\n"
        "port = 9876\n"
        "database = /tmp/test.db\n"
        "log_level = debug\n"
        "max_connections = 200\n"
        "io_threads = 4\n"
        "executor_threads = 8\n"
        "pool_min_connections = 10\n"
        "pool_max_connections = 100\n"
        "query_timeout_ms = 60000\n"
        "daemon = true\n";

    auto path = WriteTempFile(content);

    ServerConfig config;
    std::string error;
    bool ok = config.LoadFromIni(path, error);
    assert(ok);

    assert(config.host == "127.0.0.1");
    assert(config.port == 9876);
    assert(config.database_path == "/tmp/test.db");
    assert(config.log_level == "debug");
    assert(config.max_connections == 200);
    assert(config.io_threads == 4);
    assert(config.executor_threads == 8);
    assert(config.pool_min_connections == 10);
    assert(config.pool_max_connections == 100);
    assert(config.query_timeout_ms == 60000);
    assert(config.daemon == true);

    CleanupFile(path);
    std::cout << "    PASSED" << std::endl;
}

void TestLoadFromIniWithQuotes() {
    std::cout << "  Testing LoadFromIni with quotes..." << std::endl;

    std::string content =
        "host = \"localhost\"\n"
        "database = '/tmp/my db.duckdb'\n";

    auto path = WriteTempFile(content);

    ServerConfig config;
    std::string error;
    assert(config.LoadFromIni(path, error));
    assert(config.host == "localhost");
    assert(config.database_path == "/tmp/my db.duckdb");

    CleanupFile(path);
    std::cout << "    PASSED" << std::endl;
}

void TestLoadFromIniComments() {
    std::cout << "  Testing LoadFromIni with comments..." << std::endl;

    std::string content =
        "# Comment line\n"
        "; Another comment\n"
        "\n"
        "port = 1234\n"
        "# host = not_this\n";

    auto path = WriteTempFile(content);

    ServerConfig config;
    std::string error;
    assert(config.LoadFromIni(path, error));
    assert(config.port == 1234);
    assert(config.host == "0.0.0.0");  // Default, commented line ignored

    CleanupFile(path);
    std::cout << "    PASSED" << std::endl;
}

void TestLoadFromIniNonExistent() {
    std::cout << "  Testing LoadFromIni non-existent file..." << std::endl;

    ServerConfig config;
    std::string error;
    assert(!config.LoadFromIni("/nonexistent/file.conf", error));
    assert(!error.empty());

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// YAML Config File Tests
//===----------------------------------------------------------------------===//

void TestLoadFromYaml() {
    std::cout << "  Testing LoadFromYaml..." << std::endl;

    std::string content =
        "server:\n"
        "  host: 192.168.1.1\n"
        "  port: 5433\n"
        "  http_port: 8080\n"
        "  flight_port: 8815\n"
        "\n"
        "database:\n"
        "  path: /data/mydb.duckdb\n"
        "\n"
        "logging:\n"
        "  level: warn\n"
        "  file: /var/log/duckd.log\n"
        "\n"
        "threads:\n"
        "  io: 2\n"
        "  executor: 16\n"
        "\n"
        "limits:\n"
        "  max_connections: 500\n"
        "  query_timeout_ms: 120000\n"
        "  session_timeout_minutes: 60\n"
        "\n"
        "pool:\n"
        "  min: 8\n"
        "  max: 200\n"
        "  idle_timeout_seconds: 600\n"
        "  acquire_timeout_ms: 10000\n"
        "  validate_on_acquire: true\n";

    auto path = WriteTempFile(content, ".yaml");

    ServerConfig config;
    std::string error;
    bool ok = config.LoadFromYaml(path, error);
    assert(ok);

    assert(config.host == "192.168.1.1");
    assert(config.port == 5433);
    assert(config.http_port == 8080);
    assert(config.flight_port == 8815);
    assert(config.database_path == "/data/mydb.duckdb");
    assert(config.log_level == "warn");
    assert(config.log_file == "/var/log/duckd.log");
    assert(config.io_threads == 2);
    assert(config.executor_threads == 16);
    assert(config.max_connections == 500);
    assert(config.query_timeout_ms == 120000);
    assert(config.session_timeout_minutes == 60);
    assert(config.pool_min_connections == 8);
    assert(config.pool_max_connections == 200);
    assert(config.pool_idle_timeout_seconds == 600);
    assert(config.pool_acquire_timeout_ms == 10000);
    assert(config.pool_validate_on_acquire == true);

    CleanupFile(path);
    std::cout << "    PASSED" << std::endl;
}

void TestLoadFromYamlPartial() {
    std::cout << "  Testing LoadFromYaml partial config..." << std::endl;

    std::string content =
        "server:\n"
        "  port: 9999\n";

    auto path = WriteTempFile(content, ".yaml");

    ServerConfig config;
    std::string error;
    assert(config.LoadFromYaml(path, error));

    // Only port changed, rest defaults
    assert(config.port == 9999);
    assert(config.host == "0.0.0.0");
    assert(config.max_connections == 100);

    CleanupFile(path);
    std::cout << "    PASSED" << std::endl;
}

void TestLoadFromYamlNonExistent() {
    std::cout << "  Testing LoadFromYaml non-existent file..." << std::endl;

    ServerConfig config;
    std::string error;
    assert(!config.LoadFromYaml("/nonexistent/file.yaml", error));
    assert(!error.empty());

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Auto-detect Format Tests
//===----------------------------------------------------------------------===//

void TestLoadFromFileAutoDetect() {
    std::cout << "  Testing LoadFromFile auto-detect format..." << std::endl;

    // YAML by extension
    std::string yaml_content = "server:\n  port: 1111\n";
    auto yaml_path = WriteTempFile(yaml_content, ".yaml");

    ServerConfig config1;
    std::string error;
    assert(config1.LoadFromFile(yaml_path, error));
    assert(config1.port == 1111);
    CleanupFile(yaml_path);

    // YML by extension
    auto yml_path = WriteTempFile(yaml_content, ".yml");
    ServerConfig config2;
    assert(config2.LoadFromFile(yml_path, error));
    assert(config2.port == 1111);
    CleanupFile(yml_path);

    // INI by extension (default)
    std::string ini_content = "port = 2222\n";
    auto ini_path = WriteTempFile(ini_content, ".conf");
    ServerConfig config3;
    assert(config3.LoadFromFile(ini_path, error));
    assert(config3.port == 2222);
    CleanupFile(ini_path);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Command Line Parsing Tests
//===----------------------------------------------------------------------===//

void TestParseCommandLine() {
    std::cout << "  Testing ParseCommandLine..." << std::endl;

    const char* argv[] = {
        "duckd",
        "-h", "127.0.0.1",
        "-p", "9876",
        "-d", "/tmp/test.db",
        "--log-level", "debug",
        "--max-connections", "200",
        "--io-threads", "4",
        "--executor-threads", "8",
        "--http-port", "8080",
        "--flight-port", "8815",
        "--query-timeout", "60000",
        "--pool-min", "10",
        "--pool-max", "100",
        "--pool-idle-timeout", "600",
        "--daemon"
    };
    int argc = sizeof(argv) / sizeof(argv[0]);

    bool show_version;
    auto config = ParseCommandLine(argc, const_cast<char**>(argv), show_version);

    assert(!show_version);
    assert(config.host == "127.0.0.1");
    assert(config.port == 9876);
    assert(config.database_path == "/tmp/test.db");
    assert(config.log_level == "debug");
    assert(config.max_connections == 200);
    assert(config.io_threads == 4);
    assert(config.executor_threads == 8);
    assert(config.http_port == 8080);
    assert(config.flight_port == 8815);
    assert(config.query_timeout_ms == 60000);
    assert(config.pool_min_connections == 10);
    assert(config.pool_max_connections == 100);
    assert(config.pool_idle_timeout_seconds == 600);
    assert(config.daemon == true);

    std::cout << "    PASSED" << std::endl;
}

void TestParseCommandLineDefaults() {
    std::cout << "  Testing ParseCommandLine defaults..." << std::endl;

    const char* argv[] = {"duckd"};
    int argc = 1;

    bool show_version;
    auto config = ParseCommandLine(argc, const_cast<char**>(argv), show_version);

    assert(!show_version);
    assert(config.host == "0.0.0.0");
    assert(config.port == 5432);
    assert(!config.daemon);

    std::cout << "    PASSED" << std::endl;
}

void TestParseCommandLineVersion() {
    std::cout << "  Testing ParseCommandLine --version..." << std::endl;

    const char* argv[] = {"duckd", "--version"};
    int argc = 2;

    bool show_version;
    auto config = ParseCommandLine(argc, const_cast<char**>(argv), show_version);

    assert(show_version);

    std::cout << "    PASSED" << std::endl;
}

void TestParseCommandLineShortFlags() {
    std::cout << "  Testing ParseCommandLine short flags..." << std::endl;

    const char* argv[] = {
        "duckd",
        "-h", "localhost",
        "-p", "5555",
        "-d", ":memory:"
    };
    int argc = sizeof(argv) / sizeof(argv[0]);

    bool show_version;
    auto config = ParseCommandLine(argc, const_cast<char**>(argv), show_version);

    assert(config.host == "localhost");
    assert(config.port == 5555);
    assert(config.database_path == ":memory:");

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== ServerConfig Unit Tests ===" << std::endl;

    std::cout << "\n1. Default Values:" << std::endl;
    TestDefaultValues();

    std::cout << "\n2. Validation:" << std::endl;
    TestValidation();

    std::cout << "\n3. Auto Thread Counts:" << std::endl;
    TestAutoThreadCounts();

    std::cout << "\n4. INI Config File:" << std::endl;
    TestLoadFromIni();
    TestLoadFromIniWithQuotes();
    TestLoadFromIniComments();
    TestLoadFromIniNonExistent();

    std::cout << "\n5. YAML Config File:" << std::endl;
    TestLoadFromYaml();
    TestLoadFromYamlPartial();
    TestLoadFromYamlNonExistent();

    std::cout << "\n6. Auto-detect Format:" << std::endl;
    TestLoadFromFileAutoDetect();

    std::cout << "\n7. Command Line Parsing:" << std::endl;
    TestParseCommandLine();
    TestParseCommandLineDefaults();
    TestParseCommandLineVersion();
    TestParseCommandLineShortFlags();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}
