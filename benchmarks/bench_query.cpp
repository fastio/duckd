//===----------------------------------------------------------------------===//
//                         DuckD Server - Benchmarks
//
// benchmarks/bench_query.cpp
//
// Simple query throughput benchmark
//===----------------------------------------------------------------------===//

#include "benchmark_common.hpp"
#include "pg_client.hpp"
#include <iostream>
#include <sstream>

using namespace duckd::bench;

// Predefined query types
enum class QueryType {
    SELECT_1,           // SELECT 1
    SELECT_NOW,         // SELECT current_timestamp
    SELECT_GENERATE,    // SELECT * FROM generate_series(1, 100)
    SELECT_TABLE,       // SELECT from a test table
    INSERT_ROW,         // INSERT into test table
    UPDATE_ROW,         // UPDATE test table
    CUSTOM              // User-provided query
};

std::string QueryTypeToString(QueryType type) {
    switch (type) {
        case QueryType::SELECT_1: return "SELECT_1";
        case QueryType::SELECT_NOW: return "SELECT_NOW";
        case QueryType::SELECT_GENERATE: return "SELECT_GENERATE";
        case QueryType::SELECT_TABLE: return "SELECT_TABLE";
        case QueryType::INSERT_ROW: return "INSERT_ROW";
        case QueryType::UPDATE_ROW: return "UPDATE_ROW";
        case QueryType::CUSTOM: return "CUSTOM";
        default: return "UNKNOWN";
    }
}

std::string GetQuery(QueryType type, int thread_id, int op_num) {
    switch (type) {
        case QueryType::SELECT_1:
            return "SELECT 1";
        case QueryType::SELECT_NOW:
            return "SELECT current_timestamp";
        case QueryType::SELECT_GENERATE:
            return "SELECT * FROM generate_series(1, 100)";
        case QueryType::SELECT_TABLE:
            return "SELECT * FROM bench_test LIMIT 100";
        case QueryType::INSERT_ROW: {
            std::ostringstream oss;
            oss << "INSERT INTO bench_test (id, value, thread_id) VALUES ("
                << (thread_id * 1000000 + op_num) << ", "
                << "'value_" << thread_id << "_" << op_num << "', "
                << thread_id << ")";
            return oss.str();
        }
        case QueryType::UPDATE_ROW: {
            std::ostringstream oss;
            oss << "UPDATE bench_test SET value = 'updated_" << op_num
                << "' WHERE id = " << (op_num % 1000);
            return oss.str();
        }
        default:
            return "SELECT 1";
    }
}

struct QueryBenchConfig : public BenchmarkConfig {
    QueryType query_type = QueryType::SELECT_1;
    std::string custom_query;
    bool setup_table = false;
};

void SetupTestTable(PgConnection& conn) {
    // Create test table
    auto result = conn.Execute(
        "CREATE TABLE IF NOT EXISTS bench_test ("
        "  id INTEGER PRIMARY KEY,"
        "  value VARCHAR(100),"
        "  thread_id INTEGER,"
        "  created_at TIMESTAMP DEFAULT current_timestamp"
        ")"
    );
    if (!result.Ok()) {
        std::cerr << "Warning: Failed to create test table: " << result.error_message << "\n";
    }

    // Insert some initial data
    result = conn.Execute(
        "INSERT INTO bench_test (id, value, thread_id) "
        "SELECT i, 'initial_value_' || i, 0 "
        "FROM generate_series(1, 1000) AS t(i) "
        "ON CONFLICT (id) DO NOTHING"
    );
    if (!result.Ok()) {
        std::cerr << "Warning: Failed to insert initial data: " << result.error_message << "\n";
    }
}

void QueryWorker(int thread_id,
                 const QueryBenchConfig& config,
                 std::atomic<bool>& stop_flag,
                 std::atomic<size_t>& total_ops,
                 std::atomic<size_t>& success_ops,
                 std::atomic<size_t>& failed_ops,
                 LatencyStats& latency) {
    // Each thread gets its own connection
    PgConnection conn;
    if (!conn.Connect(config.host, config.port, config.user, config.database)) {
        std::cerr << "[Thread " << thread_id << "] Failed to connect: "
                  << conn.ErrorMessage() << "\n";
        return;
    }

    int ops_limit = config.operations_per_thread;
    int ops_done = 0;

    while (!stop_flag.load() && (ops_limit == 0 || ops_done < ops_limit)) {
        std::string query;
        if (config.query_type == QueryType::CUSTOM) {
            query = config.custom_query;
        } else {
            query = GetQuery(config.query_type, thread_id, ops_done);
        }

        auto start = Clock::now();
        auto result = conn.Execute(query);
        auto end = Clock::now();

        latency.Record(end - start);
        ++total_ops;

        if (result.Ok()) {
            ++success_ops;
        } else {
            ++failed_ops;
            if (config.verbose) {
                std::cerr << "[Thread " << thread_id << "] Query failed: "
                          << result.error_message << "\n";
            }
        }

        ++ops_done;
    }
}

int main(int argc, char* argv[]) {
    QueryBenchConfig config;
    config.num_threads = 4;
    config.duration_seconds = 10;

    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--host") {
            config.host = argv[++i];
        } else if (arg == "-p" || arg == "--port") {
            config.port = std::stoi(argv[++i]);
        } else if (arg == "-d" || arg == "--database") {
            config.database = argv[++i];
        } else if (arg == "-U" || arg == "--user") {
            config.user = argv[++i];
        } else if (arg == "-t" || arg == "--threads") {
            config.num_threads = std::stoi(argv[++i]);
        } else if (arg == "-T" || arg == "--duration") {
            config.duration_seconds = std::stoi(argv[++i]);
        } else if (arg == "-n" || arg == "--operations") {
            config.operations_per_thread = std::stoi(argv[++i]);
        } else if (arg == "-q" || arg == "--query") {
            std::string qtype = argv[++i];
            if (qtype == "select1" || qtype == "SELECT_1") {
                config.query_type = QueryType::SELECT_1;
            } else if (qtype == "now" || qtype == "SELECT_NOW") {
                config.query_type = QueryType::SELECT_NOW;
            } else if (qtype == "generate" || qtype == "SELECT_GENERATE") {
                config.query_type = QueryType::SELECT_GENERATE;
            } else if (qtype == "table" || qtype == "SELECT_TABLE") {
                config.query_type = QueryType::SELECT_TABLE;
                config.setup_table = true;
            } else if (qtype == "insert" || qtype == "INSERT_ROW") {
                config.query_type = QueryType::INSERT_ROW;
                config.setup_table = true;
            } else if (qtype == "update" || qtype == "UPDATE_ROW") {
                config.query_type = QueryType::UPDATE_ROW;
                config.setup_table = true;
            } else {
                config.query_type = QueryType::CUSTOM;
                config.custom_query = qtype;
            }
        } else if (arg == "--setup-table") {
            config.setup_table = true;
        } else if (arg == "-v" || arg == "--verbose") {
            config.verbose = true;
        } else if (arg == "--help") {
            std::cout << "Query Benchmark\n"
                      << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  -h, --host HOST        Server host (default: 127.0.0.1)\n"
                      << "  -p, --port PORT        Server port (default: 5432)\n"
                      << "  -d, --database DB      Database name (default: duckd)\n"
                      << "  -U, --user USER        Username (default: duckd)\n"
                      << "  -t, --threads N        Number of threads (default: 4)\n"
                      << "  -T, --duration SECS    Test duration in seconds (default: 10)\n"
                      << "  -n, --operations N     Operations per thread (overrides duration)\n"
                      << "  -q, --query TYPE       Query type or custom SQL:\n"
                      << "                         select1 (default), now, generate,\n"
                      << "                         table, insert, update, or custom SQL\n"
                      << "  --setup-table          Create test table before benchmark\n"
                      << "  -v, --verbose          Verbose output\n"
                      << "  --help                 Show this help\n";
            return 0;
        }
    }

    std::cout << "\n=== Query Benchmark ===\n\n";
    config.Print();
    std::cout << "  Query Type:  " << QueryTypeToString(config.query_type) << "\n";
    if (config.query_type == QueryType::CUSTOM) {
        std::cout << "  Custom SQL:  " << config.custom_query << "\n";
    }
    std::cout << "\n";

    // Setup if needed
    if (config.setup_table) {
        std::cout << "Setting up test table...\n";
        PgConnection setup_conn;
        if (setup_conn.Connect(config.host, config.port, config.user, config.database)) {
            SetupTestTable(setup_conn);
        } else {
            std::cerr << "Failed to connect for setup: " << setup_conn.ErrorMessage() << "\n";
            return 1;
        }
    }

    std::cout << "Running benchmark...\n\n";

    // Run benchmark with custom config
    BenchmarkResult result;
    result.name = "Query (" + QueryTypeToString(config.query_type) + ")";

    std::atomic<size_t> total_ops{0};
    std::atomic<size_t> success_ops{0};
    std::atomic<size_t> failed_ops{0};
    std::atomic<bool> stop_flag{false};

    ProgressReporter reporter(result.name);
    auto start_time = Clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < config.num_threads; ++i) {
        threads.emplace_back([&, thread_id = i]() {
            QueryWorker(thread_id, config, stop_flag, total_ops, success_ops,
                        failed_ops, result.latency);
        });
    }

    if (config.verbose) {
        reporter.Start(total_ops);
    }

    if (config.operations_per_thread > 0) {
        for (auto& t : threads) {
            t.join();
        }
    } else {
        std::this_thread::sleep_for(std::chrono::seconds(config.duration_seconds));
        stop_flag = true;
        for (auto& t : threads) {
            t.join();
        }
    }

    if (config.verbose) {
        reporter.Stop();
    }

    auto end_time = Clock::now();

    result.total_operations = total_ops.load();
    result.successful_operations = success_ops.load();
    result.failed_operations = failed_ops.load();
    result.total_time = end_time - start_time;

    result.Print();

    return result.failed_operations > 0 ? 1 : 0;
}
