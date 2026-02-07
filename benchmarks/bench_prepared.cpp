//===----------------------------------------------------------------------===//
//                         DuckD Server - Benchmarks
//
// benchmarks/bench_prepared.cpp
//
// Prepared statement (extended query protocol) benchmark
//===----------------------------------------------------------------------===//

#include "benchmark_common.hpp"
#include "pg_client.hpp"
#include <iostream>
#include <random>
#include <sstream>

using namespace duckd::bench;

struct PreparedBenchConfig : public BenchmarkConfig {
    int num_statements = 1;      // Number of prepared statements per connection
    int params_per_query = 1;    // Number of parameters per query
};

void PreparedWorker(int thread_id,
                    const PreparedBenchConfig& config,
                    std::atomic<bool>& stop_flag,
                    std::atomic<size_t>& total_ops,
                    std::atomic<size_t>& success_ops,
                    std::atomic<size_t>& failed_ops,
                    LatencyStats& latency) {
    PgConnection conn;
    if (!conn.Connect(config.host, config.port, config.user, config.database)) {
        std::cerr << "[Thread " << thread_id << "] Failed to connect: "
                  << conn.ErrorMessage() << "\n";
        return;
    }

    // Prepare statements
    std::vector<std::string> stmt_names;
    for (int i = 0; i < config.num_statements; ++i) {
        std::string stmt_name = "stmt_" + std::to_string(thread_id) + "_" + std::to_string(i);
        stmt_names.push_back(stmt_name);

        // Create query with parameters
        std::ostringstream query;
        query << "SELECT ";
        for (int p = 0; p < config.params_per_query; ++p) {
            if (p > 0) query << " + ";
            query << "$" << (p + 1) << "::INTEGER";
        }

        auto result = conn.Prepare(stmt_name, query.str());
        if (!result.Ok()) {
            std::cerr << "[Thread " << thread_id << "] Failed to prepare statement: "
                      << result.error_message << "\n";
            return;
        }
    }

    std::mt19937 rng(thread_id);
    std::uniform_int_distribution<int> stmt_dist(0, config.num_statements - 1);
    std::uniform_int_distribution<int> param_dist(1, 1000);

    int ops_limit = config.operations_per_thread;
    int ops_done = 0;

    while (!stop_flag.load() && (ops_limit == 0 || ops_done < ops_limit)) {
        // Select a random prepared statement
        int stmt_idx = stmt_dist(rng);
        const std::string& stmt_name = stmt_names[stmt_idx];

        // Generate random parameters
        std::vector<std::string> params;
        params.reserve(config.params_per_query);
        for (int p = 0; p < config.params_per_query; ++p) {
            params.push_back(std::to_string(param_dist(rng)));
        }

        auto start = Clock::now();
        auto result = conn.ExecutePrepared(stmt_name, params);
        auto end = Clock::now();

        latency.Record(end - start);
        ++total_ops;

        if (result.Ok()) {
            ++success_ops;
        } else {
            ++failed_ops;
            if (config.verbose) {
                std::cerr << "[Thread " << thread_id << "] Execute failed: "
                          << result.error_message << "\n";
            }
        }

        ++ops_done;
    }

    // Cleanup prepared statements
    for (const auto& stmt_name : stmt_names) {
        conn.Deallocate(stmt_name);
    }
}

int main(int argc, char* argv[]) {
    PreparedBenchConfig config;
    config.num_threads = 4;
    config.duration_seconds = 10;
    config.num_statements = 10;
    config.params_per_query = 3;

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
        } else if (arg == "-s" || arg == "--statements") {
            config.num_statements = std::stoi(argv[++i]);
        } else if (arg == "--params") {
            config.params_per_query = std::stoi(argv[++i]);
        } else if (arg == "-v" || arg == "--verbose") {
            config.verbose = true;
        } else if (arg == "--json") {
            config.json_output = true;
        } else if (arg == "--help") {
            std::cout << "Prepared Statement Benchmark\n"
                      << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  -h, --host HOST        Server host (default: 127.0.0.1)\n"
                      << "  -p, --port PORT        Server port (default: 5432)\n"
                      << "  -d, --database DB      Database name (default: duckd)\n"
                      << "  -U, --user USER        Username (default: duckd)\n"
                      << "  -t, --threads N        Number of threads (default: 4)\n"
                      << "  -T, --duration SECS    Test duration in seconds (default: 10)\n"
                      << "  -n, --operations N     Operations per thread (overrides duration)\n"
                      << "  -s, --statements N     Prepared statements per connection (default: 10)\n"
                      << "  --params N             Parameters per query (default: 3)\n"
                      << "  -v, --verbose          Verbose output\n"
                      << "  --json                 Output results as JSON\n"
                      << "  --help                 Show this help\n";
            return 0;
        }
    }

    std::cout << "\n=== Prepared Statement Benchmark ===\n\n";
    config.Print();
    std::cout << "  Statements:  " << config.num_statements << " per connection\n";
    std::cout << "  Parameters:  " << config.params_per_query << " per query\n\n";

    std::cout << "Running benchmark...\n\n";

    // Run benchmark
    BenchmarkResult result;
    result.name = "Prepared Statement";

    std::atomic<size_t> total_ops{0};
    std::atomic<size_t> success_ops{0};
    std::atomic<size_t> failed_ops{0};
    std::atomic<bool> stop_flag{false};

    ProgressReporter reporter(result.name);
    auto start_time = Clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < config.num_threads; ++i) {
        threads.emplace_back([&, thread_id = i]() {
            PreparedWorker(thread_id, config, stop_flag, total_ops, success_ops,
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

    if (config.json_output) {
        result.PrintJson();
    } else {
        result.Print();
    }

    return result.failed_operations > 0 ? 1 : 0;
}
