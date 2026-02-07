//===----------------------------------------------------------------------===//
//                         DuckD Server - Benchmarks
//
// benchmarks/bench_connection.cpp
//
// Connection establishment/teardown benchmark
//===----------------------------------------------------------------------===//

#include "benchmark_common.hpp"
#include "pg_client.hpp"
#include <iostream>

using namespace duckd::bench;

void ConnectionWorker(int thread_id,
                      const BenchmarkConfig& config,
                      std::atomic<bool>& stop_flag,
                      std::atomic<size_t>& total_ops,
                      std::atomic<size_t>& success_ops,
                      std::atomic<size_t>& failed_ops,
                      LatencyStats& latency) {
    int ops_limit = config.operations_per_thread;
    int ops_done = 0;

    while (!stop_flag.load() && (ops_limit == 0 || ops_done < ops_limit)) {
        auto start = Clock::now();

        PgConnection conn;
        bool success = conn.Connect(config.host, config.port, config.user, config.database);

        auto end = Clock::now();
        latency.Record(end - start);

        ++total_ops;
        if (success && conn.IsConnected()) {
            ++success_ops;
        } else {
            ++failed_ops;
            if (config.verbose) {
                std::cerr << "[Thread " << thread_id << "] Connection failed: "
                          << conn.ErrorMessage() << "\n";
            }
        }

        ++ops_done;
        // Connection automatically closed when conn goes out of scope
    }
}

int main(int argc, char* argv[]) {
    BenchmarkConfig config;
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
        } else if (arg == "-v" || arg == "--verbose") {
            config.verbose = true;
        } else if (arg == "--json") {
            config.json_output = true;
        } else if (arg == "--help") {
            std::cout << "Connection Benchmark\n"
                      << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  -h, --host HOST        Server host (default: 127.0.0.1)\n"
                      << "  -p, --port PORT        Server port (default: 5432)\n"
                      << "  -d, --database DB      Database name (default: duckd)\n"
                      << "  -U, --user USER        Username (default: duckd)\n"
                      << "  -t, --threads N        Number of threads (default: 4)\n"
                      << "  -T, --duration SECS    Test duration in seconds (default: 10)\n"
                      << "  -n, --operations N     Operations per thread (overrides duration)\n"
                      << "  -v, --verbose          Verbose output\n"
                      << "  --json                 Output results as JSON\n"
                      << "  --help                 Show this help\n";
            return 0;
        }
    }

    std::cout << "\n=== Connection Benchmark ===\n\n";
    config.Print();
    std::cout << "\nRunning benchmark...\n\n";

    auto result = RunBenchmark("Connection", config, ConnectionWorker);

    if (config.json_output) {
        result.PrintJson();
    } else {
        result.Print();
    }

    return result.failed_operations > 0 ? 1 : 0;
}
