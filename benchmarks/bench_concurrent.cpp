//===----------------------------------------------------------------------===//
//                         DuckD Server - Benchmarks
//
// benchmarks/bench_concurrent.cpp
//
// Concurrent connections stress test
//===----------------------------------------------------------------------===//

#include "benchmark_common.hpp"
#include "pg_client.hpp"
#include <iostream>
#include <random>

using namespace duckd::bench;

struct ConcurrentBenchConfig : public BenchmarkConfig {
    int max_connections = 100;        // Maximum concurrent connections
    int ramp_up_seconds = 5;          // Time to ramp up to max connections
    int queries_per_connection = 100; // Queries per connection before reconnect
    bool keep_alive = true;           // Keep connections alive or reconnect
};

void ConcurrentWorker(int thread_id,
                      const ConcurrentBenchConfig& config,
                      std::atomic<bool>& stop_flag,
                      std::atomic<size_t>& total_ops,
                      std::atomic<size_t>& success_ops,
                      std::atomic<size_t>& failed_ops,
                      std::atomic<size_t>& active_connections,
                      LatencyStats& latency) {
    std::mt19937 rng(thread_id);
    std::uniform_int_distribution<int> query_type(0, 2);

    int ops_limit = config.operations_per_thread;
    int ops_done = 0;
    int queries_in_connection = 0;

    PgConnection conn;

    auto connect = [&]() -> bool {
        if (conn.IsConnected()) return true;

        auto start = Clock::now();
        bool success = conn.Connect(config.host, config.port, config.user, config.database);
        auto end = Clock::now();

        if (success) {
            active_connections.fetch_add(1);
            latency.Record(end - start);
            return true;
        } else {
            if (config.verbose) {
                std::cerr << "[Thread " << thread_id << "] Connect failed: "
                          << conn.ErrorMessage() << "\n";
            }
            return false;
        }
    };

    auto disconnect = [&]() {
        if (conn.IsConnected()) {
            conn.Disconnect();
            active_connections.fetch_sub(1);
        }
    };

    while (!stop_flag.load() && (ops_limit == 0 || ops_done < ops_limit)) {
        // Connect if needed
        if (!connect()) {
            ++failed_ops;
            ++total_ops;
            ++ops_done;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        // Execute a query
        std::string query;
        switch (query_type(rng)) {
            case 0: query = "SELECT 1"; break;
            case 1: query = "SELECT current_timestamp"; break;
            default: query = "SELECT 1 + 1"; break;
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
            // Reconnect on failure
            disconnect();
        }

        ++ops_done;
        ++queries_in_connection;

        // Reconnect periodically if not keeping alive
        if (!config.keep_alive && queries_in_connection >= config.queries_per_connection) {
            disconnect();
            queries_in_connection = 0;
        }
    }

    disconnect();
}

int main(int argc, char* argv[]) {
    ConcurrentBenchConfig config;
    config.num_threads = 50;
    config.duration_seconds = 30;
    config.max_connections = 100;

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
        } else if (arg == "-c" || arg == "--connections") {
            config.max_connections = std::stoi(argv[++i]);
        } else if (arg == "--queries-per-conn") {
            config.queries_per_connection = std::stoi(argv[++i]);
        } else if (arg == "--no-keep-alive") {
            config.keep_alive = false;
        } else if (arg == "-v" || arg == "--verbose") {
            config.verbose = true;
        } else if (arg == "--json") {
            config.json_output = true;
        } else if (arg == "--help") {
            std::cout << "Concurrent Connections Benchmark\n"
                      << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  -h, --host HOST         Server host (default: 127.0.0.1)\n"
                      << "  -p, --port PORT         Server port (default: 5432)\n"
                      << "  -d, --database DB       Database name (default: duckd)\n"
                      << "  -U, --user USER         Username (default: duckd)\n"
                      << "  -t, --threads N         Number of threads (default: 50)\n"
                      << "  -T, --duration SECS     Test duration (default: 30)\n"
                      << "  -n, --operations N      Operations per thread\n"
                      << "  -c, --connections N     Max concurrent connections (default: 100)\n"
                      << "  --queries-per-conn N    Queries before reconnect (default: 100)\n"
                      << "  --no-keep-alive         Disconnect after queries-per-conn\n"
                      << "  -v, --verbose           Verbose output\n"
                      << "  --json                  Output results as JSON\n"
                      << "  --help                  Show this help\n";
            return 0;
        }
    }

    std::cout << "\n=== Concurrent Connections Benchmark ===\n\n";
    config.Print();
    std::cout << "  Max Connections:     " << config.max_connections << "\n";
    std::cout << "  Keep Alive:          " << (config.keep_alive ? "yes" : "no") << "\n";
    if (!config.keep_alive) {
        std::cout << "  Queries/Connection:  " << config.queries_per_connection << "\n";
    }
    std::cout << "\n";

    std::cout << "Running benchmark...\n\n";

    // Run benchmark
    BenchmarkResult result;
    result.name = "Concurrent Connections";

    std::atomic<size_t> total_ops{0};
    std::atomic<size_t> success_ops{0};
    std::atomic<size_t> failed_ops{0};
    std::atomic<size_t> active_connections{0};
    std::atomic<bool> stop_flag{false};

    auto start_time = Clock::now();

    // Start threads with staggered timing for gradual ramp-up
    std::vector<std::thread> threads;
    int delay_per_thread_ms = (config.ramp_up_seconds * 1000) / std::max(config.num_threads, 1);

    for (int i = 0; i < config.num_threads; ++i) {
        threads.emplace_back([&, thread_id = i, delay = i * delay_per_thread_ms]() {
            // Stagger thread start
            if (delay > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            }
            ConcurrentWorker(thread_id, config, stop_flag, total_ops, success_ops,
                             failed_ops, active_connections, result.latency);
        });
    }

    // Progress reporting with connection count
    std::thread reporter([&]() {
        size_t last_count = 0;
        auto last_time = Clock::now();

        while (!stop_flag.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            if (stop_flag.load()) break;

            auto now = Clock::now();
            size_t current = total_ops.load();
            double elapsed = ToSeconds(now - last_time);
            double rate = (current - last_count) / elapsed;

            std::cout << "[Progress] " << current << " ops, "
                      << std::fixed << std::setprecision(1) << rate << " ops/sec, "
                      << active_connections.load() << " active connections\n";

            last_count = current;
            last_time = now;
        }
    });

    // Wait for completion
    if (config.operations_per_thread > 0) {
        for (auto& t : threads) {
            t.join();
        }
        stop_flag = true;
    } else {
        std::this_thread::sleep_for(std::chrono::seconds(config.duration_seconds));
        stop_flag = true;
        for (auto& t : threads) {
            t.join();
        }
    }

    reporter.join();

    auto end_time = Clock::now();

    result.total_operations = total_ops.load();
    result.successful_operations = success_ops.load();
    result.failed_operations = failed_ops.load();
    result.total_time = end_time - start_time;

    if (config.json_output) {
        result.PrintJson();
    } else {
        result.Print();
        std::cout << "Peak Active Connections: ~" << config.num_threads << "\n";
    }

    return result.failed_operations > 0 ? 1 : 0;
}
