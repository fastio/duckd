//===----------------------------------------------------------------------===//
//                         DuckD Server - Benchmarks
//
// benchmarks/bench_scalability.cpp
//
// Concurrency scaling benchmark - measures throughput at increasing
// concurrency levels to evaluate scaling efficiency
//===----------------------------------------------------------------------===//

#include "benchmark_common.hpp"
#include "pg_client.hpp"
#include <iostream>
#include <sstream>

using namespace duckd::bench;

struct ScalabilityConfig : public BenchmarkConfig {
    int level_duration = 5;  // seconds per concurrency level
    std::vector<int> levels = {1, 2, 4, 8, 16, 32, 64};
};

struct LevelResult {
    int concurrency;
    double qps;
    double p50_us;
    double p99_us;
    double scaling_efficiency;  // QPS / (single_thread_qps * concurrency)
};

void ScalabilityWorker(int thread_id,
                       const BenchmarkConfig& config,
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

    while (!stop_flag.load()) {
        auto start = Clock::now();
        auto result = conn.Execute("SELECT 1");
        auto end = Clock::now();

        latency.Record(end - start);
        ++total_ops;

        if (result.Ok()) {
            ++success_ops;
        } else {
            ++failed_ops;
        }
    }
}

int main(int argc, char* argv[]) {
    ScalabilityConfig config;
    config.num_threads = 1;
    config.duration_seconds = 5;

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
        } else if (arg == "-T" || arg == "--duration") {
            config.level_duration = std::stoi(argv[++i]);
        } else if (arg == "-v" || arg == "--verbose") {
            config.verbose = true;
        } else if (arg == "--json") {
            config.json_output = true;
        } else if (arg == "--levels") {
            // Parse comma-separated list: --levels 1,2,4,8,16
            config.levels.clear();
            std::string val = argv[++i];
            std::istringstream iss(val);
            std::string token;
            while (std::getline(iss, token, ',')) {
                config.levels.push_back(std::stoi(token));
            }
        } else if (arg == "--help") {
            std::cout << "Scalability Benchmark\n"
                      << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  -h, --host HOST        Server host (default: 127.0.0.1)\n"
                      << "  -p, --port PORT        Server port (default: 5432)\n"
                      << "  -d, --database DB      Database name (default: duckd)\n"
                      << "  -U, --user USER        Username (default: duckd)\n"
                      << "  -T, --duration SECS    Duration per concurrency level (default: 5)\n"
                      << "  --levels N,N,...        Concurrency levels (default: 1,2,4,8,16,32,64)\n"
                      << "  -v, --verbose          Verbose output\n"
                      << "  --json                 Output results as JSON\n"
                      << "  --help                 Show this help\n"
                      << "\nRuns SELECT 1 at increasing concurrency levels and reports\n"
                      << "throughput (QPS), latency percentiles, and scaling efficiency.\n";
            return 0;
        }
    }

    if (!config.json_output) {
        std::cout << "\n=== Scalability Benchmark ===\n\n";
        std::cout << "  Host:              " << config.host << ":" << config.port << "\n";
        std::cout << "  Database:          " << config.database << "\n";
        std::cout << "  Duration/Level:    " << config.level_duration << "s\n";
        std::cout << "  Concurrency Levels:";
        for (int l : config.levels) std::cout << " " << l;
        std::cout << "\n\n";
    }

    std::vector<LevelResult> results;
    double baseline_qps = 0.0;

    for (int level : config.levels) {
        if (!config.json_output) {
            std::cout << "Testing concurrency=" << level << "... " << std::flush;
        }

        std::atomic<size_t> total_ops{0};
        std::atomic<size_t> success_ops{0};
        std::atomic<size_t> failed_ops{0};
        std::atomic<bool> stop_flag{false};
        LatencyStats latency;

        // Launch worker threads
        std::vector<std::thread> threads;
        for (int t = 0; t < level; ++t) {
            threads.emplace_back([&, tid = t]() {
                ScalabilityWorker(tid, config, stop_flag, total_ops, success_ops,
                                  failed_ops, latency);
            });
        }

        std::this_thread::sleep_for(std::chrono::seconds(config.level_duration));
        stop_flag = true;

        for (auto& t : threads) {
            t.join();
        }

        double elapsed = static_cast<double>(config.level_duration);
        double qps = success_ops.load() / elapsed;

        if (baseline_qps == 0.0) {
            baseline_qps = qps;
        }

        LevelResult lr;
        lr.concurrency = level;
        lr.qps = qps;
        lr.p50_us = latency.Percentile(50);
        lr.p99_us = latency.Percentile(99);
        lr.scaling_efficiency = (baseline_qps > 0 && level > 0)
            ? (qps / (baseline_qps * level)) * 100.0
            : 100.0;
        results.push_back(lr);

        if (!config.json_output) {
            std::cout << std::fixed << std::setprecision(0) << qps << " QPS, "
                      << "P50=" << std::setprecision(0) << lr.p50_us << "us, "
                      << "P99=" << std::setprecision(0) << lr.p99_us << "us, "
                      << "efficiency=" << std::setprecision(1) << lr.scaling_efficiency << "%\n";
        }
    }

    if (config.json_output) {
        std::ostringstream os;
        os << std::fixed << std::setprecision(2);
        os << "{\"benchmark\":\"Scalability\""
           << ",\"duration_per_level_s\":" << config.level_duration
           << ",\"baseline_qps\":" << baseline_qps
           << ",\"levels\":[";
        for (size_t i = 0; i < results.size(); ++i) {
            if (i > 0) os << ",";
            const auto& r = results[i];
            os << "{\"concurrency\":" << r.concurrency
               << ",\"qps\":" << r.qps
               << ",\"p50_us\":" << r.p50_us
               << ",\"p99_us\":" << r.p99_us
               << ",\"scaling_efficiency_pct\":" << r.scaling_efficiency
               << "}";
        }
        os << "]}";
        std::cout << os.str() << "\n";
    } else {
        std::cout << "\n========================================\n"
                  << "Scalability Results\n"
                  << "========================================\n"
                  << "  Concurrency |       QPS |  P50 (us) |  P99 (us) | Efficiency\n"
                  << "  ------------+-----------+-----------+-----------+-----------\n";
        for (const auto& r : results) {
            std::cout << "  " << std::setw(11) << r.concurrency << " | "
                      << std::setw(9) << std::fixed << std::setprecision(0) << r.qps << " | "
                      << std::setw(9) << std::setprecision(0) << r.p50_us << " | "
                      << std::setw(9) << std::setprecision(0) << r.p99_us << " | "
                      << std::setw(8) << std::setprecision(1) << r.scaling_efficiency << "%\n";
        }
        std::cout << "\nBaseline (1 thread): " << std::fixed << std::setprecision(0)
                  << baseline_qps << " QPS\n";
    }

    return 0;
}
