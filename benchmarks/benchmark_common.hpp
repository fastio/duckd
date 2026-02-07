//===----------------------------------------------------------------------===//
//                         DuckD Server - Benchmarks
//
// benchmarks/benchmark_common.hpp
//
// Common utilities for benchmarking
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace duckd::bench {

//===----------------------------------------------------------------------===//
// Timing Utilities
//===----------------------------------------------------------------------===//

using Clock = std::chrono::high_resolution_clock;
using TimePoint = Clock::time_point;
using Duration = std::chrono::nanoseconds;

inline double ToMilliseconds(Duration d) {
    return std::chrono::duration<double, std::milli>(d).count();
}

inline double ToSeconds(Duration d) {
    return std::chrono::duration<double>(d).count();
}

inline double ToMicroseconds(Duration d) {
    return std::chrono::duration<double, std::micro>(d).count();
}

//===----------------------------------------------------------------------===//
// Statistics
//===----------------------------------------------------------------------===//

struct LatencyStats {
    std::vector<double> samples;  // in microseconds
    mutable std::unique_ptr<std::mutex> mutex = std::make_unique<std::mutex>();

    LatencyStats() = default;
    LatencyStats(LatencyStats&& other) noexcept : samples(std::move(other.samples)) {
        // Create new mutex for moved-to object
    }
    LatencyStats& operator=(LatencyStats&& other) noexcept {
        if (this != &other) {
            samples = std::move(other.samples);
        }
        return *this;
    }

    void Record(Duration d) {
        std::lock_guard<std::mutex> lock(*mutex);
        samples.push_back(ToMicroseconds(d));
    }

    void Clear() {
        std::lock_guard<std::mutex> lock(*mutex);
        samples.clear();
    }

    size_t Count() const {
        std::lock_guard<std::mutex> lock(*mutex);
        return samples.size();
    }

    double Min() const {
        std::lock_guard<std::mutex> lock(*mutex);
        if (samples.empty()) return 0;
        return *std::min_element(samples.begin(), samples.end());
    }

    double Max() const {
        std::lock_guard<std::mutex> lock(*mutex);
        if (samples.empty()) return 0;
        return *std::max_element(samples.begin(), samples.end());
    }

    double Mean() const {
        std::lock_guard<std::mutex> lock(*mutex);
        if (samples.empty()) return 0;
        return std::accumulate(samples.begin(), samples.end(), 0.0) / samples.size();
    }

    double StdDev() const {
        std::lock_guard<std::mutex> lock(*mutex);
        if (samples.size() < 2) return 0;
        double mean = std::accumulate(samples.begin(), samples.end(), 0.0) / samples.size();
        double sq_sum = 0;
        for (double v : samples) {
            sq_sum += (v - mean) * (v - mean);
        }
        return std::sqrt(sq_sum / (samples.size() - 1));
    }

    double Percentile(double p) const {
        std::lock_guard<std::mutex> lock(*mutex);
        if (samples.empty()) return 0;
        std::vector<double> sorted = samples;
        std::sort(sorted.begin(), sorted.end());
        size_t idx = static_cast<size_t>(p / 100.0 * (sorted.size() - 1));
        return sorted[idx];
    }

    void Print(const std::string& label = "Latency") const {
        std::cout << label << " Statistics (microseconds):\n"
                  << "  Count:  " << Count() << "\n"
                  << "  Min:    " << std::fixed << std::setprecision(2) << Min() << "\n"
                  << "  Max:    " << std::fixed << std::setprecision(2) << Max() << "\n"
                  << "  Mean:   " << std::fixed << std::setprecision(2) << Mean() << "\n"
                  << "  StdDev: " << std::fixed << std::setprecision(2) << StdDev() << "\n"
                  << "  P50:    " << std::fixed << std::setprecision(2) << Percentile(50) << "\n"
                  << "  P90:    " << std::fixed << std::setprecision(2) << Percentile(90) << "\n"
                  << "  P95:    " << std::fixed << std::setprecision(2) << Percentile(95) << "\n"
                  << "  P99:    " << std::fixed << std::setprecision(2) << Percentile(99) << "\n";
    }

    std::string ToJson() const {
        std::ostringstream os;
        os << std::fixed << std::setprecision(2);
        os << "{"
           << "\"count\":" << Count()
           << ",\"min_us\":" << Min()
           << ",\"max_us\":" << Max()
           << ",\"mean_us\":" << Mean()
           << ",\"stddev_us\":" << StdDev()
           << ",\"p50_us\":" << Percentile(50)
           << ",\"p90_us\":" << Percentile(90)
           << ",\"p95_us\":" << Percentile(95)
           << ",\"p99_us\":" << Percentile(99)
           << "}";
        return os.str();
    }
};

//===----------------------------------------------------------------------===//
// Benchmark Results
//===----------------------------------------------------------------------===//

struct BenchmarkResult {
    std::string name;
    size_t total_operations = 0;
    size_t successful_operations = 0;
    size_t failed_operations = 0;
    Duration total_time{0};
    LatencyStats latency;

    double OperationsPerSecond() const {
        double secs = ToSeconds(total_time);
        return secs > 0 ? successful_operations / secs : 0;
    }

    double SuccessRate() const {
        return total_operations > 0 ? 100.0 * successful_operations / total_operations : 0;
    }

    void Print() const {
        std::cout << "\n========================================\n"
                  << "Benchmark: " << name << "\n"
                  << "========================================\n"
                  << "Total Operations:      " << total_operations << "\n"
                  << "Successful Operations: " << successful_operations << "\n"
                  << "Failed Operations:     " << failed_operations << "\n"
                  << "Total Time:            " << std::fixed << std::setprecision(3)
                  << ToSeconds(total_time) << " seconds\n"
                  << "Throughput:            " << std::fixed << std::setprecision(2)
                  << OperationsPerSecond() << " ops/sec\n"
                  << "Success Rate:          " << std::fixed << std::setprecision(2)
                  << SuccessRate() << "%\n\n";
        latency.Print();
    }

    void PrintJson() const {
        std::ostringstream os;
        os << std::fixed << std::setprecision(3);
        os << "{"
           << "\"benchmark\":\"" << name << "\""
           << ",\"total_operations\":" << total_operations
           << ",\"successful_operations\":" << successful_operations
           << ",\"failed_operations\":" << failed_operations
           << ",\"total_time_s\":" << ToSeconds(total_time)
           << ",\"throughput_ops_sec\":" << std::setprecision(2) << OperationsPerSecond()
           << ",\"success_rate_pct\":" << SuccessRate()
           << ",\"latency\":" << latency.ToJson()
           << "}";
        std::cout << os.str() << "\n";
    }
};

//===----------------------------------------------------------------------===//
// Benchmark Configuration
//===----------------------------------------------------------------------===//

struct BenchmarkConfig {
    std::string host = "127.0.0.1";
    int port = 5432;
    std::string database = "duckd";
    std::string user = "duckd";
    std::string password = "";

    int num_threads = 1;
    int num_connections = 1;
    int duration_seconds = 10;
    int warmup_seconds = 2;
    int operations_per_thread = 0;  // 0 = time-based, >0 = operation-based

    bool verbose = false;
    bool json_output = false;

    std::string ConnectionString() const {
        return "host=" + host +
               " port=" + std::to_string(port) +
               " dbname=" + database +
               " user=" + user +
               (password.empty() ? "" : " password=" + password);
    }

    void Print() const {
        std::cout << "Benchmark Configuration:\n"
                  << "  Host:        " << host << ":" << port << "\n"
                  << "  Database:    " << database << "\n"
                  << "  User:        " << user << "\n"
                  << "  Threads:     " << num_threads << "\n"
                  << "  Connections: " << num_connections << "\n";
        if (operations_per_thread > 0) {
            std::cout << "  Operations:  " << operations_per_thread << " per thread\n";
        } else {
            std::cout << "  Duration:    " << duration_seconds << " seconds\n";
            std::cout << "  Warmup:      " << warmup_seconds << " seconds\n";
        }
    }
};

//===----------------------------------------------------------------------===//
// Progress Reporter
//===----------------------------------------------------------------------===//

class ProgressReporter {
public:
    ProgressReporter(const std::string& name, int interval_seconds = 1)
        : name_(name), interval_(interval_seconds), running_(false) {}

    void Start(std::atomic<size_t>& counter) {
        running_ = true;
        thread_ = std::thread([this, &counter]() {
            size_t last_count = 0;
            auto last_time = Clock::now();

            while (running_) {
                std::this_thread::sleep_for(std::chrono::seconds(interval_));
                if (!running_) break;

                auto now = Clock::now();
                size_t current = counter.load();
                double elapsed = ToSeconds(now - last_time);
                double rate = (current - last_count) / elapsed;

                std::cout << "[" << name_ << "] " << current << " ops, "
                          << std::fixed << std::setprecision(1) << rate << " ops/sec\n";

                last_count = current;
                last_time = now;
            }
        });
    }

    void Stop() {
        running_ = false;
        if (thread_.joinable()) {
            thread_.join();
        }
    }

private:
    std::string name_;
    int interval_;
    std::atomic<bool> running_;
    std::thread thread_;
};

//===----------------------------------------------------------------------===//
// Benchmark Runner Helper
//===----------------------------------------------------------------------===//

template <typename WorkerFunc>
BenchmarkResult RunBenchmark(const std::string& name,
                              const BenchmarkConfig& config,
                              WorkerFunc worker_func) {
    BenchmarkResult result;
    result.name = name;

    std::atomic<size_t> total_ops{0};
    std::atomic<size_t> success_ops{0};
    std::atomic<size_t> failed_ops{0};
    std::atomic<bool> stop_flag{false};

    ProgressReporter reporter(name);

    auto start_time = Clock::now();

    // Start worker threads
    std::vector<std::thread> threads;
    for (int i = 0; i < config.num_threads; ++i) {
        threads.emplace_back([&, thread_id = i]() {
            worker_func(thread_id, config, stop_flag, total_ops, success_ops,
                        failed_ops, result.latency);
        });
    }

    // Progress reporting
    if (config.verbose) {
        reporter.Start(total_ops);
    }

    // Wait for completion
    if (config.operations_per_thread > 0) {
        // Operation-based: wait for threads
        for (auto& t : threads) {
            t.join();
        }
    } else {
        // Time-based: wait for duration
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

    return result;
}

}  // namespace duckd::bench
