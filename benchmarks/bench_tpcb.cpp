//===----------------------------------------------------------------------===//
//                         DuckD Server - Benchmarks
//
// benchmarks/bench_tpcb.cpp
//
// TPC-B like benchmark (similar to pgbench)
//===----------------------------------------------------------------------===//

#include "benchmark_common.hpp"
#include "pg_client.hpp"
#include <iostream>
#include <random>
#include <sstream>

using namespace duckd::bench;

struct TpcbBenchConfig : public BenchmarkConfig {
    int scale_factor = 1;        // Scale factor (like pgbench -s)
    bool initialize = false;     // Initialize tables
    bool use_transactions = true; // Use BEGIN/COMMIT
    bool use_prepared = true;    // Use prepared statements
};

// TPC-B tables
// pgbench_accounts: aid, bid, abalance, filler
// pgbench_branches: bid, bbalance, filler
// pgbench_tellers: tid, bid, tbalance, filler
// pgbench_history: tid, bid, aid, delta, mtime, filler

void InitializeTpcbTables(PgConnection& conn, int scale_factor) {
    std::cout << "Initializing TPC-B tables with scale factor " << scale_factor << "...\n";

    // Drop existing tables
    conn.Execute("DROP TABLE IF EXISTS pgbench_history");
    conn.Execute("DROP TABLE IF EXISTS pgbench_tellers");
    conn.Execute("DROP TABLE IF EXISTS pgbench_accounts");
    conn.Execute("DROP TABLE IF EXISTS pgbench_branches");

    // Create tables
    auto result = conn.Execute(
        "CREATE TABLE pgbench_branches ("
        "  bid INTEGER NOT NULL PRIMARY KEY,"
        "  bbalance INTEGER,"
        "  filler VARCHAR(88)"
        ")"
    );
    if (!result.Ok()) {
        throw std::runtime_error("Failed to create pgbench_branches: " + result.error_message);
    }

    result = conn.Execute(
        "CREATE TABLE pgbench_tellers ("
        "  tid INTEGER NOT NULL PRIMARY KEY,"
        "  bid INTEGER,"
        "  tbalance INTEGER,"
        "  filler VARCHAR(84)"
        ")"
    );
    if (!result.Ok()) {
        throw std::runtime_error("Failed to create pgbench_tellers: " + result.error_message);
    }

    result = conn.Execute(
        "CREATE TABLE pgbench_accounts ("
        "  aid INTEGER NOT NULL PRIMARY KEY,"
        "  bid INTEGER,"
        "  abalance INTEGER,"
        "  filler VARCHAR(84)"
        ")"
    );
    if (!result.Ok()) {
        throw std::runtime_error("Failed to create pgbench_accounts: " + result.error_message);
    }

    result = conn.Execute(
        "CREATE TABLE pgbench_history ("
        "  tid INTEGER,"
        "  bid INTEGER,"
        "  aid INTEGER,"
        "  delta INTEGER,"
        "  mtime TIMESTAMP,"
        "  filler VARCHAR(22)"
        ")"
    );
    if (!result.Ok()) {
        throw std::runtime_error("Failed to create pgbench_history: " + result.error_message);
    }

    // Insert data
    // 1 branch per scale factor
    // 10 tellers per scale factor
    // 100000 accounts per scale factor

    int num_branches = scale_factor;
    int num_tellers = scale_factor * 10;
    int num_accounts = scale_factor * 100000;

    std::cout << "  Inserting " << num_branches << " branches...\n";
    for (int i = 1; i <= num_branches; ++i) {
        std::ostringstream sql;
        sql << "INSERT INTO pgbench_branches (bid, bbalance) VALUES (" << i << ", 0)";
        conn.Execute(sql.str());
    }

    std::cout << "  Inserting " << num_tellers << " tellers...\n";
    for (int i = 1; i <= num_tellers; ++i) {
        std::ostringstream sql;
        sql << "INSERT INTO pgbench_tellers (tid, bid, tbalance) VALUES ("
            << i << ", " << ((i - 1) / 10 + 1) << ", 0)";
        conn.Execute(sql.str());
    }

    std::cout << "  Inserting " << num_accounts << " accounts...\n";
    // Insert accounts in batches
    int batch_size = 1000;
    for (int start = 1; start <= num_accounts; start += batch_size) {
        int end = std::min(start + batch_size - 1, num_accounts);
        std::ostringstream sql;
        sql << "INSERT INTO pgbench_accounts (aid, bid, abalance) "
            << "SELECT i, ((i - 1) / 100000) + 1, 0 "
            << "FROM generate_series(" << start << ", " << end << ") AS t(i)";
        result = conn.Execute(sql.str());
        if (!result.Ok()) {
            std::cerr << "Warning: Failed to insert accounts: " << result.error_message << "\n";
        }

        if (start % 10000 == 1) {
            std::cout << "    " << std::min(end, num_accounts) << "/" << num_accounts << "\r" << std::flush;
        }
    }
    std::cout << "\n";

    std::cout << "TPC-B initialization complete.\n";
}

void TpcbWorker(int thread_id,
                const TpcbBenchConfig& config,
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

    int num_accounts = config.scale_factor * 100000;
    int num_tellers = config.scale_factor * 10;
    int num_branches = config.scale_factor;

    std::mt19937 rng(thread_id + static_cast<unsigned>(std::random_device{}()));
    std::uniform_int_distribution<int> account_dist(1, num_accounts);
    std::uniform_int_distribution<int> teller_dist(1, num_tellers);
    std::uniform_int_distribution<int> branch_dist(1, num_branches);
    std::uniform_int_distribution<int> delta_dist(-5000, 5000);

    // Prepare statements if enabled
    if (config.use_prepared) {
        conn.Prepare("update_accounts",
            "UPDATE pgbench_accounts SET abalance = abalance + $1 WHERE aid = $2");
        conn.Prepare("select_accounts",
            "SELECT abalance FROM pgbench_accounts WHERE aid = $1");
        conn.Prepare("update_tellers",
            "UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $2");
        conn.Prepare("update_branches",
            "UPDATE pgbench_branches SET bbalance = bbalance + $1 WHERE bid = $2");
        conn.Prepare("insert_history",
            "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) "
            "VALUES ($1, $2, $3, $4, current_timestamp)");
    }

    int ops_limit = config.operations_per_thread;
    int ops_done = 0;

    while (!stop_flag.load() && (ops_limit == 0 || ops_done < ops_limit)) {
        int aid = account_dist(rng);
        int tid = teller_dist(rng);
        int bid = branch_dist(rng);
        int delta = delta_dist(rng);

        auto start = Clock::now();

        bool tx_success = true;

        if (config.use_transactions) {
            auto r = conn.Begin();
            if (!r.Ok()) tx_success = false;
        }

        if (tx_success) {
            QueryResult result;
            if (config.use_prepared) {
                // UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid
                result = conn.ExecutePrepared("update_accounts",
                    {std::to_string(delta), std::to_string(aid)});
            } else {
                std::ostringstream sql;
                sql << "UPDATE pgbench_accounts SET abalance = abalance + " << delta
                    << " WHERE aid = " << aid;
                result = conn.Execute(sql.str());
            }
            if (!result.Ok()) tx_success = false;
        }

        if (tx_success) {
            QueryResult result;
            if (config.use_prepared) {
                // SELECT abalance FROM pgbench_accounts WHERE aid = :aid
                result = conn.ExecutePrepared("select_accounts", {std::to_string(aid)});
            } else {
                std::ostringstream sql;
                sql << "SELECT abalance FROM pgbench_accounts WHERE aid = " << aid;
                result = conn.Execute(sql.str());
            }
            if (!result.Ok()) tx_success = false;
        }

        if (tx_success) {
            QueryResult result;
            if (config.use_prepared) {
                // UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid
                result = conn.ExecutePrepared("update_tellers",
                    {std::to_string(delta), std::to_string(tid)});
            } else {
                std::ostringstream sql;
                sql << "UPDATE pgbench_tellers SET tbalance = tbalance + " << delta
                    << " WHERE tid = " << tid;
                result = conn.Execute(sql.str());
            }
            if (!result.Ok()) tx_success = false;
        }

        if (tx_success) {
            QueryResult result;
            if (config.use_prepared) {
                // UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid
                result = conn.ExecutePrepared("update_branches",
                    {std::to_string(delta), std::to_string(bid)});
            } else {
                std::ostringstream sql;
                sql << "UPDATE pgbench_branches SET bbalance = bbalance + " << delta
                    << " WHERE bid = " << bid;
                result = conn.Execute(sql.str());
            }
            if (!result.Ok()) tx_success = false;
        }

        if (tx_success) {
            QueryResult result;
            if (config.use_prepared) {
                // INSERT INTO pgbench_history
                result = conn.ExecutePrepared("insert_history",
                    {std::to_string(tid), std::to_string(bid),
                     std::to_string(aid), std::to_string(delta)});
            } else {
                std::ostringstream sql;
                sql << "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES ("
                    << tid << ", " << bid << ", " << aid << ", " << delta
                    << ", current_timestamp)";
                result = conn.Execute(sql.str());
            }
            if (!result.Ok()) tx_success = false;
        }

        if (config.use_transactions) {
            if (tx_success) {
                conn.Commit();
            } else {
                conn.Rollback();
            }
        }

        auto end = Clock::now();
        latency.Record(end - start);
        ++total_ops;

        if (tx_success) {
            ++success_ops;
        } else {
            ++failed_ops;
        }

        ++ops_done;
    }

    // Cleanup prepared statements
    if (config.use_prepared) {
        conn.Deallocate("update_accounts");
        conn.Deallocate("select_accounts");
        conn.Deallocate("update_tellers");
        conn.Deallocate("update_branches");
        conn.Deallocate("insert_history");
    }
}

int main(int argc, char* argv[]) {
    TpcbBenchConfig config;
    config.num_threads = 4;
    config.duration_seconds = 60;
    config.scale_factor = 1;

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
        } else if (arg == "-s" || arg == "--scale") {
            config.scale_factor = std::stoi(argv[++i]);
        } else if (arg == "-i" || arg == "--initialize") {
            config.initialize = true;
        } else if (arg == "--no-transactions") {
            config.use_transactions = false;
        } else if (arg == "--no-prepared") {
            config.use_prepared = false;
        } else if (arg == "-v" || arg == "--verbose") {
            config.verbose = true;
        } else if (arg == "--json") {
            config.json_output = true;
        } else if (arg == "--help") {
            std::cout << "TPC-B Like Benchmark (similar to pgbench)\n"
                      << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  -h, --host HOST        Server host (default: 127.0.0.1)\n"
                      << "  -p, --port PORT        Server port (default: 5432)\n"
                      << "  -d, --database DB      Database name (default: duckd)\n"
                      << "  -U, --user USER        Username (default: duckd)\n"
                      << "  -t, --threads N        Number of threads (default: 4)\n"
                      << "  -T, --duration SECS    Test duration (default: 60)\n"
                      << "  -n, --operations N     Operations per thread\n"
                      << "  -s, --scale N          Scale factor (default: 1)\n"
                      << "  -i, --initialize       Initialize TPC-B tables\n"
                      << "  --no-transactions      Don't use transactions\n"
                      << "  --no-prepared          Don't use prepared statements\n"
                      << "  -v, --verbose          Verbose output\n"
                      << "  --json                 Output results as JSON\n"
                      << "  --help                 Show this help\n"
                      << "\nScale factor determines data size:\n"
                      << "  Branches: 1 per scale\n"
                      << "  Tellers: 10 per scale\n"
                      << "  Accounts: 100,000 per scale\n";
            return 0;
        }
    }

    std::cout << "\n=== TPC-B Like Benchmark ===\n\n";
    config.Print();
    std::cout << "  Scale Factor:  " << config.scale_factor << "\n";
    std::cout << "  Transactions:  " << (config.use_transactions ? "yes" : "no") << "\n";
    std::cout << "  Prepared:      " << (config.use_prepared ? "yes" : "no") << "\n\n";

    // Initialize if requested
    if (config.initialize) {
        PgConnection conn;
        if (!conn.Connect(config.host, config.port, config.user, config.database)) {
            std::cerr << "Failed to connect: " << conn.ErrorMessage() << "\n";
            return 1;
        }
        try {
            InitializeTpcbTables(conn, config.scale_factor);
        } catch (const std::exception& e) {
            std::cerr << "Initialization failed: " << e.what() << "\n";
            return 1;
        }
        std::cout << "\n";
    }

    std::cout << "Running benchmark...\n\n";

    // Run benchmark
    BenchmarkResult result;
    result.name = "TPC-B";

    std::atomic<size_t> total_ops{0};
    std::atomic<size_t> success_ops{0};
    std::atomic<size_t> failed_ops{0};
    std::atomic<bool> stop_flag{false};

    ProgressReporter reporter(result.name);
    auto start_time = Clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < config.num_threads; ++i) {
        threads.emplace_back([&, thread_id = i]() {
            TpcbWorker(thread_id, config, stop_flag, total_ops, success_ops,
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

        // TPC-B specific output
        std::cout << "\nTPC-B Metrics:\n"
                  << "  TPS (Transactions Per Second): "
                  << std::fixed << std::setprecision(2) << result.OperationsPerSecond() << "\n";
    }

    return result.failed_operations > 0 ? 1 : 0;
}
