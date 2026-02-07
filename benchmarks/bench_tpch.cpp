//===----------------------------------------------------------------------===//
//                         DuckD Server - Benchmarks
//
// benchmarks/bench_tpch.cpp
//
// TPC-H benchmark using DuckDB's built-in tpch extension
//===----------------------------------------------------------------------===//

#include "benchmark_common.hpp"
#include "pg_client.hpp"
#include <iostream>
#include <sstream>

using namespace duckd::bench;

// All 22 standard TPC-H queries
static const char* TPCH_QUERIES[] = {
    // Q1: Pricing Summary Report
    R"(SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as sum_base_price,
       sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
       avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,
       avg(l_discount) as avg_disc, count(*) as count_order
    FROM lineitem WHERE l_shipdate <= date '1998-12-01' - interval '90' day
    GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus)",

    // Q2: Minimum Cost Supplier
    R"(SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
    FROM part, supplier, partsupp, nation, region
    WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
      AND p_size = 15 AND p_type LIKE '%BRASS'
      AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
      AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region
        WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
          AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE')
    ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100)",

    // Q3: Shipping Priority
    R"(SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
    FROM customer, orders, lineitem
    WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
      AND o_orderdate < date '1995-03-15' AND l_shipdate > date '1995-03-15'
    GROUP BY l_orderkey, o_orderdate, o_shippriority
    ORDER BY revenue DESC, o_orderdate LIMIT 10)",

    // Q4: Order Priority Checking
    R"(SELECT o_orderpriority, count(*) as order_count FROM orders
    WHERE o_orderdate >= date '1993-07-01' AND o_orderdate < date '1993-07-01' + interval '3' month
      AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
    GROUP BY o_orderpriority ORDER BY o_orderpriority)",

    // Q5: Local Supplier Volume
    R"(SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
    FROM customer, orders, lineitem, supplier, nation, region
    WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey
      AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey AND r_name = 'ASIA'
      AND o_orderdate >= date '1994-01-01' AND o_orderdate < date '1994-01-01' + interval '1' year
    GROUP BY n_name ORDER BY revenue DESC)",

    // Q6: Forecasting Revenue Change
    R"(SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem
    WHERE l_shipdate >= date '1994-01-01' AND l_shipdate < date '1994-01-01' + interval '1' year
      AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24)",

    // Q7: Volume Shipping
    R"(SELECT supp_nation, cust_nation, l_year, sum(volume) as revenue FROM (
      SELECT n1.n_name as supp_nation, n2.n_name as cust_nation,
        extract(year FROM l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
      FROM supplier, lineitem, orders, customer, nation n1, nation n2
      WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
          OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN date '1995-01-01' AND date '1996-12-31'
    ) AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year)",

    // Q8: National Market Share
    R"(SELECT o_year, sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) as mkt_share
    FROM (SELECT extract(year FROM o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation
      FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
      WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN date '1995-01-01' AND date '1996-12-31'
        AND p_type = 'ECONOMY ANODIZED STEEL'
    ) AS all_nations GROUP BY o_year ORDER BY o_year)",

    // Q9: Product Type Profit Measure
    R"(SELECT nation, o_year, sum(amount) as sum_profit FROM (
      SELECT n_name as nation, extract(year FROM o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
      FROM part, supplier, lineitem, partsupp, orders, nation
      WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
        AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%'
    ) AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC)",

    // Q10: Returned Item Reporting
    R"(SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal, n_name, c_address, c_phone, c_comment
    FROM customer, orders, lineitem, nation
    WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
      AND o_orderdate >= date '1993-10-01' AND o_orderdate < date '1993-10-01' + interval '3' month
      AND l_returnflag = 'R' AND c_nationkey = n_nationkey
    GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
    ORDER BY revenue DESC LIMIT 20)",

    // Q11: Important Stock Identification
    R"(SELECT ps_partkey, sum(ps_supplycost * ps_availqty) as value
    FROM partsupp, supplier, nation
    WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
    GROUP BY ps_partkey
    HAVING sum(ps_supplycost * ps_availqty) > (
      SELECT sum(ps_supplycost * ps_availqty) * 0.0001
      FROM partsupp, supplier, nation
      WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY')
    ORDER BY value DESC)",

    // Q12: Shipping Modes and Order Priority
    R"(SELECT l_shipmode,
      sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) as high_line_count,
      sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) as low_line_count
    FROM orders, lineitem
    WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
      AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
      AND l_receiptdate >= date '1994-01-01' AND l_receiptdate < date '1994-01-01' + interval '1' year
    GROUP BY l_shipmode ORDER BY l_shipmode)",

    // Q13: Customer Distribution
    R"(SELECT c_count, count(*) as custdist FROM (
      SELECT c_custkey, count(o_orderkey) as c_count
      FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
      GROUP BY c_custkey
    ) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC)",

    // Q14: Promotion Effect
    R"(SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
       / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    FROM lineitem, part
    WHERE l_partkey = p_partkey AND l_shipdate >= date '1995-09-01'
      AND l_shipdate < date '1995-09-01' + interval '1' month)",

    // Q15: Top Supplier
    R"(WITH revenue AS (
      SELECT l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
      FROM lineitem
      WHERE l_shipdate >= date '1996-01-01' AND l_shipdate < date '1996-01-01' + interval '3' month
      GROUP BY l_suppkey)
    SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
    FROM supplier, revenue
    WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM revenue)
    ORDER BY s_suppkey)",

    // Q16: Parts/Supplier Relationship
    R"(SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) as supplier_cnt
    FROM partsupp, part
    WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45'
      AND p_type NOT LIKE 'MEDIUM POLISHED%'
      AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
      AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%')
    GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size)",

    // Q17: Small-Quantity-Order Revenue
    R"(SELECT sum(l_extendedprice) / 7.0 as avg_yearly FROM lineitem, part
    WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX'
      AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey))",

    // Q18: Large Volume Customer
    R"(SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
    FROM customer, orders, lineitem
    WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
      AND c_custkey = o_custkey AND o_orderkey = l_orderkey
    GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
    ORDER BY o_totalprice DESC, o_orderdate LIMIT 100)",

    // Q19: Discounted Revenue
    R"(SELECT sum(l_extendedprice* (1 - l_discount)) as revenue FROM lineitem, part
    WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')
      OR (p_partkey = l_partkey AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')
      OR (p_partkey = l_partkey AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'))",

    // Q20: Potential Part Promotion
    R"(SELECT s_name, s_address FROM supplier, nation
    WHERE s_suppkey IN (
      SELECT ps_suppkey FROM partsupp
      WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
        AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM lineitem
          WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
            AND l_shipdate >= date '1994-01-01' AND l_shipdate < date '1994-01-01' + interval '1' year))
      AND s_nationkey = n_nationkey AND n_name = 'CANADA'
    ORDER BY s_name)",

    // Q21: Suppliers Who Kept Orders Waiting
    R"(SELECT s_name, count(*) as numwait FROM supplier, lineitem l1, orders, nation
    WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F'
      AND l1.l_receiptdate > l1.l_commitdate
      AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
      AND NOT EXISTS (SELECT * FROM lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
      AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
    GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100)",

    // Q22: Global Sales Opportunity
    R"(SELECT cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal FROM (
      SELECT substring(c_phone FROM 1 FOR 2) as cntrycode, c_acctbal
      FROM customer
      WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (SELECT avg(c_acctbal) FROM customer
          WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'))
        AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)
    ) AS custsale GROUP BY cntrycode ORDER BY cntrycode)",
};

static constexpr int NUM_TPCH_QUERIES = 22;

struct TpchBenchConfig : public BenchmarkConfig {
    int scale_factor = 1;
};

bool SetupTpchData(PgConnection& conn, int scale_factor) {
    std::cout << "Loading TPC-H extension and generating data (sf=" << scale_factor << ")...\n";

    auto r = conn.Execute("INSTALL tpch");
    if (!r.Ok()) {
        std::cerr << "Failed to install tpch: " << r.error_message << "\n";
        return false;
    }

    r = conn.Execute("LOAD tpch");
    if (!r.Ok()) {
        std::cerr << "Failed to load tpch: " << r.error_message << "\n";
        return false;
    }

    std::ostringstream sql;
    sql << "CALL dbgen(sf=" << scale_factor << ")";
    r = conn.Execute(sql.str());
    if (!r.Ok()) {
        std::cerr << "Failed to generate TPC-H data: " << r.error_message << "\n";
        return false;
    }

    std::cout << "TPC-H data generated successfully.\n\n";
    return true;
}

int main(int argc, char* argv[]) {
    TpchBenchConfig config;
    config.num_threads = 1;
    config.duration_seconds = 0;

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
        } else if (arg == "-s" || arg == "--scale") {
            config.scale_factor = std::stoi(argv[++i]);
        } else if (arg == "-v" || arg == "--verbose") {
            config.verbose = true;
        } else if (arg == "--json") {
            config.json_output = true;
        } else if (arg == "--help") {
            std::cout << "TPC-H Benchmark\n"
                      << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  -h, --host HOST        Server host (default: 127.0.0.1)\n"
                      << "  -p, --port PORT        Server port (default: 5432)\n"
                      << "  -d, --database DB      Database name (default: duckd)\n"
                      << "  -U, --user USER        Username (default: duckd)\n"
                      << "  -s, --scale N          TPC-H scale factor (default: 1)\n"
                      << "  -v, --verbose          Verbose output\n"
                      << "  --json                 Output results as JSON\n"
                      << "  --help                 Show this help\n"
                      << "\nRuns all 22 standard TPC-H queries and reports per-query timing.\n";
            return 0;
        }
    }

    if (!config.json_output) {
        std::cout << "\n=== TPC-H Benchmark ===\n\n";
        std::cout << "  Host:         " << config.host << ":" << config.port << "\n";
        std::cout << "  Database:     " << config.database << "\n";
        std::cout << "  Scale Factor: " << config.scale_factor << "\n\n";
    }

    // Connect and setup
    PgConnection conn;
    if (!conn.Connect(config.host, config.port, config.user, config.database)) {
        std::cerr << "Failed to connect: " << conn.ErrorMessage() << "\n";
        return 1;
    }

    if (!SetupTpchData(conn, config.scale_factor)) {
        return 1;
    }

    // Run all 22 queries
    std::vector<double> query_times_ms(NUM_TPCH_QUERIES, 0.0);
    int queries_passed = 0;
    int queries_failed = 0;
    double total_time_ms = 0.0;

    auto total_start = Clock::now();

    for (int q = 0; q < NUM_TPCH_QUERIES; ++q) {
        if (!config.json_output) {
            std::cout << "Running Q" << (q + 1) << "... " << std::flush;
        }

        auto start = Clock::now();
        auto result = conn.Execute(TPCH_QUERIES[q]);
        auto end = Clock::now();

        double elapsed_ms = ToMilliseconds(end - start);
        query_times_ms[q] = elapsed_ms;
        total_time_ms += elapsed_ms;

        if (result.Ok()) {
            ++queries_passed;
            if (!config.json_output) {
                std::cout << std::fixed << std::setprecision(1) << elapsed_ms << " ms"
                          << " (" << result.NumRows() << " rows)\n";
            }
        } else {
            ++queries_failed;
            query_times_ms[q] = -1.0;
            if (!config.json_output) {
                std::cout << "FAILED: " << result.error_message << "\n";
            }
        }
    }

    auto total_end = Clock::now();
    double wall_time_ms = ToMilliseconds(total_end - total_start);

    // Compute geometric mean of successful query times
    double geo_mean = 0.0;
    if (queries_passed > 0) {
        double log_sum = 0.0;
        int count = 0;
        for (int q = 0; q < NUM_TPCH_QUERIES; ++q) {
            if (query_times_ms[q] > 0) {
                log_sum += std::log(query_times_ms[q]);
                ++count;
            }
        }
        if (count > 0) {
            geo_mean = std::exp(log_sum / count);
        }
    }

    if (config.json_output) {
        std::ostringstream os;
        os << std::fixed << std::setprecision(2);
        os << "{\"benchmark\":\"TPC-H\""
           << ",\"scale_factor\":" << config.scale_factor
           << ",\"queries_passed\":" << queries_passed
           << ",\"queries_failed\":" << queries_failed
           << ",\"total_query_time_ms\":" << total_time_ms
           << ",\"wall_time_ms\":" << wall_time_ms
           << ",\"geometric_mean_ms\":" << geo_mean
           << ",\"queries\":[";
        for (int q = 0; q < NUM_TPCH_QUERIES; ++q) {
            if (q > 0) os << ",";
            os << "{\"query\":" << (q + 1);
            if (query_times_ms[q] >= 0) {
                os << ",\"time_ms\":" << query_times_ms[q]
                   << ",\"status\":\"ok\"";
            } else {
                os << ",\"time_ms\":null,\"status\":\"failed\"";
            }
            os << "}";
        }
        os << "]}";
        std::cout << os.str() << "\n";
    } else {
        std::cout << "\n========================================\n"
                  << "TPC-H Results (SF=" << config.scale_factor << ")\n"
                  << "========================================\n"
                  << "Queries Passed:    " << queries_passed << "/" << NUM_TPCH_QUERIES << "\n"
                  << "Total Query Time:  " << std::fixed << std::setprecision(1) << total_time_ms << " ms\n"
                  << "Wall Clock Time:   " << std::fixed << std::setprecision(1) << wall_time_ms << " ms\n"
                  << "Geometric Mean:    " << std::fixed << std::setprecision(1) << geo_mean << " ms\n\n";

        // Summary table
        std::cout << "Per-Query Breakdown:\n"
                  << "  Query  |  Time (ms)\n"
                  << "  -------+-----------\n";
        for (int q = 0; q < NUM_TPCH_QUERIES; ++q) {
            std::cout << "  Q" << std::setw(2) << (q + 1) << "    | ";
            if (query_times_ms[q] >= 0) {
                std::cout << std::fixed << std::setprecision(1) << std::setw(10) << query_times_ms[q];
            } else {
                std::cout << "    FAILED";
            }
            std::cout << "\n";
        }
    }

    return queries_failed > 0 ? 1 : 0;
}
