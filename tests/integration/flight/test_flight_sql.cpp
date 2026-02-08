//===----------------------------------------------------------------------===//
//                         DuckD Server - Integration Tests
//
// tests/integration/flight/test_flight_sql.cpp
//
// End-to-end integration tests for Arrow Flight SQL protocol
//===----------------------------------------------------------------------===//

#include "protocol/flight/flight_sql_server.hpp"
#include "session/session_manager.hpp"
#include "executor/executor_pool.hpp"
#include "duckdb.hpp"

#include <arrow/flight/sql/client.h>
#include <arrow/flight/client.h>
#include <arrow/api.h>
#include <arrow/table.h>

#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>

using namespace duckdb_server;

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

//===----------------------------------------------------------------------===//
// Test Fixture
//===----------------------------------------------------------------------===//

class FlightSqlTest {
public:
    FlightSqlTest() {
        db_ = std::make_shared<duckdb::DuckDB>(nullptr);

        SessionManager::Config sm_config;
        sm_config.max_sessions = 100;
        sm_config.pool_min_connections = 2;
        sm_config.pool_max_connections = 10;
        session_manager_ = std::make_shared<SessionManager>(db_, sm_config);

        executor_pool_ = std::make_shared<ExecutorPool>(2);
        executor_pool_->Start();

        // Start Flight SQL server on a random high port
        server_ = std::make_unique<DuckDBFlightSqlServer>(
            session_manager_.get(), executor_pool_.get(),
            "localhost", 0);  // Port 0 = OS picks

        auto status = server_->Init();
        assert(status.ok());

        // Run the server in a background thread
        auto serve_status = server_->ServeAsync();
        assert(serve_status.ok());

        // Give server time to start
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        // Get the actual port the server is listening on
        auto location = server_->location();
        auto uri = location.ToString();

        // Connect client
        auto client_location = flight::Location::Parse(uri);
        assert(client_location.ok());

        auto flight_client = flight::FlightClient::Connect(client_location.MoveValueUnsafe());
        assert(flight_client.ok());

        client_ = std::make_unique<flightsql::FlightSqlClient>(
            std::move(flight_client.MoveValueUnsafe()));
    }

    ~FlightSqlTest() {
        client_.reset();
        server_->Shutdown();
        executor_pool_->Stop();
    }

    std::shared_ptr<duckdb::DuckDB> db_;
    std::shared_ptr<SessionManager> session_manager_;
    std::shared_ptr<ExecutorPool> executor_pool_;
    std::unique_ptr<DuckDBFlightSqlServer> server_;
    std::unique_ptr<flightsql::FlightSqlClient> client_;
};

//===----------------------------------------------------------------------===//
// Test Helpers
//===----------------------------------------------------------------------===//

#define TEST_BEGIN(name) \
    std::cout << "  Testing " << name << "... " << std::flush;

#define TEST_PASS() \
    std::cout << "PASS" << std::endl; \
    passed++;

#define TEST_FAIL(msg) \
    std::cout << "FAIL: " << msg << std::endl; \
    failed++;

//===----------------------------------------------------------------------===//
// Tests
//===----------------------------------------------------------------------===//

static int TestSimpleQuery(FlightSqlTest& test) {
    int passed = 0, failed = 0;

    TEST_BEGIN("Simple SELECT query");
    {
        flight::FlightCallOptions options;
        auto info = test.client_->Execute(options, "SELECT 42 AS answer, 'hello' AS greeting");
        if (!info.ok()) {
            TEST_FAIL("Execute failed: " + info.status().ToString());
        } else {
            auto& endpoints = info.ValueUnsafe()->endpoints();
            assert(!endpoints.empty());

            auto stream = test.client_->DoGet(options, endpoints[0].ticket);
            if (!stream.ok()) {
                TEST_FAIL("DoGet failed: " + stream.status().ToString());
            } else {
                auto table_result = stream.ValueUnsafe()->ToTable();
                if (!table_result.ok()) {
                    TEST_FAIL("ToTable failed: " + table_result.status().ToString());
                } else {
                    auto table = table_result.MoveValueUnsafe();
                    assert(table->num_columns() == 2);
                    assert(table->num_rows() == 1);
                    assert(table->schema()->field(0)->name() == "answer");
                    assert(table->schema()->field(1)->name() == "greeting");
                    TEST_PASS();
                }
            }
        }
    }

    TEST_BEGIN("Multi-row SELECT");
    {
        flight::FlightCallOptions options;
        auto info = test.client_->Execute(options,
            "SELECT * FROM generate_series(1, 100) AS t(i)");
        if (!info.ok()) {
            TEST_FAIL("Execute failed: " + info.status().ToString());
        } else {
            auto& endpoints = info.ValueUnsafe()->endpoints();
            auto stream = test.client_->DoGet(options, endpoints[0].ticket);
            if (!stream.ok()) {
                TEST_FAIL("DoGet failed: " + stream.status().ToString());
            } else {
                auto table = stream.ValueUnsafe()->ToTable();
                if (!table.ok()) {
                    TEST_FAIL("ToTable failed: " + table.status().ToString());
                } else {
                    assert(table.ValueUnsafe()->num_rows() == 100);
                    TEST_PASS();
                }
            }
        }
    }

    return failed;
}

static int TestPreparedStatements(FlightSqlTest& test) {
    int passed = 0, failed = 0;

    TEST_BEGIN("Create and execute prepared statement");
    {
        flight::FlightCallOptions options;
        auto prepared = test.client_->Prepare(options, "SELECT 1 AS x, 2 AS y");
        if (!prepared.ok()) {
            TEST_FAIL("Prepare failed: " + prepared.status().ToString());
        } else {
            auto& stmt = prepared.ValueUnsafe();
            auto info = stmt->Execute();
            if (!info.ok()) {
                TEST_FAIL("Execute prepared failed: " + info.status().ToString());
            } else {
                auto& endpoints = info.ValueUnsafe()->endpoints();
                auto stream = test.client_->DoGet(options, endpoints[0].ticket);
                if (!stream.ok()) {
                    TEST_FAIL("DoGet failed: " + stream.status().ToString());
                } else {
                    auto table = stream.ValueUnsafe()->ToTable();
                    if (!table.ok()) {
                        TEST_FAIL("ToTable failed: " + table.status().ToString());
                    } else {
                        assert(table.ValueUnsafe()->num_columns() == 2);
                        assert(table.ValueUnsafe()->num_rows() == 1);
                        TEST_PASS();
                    }
                }
            }

            // Close prepared statement
            auto close_status = stmt->Close();
            if (!close_status.ok()) {
                std::cerr << "Warning: Close prepared failed: " << close_status.ToString() << std::endl;
            }
        }
    }

    return failed;
}

static int TestMetadata(FlightSqlTest& test) {
    int passed = 0, failed = 0;

    TEST_BEGIN("GetCatalogs");
    {
        flight::FlightCallOptions options;
        auto info = test.client_->GetCatalogs(options);
        if (!info.ok()) {
            TEST_FAIL("GetCatalogs failed: " + info.status().ToString());
        } else {
            auto& endpoints = info.ValueUnsafe()->endpoints();
            auto stream = test.client_->DoGet(options, endpoints[0].ticket);
            if (!stream.ok()) {
                TEST_FAIL("DoGet catalogs failed: " + stream.status().ToString());
            } else {
                auto table = stream.ValueUnsafe()->ToTable();
                if (!table.ok()) {
                    TEST_FAIL("ToTable failed: " + table.status().ToString());
                } else {
                    assert(table.ValueUnsafe()->num_columns() == 1);
                    assert(table.ValueUnsafe()->num_rows() >= 1);
                    TEST_PASS();
                }
            }
        }
    }

    TEST_BEGIN("GetTableTypes");
    {
        flight::FlightCallOptions options;
        auto info = test.client_->GetTableTypes(options);
        if (!info.ok()) {
            TEST_FAIL("GetTableTypes failed: " + info.status().ToString());
        } else {
            auto& endpoints = info.ValueUnsafe()->endpoints();
            auto stream = test.client_->DoGet(options, endpoints[0].ticket);
            if (!stream.ok()) {
                TEST_FAIL("DoGet table types failed: " + stream.status().ToString());
            } else {
                auto table = stream.ValueUnsafe()->ToTable();
                if (!table.ok()) {
                    TEST_FAIL("ToTable failed: " + table.status().ToString());
                } else {
                    assert(table.ValueUnsafe()->num_rows() == 3);
                    TEST_PASS();
                }
            }
        }
    }

    TEST_BEGIN("GetTables");
    {
        flight::FlightCallOptions options;

        // First create a table
        auto create_info = test.client_->Execute(options,
            "CREATE TABLE test_table (id INTEGER, name VARCHAR)");
        if (create_info.ok()) {
            auto& eps = create_info.ValueUnsafe()->endpoints();
            if (!eps.empty()) {
                // Consume the result
                auto stream = test.client_->DoGet(options, eps[0].ticket);
                if (stream.ok()) {
                    (void)stream.ValueUnsafe()->ToTable();
                }
            }
        }

        auto info = test.client_->GetTables(options, {}, {}, {}, false, {});
        if (!info.ok()) {
            TEST_FAIL("GetTables failed: " + info.status().ToString());
        } else {
            auto& endpoints = info.ValueUnsafe()->endpoints();
            auto stream = test.client_->DoGet(options, endpoints[0].ticket);
            if (!stream.ok()) {
                TEST_FAIL("DoGet tables failed: " + stream.status().ToString());
            } else {
                auto table = stream.ValueUnsafe()->ToTable();
                if (!table.ok()) {
                    TEST_FAIL("ToTable failed: " + table.status().ToString());
                } else {
                    assert(table.ValueUnsafe()->num_rows() >= 1);
                    TEST_PASS();
                }
            }
        }
    }

    return failed;
}

static int TestParameterizedPreparedStatements(FlightSqlTest& test) {
    int passed = 0, failed = 0;

    TEST_BEGIN("Prepared statement with bound parameters");
    {
        flight::FlightCallOptions options;
        auto prepared = test.client_->Prepare(options, "SELECT $1::INTEGER + $2::INTEGER AS result");
        if (!prepared.ok()) {
            TEST_FAIL("Prepare failed: " + prepared.status().ToString());
        } else {
            auto& stmt = prepared.ValueUnsafe();

            // Build parameter record batch: two INT32 columns with values 10 and 32
            auto param_schema = arrow::schema({
                arrow::field("1", arrow::int32()),
                arrow::field("2", arrow::int32())
            });

            arrow::Int32Builder b1, b2;
            (void)b1.Append(10);
            (void)b2.Append(32);
            std::shared_ptr<arrow::Array> a1, a2;
            (void)b1.Finish(&a1);
            (void)b2.Finish(&a2);

            auto param_batch = arrow::RecordBatch::Make(param_schema, 1, {a1, a2});
            auto set_status = stmt->SetParameters(param_batch);
            if (!set_status.ok()) {
                TEST_FAIL("SetParameters failed: " + set_status.ToString());
            } else {
                auto info = stmt->Execute();
                if (!info.ok()) {
                    TEST_FAIL("Execute prepared failed: " + info.status().ToString());
                } else {
                    auto& endpoints = info.ValueUnsafe()->endpoints();
                    auto stream = test.client_->DoGet(options, endpoints[0].ticket);
                    if (!stream.ok()) {
                        TEST_FAIL("DoGet failed: " + stream.status().ToString());
                    } else {
                        auto table = stream.ValueUnsafe()->ToTable();
                        if (!table.ok()) {
                            TEST_FAIL("ToTable failed: " + table.status().ToString());
                        } else {
                            auto result_table = table.MoveValueUnsafe();
                            assert(result_table->num_rows() == 1);
                            auto col = std::static_pointer_cast<arrow::Int32Array>(
                                result_table->column(0)->chunk(0));
                            assert(col->Value(0) == 42);
                            TEST_PASS();
                        }
                    }
                }
            }

            (void)stmt->Close();
        }
    }

    return failed;
}

static int TestSessionCleanup(FlightSqlTest& test) {
    int passed = 0, failed = 0;

    TEST_BEGIN("Transient queries don't leak sessions");
    {
        auto initial_count = test.session_manager_->GetActiveSessionCount();

        flight::FlightCallOptions options;
        for (int i = 0; i < 10; i++) {
            auto info = test.client_->Execute(options, "SELECT " + std::to_string(i));
            if (info.ok()) {
                auto& endpoints = info.ValueUnsafe()->endpoints();
                if (!endpoints.empty()) {
                    auto stream = test.client_->DoGet(options, endpoints[0].ticket);
                    if (stream.ok()) {
                        (void)stream.ValueUnsafe()->ToTable();
                    }
                }
            }
        }

        auto final_count = test.session_manager_->GetActiveSessionCount();
        // Sessions should not have grown by 10 — transient queries use pooled connections
        if (final_count >= initial_count + 10) {
            TEST_FAIL("Session count grew from " + std::to_string(initial_count) +
                      " to " + std::to_string(final_count) + " (leaked)");
        } else {
            TEST_PASS();
        }
    }

    return failed;
}

static int TestErrorHandling(FlightSqlTest& test) {
    int passed = 0, failed = 0;

    TEST_BEGIN("Invalid SQL error");
    {
        flight::FlightCallOptions options;
        auto info = test.client_->Execute(options, "SELECT FROM NONEXISTENT_TABLE");
        if (info.ok()) {
            // Some implementations defer the error to DoGet
            auto& endpoints = info.ValueUnsafe()->endpoints();
            if (!endpoints.empty()) {
                auto stream = test.client_->DoGet(options, endpoints[0].ticket);
                if (stream.ok()) {
                    auto table = stream.ValueUnsafe()->ToTable();
                    if (!table.ok()) {
                        TEST_PASS();  // Error in stream
                    } else {
                        TEST_FAIL("Expected error for invalid SQL, but got result");
                    }
                } else {
                    TEST_PASS();  // Error in DoGet
                }
            } else {
                TEST_PASS();  // No endpoints means error
            }
        } else {
            TEST_PASS();  // Error in Execute (expected)
        }
    }

    return failed;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== Arrow Flight SQL Integration Tests ===" << std::endl;

    int total_failures = 0;

    {
        std::cout << "\n--- Query Tests ---" << std::endl;
        FlightSqlTest test;
        total_failures += TestSimpleQuery(test);
    }

    {
        std::cout << "\n--- Prepared Statement Tests ---" << std::endl;
        FlightSqlTest test;
        total_failures += TestPreparedStatements(test);
    }

    {
        std::cout << "\n--- Parameterized Prepared Statement Tests ---" << std::endl;
        FlightSqlTest test;
        total_failures += TestParameterizedPreparedStatements(test);
    }

    {
        std::cout << "\n--- Session Cleanup Tests ---" << std::endl;
        FlightSqlTest test;
        total_failures += TestSessionCleanup(test);
    }

    {
        std::cout << "\n--- Metadata Tests ---" << std::endl;
        FlightSqlTest test;
        total_failures += TestMetadata(test);
    }

    {
        std::cout << "\n--- Error Handling Tests ---" << std::endl;
        FlightSqlTest test;
        total_failures += TestErrorHandling(test);
    }

    std::cout << "\n=== Summary ===" << std::endl;
    if (total_failures == 0) {
        std::cout << "All tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << total_failures << " test(s) failed!" << std::endl;
        return 1;
    }
}
