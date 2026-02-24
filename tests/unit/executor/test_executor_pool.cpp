//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/executor/test_executor_pool.cpp
//
// Unit tests for ExecutorPool
//===----------------------------------------------------------------------===//

#include "executor/executor_pool.hpp"
#include <cassert>
#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include <numeric>
#include <vector>
#include <mutex>

using namespace duckdb_server;

//===----------------------------------------------------------------------===//
// Construction Tests
//===----------------------------------------------------------------------===//

void TestExecutorPoolDefaultConstruction() {
    std::cout << "  Testing default construction (auto thread count)..." << std::endl;

    ExecutorPool pool;  // thread_count = 0 → auto
    assert(pool.Size() > 0);
    assert(!pool.IsRunning());

    std::cout << "    PASSED (auto-detected " << pool.Size() << " threads)" << std::endl;
}

void TestExecutorPoolExplicitThreadCount() {
    std::cout << "  Testing explicit thread count..." << std::endl;

    ExecutorPool pool(4);
    assert(pool.Size() == 4);
    assert(!pool.IsRunning());

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Start/Stop Tests
//===----------------------------------------------------------------------===//

void TestStartStop() {
    std::cout << "  Testing Start/Stop..." << std::endl;

    ExecutorPool pool(2);
    assert(!pool.IsRunning());

    pool.Start();
    assert(pool.IsRunning());

    pool.Stop();
    assert(!pool.IsRunning());

    std::cout << "    PASSED" << std::endl;
}

void TestDoubleStart() {
    std::cout << "  Testing double Start (should be no-op)..." << std::endl;

    ExecutorPool pool(2);
    pool.Start();
    pool.Start();  // Should not crash or create duplicate threads
    assert(pool.IsRunning());

    pool.Stop();

    std::cout << "    PASSED" << std::endl;
}

void TestDoubleStop() {
    std::cout << "  Testing double Stop (should be no-op)..." << std::endl;

    ExecutorPool pool(2);
    pool.Start();
    pool.Stop();
    pool.Stop();  // Should not crash
    assert(!pool.IsRunning());

    std::cout << "    PASSED" << std::endl;
}

void TestDestructorStops() {
    std::cout << "  Testing destructor stops pool..." << std::endl;

    {
        ExecutorPool pool(2);
        pool.Start();
        assert(pool.IsRunning());
    }
    // Pool should be stopped after destruction (no hanging threads)

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Task Submission Tests
//===----------------------------------------------------------------------===//

void TestSubmitSingleTask() {
    std::cout << "  Testing Submit single task..." << std::endl;

    ExecutorPool pool(2);
    pool.Start();

    std::atomic<bool> executed{false};
    pool.Submit([&executed]() {
        executed.store(true);
    });

    // Wait for execution
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!executed.load()) {
        assert(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    assert(executed.load());
    pool.Stop();

    std::cout << "    PASSED" << std::endl;
}

void TestSubmitMultipleTasks() {
    std::cout << "  Testing Submit multiple tasks..." << std::endl;

    ExecutorPool pool(4);
    pool.Start();

    const int num_tasks = 100;
    std::atomic<int> counter{0};

    for (int i = 0; i < num_tasks; ++i) {
        pool.Submit([&counter]() {
            counter.fetch_add(1);
        });
    }

    // Wait for all tasks to complete
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (counter.load() < num_tasks) {
        assert(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    assert(counter.load() == num_tasks);
    pool.Stop();

    std::cout << "    PASSED" << std::endl;
}

void TestSubmitConcurrentExecution() {
    std::cout << "  Testing concurrent task execution..." << std::endl;

    ExecutorPool pool(4);
    pool.Start();

    std::atomic<int> max_concurrent{0};
    std::atomic<int> current_concurrent{0};
    std::mutex mtx;

    const int num_tasks = 20;
    std::atomic<int> completed{0};

    for (int i = 0; i < num_tasks; ++i) {
        pool.Submit([&]() {
            int cur = current_concurrent.fetch_add(1) + 1;
            // Track max concurrency
            int prev_max = max_concurrent.load();
            while (cur > prev_max && !max_concurrent.compare_exchange_weak(prev_max, cur)) {}

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            current_concurrent.fetch_sub(1);
            completed.fetch_add(1);
        });
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (completed.load() < num_tasks) {
        assert(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // With 4 threads and sleeping tasks, we should see concurrency > 1
    assert(max_concurrent.load() > 1);
    pool.Stop();

    std::cout << "    PASSED (max concurrent: " << max_concurrent.load() << ")" << std::endl;
}

//===----------------------------------------------------------------------===//
// SubmitWithFuture Tests
//===----------------------------------------------------------------------===//

void TestSubmitWithFuture() {
    std::cout << "  Testing SubmitWithFuture..." << std::endl;

    ExecutorPool pool(2);
    pool.Start();

    auto future = pool.SubmitWithFuture([]() -> int {
        return 42;
    });

    auto result = future.get();
    assert(result == 42);

    pool.Stop();

    std::cout << "    PASSED" << std::endl;
}

void TestSubmitWithFutureString() {
    std::cout << "  Testing SubmitWithFuture (string result)..." << std::endl;

    ExecutorPool pool(2);
    pool.Start();

    auto future = pool.SubmitWithFuture([]() -> std::string {
        return "hello from executor";
    });

    auto result = future.get();
    assert(result == "hello from executor");

    pool.Stop();

    std::cout << "    PASSED" << std::endl;
}

void TestSubmitWithFutureMultiple() {
    std::cout << "  Testing SubmitWithFuture (multiple futures)..." << std::endl;

    ExecutorPool pool(4);
    pool.Start();

    std::vector<std::future<int>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(pool.SubmitWithFuture([i]() -> int {
            return i * i;
        }));
    }

    for (int i = 0; i < 10; ++i) {
        int result = futures[i].get();
        assert(result == i * i);
    }

    pool.Stop();

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Exception Handling Tests
//===----------------------------------------------------------------------===//

void TestTaskException() {
    std::cout << "  Testing task exception handling..." << std::endl;

    ExecutorPool pool(2);
    pool.Start();

    // Submit task that throws - pool should survive
    pool.Submit([]() {
        throw std::runtime_error("test exception");
    });

    // Submit task after exception - should still work
    std::atomic<bool> executed{false};
    pool.Submit([&executed]() {
        executed.store(true);
    });

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!executed.load()) {
        assert(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    assert(executed.load());
    pool.Stop();

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// PendingTasks Tests
//===----------------------------------------------------------------------===//

void TestPendingTasks() {
    std::cout << "  Testing PendingTasks..." << std::endl;

    ExecutorPool pool(1);
    // Don't start pool yet - tasks will queue up
    assert(pool.PendingTasks() == 0);

    pool.Start();
    // After start with no tasks, pending should be 0
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(pool.PendingTasks() == 0);

    pool.Stop();

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Submit After Stop Tests
//===----------------------------------------------------------------------===//

void TestSubmitAfterStop() {
    std::cout << "  Testing Submit after Stop (should be ignored)..." << std::endl;

    ExecutorPool pool(2);
    pool.Start();
    pool.Stop();

    // Submit after stop should not crash (task is silently dropped)
    pool.Submit([]() {
        // Should never execute
        assert(false && "Should not reach here");
    });

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== ExecutorPool Unit Tests ===" << std::endl;

    std::cout << "\n1. Construction Tests:" << std::endl;
    TestExecutorPoolDefaultConstruction();
    TestExecutorPoolExplicitThreadCount();

    std::cout << "\n2. Start/Stop Tests:" << std::endl;
    TestStartStop();
    TestDoubleStart();
    TestDoubleStop();
    TestDestructorStops();

    std::cout << "\n3. Task Submission Tests:" << std::endl;
    TestSubmitSingleTask();
    TestSubmitMultipleTasks();
    TestSubmitConcurrentExecution();

    std::cout << "\n4. SubmitWithFuture Tests:" << std::endl;
    TestSubmitWithFuture();
    TestSubmitWithFutureString();
    TestSubmitWithFutureMultiple();

    std::cout << "\n5. Exception Handling:" << std::endl;
    TestTaskException();

    std::cout << "\n6. PendingTasks:" << std::endl;
    TestPendingTasks();

    std::cout << "\n7. Edge Cases:" << std::endl;
    TestSubmitAfterStop();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}