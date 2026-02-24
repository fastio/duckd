//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// executor/executor_pool.cpp
//
// Query executor thread pool implementation
//===----------------------------------------------------------------------===//

#include "executor/executor_pool.hpp"
#include "logging/logger.hpp"

namespace duckdb_server {

ExecutorPool::ExecutorPool(size_t thread_count)
    : thread_count_(thread_count)
    , running_(false)
    , stop_requested_(false) {
    
    // Determine thread count
    if (thread_count_ == 0) {
        // Each executor thread submits work to DuckDB, which has its own internal
        // thread pool. Over-subscribing with *2 causes excessive context switches.
        thread_count_ = std::thread::hardware_concurrency();
        if (thread_count_ == 0) {
            thread_count_ = 4;  // Default fallback
        }
    }
}

ExecutorPool::~ExecutorPool() {
    Stop();
}

void ExecutorPool::Start() {
    if (running_) {
        return;
    }
    
    running_ = true;
    stop_requested_ = false;
    
    // Create worker threads
    for (size_t i = 0; i < thread_count_; ++i) {
        workers_.emplace_back([this]() {
            Worker();
        });
    }
    
    LOG_INFO("executor_pool", "Executor pool started with " + 
             std::to_string(thread_count_) + " threads");
}

void ExecutorPool::Stop() {
    if (!running_) {
        return;
    }

    stop_requested_ = true;
    wake_cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    workers_.clear();
    running_ = false;

    // Drain remaining tasks
    Task task;
    while (tasks_.try_dequeue(task)) {}

    LOG_INFO("executor_pool", "Executor pool stopped");
}

void ExecutorPool::Submit(Task task) {
    if (stop_requested_) {
        return;
    }
    tasks_.enqueue(std::move(task));
    wake_cv_.notify_one();
}

size_t ExecutorPool::PendingTasks() const {
    return tasks_.size_approx();
}

void ExecutorPool::Worker() {
    while (true) {
        Task task;

        // Try to dequeue without locking first
        if (!tasks_.try_dequeue(task)) {
            // No task available, wait for notification
            std::unique_lock<std::mutex> lock(wake_mutex_);
            wake_cv_.wait(lock, [this]() {
                return stop_requested_.load() || tasks_.size_approx() > 0;
            });
            lock.unlock();

            // Always try to dequeue after waking up
            if (!tasks_.try_dequeue(task)) {
                // Spurious wake-up or another worker took the task
                if (stop_requested_) return;
                continue;
            }
        }

        // Execute task
        try {
            task();
        } catch (const std::exception& e) {
            LOG_ERROR("executor_pool", "Task exception: " + std::string(e.what()));
        } catch (...) {
            LOG_ERROR("executor_pool", "Task unknown exception");
        }
    }
}

} // namespace duckdb_server
