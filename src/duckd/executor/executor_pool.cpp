//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// executor/executor_pool.cpp
//
// Query executor thread pool implementation
//===----------------------------------------------------------------------===//

#include "executor/executor_pool.hpp"
#include "utils/logger.hpp"

namespace duckdb_server {

ExecutorPool::ExecutorPool(size_t thread_count)
    : thread_count_(thread_count)
    , running_(false)
    , stop_requested_(false) {
    
    // Determine thread count
    if (thread_count_ == 0) {
        thread_count_ = std::thread::hardware_concurrency() * 2;
        if (thread_count_ == 0) {
            thread_count_ = 8;  // Default fallback
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
    
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stop_requested_ = true;
    }
    
    condition_.notify_all();
    
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    
    workers_.clear();
    running_ = false;
    
    // Clear remaining tasks
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        while (!tasks_.empty()) {
            tasks_.pop();
        }
    }
    
    LOG_INFO("executor_pool", "Executor pool stopped");
}

void ExecutorPool::Submit(Task task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (stop_requested_) {
            return;
        }
        tasks_.push(std::move(task));
    }
    condition_.notify_one();
}

size_t ExecutorPool::PendingTasks() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return tasks_.size();
}

void ExecutorPool::Worker() {
    while (true) {
        Task task;
        
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            
            condition_.wait(lock, [this]() {
                return stop_requested_ || !tasks_.empty();
            });
            
            if (stop_requested_ && tasks_.empty()) {
                return;
            }
            
            task = std::move(tasks_.front());
            tasks_.pop();
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
