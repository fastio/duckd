//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// duckdb_server/executor/executor_pool.hpp
//
// Query executor thread pool
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_server/common.hpp"
#include <queue>
#include <future>

namespace duckdb_server {

class ExecutorPool {
public:
    using Task = std::function<void()>;
    
    explicit ExecutorPool(size_t thread_count = 0);
    ~ExecutorPool();
    
    // Non-copyable
    ExecutorPool(const ExecutorPool&) = delete;
    ExecutorPool& operator=(const ExecutorPool&) = delete;
    
    // Start the pool
    void Start();
    
    // Stop the pool
    void Stop();
    
    // Submit a task
    void Submit(Task task);
    
    // Submit with future
    template<typename F, typename... Args>
    auto SubmitWithFuture(F&& f, Args&&... args) 
        -> std::future<typename std::invoke_result<F, Args...>::type> {
        using return_type = typename std::invoke_result<F, Args...>::type;
        
        auto task = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );
        
        std::future<return_type> result = task->get_future();
        
        Submit([task]() { (*task)(); });
        
        return result;
    }
    
    // Get pool size
    size_t Size() const { return thread_count_; }
    
    // Get pending task count
    size_t PendingTasks() const;
    
    // Check if running
    bool IsRunning() const { return running_; }

private:
    // Worker function
    void Worker();

private:
    // Thread count
    size_t thread_count_;
    
    // Worker threads
    std::vector<std::thread> workers_;
    
    // Task queue
    std::queue<Task> tasks_;
    mutable std::mutex queue_mutex_;
    std::condition_variable condition_;
    
    // Running flag
    std::atomic<bool> running_;
    std::atomic<bool> stop_requested_;
};

} // namespace duckdb_server
