//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// network/io_context_pool.hpp
//
// Asio IO context thread pool
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include <asio.hpp>
#include <thread>

namespace duckdb_server {

class IoContextPool {
public:
    explicit IoContextPool(size_t pool_size = 0);
    ~IoContextPool();
    
    // Non-copyable
    IoContextPool(const IoContextPool&) = delete;
    IoContextPool& operator=(const IoContextPool&) = delete;
    
    // Start the pool
    void Start();
    
    // Stop the pool
    void Stop();
    
    // Get the next io_context to use (round-robin)
    asio::io_context& GetNextIoContext();
    
    // Get pool size
    size_t Size() const { return io_contexts_.size(); }
    
    // Check if running
    bool IsRunning() const { return running_; }

private:
    // IO contexts
    std::vector<std::unique_ptr<asio::io_context>> io_contexts_;
    
    // Work guards to keep io_contexts running
    std::vector<asio::executor_work_guard<asio::io_context::executor_type>> work_guards_;
    
    // Threads
    std::vector<std::thread> threads_;
    
    // Next io_context to use (for round-robin)
    std::atomic<size_t> next_io_context_;
    
    // Running flag
    std::atomic<bool> running_;
};

} // namespace duckdb_server
