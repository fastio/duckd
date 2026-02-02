//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// network/io_context_pool.cpp
//
// IO context thread pool implementation
//===----------------------------------------------------------------------===//

#include "duckdb_server/network/io_context_pool.hpp"
#include "duckdb_server/utils/logger.hpp"

namespace duckdb_server {

IoContextPool::IoContextPool(size_t pool_size)
    : next_io_context_(0)
    , running_(false) {
    
    // Determine pool size
    if (pool_size == 0) {
        pool_size = std::thread::hardware_concurrency();
        if (pool_size == 0) {
            pool_size = 4;  // Default fallback
        }
    }
    
    // Create io_contexts
    for (size_t i = 0; i < pool_size; ++i) {
        io_contexts_.push_back(std::make_unique<asio::io_context>());
    }
    
    LOG_DEBUG("io_pool", "Created IO context pool with " + std::to_string(pool_size) + " contexts");
}

IoContextPool::~IoContextPool() {
    Stop();
}

void IoContextPool::Start() {
    if (running_) {
        return;
    }
    
    running_ = true;
    
    // Create work guards and start threads
    for (auto& io_context : io_contexts_) {
        work_guards_.push_back(
            asio::make_work_guard(*io_context)
        );
        
        threads_.emplace_back([&io_context]() {
            io_context->run();
        });
    }
    
    LOG_INFO("io_pool", "IO context pool started with " + std::to_string(threads_.size()) + " threads");
}

void IoContextPool::Stop() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    
    // Release work guards
    work_guards_.clear();
    
    // Stop all io_contexts
    for (auto& io_context : io_contexts_) {
        io_context->stop();
    }
    
    // Join all threads
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    
    threads_.clear();
    
    LOG_INFO("io_pool", "IO context pool stopped");
}

asio::io_context& IoContextPool::GetNextIoContext() {
    // Round-robin selection
    size_t index = next_io_context_.fetch_add(1) % io_contexts_.size();
    return *io_contexts_[index];
}

} // namespace duckdb_server
