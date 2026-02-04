# DuckD Benchmarks
# benchmarks/CMakeLists.txt
#
# Benchmarks use ASIO from contrib for PostgreSQL protocol communication
# No external dependencies required (no libpq)

#===----------------------------------------------------------------------===//
# Common benchmark settings
#===----------------------------------------------------------------------===//

set(BENCHMARK_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/contrib/asio/asio/include
)

#===----------------------------------------------------------------------===//
# Connection Benchmark
#===----------------------------------------------------------------------===//

add_executable(bench_connection bench_connection.cpp)
target_include_directories(bench_connection PRIVATE ${BENCHMARK_INCLUDE_DIRS})
target_link_libraries(bench_connection PRIVATE Threads::Threads)
target_compile_features(bench_connection PUBLIC cxx_std_17)
target_compile_definitions(bench_connection PRIVATE ASIO_STANDALONE ASIO_NO_DEPRECATED)

#===----------------------------------------------------------------------===//
# Query Benchmark
#===----------------------------------------------------------------------===//

add_executable(bench_query bench_query.cpp)
target_include_directories(bench_query PRIVATE ${BENCHMARK_INCLUDE_DIRS})
target_link_libraries(bench_query PRIVATE Threads::Threads)
target_compile_features(bench_query PUBLIC cxx_std_17)
target_compile_definitions(bench_query PRIVATE ASIO_STANDALONE ASIO_NO_DEPRECATED)

#===----------------------------------------------------------------------===//
# Prepared Statement Benchmark
#===----------------------------------------------------------------------===//

add_executable(bench_prepared bench_prepared.cpp)
target_include_directories(bench_prepared PRIVATE ${BENCHMARK_INCLUDE_DIRS})
target_link_libraries(bench_prepared PRIVATE Threads::Threads)
target_compile_features(bench_prepared PUBLIC cxx_std_17)
target_compile_definitions(bench_prepared PRIVATE ASIO_STANDALONE ASIO_NO_DEPRECATED)

#===----------------------------------------------------------------------===//
# Concurrent Connections Benchmark
#===----------------------------------------------------------------------===//

add_executable(bench_concurrent bench_concurrent.cpp)
target_include_directories(bench_concurrent PRIVATE ${BENCHMARK_INCLUDE_DIRS})
target_link_libraries(bench_concurrent PRIVATE Threads::Threads)
target_compile_features(bench_concurrent PUBLIC cxx_std_17)
target_compile_definitions(bench_concurrent PRIVATE ASIO_STANDALONE ASIO_NO_DEPRECATED)

#===----------------------------------------------------------------------===//
# TPC-B Benchmark
#===----------------------------------------------------------------------===//

add_executable(bench_tpcb bench_tpcb.cpp)
target_include_directories(bench_tpcb PRIVATE ${BENCHMARK_INCLUDE_DIRS})
target_link_libraries(bench_tpcb PRIVATE Threads::Threads)
target_compile_features(bench_tpcb PUBLIC cxx_std_17)
target_compile_definitions(bench_tpcb PRIVATE ASIO_STANDALONE ASIO_NO_DEPRECATED)

#===----------------------------------------------------------------------===//
# Custom target to build all benchmarks
#===----------------------------------------------------------------------===//

add_custom_target(benchmarks
    DEPENDS
        bench_connection
        bench_query
        bench_prepared
        bench_concurrent
        bench_tpcb
    COMMENT "Building all benchmarks"
)
