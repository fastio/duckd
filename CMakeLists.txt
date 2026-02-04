# DuckD - DuckDB Server
cmake_minimum_required(VERSION 3.16)
project(duckd VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Options
option(BUILD_TESTS "Build tests" ON)
option(WITH_SYSTEMD "Enable systemd integration" OFF)

# Get git commit hash
execute_process(
    COMMAND git rev-parse --short HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE DUCKD_GIT_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)
if(NOT DUCKD_GIT_COMMIT)
    set(DUCKD_GIT_COMMIT "unknown")
endif()

# Get build time
string(TIMESTAMP DUCKD_BUILD_TIME "%Y-%m-%d %H:%M:%S")

# Generate version header
configure_file(
    ${CMAKE_SOURCE_DIR}/src/version.hpp.in
    ${CMAKE_BINARY_DIR}/generated/version.hpp
)

# Set source directory for subdirectory CMakeLists
set(DUCKD_SOURCE_DIR ${CMAKE_SOURCE_DIR})

# Add all third-party dependencies from contrib
set(DUCKDB_DIR ${CMAKE_SOURCE_DIR}/contrib/duckdb)
add_subdirectory(contrib)

# DuckDB include directories
set(DUCKDB_INCLUDE_DIRS
    ${DUCKDB_DIR}/src/include
    ${DUCKDB_DIR}/third_party/concurrentqueue
)

# Find threads
find_package(Threads REQUIRED)

# Server library
add_subdirectory(src)

# Client library
add_subdirectory(src/client)

# Programs
add_subdirectory(programs/server)
add_subdirectory(programs/client)

# Tests
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Install
install(TARGETS duckd-server duckd-cli
    RUNTIME DESTINATION bin
)
