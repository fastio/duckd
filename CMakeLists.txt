# DuckD - DuckDB Server
cmake_minimum_required(VERSION 3.16)
project(duckd VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Options
option(BUILD_TESTS "Build tests" ON)

# Set source directory for subdirectory CMakeLists
set(DUCKD_SOURCE_DIR ${CMAKE_SOURCE_DIR})

# Build DuckDB from contrib
set(DUCKDB_DIR ${CMAKE_SOURCE_DIR}/contrib/duckdb)
set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
set(ENABLE_EXTENSION_AUTOLOADING ON CACHE BOOL "" FORCE)
set(ENABLE_EXTENSION_AUTOINSTALL ON CACHE BOOL "" FORCE)

add_subdirectory(${DUCKDB_DIR} ${CMAKE_BINARY_DIR}/duckdb)

# Add asio from contrib
if(NOT TARGET duckdb_asio)
    add_subdirectory(${CMAKE_SOURCE_DIR}/contrib/asio ${CMAKE_BINARY_DIR}/asio)
endif()

# DuckDB include directories
set(DUCKDB_INCLUDE_DIRS
    ${DUCKDB_DIR}/src/include
    ${DUCKDB_DIR}/third_party/concurrentqueue
)

# Find threads
find_package(Threads REQUIRED)

# Server library
add_subdirectory(src/duckd)

# Client library
add_subdirectory(src/duckd-client)

# Programs
add_subdirectory(programs/server)
add_subdirectory(programs/client)

# Tests
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Install
install(TARGETS duckd-server duckd-client
    RUNTIME DESTINATION bin
)
