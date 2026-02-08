# contrib/CMakeLists.txt - Unified dependency management
# All third-party dependencies are managed as Git submodules
# with separate CMake configuration directories

# Check if submodules are initialized
if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/asio/asio/include")
    message(FATAL_ERROR "Submodules not initialized. Run: git submodule update --init --recursive")
endif()

if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/yaml-cpp/CMakeLists.txt")
    message(FATAL_ERROR "Submodules not initialized. Run: git submodule update --init --recursive")
endif()

if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/duckdb/CMakeLists.txt")
    message(FATAL_ERROR "Submodules not initialized. Run: git submodule update --init --recursive")
endif()

if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/spdlog/CMakeLists.txt")
    message(FATAL_ERROR "Submodules not initialized. Run: git submodule update --init --recursive")
endif()

if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/parallel-hashmap/parallel_hashmap")
    message(FATAL_ERROR "Submodules not initialized. Run: git submodule update --init --recursive")
endif()

# Add dependencies
# Note: Order matters - dependencies should be added before dependents

# spdlog (fast C++ logging library)
add_subdirectory(spdlog-cmake)

# ASIO (header-only networking library)
add_subdirectory(asio-cmake)

# yaml-cpp (YAML parser)
add_subdirectory(yaml-cpp-cmake)

# parallel-hashmap (high-performance concurrent hash map)
add_subdirectory(parallel-hashmap-cmake)

# DuckDB (analytical database)
set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
set(ENABLE_EXTENSION_AUTOLOADING ON CACHE BOOL "" FORCE)
set(ENABLE_EXTENSION_AUTOINSTALL ON CACHE BOOL "" FORCE)
# Disable sanitizers to avoid linker issues with different compilers
set(ENABLE_SANITIZER OFF CACHE BOOL "" FORCE)
set(ENABLE_UBSAN OFF CACHE BOOL "" FORCE)
set(ENABLE_THREAD_SANITIZER OFF CACHE BOOL "" FORCE)

# Suppress warnings from third-party code (DuckDB and its dependencies)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    # Save current compile options
    set(DUCKD_SAVED_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    # Suppress warnings for third-party code
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
endif()

add_subdirectory(duckdb)

# Restore compile options after DuckDB
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    set(CMAKE_CXX_FLAGS "${DUCKD_SAVED_CXX_FLAGS}")
endif()

# Arrow Flight SQL (optional, for high-performance columnar data transfer)
if(WITH_FLIGHT_SQL)
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/arrow/cpp/CMakeLists.txt")
        message(FATAL_ERROR "Arrow submodule not initialized. Run: git submodule update --init --recursive")
    endif()

    # Suppress warnings from Arrow and its bundled dependencies
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        set(DUCKD_SAVED_CXX_FLAGS_ARROW "${CMAKE_CXX_FLAGS}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
    endif()

    add_subdirectory(arrow-cmake)

    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        set(CMAKE_CXX_FLAGS "${DUCKD_SAVED_CXX_FLAGS_ARROW}")
    endif()
endif()
