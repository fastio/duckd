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

# Add dependencies
# Note: Order matters - dependencies should be added before dependents

# spdlog (fast C++ logging library)
add_subdirectory(spdlog-cmake)

# ASIO (header-only networking library)
add_subdirectory(asio-cmake)

# yaml-cpp (YAML parser)
add_subdirectory(yaml-cpp-cmake)

# DuckDB (analytical database)
set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
set(ENABLE_EXTENSION_AUTOLOADING ON CACHE BOOL "" FORCE)
set(ENABLE_EXTENSION_AUTOINSTALL ON CACHE BOOL "" FORCE)
add_subdirectory(duckdb)
