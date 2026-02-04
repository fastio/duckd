# spdlog configuration
# https://github.com/gabime/spdlog
# Version: 1.17.0
# License: MIT

set(SPDLOG_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../spdlog")

# Use header-only mode (simpler, no library to link)
# For better compile times, can switch to compiled library later
set(SPDLOG_FMT_EXTERNAL OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_BENCH OFF CACHE BOOL "" FORCE)
set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)

add_subdirectory("${SPDLOG_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/spdlog")
