# Asio Standalone (header-only library)
# https://github.com/chriskohlhoff/asio
# Version: 1.28.0
# License: Boost Software License 1.0

set(ASIO_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../asio")

add_library(duckdb_asio INTERFACE)

target_include_directories(duckdb_asio SYSTEM INTERFACE
    $<BUILD_INTERFACE:${ASIO_SOURCE_DIR}/asio/include>
)

# Use standalone mode (without Boost)
target_compile_definitions(duckdb_asio INTERFACE
    ASIO_STANDALONE
    ASIO_NO_DEPRECATED
)

# Find threads (required by Asio)
find_package(Threads REQUIRED)
target_link_libraries(duckdb_asio INTERFACE Threads::Threads)

# Platform-specific dependencies
if(WIN32)
    # Windows needs these libraries for networking
    target_link_libraries(duckdb_asio INTERFACE ws2_32 wsock32)
endif()

set_target_properties(duckdb_asio PROPERTIES EXPORT_NAME duckdb_asio)

install(TARGETS duckdb_asio
        EXPORT "${DUCKDB_EXPORT_SET}")
