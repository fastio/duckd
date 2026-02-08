# DuckD Server Library
# src/CMakeLists.txt

set(DUCKD_SRC_DIR ${CMAKE_SOURCE_DIR}/src)

set(DUCKD_SERVER_SOURCES
    # Network layer
    ${DUCKD_SRC_DIR}/network/io_context_pool.cpp
    ${DUCKD_SRC_DIR}/network/tcp_server.cpp
    ${DUCKD_SRC_DIR}/network/pg_connection.cpp

    # Protocol layer (PostgreSQL)
    ${DUCKD_SRC_DIR}/protocol/pg/pg_handler.cpp

    # Session layer
    ${DUCKD_SRC_DIR}/session/session.cpp
    ${DUCKD_SRC_DIR}/session/session_manager.cpp
    ${DUCKD_SRC_DIR}/session/connection_pool.cpp

    # Executor
    ${DUCKD_SRC_DIR}/executor/executor_pool.cpp

    # HTTP
    ${DUCKD_SRC_DIR}/http/http_server.cpp

    # Logging
    ${DUCKD_SRC_DIR}/logging/logger.cpp
)

if(WITH_FLIGHT_SQL)
    list(APPEND DUCKD_SERVER_SOURCES
        ${DUCKD_SRC_DIR}/protocol/flight/flight_sql_server.cpp
    )
endif()

add_library(duckd_server STATIC ${DUCKD_SERVER_SOURCES})

target_include_directories(duckd_server
    PUBLIC
        ${DUCKD_SRC_DIR}
    SYSTEM PRIVATE
        ${DUCKDB_INCLUDE_DIRS}
)

target_link_libraries(duckd_server
    PUBLIC
        duckdb_static
        duckdb_asio
        spdlog::spdlog
        Threads::Threads
        yaml-cpp::yaml-cpp
        phmap::phmap
)

if(WITH_FLIGHT_SQL)
    target_include_directories(duckd_server
        SYSTEM PUBLIC
            ${CMAKE_SOURCE_DIR}/contrib/arrow/cpp/src
            ${CMAKE_BINARY_DIR}/contrib/arrow-cmake/arrow/src
    )
    target_link_libraries(duckd_server
        PUBLIC
            arrow_flight_sql_static
            arrow_flight_static
            arrow_static
    )
    # arrow_bundled_dependencies is an IMPORTED target created by Arrow's build.
    # We need to link its output directly since it's a merged static lib.
    set(ARROW_BUNDLED_DEPS_LIB
        "${CMAKE_BINARY_DIR}/contrib/arrow-cmake/arrow/release/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}")
    target_link_libraries(duckd_server PUBLIC "${ARROW_BUNDLED_DEPS_LIB}")
    target_compile_definitions(duckd_server PUBLIC DUCKD_WITH_FLIGHT_SQL)
endif()

target_compile_features(duckd_server PUBLIC cxx_std_17)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(duckd_server PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()
