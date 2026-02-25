# DuckD Tests
# tests/CMakeLists.txt

#===----------------------------------------------------------------------===//
# Unit Tests - Logging
#===----------------------------------------------------------------------===//

# Common libraries for unit tests
set(DUCKD_TEST_LIBS
    duckd_server
    duckdb_main_extension
    core_functions_extension
    parquet_extension
    duckdb_generated_extension_loader
)

# Test: logger (logging system)
add_executable(test_logger
    ${CMAKE_SOURCE_DIR}/tests/unit/logging/test_logger.cpp
)
target_include_directories(test_logger
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
)
target_link_libraries(test_logger PRIVATE ${DUCKD_TEST_LIBS})
target_compile_features(test_logger PUBLIC cxx_std_17)
add_test(NAME test_logger COMMAND test_logger)

#===----------------------------------------------------------------------===//
# Unit Tests - PostgreSQL Protocol
#===----------------------------------------------------------------------===//

# Test: pg_protocol (byte order utilities and constants)
add_executable(test_pg_protocol
    ${CMAKE_SOURCE_DIR}/tests/unit/pg/test_pg_protocol.cpp
)
target_include_directories(test_pg_protocol
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
)
target_link_libraries(test_pg_protocol PRIVATE ${DUCKD_TEST_LIBS})
target_compile_features(test_pg_protocol PUBLIC cxx_std_17)
add_test(NAME test_pg_protocol COMMAND test_pg_protocol)

# Test: pg_types (DuckDB to PostgreSQL type mapping)
add_executable(test_pg_types
    ${CMAKE_SOURCE_DIR}/tests/unit/pg/test_pg_types.cpp
)
target_include_directories(test_pg_types
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
)
target_link_libraries(test_pg_types PRIVATE ${DUCKD_TEST_LIBS})
target_compile_features(test_pg_types PUBLIC cxx_std_17)
add_test(NAME test_pg_types COMMAND test_pg_types)

# Test: pg_message_writer (message serialization)
add_executable(test_pg_message_writer
    ${CMAKE_SOURCE_DIR}/tests/unit/pg/test_pg_message_writer.cpp
)
target_include_directories(test_pg_message_writer
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
)
target_link_libraries(test_pg_message_writer PRIVATE ${DUCKD_TEST_LIBS})
target_compile_features(test_pg_message_writer PUBLIC cxx_std_17)
add_test(NAME test_pg_message_writer COMMAND test_pg_message_writer)

# Test: pg_message_reader (message parsing)
add_executable(test_pg_message_reader
    ${CMAKE_SOURCE_DIR}/tests/unit/pg/test_pg_message_reader.cpp
)
target_include_directories(test_pg_message_reader
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
)
target_link_libraries(test_pg_message_reader PRIVATE ${DUCKD_TEST_LIBS})
target_compile_features(test_pg_message_reader PUBLIC cxx_std_17)
add_test(NAME test_pg_message_reader COMMAND test_pg_message_reader)

#===----------------------------------------------------------------------===//
# Unit Tests - Executor Pool
#===----------------------------------------------------------------------===//

# Test: executor_pool (async task pool)
add_executable(test_executor_pool
    ${CMAKE_SOURCE_DIR}/tests/unit/executor/test_executor_pool.cpp
)
target_include_directories(test_executor_pool
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
)
target_link_libraries(test_executor_pool PRIVATE ${DUCKD_TEST_LIBS})
target_compile_features(test_executor_pool PUBLIC cxx_std_17)
add_test(NAME test_executor_pool COMMAND test_executor_pool)

#===----------------------------------------------------------------------===//
# Unit Tests - Session Layer
#===----------------------------------------------------------------------===//

# Test: session (session management)
add_executable(test_session
    ${CMAKE_SOURCE_DIR}/tests/unit/session/test_session.cpp
)
target_include_directories(test_session
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
)
target_link_libraries(test_session PRIVATE ${DUCKD_TEST_LIBS})
target_compile_features(test_session PUBLIC cxx_std_17)
add_test(NAME test_session COMMAND test_session)

# Test: session_manager (session lifecycle and pool integration)
add_executable(test_session_manager
    ${CMAKE_SOURCE_DIR}/tests/unit/session/test_session_manager.cpp
)
target_include_directories(test_session_manager
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
)
target_link_libraries(test_session_manager PRIVATE ${DUCKD_TEST_LIBS})
target_compile_features(test_session_manager PUBLIC cxx_std_17)
add_test(NAME test_session_manager COMMAND test_session_manager)

#===----------------------------------------------------------------------===//
# Unit Tests - Configuration
#===----------------------------------------------------------------------===//

# Test: server_config (config parsing and validation)
add_executable(test_server_config
    ${CMAKE_SOURCE_DIR}/tests/unit/config/test_server_config.cpp
)
target_include_directories(test_server_config
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
)
target_link_libraries(test_server_config PRIVATE ${DUCKD_TEST_LIBS})
target_compile_features(test_server_config PUBLIC cxx_std_17)
add_test(NAME test_server_config COMMAND test_server_config)

#===----------------------------------------------------------------------===//
# Integration Tests - PostgreSQL Protocol
#===----------------------------------------------------------------------===//

# Test: pg_integration (end-to-end SQL testing)
add_executable(test_pg_integration
    ${CMAKE_SOURCE_DIR}/tests/integration/pg/test_pg_integration.cpp
)
target_include_directories(test_pg_integration
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
)
target_link_libraries(test_pg_integration
    PRIVATE
        duckd_server
        duckdb_main_extension
        core_functions_extension
        parquet_extension
        duckdb_generated_extension_loader
)
target_compile_features(test_pg_integration PUBLIC cxx_std_17)
add_test(NAME test_pg_integration COMMAND test_pg_integration)

# Note: test_duckd_remote (DUCK binary protocol) has been removed.
# The DUCK binary protocol client (remote-client/) was dead code — the server
# never implemented the corresponding handler.  The Arrow Flight SQL path
# (test_duckd_client) supersedes it.

#===----------------------------------------------------------------------===//
# Custom test target: run all PG protocol tests
#===----------------------------------------------------------------------===//

add_custom_target(test_pg
    COMMAND ${CMAKE_CTEST_COMMAND} -R "test_pg_" --output-on-failure
    DEPENDS
        test_pg_protocol
        test_pg_types
        test_pg_message_writer
        test_pg_message_reader
        test_pg_integration
    COMMENT "Running PostgreSQL protocol tests"
)

#===----------------------------------------------------------------------===//
# Integration Tests - Arrow Flight SQL (conditional)
#===----------------------------------------------------------------------===//

if(WITH_FLIGHT_SQL)
    add_executable(test_flight_sql
        ${CMAKE_SOURCE_DIR}/tests/integration/flight/test_flight_sql.cpp
    )
    target_include_directories(test_flight_sql
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src
            ${DUCKDB_INCLUDE_DIRS}
            ${CMAKE_SOURCE_DIR}/contrib/arrow/cpp/src
            ${CMAKE_BINARY_DIR}/contrib/arrow-cmake/arrow/src
    )
    target_link_libraries(test_flight_sql
        PRIVATE
            ${DUCKD_TEST_LIBS}
            arrow_flight_sql_static
            arrow_flight_static
            arrow_static
            "${ARROW_BUNDLED_DEPS_LIB}"
    )
    target_compile_features(test_flight_sql PUBLIC cxx_std_17)
    add_test(NAME test_flight_sql COMMAND test_flight_sql)

    #===------------------------------------------------------------------===//
    # Integration Tests - duckd_client extension (DuckDB SDK access)
    #===------------------------------------------------------------------===//

    add_executable(test_duckd_client
        ${CMAKE_SOURCE_DIR}/tests/integration/flight/test_duckd_client.cpp
    )
    target_include_directories(test_duckd_client
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src
            ${CMAKE_SOURCE_DIR}/src/client/duckd-client
            ${DUCKDB_INCLUDE_DIRS}
            ${CMAKE_SOURCE_DIR}/contrib/arrow/cpp/src
            ${CMAKE_BINARY_DIR}/contrib/arrow-cmake/arrow/src
    )
    target_link_libraries(test_duckd_client
        PRIVATE
            ${DUCKD_TEST_LIBS}
            duckd_flight_client_static
            arrow_flight_sql_static
            arrow_flight_static
            arrow_static
            "${ARROW_BUNDLED_DEPS_LIB}"
    )
    target_compile_features(test_duckd_client PUBLIC cxx_std_17)
    add_test(NAME test_duckd_client COMMAND test_duckd_client)

    add_custom_target(test_flight
        COMMAND ${CMAKE_CTEST_COMMAND} -R "test_flight" --output-on-failure
        DEPENDS test_flight_sql test_duckd_client
        COMMENT "Running Flight SQL tests"
    )
endif()

#===----------------------------------------------------------------------===//
# Custom test target: run logging tests
#===----------------------------------------------------------------------===//

add_custom_target(test_logging
    COMMAND ${CMAKE_CTEST_COMMAND} -R "test_logger" --output-on-failure
    DEPENDS test_logger
    COMMENT "Running logging tests"
)

#===----------------------------------------------------------------------===//
# Custom test target: run session layer tests
#===----------------------------------------------------------------------===//

add_custom_target(test_session_layer
    COMMAND ${CMAKE_CTEST_COMMAND} -R "test_session" --output-on-failure
    DEPENDS
        test_session
        test_session_manager
    COMMENT "Running session layer tests"
)
