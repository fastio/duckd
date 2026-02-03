# DuckD Tests
# tests/CMakeLists.txt

#===----------------------------------------------------------------------===//
# Unit Tests - PostgreSQL Protocol
#===----------------------------------------------------------------------===//

# Common libraries for unit tests
set(DUCKD_TEST_LIBS
    duckd_server
    duckdb_main_extension
    core_functions_extension
    parquet_extension
    duckdb_generated_extension_loader
)

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

#===----------------------------------------------------------------------===//
# Integration Tests - Remote Extension (existing)
#===----------------------------------------------------------------------===//

add_executable(test_duckd_remote ${CMAKE_SOURCE_DIR}/tests/integration/test_extension.cpp)

target_include_directories(test_duckd_remote
    PRIVATE
        ${DUCKDB_INCLUDE_DIRS}
        ${CMAKE_SOURCE_DIR}/src/client/remote-client
)

target_link_libraries(test_duckd_remote
    PRIVATE
        duckd_client
        duckdb_main_extension
        core_functions_extension
        parquet_extension
        duckdb_generated_extension_loader
)

target_compile_features(test_duckd_remote PUBLIC cxx_std_17)

add_test(NAME test_duckd_remote COMMAND test_duckd_remote)

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
