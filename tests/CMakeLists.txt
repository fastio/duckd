# DuckD Tests
# tests/CMakeLists.txt

# Integration tests
add_executable(test_duckd_remote ${CMAKE_SOURCE_DIR}/tests/integration/test_extension.cpp)

target_include_directories(test_duckd_remote
    PRIVATE
        ${DUCKDB_INCLUDE_DIRS}
        ${CMAKE_SOURCE_DIR}/src/duckd-client/remote-client
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
