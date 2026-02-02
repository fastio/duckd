# DuckD Client Executable (CLI)
# programs/client/CMakeLists.txt

# Find readline
find_library(READLINE_LIBRARY readline)
find_path(READLINE_INCLUDE_DIR readline/readline.h)

if(READLINE_LIBRARY AND READLINE_INCLUDE_DIR)
    add_executable(duckd-client ${CMAKE_SOURCE_DIR}/src/duckd-client/cli/duckdb_remote_cli.cpp)

    target_include_directories(duckd-client
        PRIVATE
            ${READLINE_INCLUDE_DIR}
            ${DUCKDB_INCLUDE_DIRS}
            ${CMAKE_SOURCE_DIR}/src/duckd-client/remote-client
    )

    target_link_libraries(duckd-client
        PRIVATE
            duckd_client
            duckdb_main_extension
            core_functions_extension
            parquet_extension
            duckdb_generated_extension_loader
            ${READLINE_LIBRARY}
    )

    target_compile_features(duckd-client PUBLIC cxx_std_17)

    install(TARGETS duckd-client RUNTIME DESTINATION bin)
else()
    message(WARNING "readline not found, duckd-client will not be built")
endif()
