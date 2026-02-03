# DuckD CLI
# programs/client/CMakeLists.txt

# Find readline
find_library(READLINE_LIBRARY readline)
find_path(READLINE_INCLUDE_DIR readline/readline.h)

if(READLINE_LIBRARY AND READLINE_INCLUDE_DIR)
    add_executable(duckd-cli ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

    target_include_directories(duckd-cli
        PRIVATE
            ${READLINE_INCLUDE_DIR}
            ${DUCKDB_INCLUDE_DIRS}
            ${CMAKE_SOURCE_DIR}/src/client/remote-client
    )

    target_link_libraries(duckd-cli
        PRIVATE
            duckd_client
            duckdb_main_extension
            core_functions_extension
            parquet_extension
            duckdb_generated_extension_loader
            ${READLINE_LIBRARY}
    )

    target_compile_features(duckd-cli PUBLIC cxx_std_17)

    install(TARGETS duckd-cli RUNTIME DESTINATION bin)
else()
    message(WARNING "readline not found, duckd-cli will not be built")
endif()
