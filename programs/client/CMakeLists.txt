# DuckD CLI
# programs/client/CMakeLists.txt
#
# The CLI requires both readline and Arrow Flight SQL support.

find_library(READLINE_LIBRARY readline)
find_path(READLINE_INCLUDE_DIR readline/readline.h)

if(READLINE_LIBRARY AND READLINE_INCLUDE_DIR AND WITH_FLIGHT_SQL)
    add_executable(duckd-cli ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

    target_include_directories(duckd-cli
        PRIVATE
            ${READLINE_INCLUDE_DIR}
            ${DUCKDB_INCLUDE_DIRS}
            ${CMAKE_SOURCE_DIR}/src/client/duckd-client
            ${CMAKE_SOURCE_DIR}/contrib/arrow/cpp/src
            ${CMAKE_BINARY_DIR}/contrib/arrow-cmake/arrow/src
    )

    target_link_libraries(duckd-cli
        PRIVATE
            duckd_flight_client_static
            duckdb_main_extension
            core_functions_extension
            parquet_extension
            duckdb_generated_extension_loader
            arrow_flight_sql_static
            arrow_flight_static
            arrow_static
            "${ARROW_BUNDLED_DEPS_LIB}"
            ${READLINE_LIBRARY}
            Threads::Threads
    )

    target_compile_definitions(duckd-cli PRIVATE DUCKD_WITH_FLIGHT_SQL)
    target_compile_features(duckd-cli PUBLIC cxx_std_17)

    install(TARGETS duckd-cli RUNTIME DESTINATION bin)
else()
    if(NOT READLINE_LIBRARY OR NOT READLINE_INCLUDE_DIR)
        message(WARNING "readline not found, duckd-cli will not be built")
    elseif(NOT WITH_FLIGHT_SQL)
        message(STATUS "duckd-cli requires WITH_FLIGHT_SQL=ON, skipping")
    endif()
endif()
