# DuckD Client Extension – Arrow Flight SQL
# src/client/duckd-client/CMakeLists.txt
#
# Builds a loadable DuckDB extension (.duckdb_extension) that allows DuckDB to
# attach a remote duckd server as a catalog via Arrow Flight SQL.

set(DUCKD_CLIENT_EXT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/flight_client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckd_catalog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckd_client_extension.cpp
)

#===----------------------------------------------------------------------===//
# Arrow include / library discovery
# (contrib/arrow is already configured by the time this runs when
#  WITH_FLIGHT_SQL is ON because contrib/CMakeLists.txt has already
#  added the arrow subdir and set ARROW_BUNDLED_DEPS_LIB)
#===----------------------------------------------------------------------===//

set(ARROW_INCLUDE_DIRS
    ${CMAKE_SOURCE_DIR}/contrib/arrow/cpp/src
    ${CMAKE_BINARY_DIR}/contrib/arrow-cmake/arrow/src
)

#===----------------------------------------------------------------------===//
# Loadable extension target (duckd_client.duckdb_extension)
#===----------------------------------------------------------------------===//

add_library(duckd_flight_client SHARED ${DUCKD_CLIENT_EXT_SOURCES})

target_include_directories(duckd_flight_client
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src
        ${DUCKDB_INCLUDE_DIRS}
        ${ARROW_INCLUDE_DIRS}
)

target_link_libraries(duckd_flight_client
    PRIVATE
        arrow_flight_sql_static
        arrow_flight_static
        arrow_static
        "${ARROW_BUNDLED_DEPS_LIB}"
        duckdb_main_extension
        duckdb_generated_extension_loader
        Threads::Threads
)

# DuckDB expects loadable extensions to use -undefined dynamic_lookup on macOS
# so that DuckDB symbols are resolved at runtime.
if(APPLE)
    target_link_options(duckd_flight_client PRIVATE -undefined dynamic_lookup)
elseif(UNIX)
    target_link_options(duckd_flight_client PRIVATE -Wl,--allow-shlib-undefined)
endif()

set_target_properties(duckd_flight_client PROPERTIES
    PREFIX  ""
    OUTPUT_NAME duckd_client
    SUFFIX  ".duckdb_extension"
    CXX_STANDARD 17
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(duckd_flight_client PRIVATE
        -Wall -Wextra -Wno-unused-parameter
    )
endif()

#===----------------------------------------------------------------------===//
# Static library variant (for embedding in tests)
#===----------------------------------------------------------------------===//

add_library(duckd_flight_client_static STATIC ${DUCKD_CLIENT_EXT_SOURCES})

target_include_directories(duckd_flight_client_static
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${DUCKDB_INCLUDE_DIRS}
        ${ARROW_INCLUDE_DIRS}
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(duckd_flight_client_static
    PUBLIC
        arrow_flight_sql_static
        arrow_flight_static
        arrow_static
        "${ARROW_BUNDLED_DEPS_LIB}"
        duckdb_main_extension
        duckdb_generated_extension_loader
        Threads::Threads
)

target_compile_features(duckd_flight_client_static PUBLIC cxx_std_17)

#===----------------------------------------------------------------------===//
# Install
#===----------------------------------------------------------------------===//

install(TARGETS duckd_flight_client
    LIBRARY DESTINATION lib/duckdb/extensions
    RUNTIME DESTINATION lib/duckdb/extensions
)
