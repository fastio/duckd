# DuckD Client Library
# src/client/CMakeLists.txt

set(DUCKD_CLIENT_SRC_DIR ${CMAKE_SOURCE_DIR}/src/client)

set(DUCKD_CLIENT_SOURCES
    ${DUCKD_CLIENT_SRC_DIR}/remote-client/remote_client.cpp
    ${DUCKD_CLIENT_SRC_DIR}/remote-client/duckdb_remote_extension.cpp
)

# Client library (static)
add_library(duckd_client STATIC ${DUCKD_CLIENT_SOURCES})

target_include_directories(duckd_client
    PUBLIC
        ${DUCKD_CLIENT_SRC_DIR}/remote-client
    PRIVATE
        ${DUCKDB_INCLUDE_DIRS}
)

target_link_libraries(duckd_client
    PUBLIC
        duckdb_static
        duckdb_asio
        Threads::Threads
)

target_compile_features(duckd_client PUBLIC cxx_std_17)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(duckd_client PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()

# Loadable extension for DuckDB
add_library(duckdb_remote SHARED ${DUCKD_CLIENT_SOURCES})

target_include_directories(duckdb_remote
    PRIVATE
        ${DUCKD_CLIENT_SRC_DIR}/remote-client
        ${DUCKDB_INCLUDE_DIRS}
)

target_link_libraries(duckdb_remote
    PRIVATE
        duckdb_asio
        Threads::Threads
)

# Platform-specific linker flags for loadable extensions
if(APPLE)
    set_target_properties(duckdb_remote PROPERTIES
        LINK_FLAGS "-undefined dynamic_lookup"
    )
elseif(UNIX AND NOT APPLE)
    set_target_properties(duckdb_remote PROPERTIES
        LINK_FLAGS "-Wl,--allow-shlib-undefined"
    )
endif()

set_target_properties(duckdb_remote PROPERTIES
    PREFIX ""
    OUTPUT_NAME duckdb_remote
    SUFFIX ".duckdb_extension"
)

target_compile_features(duckdb_remote PUBLIC cxx_std_17)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(duckdb_remote PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()

install(TARGETS duckdb_remote
    LIBRARY DESTINATION lib/duckdb/extensions
    RUNTIME DESTINATION lib/duckdb/extensions
)
