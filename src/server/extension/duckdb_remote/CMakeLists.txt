# DuckDB Remote Extension CMakeLists.txt
cmake_minimum_required(VERSION 3.16)

# Extension name and version
set(EXTENSION_NAME duckdb_remote)
set(EXTENSION_VERSION 1.0.0)

# Determine include directories based on build context
if(DEFINED DUCKDB_DIR)
    # Building from server with DUCKDB_DIR defined
    set(EXTENSION_DUCKDB_INCLUDE_DIRS
        ${DUCKDB_DIR}/src/include
        ${DUCKDB_DIR}/third_party/concurrentqueue
    )
elseif(TARGET duckdb_static)
    # Building as part of main DuckDB build
    get_target_property(DUCKDB_INCLUDES duckdb_static INTERFACE_INCLUDE_DIRECTORIES)
    if(DUCKDB_INCLUDES)
        set(EXTENSION_DUCKDB_INCLUDE_DIRS ${DUCKDB_INCLUDES})
    else()
        set(EXTENSION_DUCKDB_INCLUDE_DIRS
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_SOURCE_DIR}/third_party/concurrentqueue
        )
    endif()
else()
    find_package(DuckDB REQUIRED)
    set(EXTENSION_DUCKDB_INCLUDE_DIRS "")
endif()

# Check for asio
if(NOT TARGET duckdb_asio)
    if(DEFINED DUCKDB_DIR)
        add_subdirectory(${DUCKDB_DIR}/third_party/asio
                         ${CMAKE_CURRENT_BINARY_DIR}/third_party/asio)
    else()
        message(FATAL_ERROR "duckdb_asio target not found")
    endif()
endif()

find_package(Threads REQUIRED)

# Extension sources
set(EXTENSION_SOURCES
    duckdb_remote_extension.cpp
    remote_client.cpp
)

# Build loadable extension
# Note: Loadable extensions should NOT link duckdb_static as symbols are provided by the host
add_library(${EXTENSION_NAME} SHARED ${EXTENSION_SOURCES})

target_include_directories(${EXTENSION_NAME}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${EXTENSION_DUCKDB_INCLUDE_DIRS}
)

# For loadable extension, only link asio and threads (not duckdb itself)
# DuckDB symbols will be resolved at runtime by the host process
target_link_libraries(${EXTENSION_NAME}
    PRIVATE
        duckdb_asio
        Threads::Threads
)

# Platform-specific linker flags for loadable extensions
if(APPLE)
    # Allow undefined symbols that will be resolved at load time
    set_target_properties(${EXTENSION_NAME} PROPERTIES
        LINK_FLAGS "-undefined dynamic_lookup"
    )
elseif(UNIX AND NOT APPLE)
    # For Linux, use --allow-shlib-undefined
    set_target_properties(${EXTENSION_NAME} PROPERTIES
        LINK_FLAGS "-Wl,--allow-shlib-undefined"
    )
endif()

target_compile_features(${EXTENSION_NAME} PUBLIC cxx_std_17)

# Set extension properties
set_target_properties(${EXTENSION_NAME} PROPERTIES
    PREFIX ""
    OUTPUT_NAME ${EXTENSION_NAME}
)

# Platform-specific extension suffix
set_target_properties(${EXTENSION_NAME} PROPERTIES
    SUFFIX ".duckdb_extension"
)

# Compiler flags
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${EXTENSION_NAME} PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()

# Install
install(TARGETS ${EXTENSION_NAME}
    LIBRARY DESTINATION lib/duckdb/extensions
    RUNTIME DESTINATION lib/duckdb/extensions
)

# Also build a static library for linking into the server executable
add_library(${EXTENSION_NAME}_static STATIC ${EXTENSION_SOURCES})

target_include_directories(${EXTENSION_NAME}_static
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${EXTENSION_DUCKDB_INCLUDE_DIRS}
)

# Static library can link duckdb_static since it will be linked into the server
if(TARGET duckdb_static)
    target_link_libraries(${EXTENSION_NAME}_static
        PUBLIC
            duckdb_static
            duckdb_asio
            Threads::Threads
    )
else()
    target_link_libraries(${EXTENSION_NAME}_static
        PUBLIC
            DuckDB::duckdb
            duckdb_asio
            Threads::Threads
    )
endif()

target_compile_features(${EXTENSION_NAME}_static PUBLIC cxx_std_17)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${EXTENSION_NAME}_static PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()

# Test executable
add_executable(test_duckdb_remote test_extension.cpp)

target_include_directories(test_duckdb_remote
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${EXTENSION_DUCKDB_INCLUDE_DIRS}
)

target_link_libraries(test_duckdb_remote
    PRIVATE
        ${EXTENSION_NAME}_static
        duckdb_main_extension
        core_functions_extension
        parquet_extension
        duckdb_generated_extension_loader
)

target_compile_features(test_duckdb_remote PUBLIC cxx_std_17)

# CLI executable
add_executable(duckdb-remote-cli duckdb_remote_cli.cpp)

target_include_directories(duckdb-remote-cli
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${EXTENSION_DUCKDB_INCLUDE_DIRS}
)

# Find readline
find_library(READLINE_LIBRARY readline)
find_path(READLINE_INCLUDE_DIR readline/readline.h)

if(READLINE_LIBRARY AND READLINE_INCLUDE_DIR)
    target_include_directories(duckdb-remote-cli PRIVATE ${READLINE_INCLUDE_DIR})
    target_link_libraries(duckdb-remote-cli
        PRIVATE
            ${EXTENSION_NAME}_static
            duckdb_main_extension
            core_functions_extension
            parquet_extension
            duckdb_generated_extension_loader
            ${READLINE_LIBRARY}
    )
else()
    message(WARNING "readline not found, CLI will not be built")
endif()

target_compile_features(duckdb-remote-cli PUBLIC cxx_std_17)

# Install CLI
install(TARGETS duckdb-remote-cli
    RUNTIME DESTINATION bin
)
