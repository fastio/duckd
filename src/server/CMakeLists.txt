# DuckDB Server Module CMakeLists.txt
cmake_minimum_required(VERSION 3.16)

# Check if we're building standalone or as part of DuckDB
if(NOT TARGET duckdb_static)
    project(duckdb_server VERSION 1.0.0)
    
    # Standalone build - need to find DuckDB
    if(DEFINED DUCKDB_DIR)
        # Build DuckDB from source
        set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
        set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
        add_subdirectory(${DUCKDB_DIR} ${CMAKE_CURRENT_BINARY_DIR}/duckdb)
        
        # Add DuckDB include directories for standalone build
        set(DUCKDB_INCLUDE_DIRS
            ${DUCKDB_DIR}/src/include
            ${DUCKDB_DIR}/third_party/concurrentqueue
        )
    else()
        # Try to find installed DuckDB
        find_package(DuckDB REQUIRED)
        # DuckDB::duckdb should have includes set up
    endif()
    
    # In standalone mode, we need to add asio from third_party
    if(NOT TARGET duckdb_asio)
        add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../../third_party/asio 
                         ${CMAKE_CURRENT_BINARY_DIR}/third_party/asio)
    endif()
else()
    # Building as part of DuckDB - set include path
    set(DUCKDB_INCLUDE_DIRS
        ${PROJECT_SOURCE_DIR}/src/include
        ${PROJECT_SOURCE_DIR}/third_party/concurrentqueue
    )
    
    # asio should already be available via add_third_party(asio)
    if(NOT TARGET duckdb_asio)
        message(FATAL_ERROR "duckdb_asio target not found. Please add 'add_third_party(asio)' to the main CMakeLists.txt")
    endif()
endif()

# Find threads
find_package(Threads REQUIRED)

# Server source files
set(SERVER_SOURCES
    # Network layer
    network/io_context_pool.cpp
    network/tcp_server.cpp
    network/tcp_connection.cpp
    
    # Protocol layer
    protocol/message.cpp
    protocol/message_parser.cpp
    protocol/message_builder.cpp
    protocol/protocol_handler.cpp
    
    # Session layer
    session/session.cpp
    session/session_manager.cpp
    
    # Serialization
    serialization/arrow_serializer.cpp
    
    # Executor
    executor/query_executor.cpp
    executor/executor_pool.cpp
    
    # Config
    config/server_config.cpp
    
    # Utils
    utils/logger.cpp
)

# Create server library
add_library(duckdb_server_lib STATIC ${SERVER_SOURCES})

target_include_directories(duckdb_server_lib 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${DUCKDB_INCLUDE_DIRS}
)

# Link against DuckDB and Asio
if(TARGET duckdb_static)
    target_link_libraries(duckdb_server_lib
        PUBLIC
            duckdb_static
            duckdb_asio
            Threads::Threads
    )
else()
    target_link_libraries(duckdb_server_lib
        PUBLIC
            DuckDB::duckdb
            duckdb_asio
            Threads::Threads
    )
endif()

target_compile_features(duckdb_server_lib PUBLIC cxx_std_17)

# Compiler-specific flags
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(duckdb_server_lib PRIVATE -Wall -Wextra)
endif()

# Server executable
add_executable(duckdb-server main.cpp)

target_include_directories(duckdb-server
    PRIVATE
        ${DUCKDB_INCLUDE_DIRS}
)

# Link server library
target_link_libraries(duckdb-server
    PRIVATE
        duckdb_server_lib
)

# Link extension libraries and other dependencies for standalone build
if(DEFINED DUCKDB_DIR)
    # Link extensions that are statically linked in DuckDB
    # Note: The DUCKDB_EXTENSION_NAMES list is not accessible here due to CMake scope,
    # so we manually link the known default extensions.
    if(TARGET core_functions_extension)
        target_link_libraries(duckdb-server PRIVATE core_functions_extension)
    endif()
    
    if(TARGET parquet_extension)
        target_link_libraries(duckdb-server PRIVATE parquet_extension)
    endif()
    
    if(TARGET duckdb_generated_extension_loader)
        target_link_libraries(duckdb-server PRIVATE duckdb_generated_extension_loader)
    endif()
    
    target_link_libraries(duckdb-server PRIVATE Threads::Threads)
endif()

# Install
install(TARGETS duckdb-server
    RUNTIME DESTINATION bin
)

install(DIRECTORY include/
    DESTINATION include
    FILES_MATCHING PATTERN "*.hpp"
)

# Copy config file
install(FILES config/duckdb-server.toml
    DESTINATION etc/duckdb
    RENAME duckdb-server.toml.example
)
