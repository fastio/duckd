# DuckD Server Library
# src/CMakeLists.txt

set(DUCKD_SRC_DIR ${CMAKE_SOURCE_DIR}/src)

set(DUCKD_SERVER_SOURCES
    # Network layer
    ${DUCKD_SRC_DIR}/network/io_context_pool.cpp
    ${DUCKD_SRC_DIR}/network/tcp_server.cpp
    ${DUCKD_SRC_DIR}/network/tcp_connection.cpp

    # Protocol layer
    ${DUCKD_SRC_DIR}/protocol/message.cpp
    ${DUCKD_SRC_DIR}/protocol/message_parser.cpp
    ${DUCKD_SRC_DIR}/protocol/message_builder.cpp
    ${DUCKD_SRC_DIR}/protocol/protocol_handler.cpp

    # Session layer
    ${DUCKD_SRC_DIR}/session/session.cpp
    ${DUCKD_SRC_DIR}/session/session_manager.cpp

    # Serialization
    ${DUCKD_SRC_DIR}/serialization/arrow_serializer.cpp

    # Executor
    ${DUCKD_SRC_DIR}/executor/query_executor.cpp
    ${DUCKD_SRC_DIR}/executor/executor_pool.cpp

    # HTTP
    ${DUCKD_SRC_DIR}/http/http_server.cpp

    # Utils
    ${DUCKD_SRC_DIR}/utils/logger.cpp
)

add_library(duckd_server STATIC ${DUCKD_SERVER_SOURCES})

target_include_directories(duckd_server
    PUBLIC
        ${DUCKD_SRC_DIR}
    PRIVATE
        ${DUCKDB_INCLUDE_DIRS}
)

target_link_libraries(duckd_server
    PUBLIC
        duckdb_static
        duckdb_asio
        Threads::Threads
)

target_compile_features(duckd_server PUBLIC cxx_std_17)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(duckd_server PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()
