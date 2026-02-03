# DuckD Server Executable
# programs/server/CMakeLists.txt

add_executable(duckd-server ${CMAKE_SOURCE_DIR}/programs/server/main.cpp)

target_include_directories(duckd-server
    PRIVATE
        ${DUCKDB_INCLUDE_DIRS}
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_BINARY_DIR}/generated
)

target_link_libraries(duckd-server
    PRIVATE
        duckd_server
        duckdb_main_extension
        core_functions_extension
        parquet_extension
        duckdb_generated_extension_loader
)

# Optional systemd integration
if(WITH_SYSTEMD)
    find_package(PkgConfig)
    if(PkgConfig_FOUND)
        pkg_check_modules(SYSTEMD libsystemd)
        if(SYSTEMD_FOUND)
            target_compile_definitions(duckd-server PRIVATE WITH_SYSTEMD)
            target_include_directories(duckd-server PRIVATE ${SYSTEMD_INCLUDE_DIRS})
            target_link_libraries(duckd-server PRIVATE ${SYSTEMD_LIBRARIES})
        endif()
    endif()
endif()

target_compile_features(duckd-server PUBLIC cxx_std_17)

install(TARGETS duckd-server RUNTIME DESTINATION bin)
