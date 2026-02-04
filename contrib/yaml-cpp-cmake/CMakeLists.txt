# yaml-cpp configuration
# https://github.com/jbeder/yaml-cpp
# Version: 0.8.0
# License: MIT

set(YAML_CPP_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../yaml-cpp")

# Disable building tests and tools
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)

# Build as static library
set(YAML_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

# Suppress CMake policy warnings from yaml-cpp
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

# yaml-cpp 0.8.0 requires older CMake version compatibility
set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)

add_subdirectory("${YAML_CPP_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/yaml-cpp")
