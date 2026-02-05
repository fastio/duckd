#===----------------------------------------------------------------------===//
#                         DuckD Server
#
# contrib/parallel-hashmap-cmake/CMakeLists.txt
#
# CMake configuration for parallel-hashmap (header-only)
#===----------------------------------------------------------------------===//

# parallel-hashmap is header-only, just create an interface library
add_library(phmap INTERFACE)
add_library(phmap::phmap ALIAS phmap)

target_include_directories(phmap INTERFACE
    ${CMAKE_SOURCE_DIR}/contrib/parallel-hashmap
)

# Require C++17 for parallel-hashmap
target_compile_features(phmap INTERFACE cxx_std_17)
