# Fix for newer CMake (>= 3.27) compatibility with old bundled dependencies
# that use cmake_minimum_required with versions < 3.5
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.27")
    cmake_policy(VERSION 3.5...3.31)
endif()
