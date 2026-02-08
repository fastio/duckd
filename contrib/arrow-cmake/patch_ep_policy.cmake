# Patch Arrow's ThirdpartyToolchain.cmake to add CMAKE_POLICY_VERSION_MINIMUM
# to EP_COMMON_CMAKE_ARGS for compatibility with CMake >= 3.27
set(TOOLCHAIN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/../arrow/cpp/cmake_modules/ThirdpartyToolchain.cmake")

file(READ "${TOOLCHAIN_FILE}" TOOLCHAIN_CONTENT)

# Check if already patched
string(FIND "${TOOLCHAIN_CONTENT}" "CMAKE_POLICY_VERSION_MINIMUM" PATCH_POS)
if(PATCH_POS EQUAL -1)
    # Insert after the EP_COMMON_CMAKE_ARGS closing paren
    string(REPLACE
        "-DCMAKE_VERBOSE_MAKEFILE=\${CMAKE_VERBOSE_MAKEFILE})"
        "-DCMAKE_VERBOSE_MAKEFILE=\${CMAKE_VERBOSE_MAKEFILE})\nif(CMAKE_VERSION VERSION_GREATER_EQUAL \"3.27\")\n  list(APPEND EP_COMMON_CMAKE_ARGS -DCMAKE_POLICY_VERSION_MINIMUM=3.5)\nendif()"
        TOOLCHAIN_CONTENT "${TOOLCHAIN_CONTENT}")
    file(WRITE "${TOOLCHAIN_FILE}" "${TOOLCHAIN_CONTENT}")
    message(STATUS "Patched Arrow ThirdpartyToolchain.cmake for CMake >= 3.27 compatibility")
endif()
