# DuckD Client Library
# src/client/CMakeLists.txt
#
# The original DUCK binary-protocol client (remote-client/) has been removed.
# The only client implementation is the Arrow Flight SQL extension (duckd-client/).

#===----------------------------------------------------------------------===//
# DuckD Flight Client Extension (requires WITH_FLIGHT_SQL=ON)
#===----------------------------------------------------------------------===//

if(WITH_FLIGHT_SQL)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/duckd-client)
endif()
