cmake_minimum_required(VERSION 3.5.1)

project(bus)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS messages.proto service.proto)

add_library(bus
    bus.h bus.cpp
    connect_pool.h connect_pool.cpp
    endpoint_manager.h endpoint_manager.cpp
    error.h error.cpp
    util.h util.cpp)

set_property(TARGET bus PROPERTY CXX_STANDARD 17)

add_executable(testBus testBus.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS})

set_property(TARGET testBus PROPERTY CXX_STANDARD 17)
target_link_libraries(testBus ${Protobuf_LIBRARIES} bus)

add_executable(testService testBus.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS})

set_property(TARGET testService PROPERTY CXX_STANDARD 17)
target_link_libraries(testService ${Protobuf_LIBRARIES} bus)

add_test(NAME bus COMMAND testBus)
add_test(NAME service COMMAND testService)

#target_include_directories( main PUBLIC)


