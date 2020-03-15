cmake_minimum_required(VERSION 3.5.1)

project(bus)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS messages.proto)

add_library(bus
    bus.h bus.cpp
    connect_pool.h connect_pool.cpp)

set_property(TARGET bus PROPERTY CXX_STANDARD 17)

add_executable(main main.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS})

target_include_directories( main PUBLIC)

target_link_libraries(main ${Protobuf_LIBRARIES} bus)
