cmake_minimum_required(VERSION 3.5.1)

project(bus)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

protobuf_generate_cpp(LIB_PROTO_SRCS LIB_PROTO_HDRS service.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS messages.proto)

add_library(bus
    bus.h bus.cpp
    proto_bus.h proto_bus.cpp
    connect_pool.h connect_pool.cpp
    endpoint_manager.h endpoint_manager.cpp
    error.h error.cpp
    util.h util.cpp
    ${LIB_PROTO_HDRS} ${LIB_PROTO_SRCS})

add_executable(testBus testBus.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS})

target_link_libraries(testBus ${Protobuf_LIBRARIES} bus)

add_executable(testService testService.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS})

target_link_libraries(testService ${Protobuf_LIBRARIES} bus)

add_test(NAME bus COMMAND testBus)
add_test(NAME service COMMAND testService)

add_executable(main main.cpp ${PROTO_HDRS} ${PROTO_SRCS})

find_package(PkgConfig REQUIRED)
pkg_check_modules(JSONCPP REQUIRED jsoncpp)
target_include_directories(main PUBLIC ${JSONCPP_INCLUDE_DIRS})
target_link_libraries(main ${JSONCPP_LIBRARIES} ${Protobuf_LIBRARIES} bus)
