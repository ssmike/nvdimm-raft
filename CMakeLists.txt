cmake_minimum_required(VERSION 3.5.1)

project(raft)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")
find_package(GRPC REQUIRED)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS messages.proto)
grpc_generate_cpp(GRPC_SRCS GRPC_HDRS messages.proto)

add_executable(main main.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS}
    ${GRPC_SRCS}
    ${GRPC_HDRS})

target_include_directories( main PUBLIC)

target_link_libraries(main ${Protobuf_LIBRARIES} ${GPRC_LIBRARIES})
