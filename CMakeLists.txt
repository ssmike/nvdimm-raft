cmake_minimum_required(VERSION 3.5.1)

project(nvm-raft)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS messages.proto client.proto)

add_executable(main main.cpp engine.h ${PROTO_HDRS} ${PROTO_SRCS})

find_package(PkgConfig REQUIRED)
pkg_check_modules(JSONCPP REQUIRED jsoncpp)

find_package(spdlog REQUIRED)
add_definitions(-DSPDLOG_FMT_EXTERNAL)
find_package(fmt REQUIRED)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PMEMOBJ REQUIRED libpmemobj)
pkg_check_modules(PMEMBLK REQUIRED libpmemblk)
pkg_check_modules(PMEMLOG REQUIRED libpmemlog)
pkg_check_modules(PMEMPOOL REQUIRED libpmempool)
pkg_check_modules(PMEM REQUIRED libpmem)

find_package(LIBPMEMOBJ++ REQUIRED)

add_subdirectory(bus)
include_directories(
    bus
    ${JSONCPP_INCLUDE_DIRS}
    ${PMEMOBJ_INCLUDE_DIRECTORIES} 
    ${PMEMBLK_INCLUDE_DIRECTORIES}
    ${PMEMLOG_INCLUDE_DIRECTORIES}
    ${PMEMPOOL_INCLUDE_DIRECTORIES}
    ${LIBPMEMOBJ++_INCLUDE_DIRECTORIES})

target_link_libraries(main ${JSONCPP_LIBRARIES} ${Protobuf_LIBRARIES}
    ${spdlog_LIBRARIES} ${LIBPMEMOBJ++_LIBRARIES} fmt::fmt bus)

add_executable(client client.cpp ${PROTO_HDRS} ${PROTO_SRCS})
target_link_libraries(client ${JSONCPP_LIBRARIES} ${Protobuf_LIBRARIES} bus)

add_executable(testEngine testEngine.cpp engine.h ${PROTO_HDRS} ${PROTO_SRCS})
target_link_libraries(testEngine ${Protobuf_LIBRARIES} ${PMEMOBJ_LIBRARIES}
    ${LIBPMEMOBJ++_LIBRARIES})

add_executable(benchEngine benchEngine.cpp engine.h ${PROTO_HDRS} ${PROTO_SRCS})
target_link_libraries(benchEngine ${Protobuf_LIBRARIES} ${PMEMOBJ_LIBRARIES}
    ${LIBPMEMOBJ++_LIBRARIES})
