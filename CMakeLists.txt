cmake_minimum_required(VERSION 3.9)
project(krash)

set(CMAKE_CXX_STANDARD 11)


#INCLUDE(FindProtobuf)
#FIND_PACKAGE(Protobuf REQUIRED)
#INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})

set(CMAKE_CXX_FLAGS "-O3")

ADD_SUBDIRECTORY(proto)

INCLUDE_DIRECTORIES(../clickhouse-cpp/)
LINK_DIRECTORIES(../clickhouse-cpp/build/clickhouse/)
LINK_DIRECTORIES(../clickhouse-cpp/build/contrib/lz4)
LINK_DIRECTORIES(../clickhouse-cpp/build/contrib/cityhash)

ADD_EXECUTABLE(main main.cpp)

ADD_LIBRARY(protobuf STATIC IMPORTED)
SET_TARGET_PROPERTIES(protobuf PROPERTIES IMPORTED_LOCATION /usr/lib/x86_64-linux-gnu/libprotobuf.so.9)
TARGET_LINK_LIBRARIES(main protobuf)

TARGET_LINK_LIBRARIES(main proto ${PROTOBUF_LIBRARY})
TARGET_LINK_LIBRARIES(main clickhouse-cpp-lib lz4-lib cityhash-lib)
