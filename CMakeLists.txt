# Copyright (c) 2013, Cloudera, inc.
cmake_minimum_required(VERSION 2.8)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

find_package(Boost COMPONENTS thread system REQUIRED)
include_directories( ${Boost_INCLUDE_DIR} )
set(LIBS ${LIBS} ${Boost_LIBRARIES})

find_package(GLog REQUIRED)
include_directories(${GLOG_INCLUDE_DIR})
set(LIBS ${LIBS} ${GLOG_STATIC_LIB})

add_subdirectory(proto)
INCLUDE_DIRECTORIES( ${PROTOBUF_INCLUDE_DIR} )
set(LIBS ${LIBS} ${PROTOBUF_LIBRARY})

set(CMAKE_CXX_FLAGS "-g -Wall")

ADD_EXECUTABLE(rpc_client_test rpc_client_test.cpp rpc_client.cpp)
TARGET_LINK_LIBRARIES( rpc_client_test protos ${LIBS} )
