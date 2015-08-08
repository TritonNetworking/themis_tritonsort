CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PROJECT(LogGrep)

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${LogGrep_SOURCE_DIR}/../tritonsort/cmake_tools")

FIND_PACKAGE(Re2 REQUIRED)

INCLUDE_DIRECTORIES(${TritonSort_SOURCE_DIR}
  ${ThirdParty_SOURCE_DIR}/jsoncpp/include
  ${Re2_INCLUDE_DIR})

ADD_EXECUTABLE(loggrep loggrep.cc)

TARGET_LINK_LIBRARIES(loggrep re2 tritonsort_core)
