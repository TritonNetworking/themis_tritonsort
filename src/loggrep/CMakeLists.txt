CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PROJECT(LogGrep)

INCLUDE_DIRECTORIES(${TritonSort_SOURCE_DIR}
  ${ThirdParty_SOURCE_DIR}/jsoncpp/include
  ${Re2_INCLUDE_DIR})

ADD_EXECUTABLE(loggrep loggrep.cc)

TARGET_LINK_LIBRARIES(loggrep re2 tritonsort_core)
