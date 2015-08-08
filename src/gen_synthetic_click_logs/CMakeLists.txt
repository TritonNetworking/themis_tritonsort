SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${TritonSort_SOURCE_DIR}/cmake_tools")

CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
PROJECT(gen_synthetic_click_logs)

FIND_PACKAGE(GSL REQUIRED)

INCLUDE_DIRECTORIES(${TritonSort_SOURCE_DIR}
  ${ThirdParty_SOURCE_DIR}/jsoncpp/include
  ${GSL_INCLUDE_DIR})

ADD_EXECUTABLE(gen_synthetic_click_logs gen_synthetic_click_logs.cc)

TARGET_LINK_LIBRARIES(gen_synthetic_click_logs ${gsl_LIBRARY}
  ${gslcblas_LIBRARY})
