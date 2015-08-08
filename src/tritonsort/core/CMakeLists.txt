SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${TritonSort_SOURCE_DIR}/cmake_tools")
find_package(ZLIB REQUIRED)
include_directories(${ZLIB_INCLUDE_DIR})
set(LIBS ${LIBS} ${ZLIB_LIBRARIES})

FIND_PACKAGE(Boost COMPONENTS filesystem system REQUIRED)

FILE(GLOB CoreFiles "*.cc")

FIND_PACKAGE(pthread REQUIRED)
FIND_PACKAGE(rt REQUIRED)
FIND_PACKAGE(aio REQUIRED)

INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS}
  ${ThirdParty_SOURCE_DIR}/jsoncpp/include ${YAML_CPP_SOURCE_DIR}/include)

LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})

ADD_LIBRARY(tritonsort_core STATIC ${CoreFiles})
TARGET_LINK_LIBRARIES(tritonsort_core ${Boost_LIBRARIES}
  jsoncpp ${pthread_LIBRARY} ${rt_LIBRARY} ${aio_LIBRARY} murmurhash yaml-cpp)
