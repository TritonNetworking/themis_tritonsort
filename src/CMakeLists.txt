CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PROJECT(BalancedSystems)

# Add custom include and library path
SET(CUSTOM_USR_PATH $ENV{THEMIS_CUSTOM_USR_PATH} CACHE STRING "A custom path where includes and libraries will be found in addition to /usr, /usr/local, and /opt/local. Useful for building dependencies in home directory.")
SET(GTEST_ROOT_DIR $ENV{GTEST_ROOT_DIR} CACHE STRING "Location of root googletest source directory.")
SET(CMAKE_INCLUDE_PATH "${CUSTOM_USR_PATH}/include;/opt/local/include;/usr/local/include;/usr/include")
SET(CMAKE_LIBRARY_PATH "${CUSTOM_USR_PATH}/lib;/opt/local/lib;/usr/local/lib;/usr/lib")

# Load dependencies
SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${BalancedSystems_SOURCE_DIR}/tritonsort/cmake_tools")
INCLUDE("${BalancedSystems_SOURCE_DIR}/tritonsort/cmake_tools/RecurseCCFiles.cmake")
FIND_PACKAGE(Re2 REQUIRED)
FIND_PACKAGE(aio REQUIRED)
FIND_PACKAGE(GooglePerfTools REQUIRED)
FIND_PACKAGE(JeMalloc REQUIRED)
FIND_PACKAGE(GooglePerfTools REQUIRED)
FIND_PACKAGE(ZLIB REQUIRED)
FIND_PACKAGE(pthread REQUIRED)
FIND_PACKAGE(rt REQUIRED)
FIND_PACKAGE(Boost COMPONENTS filesystem system REQUIRED)
FIND_PACKAGE(hiredis REQUIRED)
FIND_PACKAGE(YamlCPP REQUIRED)
FIND_PACKAGE(jsoncpp REQUIRED)

MARK_AS_ADVANCED(Boost_DIR)
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})

INCLUDE_DIRECTORIES(${BalancedSystems_SOURCE_DIR})

# Options
OPTION(USE_CXX_11 "Build with C++ 11" off)
IF(USE_CXX_11)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
  SET(TRITONSORT_CXX_11 1)
ENDIF(USE_CXX_11)

ADD_SUBDIRECTORY(third-party)
ADD_SUBDIRECTORY(tritonsort)
ADD_SUBDIRECTORY(gensort)
ADD_SUBDIRECTORY(loggrep)
