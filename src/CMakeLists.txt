CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PROJECT(BalancedSystems)

# Add custom include and library path
SET(CUSTOM_USR_PATH $ENV{THEMIS_CUSTOM_USR_PATH} CACHE STRING "A custom path where includes and libraries will be found in addition to /usr, /usr/local, and /opt/local. Useful for building dependencies in home directory.")
SET(CMAKE_INCLUDE_PATH "${CUSTOM_USR_PATH}/include;/opt/local/include;/usr/local/include;/usr/include")
SET(CMAKE_LIBRARY_PATH "${CUSTOM_USR_PATH}/lib;/opt/local/lib;/usr/local/lib;/usr/lib")

ADD_SUBDIRECTORY(third-party)
ADD_SUBDIRECTORY(tritonsort)
ADD_SUBDIRECTORY(gensort)
ADD_SUBDIRECTORY(gensort_2013)
ADD_SUBDIRECTORY(vargensort)
ADD_SUBDIRECTORY(gen_sorted_file)
ADD_SUBDIRECTORY(gengridmix)
ADD_SUBDIRECTORY(loggrep)
ADD_SUBDIRECTORY(gen_synthetic_click_logs)
