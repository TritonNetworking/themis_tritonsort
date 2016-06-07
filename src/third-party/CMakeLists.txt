PROJECT(ThirdParty)

INCLUDE_DIRECTORIES(jsoncpp/include)

ADD_LIBRARY(jsoncpp
  jsoncpp/src/lib_json/json_reader.cpp
  jsoncpp/src/lib_json/json_writer.cpp
  jsoncpp/src/lib_json/json_value.cpp)

ADD_LIBRARY(murmurhash
  murmurhash/MurmurHash3.cpp)

MARK_AS_ADVANCED(BUILD_SHARED_LIBS
  APPLE_UNIVERSAL_BIN MSVC_SHARED_RT MSVC_STHREADED_RT CMAKE_INSTALL_PREFIX)
