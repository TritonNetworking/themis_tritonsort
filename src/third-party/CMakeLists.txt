PROJECT(ThirdParty)

INCLUDE_DIRECTORIES(jsoncpp/include)

ADD_LIBRARY(jsoncpp
  jsoncpp/src/lib_json/json_reader.cpp
  jsoncpp/src/lib_json/json_writer.cpp
  jsoncpp/src/lib_json/json_value.cpp)

ADD_LIBRARY(murmurhash
  murmurhash/MurmurHash3.cpp)

ADD_SUBDIRECTORY(yaml-cpp)

MARK_AS_ADVANCED(YAML_CPP_BUILD_TOOLS YAML_CPP_BUILD_CONTRIB BUILD_SHARED_LIBS
  APPLE_UNIVERSAL_BIN MSVC_SHARED_RT MSVC_STHREADED_RT CMAKE_INSTALL_PREFIX)
