FIND_PATH(hiredis_INCLUDE_DIR hiredis/hiredis.h)

FIND_LIBRARY(hiredis_LIBRARY hiredis)

MARK_AS_ADVANCED(hiredis_LIBRARY)

INCLUDE(FindPackageHandleStandardArgs)

FIND_PACKAGE_HANDLE_STANDARD_ARGS(hiredis
  "hiredis library is required: https://github.com/antirez/hiredis/"
  hiredis_LIBRARY hiredis_INCLUDE_DIR)

INCLUDE_DIRECTORIES(${hiredis_INCLUDE_DIR})
