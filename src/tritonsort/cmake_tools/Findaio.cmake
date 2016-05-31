FIND_PATH(LIBAIO_INCLUDE_DIR libaio.h)

FIND_LIBRARY(aio_LIBRARY aio)

MARK_AS_ADVANCED(aio_LIBRARY LIBAIO_INCLUDE_DIR)


INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(aio
  "aio library is required" aio_LIBRARY LIBAIO_INCLUDE_DIR)

INCLUDE_DIRECTORIES(${LIBAIO_INCLUDE_DIR})