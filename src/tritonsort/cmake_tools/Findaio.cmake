FIND_LIBRARY(aio_LIBRARY aio)

MARK_AS_ADVANCED(aio_LIBRARY)


INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(aio
  "aio library is required" aio_LIBRARY)
