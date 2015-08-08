FIND_LIBRARY(libm_LIBRARY m)

MARK_AS_ADVANCED(libm_LIBRARY)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(libm
  "libm is required"
  libm_LIBRARY)
