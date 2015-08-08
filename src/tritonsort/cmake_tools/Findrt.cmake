FIND_LIBRARY(rt_LIBRARY rt)

MARK_AS_ADVANCED(rt_LIBRARY)


INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(rt
  "rt library is required" rt_LIBRARY)
