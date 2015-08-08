FIND_LIBRARY(pthread_LIBRARY pthread)

MARK_AS_ADVANCED(pthread_LIBRARY)


INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(pthread
  "pthread library is required" pthread_LIBRARY)
