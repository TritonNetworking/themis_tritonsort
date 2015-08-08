FIND_LIBRARY(gflags_LIBRARY gflags)

MARK_AS_ADVANCED(gflags_LIBRARY)

INCLUDE(FindPackageHandleStandardArgs)

FIND_PACKAGE_HANDLE_STANDARD_ARGS(GFlags
  "Google gflags is required: http://code.google.com/p/google-gflags/"
  gflags_LIBRARY)
