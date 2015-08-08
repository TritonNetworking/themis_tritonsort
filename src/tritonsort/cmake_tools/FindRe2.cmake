# Find the re2 library for C++ regular expression parsing

FIND_PATH(Re2_INCLUDE_DIR re2/re2.h)
FIND_LIBRARY(Re2_LIBRARY re2)

MARK_AS_ADVANCED(Re2 Re2_INCLUDE_DIR Re2_LIBRARY)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Re2
  "libre2 is required: http://code.google.com/p/re2/"
  Re2_INCLUDE_DIR Re2_LIBRARY)
