find_path(GooglePerfTools_INCLUDE_DIR google/heap-profiler.h)

find_library(tcmalloc_LIBRARY tcmalloc)

find_library(stacktrace_LIBRARY stacktrace)

find_library(profiler_LIBRARY profiler)

mark_as_advanced(
  tcmalloc_LIBRARY
  stacktrace_LIBRARY
  profiler_LIBRARY
  GooglePerfTools_INCLUDE_DIR
  )

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GooglePerfTools
  "google_perftools is required: http://code.google.com/p/google-perftools/"
  tcmalloc_LIBRARY profiler_LIBRARY
  GooglePerfTools_INCLUDE_DIR)

INCLUDE_DIRECTORIES(${GooglePerfTools_INCLUDE_DIR})