# Find xfslibs

FIND_PATH(XFS_INCLUDE_DIR xfs/xfs.h)

MARK_AS_ADVANCED(XFSlibs XFS_INCLUDE_DIR)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(XFSlibs
  "xfslibs is required (xfslibs-dev package in many repos): http://oss.sgi.com/projects/xfs/"
  XFS_INCLUDE_DIR)

INCLUDE_DIRECTORIES(${XFS_INCLUDE_DIR})