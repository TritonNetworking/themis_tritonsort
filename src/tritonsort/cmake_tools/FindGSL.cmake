FIND_LIBRARY(gsl_LIBRARY gsl
  /usr/local/lib
  /usr/lib)

FIND_LIBRARY(gslcblas_LIBRARY gslcblas
  /usr/local/lib
  /usr/lib)

FIND_PATH(GSL_INCLUDE_DIR gsl/gsl_randist.h)

MARK_AS_ADVANCED(gsl_LIBRARY gslcblas_LIBRARY GSL_INCLUDE_DIR)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GSL
  "GSL and GSL CBLAS are required: http://www.gnu.org/s/gsl/"
  gsl_LIBRARY gslcblas_LIBRARY GSL_INCLUDE_DIR)
