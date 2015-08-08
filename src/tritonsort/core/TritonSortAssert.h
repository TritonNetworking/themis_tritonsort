#ifndef _TRITONSORT_ASSERT_H
#define _TRITONSORT_ASSERT_H

#include <assert.h>
#include <execinfo.h>
#include <iostream>
#include <sstream>
#include <stdarg.h>
#include <stdio.h>
#include <string>
#include <stdint.h>
#include <stdlib.h>

#include "config.h"

/// Exception thrown if an assertion fails
class AssertionFailedException : public std::exception {
public:
  /// Constructor
  AssertionFailedException() {}

  /// Constructor
  /**
     \param msg a message describing the assertion that failed
   */
  AssertionFailedException(const std::string& msg) : message(msg) {}

  /// Constructor
  /**
     \param msg a message describing the assertion that failed
     \param size the size of the message string in bytes
   */
  AssertionFailedException(const char* msg, uint64_t size) : message(msg, size) {}

  /// Destructor
  virtual ~AssertionFailedException() throw() {}

  /// Return description of the assertion that failed
  /**
     \return string description of failed assertion
   */
  const char* what() const throw();
private:
  std::string message;
};

/// \cond PRIVATE
class TritonSortAssertions {
public:
  static std::string _getAssertionMessage(const char* file, int line,
                                          const std::string& message);

  static void _ABORT(const char* file, int line, const char* format, ...);

  static void _ABORT_IF(const char* file, int line, bool condition,
                        const char* format, ...);

  static void _ASSERT(const char* file, int line, bool condition);
  static void _ASSERT(const char* file, int line, bool condition,
                      const char* format ...);

  static void dumpStack(int signal=0);

  static void useTestModeAssertions();

private:
  static bool testMode;
};
/// \endcond

#define ABORT(...)   TritonSortAssertions::_ABORT(__FILE__, __LINE__, __VA_ARGS__)
#define ABORT_IF(...)   TritonSortAssertions::_ABORT_IF(__FILE__, __LINE__, __VA_ARGS__)

#ifdef TRITONSORT_ASSERTS
# define ASSERT(...) TritonSortAssertions::_ASSERT(__FILE__, __LINE__, __VA_ARGS__)
#else
# define ASSERT(...)
#endif


#endif //_TRITONSORT_ASSERT_H
