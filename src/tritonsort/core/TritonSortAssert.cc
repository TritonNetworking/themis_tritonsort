#include <stdlib.h>
#include <string.h>

#include "config.h"
#include "core/TritonSortAssert.h"

bool TritonSortAssertions::testMode = false;

const char* AssertionFailedException::what()
  const throw() {
  return message.c_str();
}

std::string TritonSortAssertions::_getAssertionMessage(
  const char* file, int line, const std::string& message) {

  std::ostringstream oss;
  oss << "Assertion in file '" << file << "' line " << line << " failed: "
      << std::endl;
  oss << message << std::endl;
  return oss.str();
}

void TritonSortAssertions::_ABORT(
  const char* file, int line, const char* format ...) {

  va_list ap;

  va_start(ap, format);

  char buffer[500];

  vsnprintf(buffer, 500, format, ap);
  va_end(ap);

  std::string message;
  message.append(buffer);

  std::string assertMessage = _getAssertionMessage(file, line, message);

  if (testMode) {
    throw AssertionFailedException(assertMessage);
  } else {
    std::cerr << "ABORT: " << assertMessage << std::endl;
    abort();
  }
}

void TritonSortAssertions::_ABORT_IF(const char* file, int line,
                                     bool condition, const char* format ...) {
  if (condition) {
    va_list ap;

    va_start(ap, format);

    char buffer[500];

    vsnprintf(buffer, 500, format, ap);
    va_end(ap);

    std::string message;
    message.append(buffer);

    std::string assertMessage = _getAssertionMessage(file, line, message);

    if (testMode) {
      throw AssertionFailedException(assertMessage);
    } else {
      std::cerr << "ABORT: " << assertMessage << std::endl;
      abort();
    }
  }
}

void TritonSortAssertions::_ASSERT(
  const char* file, int line, bool condition) {

  if (!condition) {

    std::string message;
    std::string assertMessage = _getAssertionMessage(file, line, message);

    if (testMode) {
      throw AssertionFailedException(assertMessage);
    } else {
      std::cerr << assertMessage << std::endl;
      abort();
    }
  }
}

void TritonSortAssertions::_ASSERT(
  const char* file, int line, bool condition, const char* format ...) {

  va_list ap;
  if (!condition) {
    va_start(ap, format);

    char buffer[500];

    vsnprintf(buffer, 500, format, ap);
    va_end(ap);

    std::string message;
    message.append(buffer);

    std::string assertMessage = _getAssertionMessage(file, line, message);

    if (testMode) {
      throw AssertionFailedException(assertMessage);
    } else {
      std::cerr << assertMessage << std::endl;
      abort();
    }
  }
}

void TritonSortAssertions::useTestModeAssertions() {
  testMode = true;
}
