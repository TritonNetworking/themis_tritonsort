#include <cxxabi.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"
#include "core/TritonSortAssert.h"

bool TritonSortAssertions::testMode = false;

const char* AssertionFailedException::what()
  const throw() {
  return message.c_str();
}

void TritonSortAssertions::dumpStack(int signal) {
  uint64_t stackTraceSize = 100;
  void* rawTraceBuffer[stackTraceSize];
  size_t sz = 200; // just a guess, template names will go much wider

  uint64_t numStackFrames = backtrace(rawTraceBuffer, stackTraceSize);

  std::cerr << "Stack trace (" << numStackFrames << " frames):" << std::endl;

  char** stackStrings = backtrace_symbols(rawTraceBuffer, numStackFrames);

  if (stackStrings == NULL) {
    std::cerr << "Stack string allocation failed. Dumping mangled stack to "
      "stderr directly." << std::endl;
    // Allocating space for backtrace symbols didn't work; dump a backtrace
    // with backtrace_symbols_fd
    backtrace_symbols_fd(rawTraceBuffer, numStackFrames, 2);
    return;
  }

  // Adapted from example at
  // http://tombarta.wordpress.com/2008/08/01/c-stack-traces-with-gcc/
  for (size_t i = 0; i < numStackFrames; i++) {

    if (stackStrings[i] == NULL) {
      std::cerr << "== FRAME " << i << " INFO MISSING ==" << std::endl;
      continue;
    }

    char *begin = 0, *end = 0;
    // find the parentheses and address offset surrounding the mangled name
    for (char *j = stackStrings[i]; *j; ++j) {
      if (*j == '(') {
        begin = j;
      }
      else if (*j == '+') {
        end = j;
      }
    }

    std::cerr << "    " << stackStrings[i] << ": ";

    if (begin && end) {
      // The relevant portion of our mangled name is now in indices [begin,
      // end) of stackStrings[i]
      *begin++ = '\0';
      *end = '\0';

      int status;
      char *demangled = abi::__cxa_demangle(begin, NULL, &sz, &status);

      if (demangled == NULL) {
        // Demangling failed (probably because we ran out of memory allocating
        // space for the demangled name); just print the stack frame's string
        std::cerr << "(not enough memory to demangle)";
      } else {
        // Demangling succeeded; print the demangled string, then free it.
        std::cerr << demangled;
        free(demangled);
      }
    }

    std::cerr << std::endl;
  }
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
    dumpStack();
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
      dumpStack();
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
      dumpStack();
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
      dumpStack();
      abort();
    }
  }
}

void TritonSortAssertions::useTestModeAssertions() {
  testMode = true;
}
