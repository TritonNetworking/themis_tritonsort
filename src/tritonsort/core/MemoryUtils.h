#ifndef THEMIS_MEMORY_UTILS_H
#define THEMIS_MEMORY_UTILS_H

#include <stddef.h>

namespace themis {
  /** Similar to std::nothrow's strategy, we define a dummy struct, make a
      const, globally-scoped instance of it, and use that to create an
      implementation of new that does any Themis-specific checking we want.
  */
  struct memcheck_t {
    memcheck_t() {}
  };

  const memcheck_t memcheck;
}

void handleFailedMemoryAllocation(size_t size);
void* operator new (size_t size, const themis::memcheck_t& memcheck);
void* operator new[] (size_t size, const themis::memcheck_t& memcheck);

#endif // THEMIS_MEMORY_UTILS_H
