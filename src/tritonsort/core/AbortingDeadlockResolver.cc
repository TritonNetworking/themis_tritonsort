#include "AbortingDeadlockResolver.h"
#include "core/TritonSortAssert.h"

void* AbortingDeadlockResolver::resolveRequest(uint64_t size) {
  ABORT("Tried to resolve deadlock.");
  return NULL;
}

void AbortingDeadlockResolver::deallocate(void* memory) {
  ABORT("Tried to deallocate deadlock-allocated memory.");
}
