#ifndef THEMIS_NOP_DEADLOCK_RESOLVER_H
#define THEMIS_NOP_DEADLOCK_RESOLVER_H

#include <stdint.h>
#include <stdlib.h>

#include "core/DeadlockResolverInterface.h"
#include "core/TritonSortAssert.h"

/**
   A deadlock resolver that resolves deadlock by giving all requesters NULL
   pointers. Useful when you want to check for deadlock detection but don't
   care about resolving deadlock (which is almost certainly only true in tests).
 */
class NopDeadlockResolver : public DeadlockResolverInterface {
public:
  void* resolveRequest(uint64_t size) {
    return NULL;
  }

  void deallocate(void* memory) {
    TRITONSORT_ASSERT(memory == NULL, "NopDeadlockResolver expects to only deallocate "
           "NULL pointers, because that's all it ever allocates.");
  }
};

#endif // THEMIS_NOP_DEADLOCK_RESOLVER_H
