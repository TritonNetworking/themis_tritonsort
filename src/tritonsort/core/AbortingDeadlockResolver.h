#ifndef THEMIS_ABORTING_DEADLOCK_RESOLVER_H
#define THEMIS_ABORTING_DEADLOCK_RESOLVER_H

#include "core/DeadlockResolverInterface.h"

/**
   AbortingDeadlockResolver aborts on both resolve and allocate calls.
 */
class AbortingDeadlockResolver : public DeadlockResolverInterface {
public:
  /**
     \sa DeadlockResolverInterface::resolveRequest

     \warning Aborts
   */
  void* resolveRequest(uint64_t size);

  /**
     \sa DeadlockResolverInterface::deallocate

     \warning Aborts
   */
  void deallocate(void* memory);
};

#endif // THEMIS_ABORTING_DEADLOCK_RESOLVER_H
