#ifndef THEMIS_DEADLOCK_RESOLVER_INTERFACE_H
#define THEMIS_DEADLOCK_RESOLVER_INTERFACE_H

#include <stdint.h>

class DeadlockResolverInterface {
public:
  virtual ~DeadlockResolverInterface() {}

  /// Resolve a deadlocked request for memory
  /**
     \param size the size of the request in bytes

     \return a pointer to the new virtual memory region that can contain a
     request of the given size
   */
  virtual void* resolveRequest(uint64_t size) = 0;

  /// Deallocate a region of (virtual) memory allocated to resolve deadlock
  virtual void deallocate(void* memory) = 0;
};

#endif // THEMIS_DEADLOCK_RESOLVER_INTERFACE_H
