#ifndef TRITONSORT_MEMORY_ALLOCATOR_POLICY_H
#define TRITONSORT_MEMORY_ALLOCATOR_POLICY_H

#include <limits>
#include <string>

#include "core/MemoryUtils.h"
#include "core/Timer.h"

class MemoryAllocationContext;

/**
   A MemoryAllocatorPolicy encapsulates policy logic about how a MemoryAllocator
   schedules memory requests. The policy is responsible for all queueing logic
   and data structures. The allocator will always check with the policy before
   scheduling a request. When memory is made available, the allocator will ask
   the policy for a set of requests that can be satisfied based on its internal
   queueing logic.

   \TODO(MC): Should groupName be exposed in add/remove? Should we instead
   expose the callerID mapping.
*/
class MemoryAllocatorPolicy {
public:
  struct Request {
    uint64_t timestamp;
    uint64_t size;
    void* memory;
    const MemoryAllocationContext& context;
    bool satisfiable;
    bool resolvedOnDeadlock;

    Request(const MemoryAllocationContext& _context, uint64_t _size)
      : timestamp(Timer::posixTimeInMicros()),
        size(_size),
        memory(NULL),
        context(_context),
        satisfiable(false),
        resolvedOnDeadlock(false) {}
  };

  /// Destructor
  virtual ~MemoryAllocatorPolicy() {}

  /**
     Add an allocation request to the policy's internal queues. This method will
     be called by the allocator when the user requests memory.

     \param request the request to add

     \param groupName the name of the group that the request belongs to
  */
  virtual void addRequest(Request& request, const std::string& groupName)=0;

  /**
     Remove an allocation request from the policy's internal queues. This method
     will be called by the allocator when the user frees memory.

     \param groupName the name of the group that the request belongs to

     \param request the request to remove
  */
  virtual void removeRequest(Request& request, const std::string& groupName)=0;

  /**
     Determine whether or not a request can be scheduled given a certain amount
     of available memory. The policy should check both available memory and its
     its internal queues. This method will be called by the allocator to check
     if a thread needs to block.

     \param availability the amount of currently available memory

     \param request the request to be scheduled

     \return true if the request can be scheduled now and false otherwise
  */
  virtual bool canScheduleRequest(uint64_t availability, Request& request)=0;

  /**
     Get the next allocation request that can be scheduled given an amount of
     available memory. This method will be called by the allocator after memory
     is freed or allocated.

     \param availability the amount of currently available memory

     \return the next schedulable request, or NULL if no requests can be
     scheduled
  */
  virtual Request* nextSchedulableRequest(uint64_t availability)=0;

  /**
     Records the time between a memory allocation and its corresponding
     deallocation. This information can be recorded by the policy, or it can be
     ignored.

     \param useTime the time between corresponding allocate and deallocate calls
  */
  virtual void recordUseTime(uint64_t useTime)=0;
};

#endif // TRITONSORT_MEMORY_ALLOCATOR_POLICY_H
