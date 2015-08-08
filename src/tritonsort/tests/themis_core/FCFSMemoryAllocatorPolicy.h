#ifndef THEMIS_FCFS_MEMORY_ALLOCATOR_POLICY_H
#define THEMIS_FCFS_MEMORY_ALLOCATOR_POLICY_H

#include <list>
#include <pthread.h>

#include "core/MemoryAllocatorPolicy.h"

/**
   A simple memory allocation policy that allocates requests on a first-come,
   first-served basis.
 */
class FCFSMemoryAllocatorPolicy : public MemoryAllocatorPolicy {
public:
  /// Constructor
  FCFSMemoryAllocatorPolicy();

  /// Destructor
  virtual ~FCFSMemoryAllocatorPolicy();

  /**
     Adds a request to the back of the request queue
   */
  void addRequest(Request& request, const std::string& groupName);

  /**
     Removes the provided request from the request queue
   */
  void removeRequest(Request& request, const std::string& groupName);

  /**
     \return whether the request's size exceeds availability
   */
  bool canScheduleRequest(uint64_t availability, Request& request);

  MemoryAllocatorPolicy::Request* nextSchedulableRequest(uint64_t availability);

  /// This method is a no-op
  void recordUseTime(uint64_t useTime);

private:
  typedef std::list<MemoryAllocatorPolicy::Request*> RequestList;
  RequestList requests;
  pthread_mutex_t mutex;
};

#endif // THEMIS_FCFS_MEMORY_ALLOCATOR_POLICY_H
