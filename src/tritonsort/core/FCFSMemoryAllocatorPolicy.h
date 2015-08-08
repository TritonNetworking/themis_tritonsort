#ifndef THEMIS_FCFS_MEMORY_ALLOCATOR_POLICY_H
#define THEMIS_FCFS_MEMORY_ALLOCATOR_POLICY_H

#include <map>
#include <queue>

#include "core/MemoryAllocatorPolicy.h"

class FCFSMemoryAllocatorPolicy : public MemoryAllocatorPolicy {
public:
  void addRequest(Request& request, const std::string& groupName);

  void removeRequest(Request& request, const std::string& groupName);

  bool canScheduleRequest(uint64_t availability, Request& request);

  Request* nextSchedulableRequest(uint64_t availability);

  /// This method is a no-op
  void recordUseTime(uint64_t useTime);

private:
  std::queue<Request*> queue;
};

#endif // THEMIS_FCFS_MEMORY_ALLOCATOR_POLICY_H
