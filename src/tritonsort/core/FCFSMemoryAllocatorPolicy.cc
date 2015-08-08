#include "core/FCFSMemoryAllocatorPolicy.h"
#include "core/TritonSortAssert.h"

void FCFSMemoryAllocatorPolicy::addRequest(
  Request& request, const std::string& groupName) {

  queue.push(&request);
}
void FCFSMemoryAllocatorPolicy::removeRequest(
  Request& request, const std::string& groupName) {

  ABORT_IF(&request != queue.front(), "Expected to only ever remove the "
           "front of the queue");
  queue.pop();
}

bool FCFSMemoryAllocatorPolicy::canScheduleRequest(
  uint64_t availability, Request& request) {

  // All requests that fit in available memory can be scheduled
  return (request.size <= availability) && !queue.empty() &&
    (queue.front() == &request);
}

MemoryAllocatorPolicy::Request*
FCFSMemoryAllocatorPolicy::nextSchedulableRequest(
  uint64_t availability) {

  if (!queue.empty() && queue.front()->size <= availability) {
    return queue.front();
  } else {
    return NULL;
  }
}

void FCFSMemoryAllocatorPolicy::recordUseTime(uint64_t useTime) {
}
