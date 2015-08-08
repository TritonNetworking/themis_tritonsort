#include "FCFSPolicy.h"
#include "TritonSortAssert.h"

FCFSPolicy::~FCFSPolicy() {
  // The queue should be empty.
  ASSERT(queue.empty(),
         "queue should be empty when the FCFS policy is destroyed, but there "
         "are still %llu outstanding requests", queue.size());
}

void FCFSPolicy::addRequest(Request& request) {
  // New requests go to the back of the queue.
  queue.push_back(&request);
}

void FCFSPolicy::removeRequest(Request& request, bool force) {
  ASSERT(!force && queue.front() == &request,
         "FCFS tried to delete a request that isn't at the head of the queue.");
  queue.remove(&request);
}


bool FCFSPolicy::canScheduleRequest(Request& request) {
  // Only schedule the head of the queue.
  return !queue.empty() && (queue.front() == &request);
}

SchedulerPolicy::Request* FCFSPolicy::getNextSchedulableRequest(
  uint64_t availability) {
  // Wake the head of the queue if there are enough resources. Otherwise don't
  // wake anything.
  Request* frontOfQueue = queue.front();
  if (!queue.empty() && (availability >= frontOfQueue->size)) {
    return frontOfQueue;
  }
  return NULL;
}
