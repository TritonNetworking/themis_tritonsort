#include "MLFQPolicy.h"
#include "TritonSortAssert.h"

MLFQPolicy::MLFQPolicy()
  : averageUseTime(0),
    numCompletedRequests(0) {
}

MLFQPolicy::~MLFQPolicy() {
  // Both queues should be empty.
  TRITONSORT_ASSERT(lowPriorityQueue.empty(),
         "lowPriorityQueue should be empty when the MFLQ policy is destroyed, "
         "but there are still %llu outstanding requests",
         lowPriorityQueue.size());
  TRITONSORT_ASSERT(highPriorityQueue.empty(),
         "highPriorityQueue should be empty when the MFLQ policy is destroyed, "
         "but there are still %llu outstanding requests",
         highPriorityQueue.size());
}

void MLFQPolicy::addRequest(Request& request) {
  // New requests go into the low priority queue.
  lowPriorityQueue.push_back(&request);
}

void MLFQPolicy::removeRequest(Request& request, bool force) {
  if (force) {
    highPriorityQueue.remove(&request);
    lowPriorityQueue.remove(&request);
  } else {
    // Remove the request from whichever queue it belongs to.
    if (!highPriorityQueue.empty()) {
      TRITONSORT_ASSERT(highPriorityQueue.front() == &request,
             "MLFQ tried to delete a request in the high priority queue, but "
             "the request isn't at the front of the queue.");
      highPriorityQueue.remove(&request);
    } else {
      lowPriorityQueue.remove(&request);
    }
  }
}

bool MLFQPolicy::canScheduleRequest(Request& request) {
  // Is this request at the front of the high priority queue?
  if (!highPriorityQueue.empty()) {
    return highPriorityQueue.front() == &request;
  }

  // No restrictions on the low priority queue.
  return true;
}

SchedulerPolicy::Request* MLFQPolicy::getNextSchedulableRequest(
  uint64_t availability) {
  // Escalate any request that has waited in the low priority queue for longer
  // than the average resource usage time.
  // TODO(MC): This threshold is arbitrary. Can we study this in more detail?
  uint64_t now = Timer::posixTimeInMicros();
  std::list<Request*>::iterator iter = lowPriorityQueue.begin();
  while (iter != lowPriorityQueue.end()) {
    if (now - (*iter)->timestamp > averageUseTime) {
      // Move this request to the high priority queue.
      highPriorityQueue.push_back(*iter);
      // Delete from the low priority queue.
      iter = lowPriorityQueue.erase(iter);
    } else {
      // This request is too new, and all subsequent requests in this queue
      // will also be too new.
      break;
    }
  }

  // Wake the first high priority request.
  if (!highPriorityQueue.empty()) {
    Request* frontOfQueue = highPriorityQueue.front();
    if (availability >= frontOfQueue->size) {
      return frontOfQueue;
    }
  } else {
    // Wake any low priority thread.
    for (std::list<Request*>::iterator  iter = lowPriorityQueue.begin();
         iter != lowPriorityQueue.end(); ++iter) {
      if (availability >= (*iter)->size) {
        return *iter;
      }
    }
  }

  // No threads can be woken up.
  return NULL;
}

void MLFQPolicy::recordUseTime(uint64_t useTime) {
  // Update the average use time with the new use time

  ++numCompletedRequests;
  // All values are unsigned, so make sure subtraction always
  // results in a positive value.
  if (useTime >= averageUseTime) {
    averageUseTime += (useTime - averageUseTime) / numCompletedRequests;
  } else {
    averageUseTime -= (averageUseTime - useTime) / numCompletedRequests;
  }
}
