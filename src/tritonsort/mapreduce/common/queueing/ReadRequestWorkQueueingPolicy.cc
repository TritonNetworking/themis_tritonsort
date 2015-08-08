#include "core/TritonSortAssert.h"
#include "mapreduce/common/ReadRequest.h"
#include "mapreduce/common/queueing/ReadRequestWorkQueueingPolicy.h"

ReadRequestWorkQueueingPolicy::ReadRequestWorkQueueingPolicy(
  uint64_t numReaders)
  : WorkQueueingPolicy(numReaders) {
}

uint64_t ReadRequestWorkQueueingPolicy::getEnqueueID(Resource* workUnit) {
  // Read the diskID from the request and put it in the corresponding reader's
  // queue.
  ReadRequest* readRequest = dynamic_cast<ReadRequest*>(workUnit);
  ASSERT(readRequest != NULL,
         "Expected ReadRequest but got some other kind of work unit.");

  return readRequest->diskID % numQueues;
}
