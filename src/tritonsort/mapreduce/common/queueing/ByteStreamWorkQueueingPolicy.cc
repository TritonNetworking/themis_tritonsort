#include "common/buffers/ByteStreamBuffer.h"
#include "mapreduce/common/queueing/ByteStreamWorkQueueingPolicy.h"

ByteStreamWorkQueueingPolicy::ByteStreamWorkQueueingPolicy(
  uint64_t numConverters)
  : WorkQueueingPolicy(numConverters) {
}

uint64_t ByteStreamWorkQueueingPolicy::getEnqueueID(
  Resource* workUnit) {
  // Read the stream ID from the byte stream buffer and put it in the
  // corresponding converter's queue.
  ByteStreamBuffer* buffer = dynamic_cast<ByteStreamBuffer*>(workUnit);
  TRITONSORT_ASSERT(buffer != NULL,
         "Expected ByteStreamBuffer but got some other kind of work unit.");

  return buffer->getStreamID() % numQueues;
}
