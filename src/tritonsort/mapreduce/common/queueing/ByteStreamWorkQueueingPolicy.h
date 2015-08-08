#ifndef MAPRED_BYTE_STREAM_WORK_QUEUEING_POLICY_H
#define MAPRED_BYTE_STREAM_WORK_QUEUEING_POLICY_H

#include "core/WorkQueueingPolicy.h"

/**
   ByteStreamWorkQueueingPolicy is a tracker queuing policy specifically
   designed for ByteStreamConverters. It deterministically routes
   ByteStreamBuffers to converters in round-robin fashion based on the stream
   ID, so that each stream can be serialized at a single converter.
 */
class ByteStreamWorkQueueingPolicy : public WorkQueueingPolicy {
public:
  /// Constructor
  ByteStreamWorkQueueingPolicy(uint64_t numConverters);

private:
  /**
     Automatically inspects the stream ID of buffers and assigns them to the
     queue corresponding to the worker that handles the stream

     \sa WorkQueueingPolicy::getEnqueueID
   */
  uint64_t getEnqueueID(Resource* workUnit);

  DISALLOW_COPY_AND_ASSIGN(ByteStreamWorkQueueingPolicy);
};

#endif // MAPRED_BYTE_STREAM_WORK_QUEUEING_POLICY_H
