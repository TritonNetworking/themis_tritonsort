#ifndef MAPRED_READ_REQUEST_WORK_QUEUEING_POLICY_H
#define MAPRED_READ_REQUEST_WORK_QUEUEING_POLICY_H

#include "core/WorkQueueingPolicy.h"

/**
   The ReadRequest queueing policy enqueues requests to readers based on disk
   ID. Disks are assigned round robin to readers.
 */
class ReadRequestWorkQueueingPolicy : public WorkQueueingPolicy {
public:
  /// Constructor
  /**
     \param numReaders the number of readers
   */
  ReadRequestWorkQueueingPolicy(uint64_t numDisks);

private:
  /**
     Assign requests based on the diskID of the read request.

     \sa WorkQueueingPolicy::getEnqueueID
   */
  uint64_t getEnqueueID(Resource* workUnit);

  DISALLOW_COPY_AND_ASSIGN(ReadRequestWorkQueueingPolicy);
};

#endif // MAPRED_READ_REQUEST_WORK_QUEUEING_POLICY_H
