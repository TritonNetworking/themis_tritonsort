#include "core/WorkQueueingPolicy.h"
#include "core/WorkQueueingPolicyFactory.h"

WorkQueueingPolicyInterface* WorkQueueingPolicyFactory::newWorkQueueingPolicy(
  const std::string& phaseName, const std::string& stageName,
  const Params& params) const {
  // Just create a default policy with 1 queue.
  return new WorkQueueingPolicy(1);
}
