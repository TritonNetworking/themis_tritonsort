#ifndef THEMIS_WORK_QUEUEING_POLICY_FACTORY_H
#define THEMIS_WORK_QUEUEING_POLICY_FACTORY_H

#include <string>

#include "core/Params.h"
#include "core/WorkQueueingPolicyInterface.h"
#include "core/constants.h"

/**
   The default factory for creating work queueing policies simply creates a
   default policy with one queue. You can derive from this factory in order to
   create other kinds of policies specific to a pipeline.
 */
class WorkQueueingPolicyFactory {
public:
  WorkQueueingPolicyFactory() {}

  virtual ~WorkQueueingPolicyFactory() {}

  virtual WorkQueueingPolicyInterface* newWorkQueueingPolicy(
    const std::string& phaseName, const std::string& stageName,
    const Params& params) const;
};

#endif // THEMIS_WORK_QUEUEING_POLICY_FACTORY_H
