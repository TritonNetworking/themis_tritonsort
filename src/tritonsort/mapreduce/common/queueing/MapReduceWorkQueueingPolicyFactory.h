#ifndef MAPREDUCE_WORK_QUEUEING_POLICY_FACTORY_H
#define MAPREDUCE_WORK_QUEUEING_POLICY_FACTORY_H

#include "core/WorkQueueingPolicyFactory.h"
#include "core/constants.h"

class ChunkMap;

/**
   This factory creates work queueing policies for the MapReduce pipeline based
   on the value of WORK_QUEUEING_POLICY.phase_name.stage_name. If no policy is
   specified, the default policy will be used.
 */
class MapReduceWorkQueueingPolicyFactory : public WorkQueueingPolicyFactory {
public:
  MapReduceWorkQueueingPolicyFactory();

  virtual WorkQueueingPolicyInterface* newWorkQueueingPolicy(
    const std::string& phaseName, const std::string& stageName,
    const Params& params) const;

  void setChunkMap(ChunkMap* chunkMap);

private:
  ChunkMap* chunkMap;
};

#endif // MAPREDUCE_WORK_QUEUEING_POLICY_FACTORY_H
