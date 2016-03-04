#ifndef THEMIS_MAPRED_PARTITION_MAP_H
#define THEMIS_MAPRED_PARTITION_MAP_H

#include <map>
#include <stdint.h>

#include "core/constants.h"

class CoordinatorClientInterface;
class Params;

/**
   PartitionMap provides a way to lazily load partition counts for jobs from
   the coordinator.
 */
class PartitionMap {
public:
  /// Constructor
  /**
     \param params global params object used to construct a coordinator client

     \param phaseName the name of the phase
   */
  PartitionMap(const Params& params, const std::string& phaseName);

  virtual ~PartitionMap();

  /// Retrieve the number of partitions for a job.
  /**
     \param jobID the ID of the job
   */
  uint64_t getNumPartitions(uint64_t jobID);

  /// Retrieve the number of partitions per output disk for a job.
  /**
     \param jobID the ID of the job
   */
  uint64_t getNumPartitionsPerOutputDisk(uint64_t jobID);

  /// Retrieve the number of partitions per node for a job.
  /**
     \param jobID the ID of the job
   */
  uint64_t getNumPartitionsPerNode(uint64_t jobID);

  /// Retrieve the number of partitions per group for a job.
  /**
     \param jobID the ID of the job
   */
  uint64_t getNumPartitionsPerGroup(uint64_t jobID);

  /// Get the first partition belonging to this local node.
  /**
     \param jobID the ID of the job
   */
  uint64_t getFirstLocalPartition(uint64_t jobID);
private:
  typedef std::map<uint64_t, uint64_t> InternalCountsMap;

  const uint64_t numNodes;
  const uint64_t localNodeID;
  const uint64_t numPartitionGroups;
  const uint64_t totalNumOutputDisks;

  CoordinatorClientInterface* coordinatorClient;
  InternalCountsMap partitionMap;
};


#endif // THEMIS_MAPRED_PARTITION_MAP_H
