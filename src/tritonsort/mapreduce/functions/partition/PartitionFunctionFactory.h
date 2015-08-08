#ifndef MAPRED_PARTITION_FUNCTION_FACTORY_H
#define MAPRED_PARTITION_FUNCTION_FACTORY_H

#include <stdint.h>
#include <string>

class KeyPartitionerInterface;
class Params;
class PartitionFunctionInterface;

/**
   PartitionFunctionFactory creates new Partition functions based on application
   parameters and state or the existence of a KeyPartitionerInterface object.
 */
class PartitionFunctionFactory {
public:
  /// Constructor
  /**
     \param keyPartitioner a possibly-NULL pointer to the key partitioner object
     for the run

     \param numPartitions the number of partitions that the job using this
     factory will create
   */
  PartitionFunctionFactory(
    KeyPartitionerInterface* keyPartitioner, uint64_t numPartitions);

  /**
     Create a new PartitionFunctionInterface based on the runtime parameters of
     the system.

     \param params the parameters of the system

     \return a partition function interface for key partitioning
   */
  PartitionFunctionInterface* newPartitionFunctionInstance(
    const Params& params, const std::string& partitionFunctionName) const;

private:
  const uint64_t numPartitions;

  KeyPartitionerInterface* keyPartitioner;
};

#endif // MAPRED_PARTITION_FUNCTION_FACTORY_H
