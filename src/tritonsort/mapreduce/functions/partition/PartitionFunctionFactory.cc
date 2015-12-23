#include "core/MemoryUtils.h"
#include "core/Params.h"
#include "mapreduce/common/PartitionFunctionInterface.h"
#include "mapreduce/functions/partition/BoundaryListPartitionFunction.h"
#include "mapreduce/functions/partition/HashedBoundaryListPartitionFunction.h"
#include "mapreduce/functions/partition/PartitionFunctionFactory.h"
#include "mapreduce/functions/partition/SinglePartitionMergingPartitionFunction.h"
#include "mapreduce/functions/partition/UniformPartitionFunction.h"

PartitionFunctionFactory::PartitionFunctionFactory(
  KeyPartitionerInterface* _keyPartitioner, uint64_t _numPartitions)
  : numPartitions(_numPartitions),
    keyPartitioner(_keyPartitioner) {
}

PartitionFunctionInterface*
PartitionFunctionFactory::newPartitionFunctionInstance(
  const Params& params, const std::string& partitionFunctionName) const {

  PartitionFunctionInterface* partitionFunction = NULL;
  if (partitionFunctionName.compare(
    "BoundaryListPartitionFunction") == 0) {
    // Use the order-preserving boundary list partition function that stores
    // entire keys.

    partitionFunction = new (themis::memcheck)
      BoundaryListPartitionFunction(keyPartitioner);
  } else if (partitionFunctionName.compare(
    "HashedBoundaryListPartitionFunction") == 0) {
    // Use the hash-based boundary list partition function that does not
    // preserve order, but only stores hashes of keys.

    partitionFunction = new (themis::memcheck)
      HashedBoundaryListPartitionFunction(keyPartitioner);
  } else if (partitionFunctionName.compare(
    "SinglePartitionMergingPartitionFunction") == 0) {
    // Direct all records to a single logical disk
    partitionFunction = new (themis::memcheck)
      SinglePartitionMergingPartitionFunction();
  } else if (partitionFunctionName.compare(
    "UniformPartitionFunction") == 0) {
    // Use a uniform partitioning function.
    partitionFunction = new (themis::memcheck)
      UniformPartitionFunction(params, numPartitions);
  } else {
    ABORT("Unknown custom partition function '%s'",
          partitionFunctionName.c_str());
  }

  // Sanity check
  ASSERT(partitionFunction != NULL, "Expected a partition function to be "
         "created at this point, but partitionFunction is NULL");

  return partitionFunction;
}
