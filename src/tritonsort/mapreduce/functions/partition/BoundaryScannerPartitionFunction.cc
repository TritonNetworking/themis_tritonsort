#include "tritonsort/mapreduce/functions/partition/BoundaryScannerPartitionFunction.h"

BoundaryScannerPartitionFunction::BoundaryScannerPartitionFunction(
    uint64_t _partitionsPerNode, uint64_t _numNodes)
    : partitionsPerNode(_partitionsPerNode),
      numNodes(_numNodes),
      partitionsSeen(0) {
}

uint64_t BoundaryScannerPartitionFunction::globalPartition(
  const uint8_t* key, uint32_t keyLength) const {
  uint64_t assignedNode = partitionsSeen++ / partitionsPerNode;
  ABORT_IF(assignedNode >= numNodes,
           "Assigned node %llu too large. Num nodes %llu",
           assignedNode, numNodes);
  return assignedNode;
}

uint64_t BoundaryScannerPartitionFunction::localPartition(
  const uint8_t* key, uint32_t keyLength, uint64_t partitionGroup) const {
  ABORT("Not implemented");
  return 0;
}

bool BoundaryScannerPartitionFunction::hashesKeys() const {
  return false;
}

bool BoundaryScannerPartitionFunction::acceptedByFilter(
  const uint8_t* key, uint32_t keyLength,
  const RecordFilter& filter) const {
  ABORT("Not implemented");
  return false;
}

uint64_t BoundaryScannerPartitionFunction::numGlobalPartitions() const {
  return numNodes;
}
