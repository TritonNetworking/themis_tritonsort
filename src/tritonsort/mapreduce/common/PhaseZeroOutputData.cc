#include "PhaseZeroOutputData.h"

PhaseZeroOutputData::PhaseZeroOutputData(
  KeyPartitionerInterface* _keyPartitioner)
  : keyPartitioner(_keyPartitioner) {
}

KeyPartitionerInterface* PhaseZeroOutputData::getKeyPartitioner() const {
  return keyPartitioner;
}
