#ifndef MAPRED_PHASE_ZERO_OUTPUT_DATA_H
#define MAPRED_PHASE_ZERO_OUTPUT_DATA_H

#include "core/Resource.h"

class KeyPartitionerInterface;

class PhaseZeroOutputData : public Resource {
public:
  PhaseZeroOutputData(KeyPartitionerInterface* keyPartitioner);

  KeyPartitionerInterface* getKeyPartitioner() const;

  uint64_t getCurrentSize() const {
    return sizeof(KeyPartitionerInterface*);
  }
private:
  KeyPartitionerInterface* keyPartitioner;
};

#endif // MAPRED_PHASE_ZERO_OUTPUT_DATA_H
