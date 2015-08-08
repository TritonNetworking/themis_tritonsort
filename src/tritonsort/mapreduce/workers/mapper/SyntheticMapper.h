#ifndef MAPRED_SYNTHETIC_ROUND_ROBIN_MAPPER_H
#define MAPRED_SYNTHETIC_ROUND_ROBIN_MAPPER_H

#include "core/SelfStartingWorker.h"
#include "mapreduce/common/KVPairBufferFactory.h"

class SyntheticMapper : public SelfStartingWorker {
WORKER_IMPL

public:
  SyntheticMapper(
    uint64_t id, const std::string& name,
    MemoryAllocatorInterface& memoryAllocator, uint64_t bufferSize,
    uint64_t alignmentSize, uint64_t buffersPerPeer, uint64_t numPeers,
    bool randomizeBuffers);

  void run();

private:
  const uint64_t buffersPerPeer;
  const uint64_t numPeers;

  std::vector<uint64_t> peerIDs;

  KVPairBufferFactory bufferFactory;
};

#endif // MAPRED_SYNTHETIC_ROUND_ROBIN_MAPPER_H
