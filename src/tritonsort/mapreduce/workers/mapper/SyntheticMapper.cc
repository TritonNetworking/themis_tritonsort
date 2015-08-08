#include <algorithm>

#include "mapreduce/workers/mapper/SyntheticMapper.h"

SyntheticMapper::SyntheticMapper(
  uint64_t id, const std::string& name,
  MemoryAllocatorInterface& memoryAllocator, uint64_t bufferSize,
  uint64_t alignmentSize, uint64_t _buffersPerPeer, uint64_t _numPeers,
  bool randomizeBuffers)
  : SelfStartingWorker(id, name),
    buffersPerPeer(_buffersPerPeer),
    numPeers(_numPeers),
    bufferFactory(*this, memoryAllocator, bufferSize, alignmentSize) {

  for (uint64_t i = 0; i < buffersPerPeer; i++) {
    for (uint64_t peerID = 0; peerID < numPeers; peerID++) {
      peerIDs.push_back(peerID);
    }
  }

  if (randomizeBuffers) {
    std::random_shuffle(peerIDs.begin(), peerIDs.end());
  }
}

void SyntheticMapper::run() {
  for (std::vector<uint64_t>::iterator iter = peerIDs.begin();
       iter != peerIDs.end(); iter++) {
    uint64_t peerID = *iter;

    KVPairBuffer* buffer = bufferFactory.newInstance();
    buffer->fillWithRandomData();
    buffer->setNode(peerID);

    emitWorkUnit(buffer);

  }
}

BaseWorker* SyntheticMapper::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t numPeers = params.get<uint64_t>("NUM_PEERS");
  uint64_t bufferSize = params.get<uint64_t>(
    "DEFAULT_BUFFER_SIZE." + phaseName + "." + stageName);

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t buffersPerPeer = params.get<uint64_t>(
    "SYNTHETIC_MAPPER_BUFFERS_PER_PEER");

  bool randomizeBuffers = params.get<bool>(
    "SYNTHETIC_MAPPER_RANDOMIZE_BUFFERS");

  SyntheticMapper* mapper = new SyntheticMapper(
    id, stageName, memoryAllocator, bufferSize, alignmentSize, buffersPerPeer,
    numPeers, randomizeBuffers);

  return mapper;
}
