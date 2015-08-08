#include <limits>

#include "mapreduce/workers/buffercombiner/BufferCombiner.h"

BufferCombiner::BufferCombiner(
  uint64_t id, const std::string& name,
  MemoryAllocatorInterface& memoryAllocator, uint64_t alignmentSize)
  : SingleUnitRunnable<KVPairBuffer>(id, name),
    coalescedBufferSize(0),
    bufferFactory(*this, memoryAllocator, 0, alignmentSize) {
}

void BufferCombiner::run(KVPairBuffer* buffer) {
  coalescedBufferSize += buffer->getCurrentSize();
  buffers.push(buffer);
}

void BufferCombiner::teardown() {
  // Coalesce all buffers
  KVPairBuffer* buffer = bufferFactory.newInstance(coalescedBufferSize);

  uint64_t jobID = std::numeric_limits<uint64_t>::max();
  while (!buffers.empty()) {
    KVPairBuffer* nextBuffer = buffers.front();
    buffers.pop();

    if (jobID == std::numeric_limits<uint64_t>::max()) {
      const std::set<uint64_t>& jobIDs = nextBuffer->getJobIDs();

      ASSERT(jobIDs.size() == 1, "Expected the first buffer entering the "
             "combiner to have exactly one job ID; this one has %llu",
             jobIDs.size());
      jobID = *(jobIDs.begin());
      buffer->addJobID(jobID);
    }

    const uint8_t* appendPtr = buffer->setupAppend(
      nextBuffer->getCurrentSize());
    memcpy(
      const_cast<uint8_t*>(appendPtr), nextBuffer->getRawBuffer(),
      nextBuffer->getCurrentSize());
    buffer->commitAppend(appendPtr, nextBuffer->getCurrentSize());

    delete nextBuffer;
  }

  ABORT_IF(
    buffer->empty(), "BufferCombiner output buffer should not be empty.");

  emitWorkUnit(buffer);
}

BaseWorker* BufferCombiner::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

   uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  BufferCombiner* combiner = new BufferCombiner(
    id, stageName, memoryAllocator, alignmentSize);

  return combiner;
}
