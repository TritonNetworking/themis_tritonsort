#include <boost/bind.hpp>

#include "core/MemoryUtils.h"
#include "mapreduce/workers/boundarydecider/BoundaryDecider.h"

BoundaryDecider::BoundaryDecider(
  uint64_t id, const std::string& stageName,
  MemoryAllocatorInterface& memoryAllocator, uint64_t defaultBufferSize,
  uint64_t alignmentSize, uint64_t _numNodes)
  : MultiQueueRunnable(id, stageName),
    numNodes(_numNodes),
    jobID(0),
    bufferFactory(*this, memoryAllocator, defaultBufferSize, alignmentSize),
    writer(
      boost::bind(&BoundaryDecider::broadcastOutputChunk, this, _1),
      boost::bind(&BoundaryDecider::getOutputChunk, this, _1)),
    sortStrategy(false) {
  buffers.resize(numNodes);
  kvPairs.resize(numNodes);
}

void BoundaryDecider::run() {
  // Make sure we get boundary buffers from all peers before beginning.
  startWaitForWorkTimer();
  uint64_t peerID = 0;
  for (BufferVector::iterator iter = buffers.begin(); iter != buffers.end();
       iter++, peerID++) {
    KVPairBuffer* buffer = getNewWork(peerID);
    (*iter) = buffer;
    bool gotTuple = buffer->getNextKVPair(kvPairs.at(peerID));
    ABORT_IF(!gotTuple, "Somehow we didn't find a tuple in the buffer for "
             "node %llu", peerID);
  }
  stopWaitForWorkTimer();

  // Get the job ID from the first buffer.
  uint64_t numJobIDs = buffers.at(0)->getJobIDs().size();
  ABORT_IF(numJobIDs != 1, "Expected one job ID but got %llu", numJobIDs);
  jobID = *(buffers.at(0)->getJobIDs().begin());

  bool done = false;
  while (!done) {
    // Sort the partition boundary keys and take the median key.
    uint64_t totalBytes = 0;
    for (TupleVector::iterator iter = kvPairs.begin(); iter != kvPairs.end();
         iter++) {
      totalBytes += iter->getWriteSize();
    }

    KVPairBuffer* buffer = bufferFactory.newInstance(totalBytes);
    for (TupleVector::iterator iter = kvPairs.begin(); iter != kvPairs.end();
         iter++) {
      buffer->addKVPair(*iter);
    }

    uint64_t scratchBufferSize =
      sortStrategy.getRequiredScratchBufferSize(buffer);
    uint8_t* scratchBuffer = new (themis::memcheck) uint8_t[scratchBufferSize];

    KVPairBuffer* sortedBuffer = bufferFactory.newInstance(totalBytes);

    sortStrategy.setScratchBuffer(scratchBuffer);
    sortStrategy.sort(buffer, sortedBuffer);
    sortedBuffer->resetIterator();

    delete buffer;
    delete[] scratchBuffer;

    // Pick off the median key.
    KeyValuePair kvPair;
    uint64_t medianIndex = (numNodes - 1) / 2;
    for (uint64_t i = 0; i < medianIndex + 1; i++) {
      sortedBuffer->getNextKVPair(kvPair);
    }

    // Copy this median key to the partition boundary buffer.
    writer.write(kvPair);

    delete sortedBuffer;

    // Now fetch new tuples from each buffer, and new buffers if we run out of
    // tuples.
    uint64_t peerID = 0;
    uint64_t nodesInTeardown = 0;
    for (BufferVector::iterator iter = buffers.begin(); iter != buffers.end();
         iter++, peerID++) {
      bool gotTuple = (*iter)->getNextKVPair(kvPairs.at(peerID));
      if (!gotTuple) {
        // This buffer is out of tuples, so delete it and get a new one.
        delete (*iter);
        (*iter) = getNewWork(peerID);
        if ((*iter) == NULL) {
          // We ran out of buffers for this node, so make sure we run out of
          // buffers for all peers simultaneously.
          nodesInTeardown++;
        } else {
          // Got a buffer, so get its first tuple.
          bool gotTuple = (*iter)->getNextKVPair(kvPairs.at(peerID));
          ABORT_IF(!gotTuple, "Somehow we didn't find a tuple in the buffer "
                   "for node %llu", peerID);
        }
      }
    }

    if (nodesInTeardown > 0) {
      // Make sure all nodes are simultaneously in teardown.
      ABORT_IF(nodesInTeardown != numNodes, "All nodes should be in teardown "
               "simultaneously, but we only ran out of buffers for %llu of "
               "%llu nodes", nodesInTeardown, numNodes);

      // Break out of the while loop.
      done = true;
    }
  }

  // Flush any remaining buffers from the writer.
  writer.flushBuffers();
}

KVPairBuffer* BoundaryDecider::getOutputChunk(uint64_t tupleSize) {
  KVPairBuffer* buffer = NULL;

  if (tupleSize > bufferFactory.getDefaultSize()) {
    buffer = bufferFactory.newInstance(tupleSize);
  } else {
    buffer = bufferFactory.newInstance();
  }

  return buffer;
}

void BoundaryDecider::broadcastOutputChunk(KVPairBuffer* outputChunk) {
  // Send a copy of this chunk to each node.
  for (uint64_t i = 0; i < numNodes; i++) {
    KVPairBuffer* buffer = NULL;
    if (i != numNodes - 1) {
      // Create a copy of the chunk.
      buffer = bufferFactory.newInstance(outputChunk->getCurrentSize());
      buffer->append(
        outputChunk->getRawBuffer(), outputChunk->getCurrentSize());
    } else {
      // Use the original chunk for the last peer.
      buffer = outputChunk;
    }

    buffer->setNode(i);
    buffer->addJobID(jobID);
    emitWorkUnit(buffer);
  }
}

BaseWorker* BoundaryDecider::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t defaultBufferSize = params.get<uint64_t>(
    "DEFAULT_BUFFER_SIZE." + phaseName + "." + stageName);

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t numNodes = params.get<uint64_t>("NUM_PEERS");

  BoundaryDecider* decider = new BoundaryDecider(
    id, stageName, memoryAllocator, defaultBufferSize, alignmentSize, numNodes);

  return decider;
}
