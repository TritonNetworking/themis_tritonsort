#include <algorithm>
#include <limits>
#include <new>

#include "Sorter.h"
#include "core/MemoryAllocationContext.h"
#include "core/MemoryUtils.h"
#include "core/RawBuffer.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/sorting/SortStrategyFactory.h"

Sorter::Sorter(
  uint64_t id, const std::string& name,
  MemoryAllocatorInterface& _memoryAllocator, uint64_t _alignmentSize,
  const SortStrategyFactory& sortStrategyFactory,
  uint64_t _maxRadixSortScratchSize)
  : SingleUnitRunnable<KVPairBuffer>(id, name),
    alignmentSize(_alignmentSize),
    memoryAllocator(_memoryAllocator),
    maxRadixSortScratchSize(_maxRadixSortScratchSize),
    logger(name, id) {

  sortStrategyFactory.populateOrderedSortStrategyList(sortStrategies);

  // Register caller ID with the memory allocator
  scratchMemoryCallerID = memoryAllocator.registerCaller(*this),

  // Initialize statistics
  numTuplesIn = 0;
  numTuplesOut = 0;
  totalBytesIn = 0;
  totalBytesOut = 0;

  // Initialize logger
  sortTimeStatID = logger.registerStat("sort_time");
  numTuplesStatID = logger.registerStat("num_tuples");
  tuplesInStatID = logger.registerStat("tuples_in");
  tuplesOutStatID = logger.registerStat("tuples_out");
  bytesInStatID = logger.registerStat("bytes_in");
  bytesOutStatID = logger.registerStat("bytes_out");
  sortAlgorithmUsedStatID = logger.registerHistogramStat("algorithm_used", 1);
}

KVPairBuffer* Sorter::newOutputBuffer(uint8_t* memory, uint64_t size) {
  return new (themis::memcheck) KVPairBuffer(
    memoryAllocator, memory, size, alignmentSize);
}

void Sorter::run(KVPairBuffer* inputBuffer) {
  uint64_t tuplesIn = inputBuffer->getNumTuples();
  uint64_t bytesIn = inputBuffer->getCurrentSize();
  uint64_t logicalDiskID = inputBuffer->getLogicalDiskID();
  uint64_t outputBufferSize = getOutputBufferSize(*inputBuffer);

  uint64_t requiredMemorySize = 0;
  SortStrategyInterface* selectedStrategy = NULL;
  for (SortStrategyInterfaceList::iterator iter = sortStrategies.begin();
       iter != sortStrategies.end(); iter++) {
    SortStrategyInterface* currentStrategy = *iter;
    uint64_t requiredScratchBufferSize =
      currentStrategy->getRequiredScratchBufferSize(inputBuffer);

    // Only use Radix Sort if all keys are the same length and the scratch size
    // does not exceed the user specified maximum.
    if (currentStrategy->getSortAlgorithmID() == RADIX_SORT_MAPREDUCE &&
        (inputBuffer->getMinKeyLength() != inputBuffer->getMaxKeyLength() ||
         requiredScratchBufferSize > maxRadixSortScratchSize)) {
      continue;
    }

    // Use the first strategy in the list with an acceptable size.
    requiredMemorySize = requiredScratchBufferSize + outputBufferSize;
    selectedStrategy = *iter;
    break;
  }

  ABORT_IF(requiredMemorySize == 0,
           "Could not find an acceptable sort strategy.");

  MemoryAllocationContext scratchContext(
    scratchMemoryCallerID, requiredMemorySize + alignmentSize);

  // Allocate memory for both the scratch space and the buffer.
  uint64_t allocatedMemorySize;
  uint8_t* allocatedMemory = static_cast<uint8_t*>(
    memoryAllocator.allocate(scratchContext, allocatedMemorySize));

  // Grab an output buffer using the entire memory region but only as large as
  // is required.
  KVPairBuffer *outputBuffer = newOutputBuffer(
    allocatedMemory, outputBufferSize);

  // As a bit of trickery, we're going to allocate scratch memory as part of
  // the buffer's memory so that we can garbage collect both of them in the
  // reducer without making separate allocation requests
  uint8_t* scratchMemory =
    const_cast<uint8_t*>(outputBuffer->getRawBuffer()) + outputBufferSize;

  // Do whatever you need to do to prepare the output buffer for accepting
  // tuples from the provided input buffer
  prepareToWriteToOutputBuffer(outputBuffer, inputBuffer);

  // Set the output buffer LD so Writers will know how to handle it
  outputBuffer->setLogicalDiskID(logicalDiskID);

  // Copy the input buffer's job IDs to the output buffer so downstream stages
  // will know how to handle it
  outputBuffer->addJobIDSet(inputBuffer->getJobIDs());

  selectedStrategy->setScratchBuffer(scratchMemory);
  logger.add(sortAlgorithmUsedStatID, selectedStrategy->getSortAlgorithmID());

  // Sort the buffer
  sortTimer.start();
  selectedStrategy->sort(inputBuffer, outputBuffer);
  sortTimer.stop();

  // Don't need to deallocate scratch memory at this point; output buffer will
  // deallocate it for us on deletion

  delete inputBuffer;

  // Get num tuples and num pairs
  uint64_t tuplesOut = outputBuffer->getNumTuples();
  uint64_t bytesOut = outputBuffer->getCurrentSize();

  // Sorter should produce the same number of output tuples/bytes
  TRITONSORT_ASSERT(bytesOut == bytesIn, "Sorter output buffer size is not equal to input "
         "buffer size (%llu bytes != %llu bytes)", bytesOut, bytesIn);
  TRITONSORT_ASSERT(tuplesOut == tuplesIn, "Sorter output buffer tuple count is not equal "
         "input buffer tuple count (%llu tuples != %llu tuples)", tuplesOut,
         tuplesIn);

  // Send output buffer downstream
  emitOutputBuffer(outputBuffer);

  // Record statistics
  logger.add(sortTimeStatID, sortTimer.getElapsed());
  logger.add(numTuplesStatID, tuplesIn);
  numTuplesIn += tuplesIn;
  numTuplesOut += tuplesOut;
  totalBytesIn += bytesIn;
  totalBytesOut += bytesOut;
}

void Sorter::teardown() {
  logger.add(tuplesInStatID, numTuplesIn);
  logger.add(tuplesOutStatID, numTuplesOut);
  logger.add(bytesInStatID, totalBytesIn);
  logger.add(bytesOutStatID, totalBytesOut);

  for (SortStrategyInterfaceList::iterator iter = sortStrategies.begin();
       iter != sortStrategies.end(); iter++) {
    delete *iter;
  }

  sortStrategies.clear();
}

void Sorter::prepareToWriteToOutputBuffer(KVPairBuffer* outputBuffer,
                                          KVPairBuffer* inputBuffer) {
  outputBuffer->clear();
}

void Sorter::emitOutputBuffer(KVPairBuffer* outputBuffer) {
  emitWorkUnit(outputBuffer);
}

uint64_t Sorter::getOutputBufferSize(KVPairBuffer& inputBuffer) {
  return inputBuffer.getCurrentSize();
}

BaseWorker* Sorter::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  SortStrategyFactory sortStrategyFactory(
    params.get<std::string>("SORT_STRATEGY"),
    params.get<bool>("USE_SECONDARY_KEYS"));

  // Create the sorter using a given strategy.
  Sorter* sorter = new Sorter(
    id, stageName, memoryAllocator, alignmentSize, sortStrategyFactory,
    params.get<uint64_t>("MAX_RADIX_SORT_SCRATCH_SIZE"));

  return sorter;
}
