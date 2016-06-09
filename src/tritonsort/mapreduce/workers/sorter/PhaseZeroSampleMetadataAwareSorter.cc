#include <boost/bind.hpp>

#include "PhaseZeroSampleMetadataAwareSorter.h"
#include "common/sort_constants.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/PhaseZeroSampleMetadata.h"
#include "mapreduce/common/SimpleKVPairWriter.h"
#include "mapreduce/common/buffers/SampleMetadataKVPairBuffer.h"
#include "mapreduce/common/sorting/SortStrategyFactory.h"

PhaseZeroSampleMetadataAwareSorter::PhaseZeroSampleMetadataAwareSorter(
  uint64_t id, const std::string& name, uint64_t _outputNodeID,
  MemoryAllocatorInterface& memoryAllocator, uint64_t alignmentSize,
  uint64_t _minBufferSize, const SortStrategyFactory& sortStrategyFactory,
  uint64_t maxRadixSortScratchSize)
  : Sorter(
    id, name, memoryAllocator, alignmentSize, sortStrategyFactory,
    maxRadixSortScratchSize),
    minBufferSize(_minBufferSize),
    outputNodeID(_outputNodeID),
    bufferFactory(*this, memoryAllocator, _minBufferSize, alignmentSize) {
}

void PhaseZeroSampleMetadataAwareSorter::prepareToWriteToOutputBuffer(
  KVPairBuffer* outputBuffer, KVPairBuffer* inputBuffer) {
  // Store job IDs.
  jobIDs = inputBuffer->getJobIDs();

  // Clear any data that might already be in the buffer
  outputBuffer->clear();

  // Copy sample metadata that was populated by the mapper from the input
  // buffer to the output buffer
  SampleMetadataKVPairBuffer* inputBufferWithMetadata =
    dynamic_cast<SampleMetadataKVPairBuffer*>(inputBuffer);
  ABORT_IF(inputBufferWithMetadata == NULL, "Dynamic casting input buffer to "
           "SampleMetadataKVPairBuffer failed; are you sure you're giving "
           "this stage the right kind of buffers?");
  SampleMetadataKVPairBuffer* outputBufferWithMetadata =
    dynamic_cast<SampleMetadataKVPairBuffer*>(outputBuffer);
  ABORT_IF(outputBufferWithMetadata == NULL, "Dynamic casting output buffer to "
           "SampleMetadataKVPairBuffer failed; are you sure you're giving "
           "this stage the right kind of buffers?");

  PhaseZeroSampleMetadata* metadata =
    inputBufferWithMetadata->getSampleMetadata();
  outputBufferWithMetadata->setSampleMetadata(*metadata);

  delete metadata;
}

void PhaseZeroSampleMetadataAwareSorter::emitOutputBuffer(
  KVPairBuffer* outputBuffer) {
  SampleMetadataKVPairBuffer* outputBufferWithMetadata =
    dynamic_cast<SampleMetadataKVPairBuffer*>(outputBuffer);

  ABORT_IF(outputBufferWithMetadata == NULL, "Dynamic casting output buffer to "
           "SampleMetadataKVPairBuffer failed; are you sure you're giving "
           "this stage the right kind of buffers?");

  outputBufferWithMetadata->prepareBufferForSending();

  // Since this output buffer can be gigabytes in size, we want to emit
  // smaller chunks to the coordinator node so it won't get overloaded.
  outputBufferWithMetadata->resetIterator();
  KeyValuePair kvPair;
  SimpleKVPairWriter writer(
    boost::bind(&PhaseZeroSampleMetadataAwareSorter::emitWorkUnit, this, _1),
    boost::bind(&PhaseZeroSampleMetadataAwareSorter::getOutputChunk, this, _1));
  while (outputBufferWithMetadata->getNextKVPair(kvPair)) {
    writer.write(kvPair);
  }

  writer.flushBuffers();

  delete outputBufferWithMetadata;
}

KVPairBuffer* PhaseZeroSampleMetadataAwareSorter::getOutputChunk(
  uint64_t tupleSize) {
  KVPairBuffer* buffer = NULL;

  if (tupleSize > minBufferSize) {
    buffer = bufferFactory.newInstance(tupleSize);
  } else {
    buffer = bufferFactory.newInstance();
  }

  buffer->setNode(outputNodeID);
  // Make sure we set the appropriate job ID
  buffer->addJobIDSet(jobIDs);

  return buffer;
}

KVPairBuffer* PhaseZeroSampleMetadataAwareSorter::newOutputBuffer(
  uint8_t* memory, uint64_t size) {
  return new (themis::memcheck) SampleMetadataKVPairBuffer(
    memoryAllocator, memory, size, alignmentSize);
}

uint64_t PhaseZeroSampleMetadataAwareSorter::getOutputBufferSize(
  KVPairBuffer& inputBuffer) {
  return inputBuffer.getCurrentSize() + PhaseZeroSampleMetadata::tupleSize();
}

BaseWorker* PhaseZeroSampleMetadataAwareSorter::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t minBufferSize = params.getv<uint64_t>(
    "DEFAULT_BUFFER_SIZE.%s.%s",  phaseName.c_str(), stageName.c_str());

  // Phase zero sorters should never reference secondary keys
  SortStrategyFactory sortStrategyFactory(
    params.get<std::string>("SORT_STRATEGY"), false);

  uint64_t mergeNodeID = params.get<uint64_t>("MERGE_NODE_ID");

  PhaseZeroSampleMetadataAwareSorter* sorter =
    new PhaseZeroSampleMetadataAwareSorter(
      id, stageName, mergeNodeID, memoryAllocator, alignmentSize,
      minBufferSize, sortStrategyFactory,
      params.get<uint64_t>("MAX_RADIX_SORT_SCRATCH_SIZE"));

  return sorter;
}
