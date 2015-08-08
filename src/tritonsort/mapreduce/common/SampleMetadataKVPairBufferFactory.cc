#include "core/MemoryUtils.h"
#include "mapreduce/common/PhaseZeroSampleMetadata.h"
#include "mapreduce/common/SampleMetadataKVPairBufferFactory.h"
#include "mapreduce/common/buffers/SampleMetadataKVPairBuffer.h"

SampleMetadataKVPairBufferFactory::SampleMetadataKVPairBufferFactory(
  BaseWorker& parentWorker, MemoryAllocatorInterface& memoryAllocator,
  uint64_t capacity, uint64_t alignmentMultiple)
  : BufferFactory<SampleMetadataKVPairBuffer>(
    parentWorker, memoryAllocator,
    capacity + PhaseZeroSampleMetadata::tupleSize(), alignmentMultiple) {
}

SampleMetadataKVPairBuffer* SampleMetadataKVPairBufferFactory::newInstance() {
  return BufferFactory<SampleMetadataKVPairBuffer>::newInstance();
}

SampleMetadataKVPairBuffer*
SampleMetadataKVPairBufferFactory::attemptNewInstance() {
  return BufferFactory<SampleMetadataKVPairBuffer>::attemptNewInstance();
}

SampleMetadataKVPairBuffer* SampleMetadataKVPairBufferFactory::newInstance(
  uint64_t capacity) {
  return BufferFactory<SampleMetadataKVPairBuffer>::newInstance(
    capacity + PhaseZeroSampleMetadata::tupleSize());
}

SampleMetadataKVPairBuffer*
SampleMetadataKVPairBufferFactory::attemptNewInstance(uint64_t capacity) {
  return BufferFactory<SampleMetadataKVPairBuffer>::attemptNewInstance(
    capacity + PhaseZeroSampleMetadata::tupleSize());
}


SampleMetadataKVPairBuffer* SampleMetadataKVPairBufferFactory::createNewBuffer(
  MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
  uint64_t capacity, uint64_t alignmentMultiple) {

  SampleMetadataKVPairBuffer* buffer = new (themis::memcheck)
    SampleMetadataKVPairBuffer(
      memoryAllocator, memoryRegion, capacity, alignmentMultiple);

  return buffer;
}
