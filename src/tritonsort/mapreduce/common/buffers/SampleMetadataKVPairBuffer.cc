#include "SampleMetadataKVPairBuffer.h"
#include "mapreduce/common/PhaseZeroSampleMetadata.h"
#include "mapreduce/common/KeyValuePair.h"

SampleMetadataKVPairBuffer::SampleMetadataKVPairBuffer(
  MemoryAllocatorInterface& memoryAllocator, uint64_t callerID,
  uint64_t size, uint64_t alignmentMultiple)
  : KVPairBuffer(
    memoryAllocator, callerID, size + PhaseZeroSampleMetadata::tupleSize(),
    alignmentMultiple),
    metadataLocation(const_cast<uint8_t*>(getRawBuffer())) {
  seekForward(PhaseZeroSampleMetadata::tupleSize());
}

// We assume that constructing the buffer from existing memory means that the
// caller has accounted for the metadata tuple in the size parameter, so no need
// to add it as in the previous constructor.
SampleMetadataKVPairBuffer::SampleMetadataKVPairBuffer(
  MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
  uint64_t size, uint64_t alignmentMultiple)
  : KVPairBuffer(memoryAllocator, memoryRegion, size, alignmentMultiple),
    metadataLocation(const_cast<uint8_t*>(getRawBuffer())) {
  seekForward(PhaseZeroSampleMetadata::tupleSize());
}

void SampleMetadataKVPairBuffer::setSampleMetadata(
  PhaseZeroSampleMetadata& metadata) {
  KeyValuePair kvPair;
  metadata.write(kvPair);
  kvPair.serialize(metadataLocation);
}

PhaseZeroSampleMetadata* SampleMetadataKVPairBuffer::getSampleMetadata() const {
  KeyValuePair kvPair;
  kvPair.deserialize(metadataLocation);
  PhaseZeroSampleMetadata* metadata = new PhaseZeroSampleMetadata(kvPair);

  return metadata;
}

void SampleMetadataKVPairBuffer::prepareBufferForSending() {
  seekBackward(PhaseZeroSampleMetadata::tupleSize());
  resetIterator();
  cached = false;
}
