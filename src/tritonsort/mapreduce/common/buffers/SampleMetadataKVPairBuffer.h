#ifndef MAPRED_SAMPLE_METADATA_KV_PAIR_BUFFER_H
#define MAPRED_SAMPLE_METADATA_KV_PAIR_BUFFER_H

#include "KVPairBuffer.h"

class PhaseZeroSampleMetadata;

/**
   A KVPairBuffer that maintains sample metadata for phase zero in the front of
   the buffer.
 */
class SampleMetadataKVPairBuffer : public KVPairBuffer {
public:
  /// Constructor
  /**
     \sa KVPairBuffer::KVPairBuffer(
     MemoryAllocatorInterface& memoryAllocator, uint64_t callerID,
     uint64_t capacity, uint64_t alignmentMultiple)
   */
  SampleMetadataKVPairBuffer(
    MemoryAllocatorInterface& memoryAllocator, uint64_t callerID,
    uint64_t capacity, uint64_t alignmentMultiple = 0);

  /// Constructor
  /**
     \sa KVPairBuffer::KVPairBuffer(
     MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
     uint64_t capacity, uint64_t alignmentMultiple)
   */
  SampleMetadataKVPairBuffer(
    MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
    uint64_t capacity, uint64_t alignmentMultiple = 0);

  /**
     Write sample metadata as a key value pair to the memory region before the
     start of the buffer.

     \param metadata the metadata to write
   */
  void setSampleMetadata(PhaseZeroSampleMetadata& metadata);

  /**
     Interpret the memory region in front of the buffer as a key value pair and
     create a PhaseZeroSampleMetadata from that.

     \return the PhaseZeroSampleMetadata for this bfufer
   */
  PhaseZeroSampleMetadata* getSampleMetadata() const;

  /// Seeks the buffer backward and resets the internal iterator so that
  /// metadata tuple will be included in subsequent sends
  void prepareBufferForSending();

private:
  uint8_t* const metadataLocation;
};

#endif // MAPRED_SAMPLE_METADATA_KV_PAIR_BUFFER_H
