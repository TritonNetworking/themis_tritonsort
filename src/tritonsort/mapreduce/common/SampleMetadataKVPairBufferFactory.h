#ifndef MAPRED_SAMPLE_METADATA_KV_PAIR_BUFFER_FACTORY_H
#define MAPRED_SAMPLE_METADATA_KV_PAIR_BUFFER_FACTORY_H

#include "common/BufferFactory.h"
#include "core/ResourceFactory.h"
#include "mapreduce/common/buffers/SampleMetadataKVPairBuffer.h"

class SampleMetadataKVPairBufferFactory
  : public BufferFactory<SampleMetadataKVPairBuffer> {
public:
  SampleMetadataKVPairBufferFactory(
    BaseWorker& parentWorker, MemoryAllocatorInterface& memoryAllocator,
    uint64_t capacity, uint64_t alignmentMultiple = 0);


  SampleMetadataKVPairBuffer* newInstance();
  SampleMetadataKVPairBuffer* newInstance(uint64_t capacity);
  SampleMetadataKVPairBuffer* attemptNewInstance();
  SampleMetadataKVPairBuffer* attemptNewInstance(uint64_t capacity);

protected:
  SampleMetadataKVPairBuffer* createNewBuffer(
    MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
    uint64_t capacity, uint64_t alignmentMultiple);

};

#endif // MAPRED_SAMPLE_METADATA_KV_PAIR_BUFFER_FACTORY_H
