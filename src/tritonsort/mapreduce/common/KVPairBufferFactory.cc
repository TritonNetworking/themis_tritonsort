#include "core/ByteOrder.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/KVPairBufferFactory.h"

KVPairBufferFactory::KVPairBufferFactory(
  BaseWorker& parentWorker, MemoryAllocatorInterface& memoryAllocator,
  uint64_t defaultSize, uint64_t alignmentMultiple)
  : BufferFactory(
    parentWorker, memoryAllocator, defaultSize, alignmentMultiple) {
}

KVPairBuffer*
KVPairBufferFactory::createNewBuffer(MemoryAllocatorInterface& memoryAllocator,
                                     uint8_t* memoryRegion,
                                     uint64_t capacity,
                                     uint64_t alignmentMultiple) {

  KVPairBuffer* buffer = new (themis::memcheck) KVPairBuffer(
    memoryAllocator, memoryRegion, capacity, alignmentMultiple);

  ABORT_IF(buffer == NULL, "Could not allocate KVPairBuffer");

  return buffer;
}

KVPairBuffer* KVPairBufferFactory::newInstanceFromNetwork(
  uint8_t* metadata, uint64_t peerID) {
  KVPairBuffer::NetworkMetadata* networkMetadata =
    reinterpret_cast<KVPairBuffer::NetworkMetadata*>(metadata);
  networkMetadata->bufferLength =
    bigEndianToHost64(networkMetadata->bufferLength);
  networkMetadata->jobID = bigEndianToHost64(networkMetadata->jobID);
  networkMetadata->partitionGroup =
    bigEndianToHost64(networkMetadata->partitionGroup);
  networkMetadata->partitionID =
    bigEndianToHost64(networkMetadata->partitionID);

  KVPairBuffer* buffer = newInstance(networkMetadata->bufferLength);
  buffer->clear();
  // Set job ID for multi-job support.
  buffer->addJobID(networkMetadata->jobID);
  // Set node for BoundaryCalculator merge logic.
  buffer->setNode(peerID);
  // Set partition group associated with records in this buffer.
  buffer->setPartitionGroup(networkMetadata->partitionGroup);
  // Set logical disk ID from partition ID.
  buffer->setLogicalDiskID(networkMetadata->partitionID);

  return buffer;
}
