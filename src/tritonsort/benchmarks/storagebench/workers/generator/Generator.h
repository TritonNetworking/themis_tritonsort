#ifndef STORAGEBENCH_GENERATOR_H
#define STORAGEBENCH_GENERATOR_H

#include "core/SelfStartingWorker.h"
#include "mapreduce/common/KVPairBufferFactory.h"

/**
   Generator is a worker that generates write buffers to be used in the storage
   benchmark when the reader is disabled. It extracts information from params
   related to the way the data should be created, and then emits buffers either
   sequentially or in round-robin order. This allows the use of quota
   enforcement to stall the generator while disks are busy, enabling benchmark
   data greater than the total size of memory.
 */
class Generator : public SelfStartingWorker {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker id

     \param name the stage name

     \param memoryAllocator an allocator to use when creating new buffers

     \param partitionsPerWorker the number of partitions to create

     \param bufferSize the size of buffers to be written to the partitions

     \param buffersPerPartition the number of buffers to be written to each
     partition

     \param aligmentSize direct IO alignment size for the buffers

     \param sequential if true, generate sequential write buffers, otherwise
     round-robin buffers across partitions.
   */
  Generator(
    uint64_t id, const std::string& name,
    MemoryAllocatorInterface& memoryAllocator, uint64_t partitionsPerWorker,
    uint64_t bufferSize, uint64_t buffersPerPartition, uint64_t alignmentSize,
    bool sequential);

  /// Destructor
  virtual ~Generator() {}

  /// Generate buffers to be written to disk
  void run();

private:
  const uint64_t firstPartition;
  const uint64_t partitionsPerWorker;
  const uint64_t buffersPerPartition;
  const uint64_t bufferSize;
  const bool sequential;
  KVPairBufferFactory bufferFactory;
};

#endif // STORAGEBENCH_GENERATOR_H
