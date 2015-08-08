#include "benchmarks/storagebench/workers/generator/Generator.h"

Generator::Generator(
  uint64_t id, const std::string& name,
  MemoryAllocatorInterface& memoryAllocator, uint64_t _partitionsPerWorker,
  uint64_t _bufferSize, uint64_t _buffersPerPartition, uint64_t alignmentSize,
  bool _sequential)
  : SelfStartingWorker(id, name),
    firstPartition(_partitionsPerWorker * id),
    partitionsPerWorker(_partitionsPerWorker),
    buffersPerPartition(_buffersPerPartition),
    bufferSize(_bufferSize),
    sequential(_sequential),
    bufferFactory(*this, memoryAllocator, _bufferSize, alignmentSize) {
}

void Generator::run() {
  // Use a byte buffer for sourcing data.
  uint8_t* bytes = new uint8_t[bufferSize];

  if (sequential) {
    // Generate all buffers for a file before moving onto the next.
    for (uint64_t partition = firstPartition;
         partition < firstPartition + partitionsPerWorker; partition++) {
      for (uint64_t i = 0; i < buffersPerPartition; i++) {
        // Make sure this buffer is different from the last.
        memset(bytes, i, bufferSize);

        KVPairBuffer* buffer = bufferFactory.newInstance();
        // Touch key/value pair buffer's memory.
        buffer->append(bytes, bufferSize);
        buffer->setLogicalDiskID(partition);
        buffer->setCurrentSize(buffer->getCapacity());
        buffer->addJobID(0);
        emitWorkUnit(buffer);
      }
    }
  } else {
    // Generate buffers for files in round-robin order.
    for (uint64_t i = 0; i < buffersPerPartition; i++) {
    for (uint64_t partition = firstPartition;
         partition < firstPartition + partitionsPerWorker; partition++) {
        // Make sure this buffer is different from the last.
        memset(bytes, partition, bufferSize);

        KVPairBuffer* buffer = bufferFactory.newInstance();
        // Touch key/value pair buffer's memory.
        buffer->append(bytes, bufferSize);
        buffer->setLogicalDiskID(partition);
        buffer->setCurrentSize(buffer->getCapacity());
        buffer->addJobID(0);
        emitWorkUnit(buffer);
      }
    }
  }

  delete[] bytes;
}

BaseWorker* Generator::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t dataSize = params.getv<uint64_t>(
    "BENCHMARK_DATA_SIZE_PER_NODE.%s", phaseName.c_str());
  uint64_t partitionSize = params.get<uint64_t>("PARTITION_SIZE");
  uint64_t numPartitions = dataSize / partitionSize;

  // Spread input files evenly across all generators.
  uint64_t numWorkers = params.getv<uint64_t>(
    "NUM_WORKERS.%s.%s", phaseName.c_str(), stageName.c_str());

  ABORT_IF(numPartitions % numWorkers != 0,
           "Number of workers per node %llu should divide the number of "
           "partitions %llu", numWorkers, numPartitions);
  // Compute the number of partitions each worker would be responsible.
  uint64_t partitionsPerWorker = numPartitions / numWorkers;

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t bufferSize = params.getv<uint64_t>(
    "DEFAULT_BUFFER_SIZE.%s.%s", phaseName.c_str(), stageName.c_str());
  ABORT_IF(alignmentSize > 0 && bufferSize % alignmentSize != 0,
           "Generator buffer size %llu is not a multiple of alignment size "
           "%llu.", bufferSize, alignmentSize);

  uint64_t buffersPerPartition = partitionSize / bufferSize;

  bool sequential = params.get<bool>("GENERATE_SEQUENTIAL_WRITE_BUFFERS");

  Generator* generator = new Generator(
    id, stageName, memoryAllocator, partitionsPerWorker, bufferSize,
    buffersPerPartition, alignmentSize, sequential);

  return generator;
}
