#ifndef MAPRED_DISK_BENCHMARK_MAP_FUNCTION_H
#define MAPRED_DISK_BENCHMARK_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

/**
   A map task for benchmarking the write performance of a disk
 */
class DiskBenchmarkMapFunction : public MapFunction {
public:
  /// Constructor
  /**
     \param hostname the name of the host on which the mapper is running

     \param writeSize the maximum size of a single write() syscall

     \param alignmentSize the number of bytes on which to align the memory used
     when writing

     \param writeBufferSize the approximate size of the write buffer to
     allocate (will actually allocate the nearest multiple of alignmentSize
     less than writeBufferSize)

     \param benchmarkDataSize the amount of data to be written in order to
     benchmark the disk's speed
   */
  DiskBenchmarkMapFunction(const std::string& hostname, uint64_t writeSize,
                           uint64_t alignmentSize, uint64_t writeBufferSize,
                           uint64_t benchmarkDataSize);

  /// Destructor
  virtual ~DiskBenchmarkMapFunction();

  /// Benchmark the performance of the provided disk
  /**
     Expects to receive a tuple whose key is the disk to benchmark and whose
     value is zero-length. Creates and opens a file inside the provided disk
     directory, writes some amount of data to it and times how long that write
     (plus flush and close) takes. Emits a single (0, mean write rate (32-bit,
     network encoded) + hostname + ':' + disk path) tuple.
   */
  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  const uint64_t alignmentSize;
  const std::string hostname;
  const uint64_t writeSize;
  const uint64_t benchmarkDataSize;

  /// A one-byte key containing 0, guaranteeing that everything is routed to
  /// one node on one disk
  uint8_t* zeroKey;
  const uint32_t zeroKeyLength;

  uint8_t* randomData;
  uint8_t* unalignedRandomData;
  const uint32_t randomDataLength;
};

#endif // MAPRED_DISK_BENCHMARK_MAP_FUNCTION_H
