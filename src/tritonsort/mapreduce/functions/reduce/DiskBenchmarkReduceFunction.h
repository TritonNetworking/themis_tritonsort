#ifndef MAPRED_DISK_BENCHMARK_REDUCE_FUNCTION_H
#define MAPRED_DISK_BENCHMARK_REDUCE_FUNCTION_H

#include "mapreduce/functions/reduce/ReduceFunction.h"

/**
   Turns the output from DiskBenchmarkMapFunction into something that can be
   more easily read
 */
class DiskBenchmarkReduceFunction : public ReduceFunction {
public:
  void reduce(
    const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
    KVPairWriterInterface& writer);
};

#endif // MAPRED_DISK_BENCHMARK_REDUCE_FUNCTION_H
