#ifndef MAPRED_KEYS_ONLY_MAPPER_H
#define MAPRED_KEYS_ONLY_MAPPER_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/SampleMetadataKVPairBufferFactory.h"
#include "mapreduce/common/TupleSizeHistogramLoggingStrategy.h"

class CoordinatorClientInterface;
class KVPairBuffer;
class KVPairWriterInterface;
class MapFunction;
class PartitionFunctionMap;
class RecordFilterMap;
class SampleMetadataKVPairBuffer;

/**
   A variant of the Mapper stage that only emits keys and stops mapping tuples
   after it fills and emits a single buffer
 */
class KeysOnlySingleBufferMapper : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker ID for the worker

     \param name the name of the stage for which the worker is working

     \param inputTupleSampleRate how many tuples to skip between samples

     \param memoryAllocator the memory allocator that this worker will use to
     allocate buffers

     \param keyBufferSize key buffers allocated by this worker will be a
     multiple of this size

     \param alignmentSize if non-zero, buffers will be aligned to be a multiple
     of this size

     \param coordinatorClient coordinator client that the worker will use to
     get the map function for its job

     \param params a Params object that will be used to initialize the map
     function

     \param partitionFunctionMap a map from job IDs to partition functions

     \param recordFilterMap a map from job IDs to record filters
   */
  KeysOnlySingleBufferMapper(
    uint64_t id, const std::string& name,
    uint64_t inputTupleSampleRate, MemoryAllocatorInterface& memoryAllocator,
    uint64_t keyBufferSize, uint64_t alignmentSize,
    CoordinatorClientInterface& coordinatorClient, const Params& params,
    PartitionFunctionMap& partitionFunctionMap,
    RecordFilterMap& recordFilterMap);

  /// Destructor
  /**
     Deletes the map function passed at construction
   */
  virtual ~KeysOnlySingleBufferMapper();

  /**
     \sa ResourceMonitorClient::resourceMonitorOutput
   */
  void resourceMonitorOutput(Json::Value& obj);

  /// Map tuples until you emit a buffer full of keys
  /**
     Once a buffer full of keys has been emitted, all buffers passed to this
     method will be immediately returned to their pools

     \param buffer the buffer whose tuples will be mapped
   */
  void run(KVPairBuffer* buffer);

  /// Flushes buffers from the writer and logs statistics
  void teardown();

  /// Retrieves a buffer from GET_POOL for the writer
  KVPairBuffer* getBufferForWriter(uint64_t minCapacity);

  /// Returns one of the writer's unemitted buffers to GET_POOL
  /**
     \param buffer the buffer to return to GET_POOL
   */
  void putBufferFromWriter(KVPairBuffer* buffer);

  /// Pass the buffer to the next tracker and stop processing additional tuples
  /**
     \param buffer the buffer to pass to the next tracker

     \param bufferNumber the buffer number for the buffer; should be 0 because
     we are only writing mapped keys to a single buffer
   */
  void emitBufferFromWriter(KVPairBuffer* buffer, uint64_t bufferNumber);

  void logSampleOutput(KeyValuePair& kvPair);

  void logWriteStats(
    uint64_t numTuplesWritten, uint64_t numTuplesSeen,
    uint64_t numBytesWritten, uint64_t numBytesSeen);

private:
  const uint64_t inputTupleSampleRate;
  const uint64_t keyBufferSize;
  const Params& params;

  PartitionFunctionMap& partitionFunctionMap;
  RecordFilterMap& recordFilterMap;

  SampleMetadataKVPairBufferFactory keyBufferFactory;

  uint64_t tuplesIn;
  uint64_t bytesIn;
  uint64_t bytesOut;

  StatLogger logger;

  // Set to false when we should stop mapping tuples
  bool keepMappingTuples;

  MapFunction* mapFunction;
  KVPairWriterInterface* writer;

  uint64_t jobID;

  CoordinatorClientInterface& coordinatorClient;

  TupleSizeHistogramLoggingStrategy mapperInputLoggingStrategy;
  TupleSizeHistogramLoggingStrategy mapperOutputLoggingStrategy;
};

#endif // MAPRED_KEYS_ONLY_MAPPER_H
