#ifndef MAPRED_MAPPER_H
#define MAPRED_MAPPER_H

#include "core/SingleUnitRunnable.h"
#include "core/constants.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/TupleSizeHistogramLoggingStrategy.h"

class CoordinatorClientInterface;
class MapFunction;
class PartitionFunctionMap;
class RecordFilterMap;
class TupleSizeLoggingStrategyInterface;
class KVPairWriterInterface;

/**
   A worker responsible for running a map function (see MapFunction) on a
   buffer of key/value pairs
 */
class Mapper : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker ID for this worker within its parent stage

     \param name the name of the stage for which the worker is working

     \param myNodeID the node ID of the worker on which this mapper is running

     \param inputTupleSampleRate the number of tuples to skip between samples

     \param memoryAllocator the memory allocator that this worker will use to
     allocate memory for buffers

     \param minBufferSize the smallest buffer allocated by this worker

     \param alignmentSize if non-zero, buffers will be aligned to be a multiple
     of this size

     \param serializeWithoutHeaders if true, write tuples to map output buffers
     without headers

     \param partitionFunctionMap a map from job ID to partition function

     \param recordFilterMap a map from job ID to record filter

     \param params a Params object used to configure map functions

     \param shuffle if true this is a shuffle mapper in phase zero

     \param reservoirSample if true this is a reservoir sample mapper in phase
     zero

     \param numNodes the number of nodes
   */
  Mapper(
    uint64_t id, const std::string& name, uint64_t myNodeID,
    uint64_t inputTupleSampleRate, MemoryAllocatorInterface& memoryAllocator,
    uint64_t minBufferSize, uint64_t alignmentSize,
    bool serializeWithoutHeaders, CoordinatorClientInterface& coordinatorClient,
    const Params& params, PartitionFunctionMap& partitionFunctionMap,
    RecordFilterMap& recordFilterMap, bool shuffle, bool reservoirSample,
    uint64_t numNodes);

  /// Destructor
  virtual ~Mapper();

  /**
     \sa ResourceMonitorClient::resourceMonitorOutput
   */
  void resourceMonitorOutput(Json::Value& obj);

  /// Apply the map function to each tuple in the given buffer
  /**
     Tuples emitted by the map function will be buffered and emitted to
     downstream stages as buffers fill for sending. If the buffer of tuples
     emitted is destined for the same node on which this Mapper is running, the
     buffer will be passed directly to the local receiver-side pipeline without
     being sent via sockets.

     \param buffer the buffer whose tuples are to be mapped
   */
  void run(KVPairBuffer* buffer);

  void map(KVPairBuffer* buffer, uint64_t bufferJobID);

  /// Emit any partially-full buffers of map output tuples and log statistics
  void teardown();

  KVPairBuffer* getBufferForWriter(uint64_t minimumCapacity);

  void putBufferFromWriter(KVPairBuffer* buffer);

  void emitBufferFromWriter(KVPairBuffer* buffer, uint64_t bufferNumber);

  void logSampleOutput(KeyValuePair& kvPair);

  void logWriteStats(uint64_t numBytesWritten, uint64_t numTuplesWritten);
private:
  DISALLOW_COPY_AND_ASSIGN(Mapper);

  const uint64_t inputTupleSampleRate;
  const uint64_t myNodeID;
  const uint64_t minBufferSize;
  const bool serializeWithoutHeaders;
  const Params& params;

  const bool shuffle;
  const bool reservoirSample;
  const uint64_t numNodes;

  PartitionFunctionMap& partitionFunctionMap;
  RecordFilterMap& recordFilterMap;

  KVPairBufferFactory bufferFactory;
  StatLogger logger;

  MapFunction* mapFunction;
  KVPairWriterInterface* writer;

  uint64_t jobID;
  TupleSizeHistogramLoggingStrategy mapInputLoggingStrategy;
  TupleSizeHistogramLoggingStrategy mapOutputLoggingStrategy;

  uint64_t bytesIn;
  uint64_t tuplesIn;
  uint64_t bytesOut;

  CoordinatorClientInterface& coordinatorClient;
};

#endif //MAPRED_MAPPER_H
