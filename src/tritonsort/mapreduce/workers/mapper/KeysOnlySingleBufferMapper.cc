#include <boost/bind.hpp>

#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/HashedPhaseZeroKVPairWriteStrategy.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/KVPairWriter.h"
#include "mapreduce/common/PartitionFunctionInterface.h"
#include "mapreduce/common/PhaseZeroKVPairWriteStrategy.h"
#include "mapreduce/common/PhaseZeroSampleMetadata.h"
#include "mapreduce/common/ReservoirSamplingKVPairWriter.h"
#include "mapreduce/common/buffers/SampleMetadataKVPairBuffer.h"
#include "mapreduce/common/filter/RecordFilterMap.h"
#include "mapreduce/functions/map/MapFunction.h"
#include "mapreduce/functions/map/MapFunctionFactory.h"
#include "mapreduce/functions/partition/PartitionFunctionMap.h"
#include "mapreduce/functions/partition/SinglePartitionMergingPartitionFunction.h"
#include "mapreduce/workers/mapper/KeysOnlySingleBufferMapper.h"

KeysOnlySingleBufferMapper::KeysOnlySingleBufferMapper(
  uint64_t id, const std::string& name,
  uint64_t _inputTupleSampleRate, MemoryAllocatorInterface& memoryAllocator,
  uint64_t _keyBufferSize, uint64_t alignmentSize,
  CoordinatorClientInterface& _coordinatorClient, const Params& _params,
  PartitionFunctionMap& _partitionFunctionMap,
  RecordFilterMap& _recordFilterMap)
  : SingleUnitRunnable<KVPairBuffer>(id, name),
    inputTupleSampleRate(_inputTupleSampleRate),
    keyBufferSize(_keyBufferSize),
    params(_params),
    partitionFunctionMap(_partitionFunctionMap),
    recordFilterMap(_recordFilterMap),
    keyBufferFactory(*this, memoryAllocator, _keyBufferSize, alignmentSize),
    tuplesIn(0),
    bytesIn(0),
    bytesOut(0),
    logger(name, id),
    keepMappingTuples(true),
    mapFunction(NULL),
    writer(NULL),
    jobID(0),
    coordinatorClient(_coordinatorClient),
    mapperInputLoggingStrategy("map_input", _params, true),
    mapperOutputLoggingStrategy("map_output", _params, false) {

  mapperInputLoggingStrategy.registerStats(logger);
  mapperOutputLoggingStrategy.registerStats(logger);
}

KeysOnlySingleBufferMapper::~KeysOnlySingleBufferMapper() {
  if (mapFunction != NULL) {
    delete mapFunction;
    mapFunction = NULL;
  }

  if (writer != NULL) {
    delete writer;
    writer = NULL;
  }

  delete &coordinatorClient;
}

void KeysOnlySingleBufferMapper::resourceMonitorOutput(Json::Value& obj) {
  SingleUnitRunnable<KVPairBuffer>::resourceMonitorOutput(obj);
  obj["bytes_in"] = Json::UInt64(bytesIn);
  obj["bytes_out"] = Json::UInt64(bytesOut);
  obj["tuples_in"] = Json::UInt64(tuplesIn);
}

void KeysOnlySingleBufferMapper::run(KVPairBuffer* buffer) {
  const std::set<uint64_t>& jobIDs = buffer->getJobIDs();
  ABORT_IF(jobIDs.size() != 1, "Expect a sampled buffer to be "
           "associated with a single job ID; this buffer is associated "
           "with %llu", jobIDs.size());

  uint64_t bufferJobID = *(jobIDs.begin());

  if (mapFunction == NULL) {
    jobID = bufferJobID;

    JobInfo* jobInfo = coordinatorClient.getJobInfo(jobID);

    // Construct the appropriate map function for this job
    mapFunction = MapFunctionFactory::getNewMapFunctionInstance(
      jobInfo->mapFunction, params);

    delete jobInfo;

    // Construct the appropriate KVPairWriter based on the job's partition
    // function.
    const PartitionFunctionInterface* partitionFunction =
      partitionFunctionMap.get(jobID);
    const RecordFilter* recordFilter = recordFilterMap.get(jobID);

    KVPairWriteStrategyInterface* writeStrategy = NULL;
    if (partitionFunction->hashesKeys()) {
      writeStrategy = new HashedPhaseZeroKVPairWriteStrategy();
    } else {
      writeStrategy = new PhaseZeroKVPairWriteStrategy();
    }

    // Always use reservoir sampling:
    writer = new ReservoirSamplingKVPairWriter(
      params.get<uint64_t>("MAP_OUTPUT_TUPLE_SAMPLE_RATE"), recordFilter,
      boost::bind(
        &KeysOnlySingleBufferMapper::emitBufferFromWriter, this, _1, _2),
      boost::bind(&KeysOnlySingleBufferMapper::getBufferForWriter, this, _1),
      boost::bind(&KeysOnlySingleBufferMapper::putBufferFromWriter, this, _1),
      boost::bind(&KeysOnlySingleBufferMapper::logSampleOutput, this, _1),
      boost::bind(
        &KeysOnlySingleBufferMapper::logWriteStats, this, _1, _2, _3, _4),
      writeStrategy);
  } else {
    // Make sure we got the right job ID.
    ABORT_IF(bufferJobID != jobID, "Currently KeysOnlySingleBufferMappers only "
             "support using one map function at a time");
  }

  // Configure the map function to properly handle tuples from this buffer.
  mapFunction->configure(buffer);

  buffer->resetIterator();

  KeyValuePair kvPair;

  while (keepMappingTuples && buffer->getNextKVPair(kvPair)) {
    bytesIn += kvPair.getReadSize();

    if (tuplesIn % inputTupleSampleRate == 0) {
      mapperInputLoggingStrategy.logTuple(logger, kvPair);
    }

    mapFunction->map(kvPair, *writer);
    tuplesIn++;
  }

  delete buffer;
}

void KeysOnlySingleBufferMapper::teardown() {
  if (mapFunction != NULL) {
    mapFunction->teardown(*writer);
  }

  if (writer != NULL) {
    writer->flushBuffers();
  }

  uint64_t totalBytesWritten = 0;

  if (writer != NULL) {
    totalBytesWritten = writer->getNumBytesWritten();
  }

  logger.logDatum("total_tuples_in", tuplesIn);
  logger.logDatum("total_bytes_in", bytesIn);
  logger.logDatum("total_bytes_out", totalBytesWritten);
}

KVPairBuffer* KeysOnlySingleBufferMapper::getBufferForWriter(
  uint64_t minCapacity) {
  KVPairBuffer* buffer = NULL;

  if (minCapacity > keyBufferSize) {
    buffer = keyBufferFactory.newInstance(minCapacity);
  } else {
    buffer = keyBufferFactory.newInstance();
  }

  buffer->clear();
  return buffer;
}

void KeysOnlySingleBufferMapper::putBufferFromWriter(KVPairBuffer* buffer) {
  delete buffer;
}

void KeysOnlySingleBufferMapper::emitBufferFromWriter(
  KVPairBuffer* buffer, uint64_t bufferNumber) {

  if (keepMappingTuples) {
    SampleMetadataKVPairBuffer* bufferWithMetadata =
      dynamic_cast<SampleMetadataKVPairBuffer*>(buffer);
    ABORT_IF(bufferWithMetadata == NULL, "Dynamic cast to "
             "SampleMetadataKVPairBuffer failed; mapper expects to receive "
             "SampleMetadataKVPairBuffers from the KVPairWriter");

    PhaseZeroSampleMetadata metadata(
      jobID, tuplesIn, bytesIn, writer->getNumTuplesWritten(),
      writer->getNumBytesWritten(), writer->getNumBytesCallerTriedToWrite());
    bufferWithMetadata->setSampleMetadata(metadata);

    keepMappingTuples = false;
    bytesOut += buffer->getCurrentSize();

    buffer->addJobID(jobID);

    // Before we emit this buffer, upload our statistics to the coordinator.
    coordinatorClient.uploadSampleStatistics(
      jobID, metadata.getBytesIn(), metadata.getBytesMapped());

    emitWorkUnit(buffer);
  } else {
    // If we've got a partially full second buffer (which is possible given how
    // the KVPairWriter asks for buffers), just put it back; if we emit it, the
    // sorter will deadlock getting an output buffer
    delete buffer;
  }
}

void KeysOnlySingleBufferMapper::logSampleOutput(KeyValuePair& kvPair) {
  mapperOutputLoggingStrategy.logTuple(logger, kvPair);
}

void KeysOnlySingleBufferMapper::logWriteStats(
  uint64_t numBytesWritten, uint64_t numBytesSeen, uint64_t numTuplesWritten,
  uint64_t numTuplesSeen) {

  logger.logDatum("total_tuples_out", numTuplesWritten);
  logger.logDatum("total_tuples_mapped", numTuplesSeen);
  logger.logDatum("total_bytes_out", numBytesWritten);
  logger.logDatum("total_bytes_mapped", numBytesSeen);
}

BaseWorker* KeysOnlySingleBufferMapper::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  // If this worker was spawned by a MultiJobMapper, want to make sure it gets
  // configured according to the parameters in use by its parent stage
  std::string parentStageName(stageName, 0, stageName.find_first_of(':'));

  CoordinatorClientInterface* coordinatorClient =
    CoordinatorClientFactory::newCoordinatorClient(
      params, phaseName, parentStageName, id);

  PartitionFunctionMap* partitionFunctionMap =
    dependencies.get<PartitionFunctionMap>("partition_function_map");

  RecordFilterMap* recordFilterMap = dependencies.get<RecordFilterMap>(
    "record_filter_map");

  uint64_t inputTupleSampleRate = params.get<uint64_t>(
    "MAP_INPUT_TUPLE_SAMPLE_RATE");

  uint64_t keyBufferSize = params.getv<uint64_t>(
    "DEFAULT_BUFFER_SIZE.%s.%s", phaseName.c_str(), parentStageName.c_str());

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  KeysOnlySingleBufferMapper* mapper = new KeysOnlySingleBufferMapper(
    id, stageName, inputTupleSampleRate, memoryAllocator, keyBufferSize,
    alignmentSize, *coordinatorClient, params, *partitionFunctionMap,
    *recordFilterMap);

  return mapper;
}
