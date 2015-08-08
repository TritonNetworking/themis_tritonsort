#include <boost/bind.hpp>
#include <limits>

#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/DefaultKVPairWriteStrategy.h"
#include "mapreduce/common/FastKVPairWriter.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/KVPairWriter.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/PartitionFunctionInterface.h"
#include "mapreduce/common/ReservoirSamplingKVPairWriter.h"
#include "mapreduce/common/filter/RecordFilterMap.h"
#include "mapreduce/common/filter/RecordFilterMap.h"
#include "mapreduce/functions/map/MapFunction.h"
#include "mapreduce/functions/map/MapFunctionFactory.h"
#include "mapreduce/functions/map/PassThroughMapFunction.h"
#include "mapreduce/functions/partition/PartitionFunctionMap.h"
#include "mapreduce/functions/partition/RandomNodePartitionFunction.h"
#include "mapreduce/workers/mapper/Mapper.h"

Mapper::Mapper(
  uint64_t id, const std::string& name,
  uint64_t _myNodeID, uint64_t _inputTupleSampleRate,
  MemoryAllocatorInterface& memoryAllocator, uint64_t _minBufferSize,
  uint64_t alignmentSize, bool _serializeWithoutHeaders,
  CoordinatorClientInterface& _coordinatorClient, const Params& _params,
  PartitionFunctionMap& _partitionFunctionMap,
  RecordFilterMap& _recordFilterMap, bool _shuffle, bool _reservoirSample,
  uint64_t _numNodes)
  : SingleUnitRunnable<KVPairBuffer>(id, name),
    inputTupleSampleRate(_inputTupleSampleRate),
    myNodeID(_myNodeID),
    minBufferSize(_minBufferSize),
    serializeWithoutHeaders(_serializeWithoutHeaders),
    params(_params),
    shuffle(_shuffle),
    reservoirSample(_reservoirSample),
    numNodes(_numNodes),
    partitionFunctionMap(_partitionFunctionMap),
    recordFilterMap(_recordFilterMap),
    bufferFactory(*this, memoryAllocator, _minBufferSize, alignmentSize),
    logger(name, id),
    mapFunction(NULL),
    writer(NULL),
    jobID(0),
    mapInputLoggingStrategy("map_input", _params, true),
    mapOutputLoggingStrategy("map_output", _params, false),
    bytesIn(0),
    tuplesIn(0),
    bytesOut(0),
    coordinatorClient(_coordinatorClient) {
  mapInputLoggingStrategy.registerStats(logger);
  mapOutputLoggingStrategy.registerStats(logger);
}

Mapper::~Mapper() {
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

void Mapper::resourceMonitorOutput(Json::Value& obj) {
  SingleUnitRunnable<KVPairBuffer>::resourceMonitorOutput(obj);
  obj["bytes_in"] = Json::UInt64(bytesIn);
  obj["bytes_out"] = Json::UInt64(bytesOut);
  obj["tuples_in"] = Json::UInt64(tuplesIn);
}

void Mapper::map(KVPairBuffer* buffer, uint64_t bufferJobID) {
  if (jobID == 0) {
    jobID = bufferJobID;
  } else {
    ASSERT(jobID == bufferJobID, "Expected all buffers entering this mapper to "
           "have the same job ID");
  }

  if (mapFunction == NULL) {
    JobInfo* jobInfo = coordinatorClient.getJobInfo(jobID);

    if (shuffle || reservoirSample) {
      // Phase zero shuffle and reservoir sampling mappers must use identity
      // map function.
      mapFunction = new PassThroughMapFunction();
    } else {
      mapFunction = MapFunctionFactory::getNewMapFunctionInstance(
        jobInfo->mapFunction, params);
    }

    const PartitionFunctionInterface* partitionFunction =
      partitionFunctionMap.get(jobID);
    ASSERT(partitionFunction != NULL, "Expected partition function map to "
           "contain a partition function for job %llu but it didn't", jobID);

    const RecordFilter* recordFilter = recordFilterMap.get(jobID);

    uint64_t outputTupleSampleRate = params.get<uint64_t>(
      "MAP_OUTPUT_TUPLE_SAMPLE_RATE");

    if (reservoirSample) {
      KVPairWriteStrategyInterface* writeStrategy =
        new DefaultKVPairWriteStrategy();
      /// \TODO(MC): logWriteStats has the wrong number of arguments and will
      /// log incorrect information. We really shouldn't have two functions with
      /// the same name and different argument counts.
      writer = new ReservoirSamplingKVPairWriter(
        outputTupleSampleRate, recordFilter,
        boost::bind(&Mapper::emitBufferFromWriter, this, _1, _2),
        boost::bind(&Mapper::getBufferForWriter, this, _1),
        boost::bind(&Mapper::putBufferFromWriter, this, _1),
        boost::bind(&Mapper::logSampleOutput, this, _1),
        boost::bind(&Mapper::logWriteStats, this, _1, _3),
        writeStrategy);
    } else {
      if (recordFilter != NULL) {
        writer = new KVPairWriter(
          partitionFunction->numGlobalPartitions(), *partitionFunction, NULL,
          outputTupleSampleRate, recordFilter,
          boost::bind(&Mapper::emitBufferFromWriter, this, _1, _2),
          boost::bind(&Mapper::getBufferForWriter, this, _1),
          boost::bind(&Mapper::putBufferFromWriter, this, _1),
          boost::bind(&Mapper::logSampleOutput, this, _1),
          boost::bind(&Mapper::logWriteStats, this, _1, _2));
      } else {
        bool garbageCollect = false;
        PartitionFunctionInterface* mapperPartitionFunction =
          const_cast<PartitionFunctionInterface*>(partitionFunction);
        if (shuffle) {
          // This is a mapper in the shuffle phase of phase zero
          mapperPartitionFunction =
            new RandomNodePartitionFunction(numNodes);
          garbageCollect = true;
        }

        writer = new FastKVPairWriter(
          mapperPartitionFunction, outputTupleSampleRate,
          boost::bind(&Mapper::emitBufferFromWriter, this, _1, _2),
          boost::bind(&Mapper::getBufferForWriter, this, _1),
          boost::bind(&Mapper::putBufferFromWriter, this, _1),
          boost::bind(&Mapper::logSampleOutput, this, _1),
          boost::bind(&Mapper::logWriteStats, this, _1, _2), garbageCollect,
          serializeWithoutHeaders);
      }
    }

    delete jobInfo;
  } else {
    ABORT_IF(bufferJobID != jobID, "Currently Mappers only support using one "
             "map function at a time");
  }

  // Configure the map function to properly handle tuples from this buffer.
  mapFunction->configure(buffer);

  // Update statistics
  bytesIn += buffer->getCurrentSize();

  buffer->resetIterator();
  KeyValuePair kvPair;

  while (buffer->getNextKVPair(kvPair)) {
    if (tuplesIn % inputTupleSampleRate == 0) {
      mapInputLoggingStrategy.logTuple(logger, kvPair);
    }

    mapFunction->map(kvPair, *writer);
    tuplesIn++;
  }
}

void Mapper::run(KVPairBuffer* buffer) {
  const std::set<uint64_t>& jobIDs = buffer->getJobIDs();
  ABORT_IF(jobIDs.size() != 1, "Expect a mapper buffer to be "
           "associated with a single job ID; this buffer is associated "
           "with %llu", jobIDs.size());

  uint64_t bufferJobID = *(jobIDs.begin());

  map(buffer, bufferJobID);

  delete buffer;
}

KVPairBuffer* Mapper::getBufferForWriter(uint64_t minimumCapacity) {
  KVPairBuffer* buffer = NULL;

  if (minimumCapacity > minBufferSize) {
    buffer = bufferFactory.newInstance(minimumCapacity);
  } else {
    buffer = bufferFactory.newInstance();
  }

  buffer->clear();

  return buffer;
}

void Mapper::putBufferFromWriter(KVPairBuffer* buffer) {
  delete buffer;
}

void Mapper::emitBufferFromWriter(KVPairBuffer* buffer, uint64_t bufferNumber) {
  uint64_t bufferSize = buffer->getCurrentSize();
  bytesOut += bufferSize;

  buffer->addJobID(jobID);
  if (shuffle) {
    buffer->setNode(bufferNumber);
  } else {
    buffer->setPartitionGroup(bufferNumber);
  }

  // Let the queueing policy figure out where to put this buffer.
  emitWorkUnit(buffer);
}

void Mapper::logSampleOutput(KeyValuePair& kvPair) {
  mapOutputLoggingStrategy.logTuple(logger, kvPair);
}

void Mapper::logWriteStats(
  uint64_t numBytesWritten, uint64_t numTuplesWritten) {
  logger.logDatum("total_tuples_out", numTuplesWritten);
  logger.logDatum("total_bytes_out", numBytesWritten);
}


void Mapper::teardown() {
  if (mapFunction != NULL && writer != NULL) {
    mapFunction->teardown(*writer);
  }

  if (writer != NULL) {
    writer->flushBuffers();
  }

  // Log statistics
  logger.logDatum("total_tuples_in", tuplesIn);
  logger.logDatum("total_bytes_in", bytesIn);
  logger.logDatum("total_bytes_out", bytesOut);
}

BaseWorker* Mapper::newInstance(
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

  uint64_t myNodeID = params.get<uint64_t>("MYPEERID");
  uint64_t inputTupleSampleRate = params.get<uint64_t>(
    "MAP_INPUT_TUPLE_SAMPLE_RATE");

  uint64_t minBufferSize = params.getv<uint64_t>(
    "DEFAULT_BUFFER_SIZE.%s.%s",  phaseName.c_str(), parentStageName.c_str());

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  bool serializeWithoutHeaders = params.getv<bool>(
    "SERIALIZE_WITHOUT_HEADERS.%s.%s", phaseName.c_str(),
    parentStageName.c_str());

  bool shuffle = false;
  if (dependencies.contains<bool>(parentStageName.c_str(), "shuffle")) {
    shuffle = dependencies.get<bool>(parentStageName.c_str(), "shuffle");
  }

  bool reservoirSample = false;
  if (dependencies.contains<bool>(
        parentStageName.c_str(), "reservoir_sample")) {
    reservoirSample = dependencies.get<bool>(
      parentStageName.c_str(), "reservoir_sample");
  }

  ABORT_IF(shuffle && reservoirSample, "Cannot both be a shuffle mapper and a "
           "reservoir sampling mapper");

  uint64_t numNodes = params.get<uint64_t>("NUM_PEERS");

  Mapper* mapper = new Mapper(
    id, stageName, myNodeID, inputTupleSampleRate, memoryAllocator,
    minBufferSize, alignmentSize, serializeWithoutHeaders, *coordinatorClient,
    params, *partitionFunctionMap, *recordFilterMap, shuffle, reservoirSample,
    numNodes);

  return mapper;
}
