#include <boost/bind.hpp>
#include <limits>

#include "core/Comparison.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/PartialKVPairWriter.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/functions/reduce/ReduceFunction.h"
#include "mapreduce/functions/reduce/ReduceFunctionFactory.h"
#include "mapreduce/workers/reducer/ReduceKVPairIterator.h"
#include "mapreduce/workers/reducer/Reducer.h"

Reducer::Reducer(
  uint64_t id, const std::string& name, uint64_t _nodeID,
  uint64_t alignmentSize, MemoryAllocatorInterface& memoryAllocator,
  uint64_t defaultBufferSize, CoordinatorClientInterface& _coordinatorClient,
  const Params& _params, uint64_t _outputReplicationLevel,
  const std::string& phaseName, uint64_t _numNodes)
  : SingleUnitRunnable(id, name),
    nodeID(_nodeID),
    params(_params),
    outputReplicationLevel(_outputReplicationLevel),
    numNodes(_numNodes),
    writer(NULL),
    reduceFunction(NULL),
    bufferFactory(
      *this, memoryAllocator, defaultBufferSize, alignmentSize),
    logger(name, id),
    jobID(0),
    partitionMap(params, phaseName),
    coordinatorClient(_coordinatorClient) {

  bytesIn = 0;
  bytesOut = 0;

  logicalDiskUID = std::numeric_limits<uint64_t>::max();
}

Reducer::~Reducer() {
  if (reduceFunction != NULL) {
    delete reduceFunction;
  }

  if (writer != NULL) {
    delete writer;
  }

  delete &coordinatorClient;
}

void Reducer::run(KVPairBuffer* buffer) {
  const std::set<uint64_t>& jobIDs = buffer->getJobIDs();
  ABORT_IF(jobIDs.size() != 1, "Expect a reducer buffer to be "
           "associated with a single job ID; this buffer is associated "
           "with %llu", jobIDs.size());

  uint64_t bufferJobID = *(jobIDs.begin());

  if (jobID == 0) {
    jobID = bufferJobID;
  } else {
    ASSERT(jobID == bufferJobID, "Expected all buffers entering this reducer "
           "to have the same job ID");
  }

  if (reduceFunction == NULL) {
    JobInfo* jobInfo = coordinatorClient.getJobInfo(jobID);

    reduceFunction = ReduceFunctionFactory::getNewReduceFunctionInstance(
      jobInfo->reduceFunction, params);
    delete jobInfo;
  } else {
    ABORT_IF(bufferJobID != jobID, "Currently Reducers only support using one "
             "reduce function at a time");
  }

  // Update statistics
  bytesIn += buffer->getCurrentSize();

  // If the buffer is empty, return it to the pool immediately
  if (buffer->getCurrentSize() == 0) {
    delete buffer;
    return;
  }

  // Set the logical disk ID in the KVPairWriter
  logicalDiskUID = buffer->getLogicalDiskID();

  // If the reduce function is stateful, it may need to be configured.
  reduceFunction->configure();

  ReduceKVPairIterator iterator(*buffer);

  const uint8_t* key = NULL;
  uint32_t keyLength = 0;

  while (iterator.startNextKey(key, keyLength)) {
    reduceFunction->reduce(key, keyLength, iterator, *writer);
  }

  // Delete the buffer to reclaim its memory
  delete buffer;

  // Flush write buffers to the writer.
  if (writer != NULL) {
    writer->flushBuffers();
  }
}

void Reducer::teardown() {
  if (writer != NULL) {
    writer->flushBuffers();
  }

  // Log statistics
  logger.logDatum("total_bytes_in", bytesIn);
  logger.logDatum("total_bytes_out", bytesOut);
}

KVPairBuffer* Reducer::newBuffer() {
  KVPairBuffer* buffer = bufferFactory.newInstance();

  buffer->clear();
  buffer->setLogicalDiskID(logicalDiskUID);

  return buffer;
}

void Reducer::emitBuffer(KVPairBuffer* buffer, uint64_t unused) {
  // Ignore second parameter - it's used in the mapper.

  /// \TODO - Why is this cast here???
  KVPairBuffer* kvPairBuffer = dynamic_cast<KVPairBuffer*>(buffer);
  ASSERT(kvPairBuffer != NULL, "Should only be emitting KVPairBuffers, since "
         "that's what newBuffer is creating; something strange is going on");

  bytesOut += kvPairBuffer->getCurrentSize();

  ASSERT(jobID > 0, "Encountered unset job ID when emitting a buffer from the "
         "reducer");
  kvPairBuffer->addJobID(jobID);

  uint64_t firstPartition = partitionMap.getFirstLocalPartition(jobID);
  uint64_t partitionID = kvPairBuffer->getLogicalDiskID();
  // Create replica buffers if replication level > 1.
  for (uint64_t i = 1; i < outputReplicationLevel; i++) {
    KVPairBuffer* replicaBuffer = bufferFactory.newInstance();
    replicaBuffer->append(
      kvPairBuffer->getRawBuffer(), kvPairBuffer->getCurrentSize());
    replicaBuffer->setLogicalDiskID(partitionID);
    replicaBuffer->addJobID(jobID);

    // Send to consecutive nodes, but round-robin partition files.
    uint64_t replicaOffset = (partitionID - firstPartition) % (numNodes - 1);
    // If using more than one replica, shift again.
    replicaBuffer->setNode(replicaOffset + (i - 1));

    // Replica pipeline is on the second output tracker.
    emitWorkUnit(1, replicaBuffer);
  }

  emitWorkUnit(kvPairBuffer);
}

void Reducer::setWriter(KVPairWriterInterface* _writer) {
  writer = _writer;
}

BaseWorker* Reducer::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  // If this worker was spawned by a MultiJobDemux, want to make sure it gets
  // configured according to the parameters in use by its parent stage
  std::string parentStageName(stageName, 0, stageName.find_first_of(':'));

  CoordinatorClientInterface* coordinatorClient =
    CoordinatorClientFactory::newCoordinatorClient(
      params, phaseName, parentStageName, id);

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t nodeID = params.get<uint64_t>("MYPEERID");

  uint64_t defaultBufferSize = params.get<uint64_t>(
    "DEFAULT_BUFFER_SIZE." + phaseName + "." + parentStageName);

  uint64_t replicationLevel = params.get<uint64_t>("OUTPUT_REPLICATION_LEVEL");

  uint64_t numNodes = params.get<uint64_t>("NUM_PEERS");

  Reducer* reducer = new Reducer(
    id, stageName, nodeID, alignmentSize, memoryAllocator, defaultBufferSize,
    *coordinatorClient, params, replicationLevel, phaseName, numNodes);

  bool serializeWithoutHeaders = params.getv<bool>(
    "SERIALIZE_WITHOUT_HEADERS.%s.%s", phaseName.c_str(),
    parentStageName.c_str());

  // Use a writer that can write partial tuples in order to exactly fill
  // reducer-output buffers. Use only 1 partition since we're operating on
  // entire partitions at a time.
  PartialKVPairWriter* partialKVPairWriter = new PartialKVPairWriter(
    boost::bind(&Reducer::emitBuffer, reducer, _1, _2),
    boost::bind(&Reducer::newBuffer, reducer),
    serializeWithoutHeaders);

  reducer->setWriter(partialKVPairWriter);

  return reducer;
}
