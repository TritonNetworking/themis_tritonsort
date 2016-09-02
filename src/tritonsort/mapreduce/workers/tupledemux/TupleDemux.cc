#include <boost/bind.hpp>

#include "core/MemoryUtils.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/PartialKVPairWriter.h"
#include "mapreduce/common/PartitionFunctionInterface.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/buffers/ListableKVPairBuffer.h"
#include "mapreduce/functions/partition/PartitionFunctionMap.h"
#include "mapreduce/workers/tupledemux/TupleDemux.h"

TupleDemux::TupleDemux(
  uint64_t id, const std::string& name, uint64_t _nodeID,
  PartitionFunctionMap& _partitionFunctionMap,
  MemoryAllocatorInterface& _memoryAllocator, uint64_t defaultBufferSize,
  uint64_t alignmentSize, bool _serializeWithoutHeaders,
  bool _deserializeWithoutHeaders,uint32_t _fixedKeyLength,
  uint32_t _fixedValueLength, uint64_t _numDemuxes, const Params& params,
  const std::string& phaseName, bool _minutesort)
  : SingleUnitRunnable<KVPairBuffer>(id, name),
    nodeID(_nodeID),
    numDemuxes(_numDemuxes),
    serializeWithoutHeaders(_serializeWithoutHeaders),
    deserializeWithoutHeaders(_deserializeWithoutHeaders),
    fixedKeyLength(_fixedKeyLength),
    fixedValueLength(_fixedValueLength),
    minutesort(_minutesort),
    listableBufferFactory(
      *this, _memoryAllocator, defaultBufferSize, alignmentSize),
    logger(name, id),
    ldBufferGets(0),
    partitionFunctionMap(_partitionFunctionMap),
    partitionFunction(NULL),
    writer(NULL),
    partitionMap(params, phaseName),
    partitionGroup(0),
    jobID(0),
    partitionsPerGroup(0),
    partitionOffset(0),
    flushedWriter(false) {
}

void TupleDemux::run(KVPairBuffer* buffer) {
  ABORT_IF(buffer->getJobIDs().size() == 0, "Expected a job ID to be set at "
           "this point");

  // Check that this buffer should be handled by this demux.
  partitionGroup = buffer->getPartitionGroup() % numDemuxes;
  TRITONSORT_ASSERT(partitionGroup == getID(),
         "Demux %llu should only be servicing local partition group %llu but "
         "got a buffer for group %llu.", getID(), getID(), partitionGroup);

  // Extract job ID
  const std::set<uint64_t>& jobIDs = buffer->getJobIDs();
  TRITONSORT_ASSERT(jobIDs.size() == 1, "Expected buffers entering tuple demux to have "
         "exactly one job ID; this one has %llu", jobIDs.size());
  uint64_t currentJobID = *(jobIDs.begin());

  if (jobID != 0) {
    TRITONSORT_ASSERT(currentJobID == jobID, "Expect all buffers encountered by a "
           "TupleDemux to have the same job ID");
  } else {
    jobID = currentJobID;

    partitionsPerGroup = partitionMap.getNumPartitionsPerGroup(jobID);
    // The partition offset, which is the first partition ID for this group, is
    // required so data structures can be 0-indexed.
    partitionOffset = (partitionMap.getNumPartitionsPerNode(jobID) * nodeID) +
      (partitionsPerGroup * getID());
  }

  if (partitionFunction == NULL) {
    partitionFunction = partitionFunctionMap.get(jobID);
  }

  if (writer == NULL) {
    writer = new PartialKVPairWriter(
      boost::bind(&TupleDemux::emitBuffer, this, _1, _2),
      boost::bind(&TupleDemux::newBuffer, this),
      serializeWithoutHeaders, !minutesort);

    // Instruct the writer to use the partition function local to this partition
    // group.
    writer->setPartitionFunction(
      *partitionFunction, partitionsPerGroup, true, partitionGroup,
      partitionOffset);
  }

  // Iterate over the buffer, and write tuples to output buffers.
  buffer->resetIterator();
  KeyValuePair kvPair;
  kvPair.setWriteWithoutHeader(serializeWithoutHeaders);

  if (deserializeWithoutHeaders) {
    // Deserialize from a buffer without headers
    kvPair.readWithoutHeader(fixedKeyLength, fixedValueLength);
  }

  while (buffer->getNextKVPair(kvPair)) {
    writer->write(kvPair);
  }

  delete buffer;
}

void TupleDemux::teardown() {
  if (writer != NULL) {
    // Only flush if we actually received any data in this demux.
    flushedWriter = true;
    writer->flushBuffers();

    logger.logDatum("ld_buffer_gets", ldBufferGets);
    logger.logDatum("bytes_received", writer->getNumBytesWritten());
    logger.logDatum("tuples_received", writer->getNumTuplesWritten());

    delete writer;
  }
}

void TupleDemux::emitBuffer(KVPairBuffer* buffer, uint64_t partition) {
  // The writer uses 0-indexed partitions, so shift by the offset.
  buffer->setLogicalDiskID(partition + partitionOffset);

  std::set<uint64_t>::iterator iter = largePartitions.find(partition);
  if ((minutesort && !flushedWriter) || iter != largePartitions.end()) {
    // We're running minutesort and the writer did not flush buffers, so we're
    // not in teardown yet. This partition is a large partition, so emit
    // directly to the writer.
    emitWorkUnit(1, buffer);
    largePartitions.insert(partition);
  } else {
    // Emit the buffer downstream
    emitWorkUnit(buffer);
  }
}

KVPairBuffer* TupleDemux::newBuffer() {
  // Get a default-sized buffer.
  ListableKVPairBuffer* buffer = listableBufferFactory.newInstance();
  ldBufferGets++;

  // Set its fields appropriately.
  buffer->clear();

  // Set job ID.
  TRITONSORT_ASSERT(jobID > 0, "Expected job ID to be set before getting a buffer.");
  buffer->addJobID(jobID);

  return buffer;
}

BaseWorker* TupleDemux::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  // If this worker was spawned by a MultiJobDemux, want to make sure it gets
  // configured according to the parameters in use by its parent stage
  std::string parentStageName(stageName, 0, stageName.find_first_of(':'));

  uint64_t nodeID = params.get<uint64_t>("MYPEERID");

  uint64_t defaultBufferSize = params.get<uint64_t>(
    "DEFAULT_BUFFER_SIZE." + phaseName + "." + parentStageName);

  PartitionFunctionMap* partitionFunctionMap =
    dependencies.get<PartitionFunctionMap>("partition_function_map");

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  bool serializeWithoutHeaders = params.getv<bool>(
    "SERIALIZE_WITHOUT_HEADERS.%s.%s", phaseName.c_str(),
    parentStageName.c_str());
  bool deserializeWithoutHeaders = params.getv<bool>(
    "DESERIALIZE_WITHOUT_HEADERS.%s.%s", phaseName.c_str(),
    parentStageName.c_str());

  // Fetch fixed key and value length parameters even if we're not using them.
  // Usage is dictated by the serialize and deserialize parameters above.
  uint32_t fixedKeyLength = 0;
  if (params.contains("REDUCE_INPUT_FIXED_KEY_LENGTH")) {
    fixedKeyLength = params.get<uint32_t>("REDUCE_INPUT_FIXED_KEY_LENGTH");
  }

  uint32_t fixedValueLength = 0;
  if (params.contains("REDUCE_INPUT_FIXED_VALUE_LENGTH")) {
    fixedValueLength = params.get<uint32_t>("REDUCE_INPUT_FIXED_VALUE_LENGTH");
  }

  uint64_t numDemuxes = params.getv<uint64_t>(
    "NUM_WORKERS.%s.%s", phaseName.c_str(), stageName.c_str());

  bool minutesort = false;
  if (dependencies.contains<bool>("minutesort")) {
    minutesort = dependencies.get<bool>("minutesort");
  }

  TupleDemux* demux = new TupleDemux(
    id, stageName, nodeID, *partitionFunctionMap, memoryAllocator,
    defaultBufferSize, alignmentSize, serializeWithoutHeaders,
    deserializeWithoutHeaders, fixedKeyLength, fixedValueLength, numDemuxes,
    params, phaseName, minutesort);

  return demux;
}
