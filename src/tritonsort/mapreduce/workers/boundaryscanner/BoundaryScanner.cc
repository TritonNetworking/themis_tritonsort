#include <boost/bind.hpp>

#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/Utils.h"
#include "mapreduce/functions/partition/BoundaryScannerPartitionFunction.h"
#include "mapreduce/workers/boundaryscanner/BoundaryScanner.h"

BoundaryScanner::BoundaryScanner(
  uint64_t id, const std::string& name, const std::string& _phaseName,
  MemoryAllocatorInterface& memoryAllocator, uint64_t defaultBufferSize,
  uint64_t alignmentSize, const Params& _params, uint64_t _numNodes,
  bool _isCoordinatorNode, uint64_t _coordinatorNodeID)
  : SingleUnitRunnable<KVPairBuffer>(id, name),
    isCoordinatorNode(_isCoordinatorNode),
    coordinatorNodeID(_coordinatorNodeID),
    params(_params),
    phaseName(_phaseName),
    numNodes(_numNodes),
    jobID(0),
    bufferFactory(*this, memoryAllocator, defaultBufferSize, alignmentSize),
    sampleMetadata(NULL),
    numPartitions(0),
    bytesPerPartition(0),
    remainder(0),
    numPartitionsPicked(0),
    nextPartitionBytes(0),
    bytesScanned(0),
    tuplesScanned(0) {
}

BoundaryScanner::~BoundaryScanner() {
  if (sampleMetadata != NULL) {
    delete sampleMetadata;
    sampleMetadata = NULL;
  }
}

void BoundaryScanner::run(KVPairBuffer* buffer) {
  if (sampleMetadata == NULL) {
    // Get the job ID from the first buffer.
    uint64_t numJobIDs = buffer->getJobIDs().size();
    ABORT_IF(numJobIDs != 1, "Expected one job ID but got %llu", numJobIDs);
    jobID = *(buffer->getJobIDs().begin());

    CoordinatorClientInterface* coordinatorClient =
      CoordinatorClientFactory::newCoordinatorClient(
        params, phaseName, getName(), getID());

    // Compute the number of partitions.
    if (isCoordinatorNode) {
      uint64_t totalInputBytes;
      uint64_t totalIntermediateBytes;
      // Block until we have sample statistics from all nodes.
      coordinatorClient->getSampleStatisticsSums(
        jobID, numNodes, totalInputBytes, totalIntermediateBytes);

      double intermediateToInputRatio = totalIntermediateBytes /
        ((double) totalInputBytes);
      StatusPrinter::add("Total # input bytes: %llu", totalInputBytes);
      StatusPrinter::add("Estimated intermediate:input data size ratio: %.2f:1",
                         intermediateToInputRatio);

      numPartitions = setNumPartitions(jobID, intermediateToInputRatio, params);
    } else {
      // If we're not the coordinator, let the coordinator figure it out for us.
      numPartitions = coordinatorClient->getNumPartitions(jobID);
    }

    writer = new FastKVPairWriter(
      new BoundaryScannerPartitionFunction(numPartitions / numNodes, numNodes),
      0,
      boost::bind(&BoundaryScanner::emitWorkUnit, this, _1),
      boost::bind(&BoundaryScanner::getOutputChunk, this, _1),
      boost::bind(&BoundaryScanner::putBufferFromWriter, this, _1),
      NULL, NULL, true, false);

    delete coordinatorClient;

    // Get the sample metadata from the first tuple.
    KeyValuePair kvPair;
    bool gotTuple = buffer->getNextKVPair(kvPair);
    ABORT_IF(!gotTuple, "Could not get the first tuple from the first "
             "buffer to read number of samples");

    sampleMetadata = new PhaseZeroSampleMetadata(kvPair);

    bytesPerPartition = sampleMetadata->getBytesOut() / numPartitions;
    remainder = sampleMetadata->getBytesOut() % numPartitions;
  }

  // Scan tuples until we find boundary keys.
  KeyValuePair kvPair;
  while (buffer->getNextKVPair(kvPair)) {
    uint64_t bytesRead = *((uint32_t*)kvPair.getValue());
    if (bytesScanned > nextPartitionBytes) {
      // This is a boundary key, so write it to the output buffer, but with a 0
      // value.
      kvPair.setValue(NULL, 0);
      writer->write(kvPair);

      // Update next partition boundary.
      nextPartitionBytes += bytesPerPartition;
      if (remainder > 0) {
        nextPartitionBytes++;
        remainder--;
      }
      numPartitionsPicked++;
    }

    // The value is the number of bytes the actual map output tuple took up.
    bytesScanned += bytesRead;
    tuplesScanned++;
  }
}

void BoundaryScanner::teardown() {
  writer->flushBuffers();

  TRITONSORT_ASSERT(bytesScanned == sampleMetadata->getBytesOut(),
         "We were supposed to scan %llu bytes but only scanned %llu",
         sampleMetadata->getBytesOut(), bytesScanned);
  TRITONSORT_ASSERT(tuplesScanned == sampleMetadata->getTuplesOut(),
         "We were supposed to scan %llu tuples but only scanned %llu",
         sampleMetadata->getTuplesOut(), tuplesScanned);
  TRITONSORT_ASSERT(numPartitionsPicked == numPartitions,
         "We were supposed to pick %llu partitions but picked %llu",
         numPartitions, numPartitionsPicked);
}

void BoundaryScanner::emitBufferFromWriter(
  KVPairBuffer* buffer, uint64_t bufferNumber) {
  buffer->setNode(bufferNumber);
  emitWorkUnit(buffer);
}

void BoundaryScanner::putBufferFromWriter(KVPairBuffer* buffer) {
  delete buffer;
}

KVPairBuffer* BoundaryScanner::getOutputChunk(uint64_t tupleSize) {
  KVPairBuffer* buffer = NULL;

  if (tupleSize > bufferFactory.getDefaultSize()) {
    buffer = bufferFactory.newInstance(tupleSize);
  } else {
    buffer = bufferFactory.newInstance();
  }

  buffer->setNode(coordinatorNodeID);

  buffer->addJobID(jobID);

  return buffer;
}

BaseWorker* BoundaryScanner::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t defaultBufferSize = params.get<uint64_t>(
    "DEFAULT_BUFFER_SIZE." + phaseName + "." + stageName);

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t numNodes = params.get<uint64_t>("NUM_PEERS");

  uint64_t coordinatorNodeID = params.get<uint64_t>("MERGE_NODE_ID");
  bool isCoordinatorNode = (
    params.get<uint64_t>("MYPEERID") == coordinatorNodeID);

  BoundaryScanner* scanner = new BoundaryScanner(
    id, stageName, phaseName, memoryAllocator, defaultBufferSize, alignmentSize,
    params, numNodes, isCoordinatorNode, coordinatorNodeID);

  return scanner;
}
