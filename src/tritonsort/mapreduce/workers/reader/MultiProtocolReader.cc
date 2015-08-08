#include "core/ResourceMonitor.h"
#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/hdfs/HDFSReader.h"
#include "mapreduce/workers/reader/ByteStreamReader.h"
#include "mapreduce/workers/reader/MultiProtocolReader.h"
#include "mapreduce/workers/reader/WholeFileReader.h"

MultiProtocolReader::MultiProtocolReader(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  Params& params)
  : SingleUnitRunnable<ReadRequest>(id, stageName) {
}

MultiProtocolReader::~MultiProtocolReader() {
  for (ProtocolReaderMap::iterator iter = protocolReaderMap.begin();
       iter != protocolReaderMap.end(); iter++) {
    delete iter->second;
  }

  protocolReaderMap.clear();
}

void MultiProtocolReader::init() {
  for (ProtocolReaderMap::iterator iter = protocolReaderMap.begin();
       iter != protocolReaderMap.end(); iter++) {
    iter->second->init();
  }
}

void MultiProtocolReader::teardown() {
  for (ProtocolReaderMap::iterator iter = protocolReaderMap.begin();
       iter != protocolReaderMap.end(); iter++) {
    iter->second->teardown();
  }
}

void MultiProtocolReader::registerWithResourceMonitor() {
  for (ProtocolReaderMap::iterator iter = protocolReaderMap.begin();
       iter != protocolReaderMap.end(); iter++) {
    std::ostringstream oss;
    oss << iter->second->getName() << ':'
        << ReadRequest::protocolName(iter->first);

    std::string registrantName(oss.str());

    ResourceMonitor::registerClient(iter->second, registrantName.c_str());
  }
}

void MultiProtocolReader::unregisterWithResourceMonitor() {
  for (ProtocolReaderMap::iterator iter = protocolReaderMap.begin();
       iter != protocolReaderMap.end(); iter++) {
    ResourceMonitor::unregisterClient(iter->second);
  }
}

void MultiProtocolReader::addProtocolReader(
  ReadRequest::Protocol protocol, ProtocolReader* reader) {
  protocolReaderMap[protocol] = reader;
}

void MultiProtocolReader::run(ReadRequest* readRequest) {
  ProtocolReaderMap::iterator iter = protocolReaderMap.find(
    readRequest->protocol);

  ABORT_IF(iter == protocolReaderMap.end(), "Can't find a protocol reader "
           "for protocol '%d'", readRequest->protocol);

  ProtocolReader* protocolReader = iter->second;

  protocolReader->run(readRequest);
}

void MultiProtocolReader::setTracker(
  WorkerTrackerInterface* workerTracker) {

  ProtocolReader::setTracker(workerTracker);

  for (ProtocolReaderMap::iterator iter = protocolReaderMap.begin();
       iter != protocolReaderMap.end(); iter++) {
    (iter->second)->setTracker(workerTracker);
  }
}

void MultiProtocolReader::addDownstreamTracker(
  WorkerTrackerInterface* downstreamTracker) {

  ProtocolReader::addDownstreamTracker(downstreamTracker);

  for (ProtocolReaderMap::iterator iter = protocolReaderMap.begin();
       iter != protocolReaderMap.end(); iter++) {
    (iter->second)->addDownstreamTracker(downstreamTracker);
  }
}

void MultiProtocolReader::addDownstreamTracker(
  WorkerTrackerInterface* downstreamTracker,
  const std::string& trackerDescription) {

  ProtocolReader::addDownstreamTracker(
    downstreamTracker, trackerDescription);

  for (ProtocolReaderMap::iterator iter = protocolReaderMap.begin();
       iter != protocolReaderMap.end(); iter++) {
    (iter->second)->addDownstreamTracker(downstreamTracker, trackerDescription);
  }
}

BaseWorker* MultiProtocolReader::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  MultiProtocolReader* reader = new MultiProtocolReader(
    phaseName, stageName, id, params);

  /// \todo(AR) This makes me sad. It is probably a good idea to unify the two
  /// types of readers under one class
  ProtocolReader* fileReader = NULL;

  if (params.contains("FORMAT_READER." + phaseName)) {
    fileReader = dynamic_cast<ProtocolReader*>(
      ByteStreamReader::newInstance(
        phaseName, stageName, id, params, memoryAllocator, dependencies));
  } else {
    fileReader = dynamic_cast<ProtocolReader*>(
      WholeFileReader::newInstance(
        phaseName, stageName, id, params, memoryAllocator, dependencies));
  }

  ABORT_IF(fileReader == NULL, "dynamic_cast<>ing 'file' protocol reader "
           "to a SingleUnitRunnable<ReadRequest> failed");

  reader->addProtocolReader(ReadRequest::FILE, fileReader);

  ProtocolReader* hdfsReader = dynamic_cast<ProtocolReader*>(
    HDFSReader::newInstance(
      phaseName, stageName, id, params, memoryAllocator, dependencies));

  reader->addProtocolReader(ReadRequest::HDFS, hdfsReader);

  return reader;
}
