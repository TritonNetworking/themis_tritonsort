#include <algorithm>

#include "common/PartitionFile.h"
#include "mapreduce/common/FilenameToStreamIDMap.h"
#include "mapreduce/common/StreamInfo.h"
#include "mapreduce/common/hdfs/HDFSClient.h"
#include "mapreduce/common/hdfs/HDFSReader.h"

HDFSReader::HDFSReader(
  uint64_t id, const std::string& stageName,
  FilenameToStreamIDMap* _filenameToStreamIDMap,
  uint64_t _numDownstreamConverters, MemoryAllocatorInterface& memoryAllocator,
  uint64_t byteStreamBufferSize, bool _wholeFileInSingleBuffer)
  : SingleUnitRunnable<ReadRequest>(id, stageName),
    numDownstreamConverters(_numDownstreamConverters),
    wholeFileInSingleBuffer(_wholeFileInSingleBuffer),
    totalBytesProduced(0),
    currentBytesProduced(0),
    filenameToStreamIDMap(_filenameToStreamIDMap),
    byteStreamBufferFactory(*this, memoryAllocator, byteStreamBufferSize),
    kvPairBufferFactory(*this, memoryAllocator, 0),
    currentBuffer(NULL) {
}

HDFSReader::~HDFSReader() {
}

void HDFSReader::resourceMonitorOutput(Json::Value& obj) {
  SingleUnitRunnable<ReadRequest>::resourceMonitorOutput(obj);

  obj["bytes_read"] = Json::UInt64(totalBytesProduced);
}

void HDFSReader::run(ReadRequest* request) {
  path.assign(request->path);

  ASSERT(request->jobIDs.size() > 0, "Expected read request to have at "
         "least one job ID");

  if (filenameToStreamIDMap != NULL) {
    filenameToStreamIDMap->addFilename(path, request->jobIDs);
  }

  HDFSClient client(request->host, request->port);

  fileSize = client.length(path);

  uint64_t readSize = std::min(fileSize, request->length);

  currentBytesProduced = 0;
  client.read(path, request->offset, request->length, *this);

  ABORT_IF(currentBytesProduced != readSize, "Read fewer bytes than "
         "expected from '%s' (expected to read %llu, but read %llu)",
         request->path.c_str(), request->length, currentBytesProduced);

  if (currentBuffer != NULL) {
    emitCurrentBuffer();
  }

  // Send "stream-closed" buffer to converter

  if (wholeFileInSingleBuffer) {
    currentBuffer = kvPairBufferFactory.newInstance();
  } else {
    currentBuffer = byteStreamBufferFactory.newInstance();
  }

  emitCurrentBuffer(false);

  delete request;
}

uint64_t HDFSReader::handleHDFSRead(void* resultBytes, uint64_t size) {
  if (currentBuffer == NULL) {
    if (wholeFileInSingleBuffer) {
      currentBuffer = kvPairBufferFactory.newInstance(fileSize);
    } else {
      currentBuffer = byteStreamBufferFactory.newInstance();
    }

    currentBuffer->clear();
  }

  uint64_t bytesRemaining = size;

  while (bytesRemaining > 0) {
    uint64_t bytesToAppend = std::min<uint64_t>(
      bytesRemaining,
      currentBuffer->getCapacity() - currentBuffer->getCurrentSize());

    currentBuffer->append(static_cast<uint8_t*>(
      resultBytes) + (size - bytesRemaining), bytesToAppend);

    if (currentBuffer->full()) {
      emitCurrentBuffer();
      currentBuffer = byteStreamBufferFactory.newInstance();
      currentBuffer->clear();
    }

    totalBytesProduced += bytesToAppend;
    currentBytesProduced += bytesToAppend;
    bytesRemaining -= bytesToAppend;
  }

  return size;
}

BaseWorker* HDFSReader::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  // If this worker was spawned by a CoordinatorRequestReader, want to make
  // sure it gets configured according to the parameters in use by its parent
  // stage
  std::string parentStageName(stageName, 0, stageName.find_first_of(':'));

  uint64_t bufferSize = params.get<uint64_t>(
    "DEFAULT_BUFFER_SIZE." + phaseName + "." + parentStageName);

  bool wholeFileInSingleBuffer = (phaseName == "phase_two");

  FilenameToStreamIDMap* filenameToStreamIDMap = NULL;
  uint64_t numDownstreamConverters = 0;

  // We only need to map filenames to stream IDs if there's a downstream
  // converter
  /// \todo(AR) this whole stream to filename mapping thing is really
  /// inelegant; would be good to investigate alternative solutions
  if (!wholeFileInSingleBuffer) {
    filenameToStreamIDMap = dependencies.get<FilenameToStreamIDMap>(
      parentStageName, "filename_to_stream_id_map");

    const std::string& converterStageName = *(
      dependencies.get<std::string>(parentStageName, "converter_stage_name"));

    numDownstreamConverters = params.get<uint64_t>(
      "NUM_WORKERS." + phaseName + "." + converterStageName);
  }

  HDFSReader* reader = new HDFSReader(
    id, stageName, filenameToStreamIDMap, numDownstreamConverters,
    memoryAllocator, bufferSize, wholeFileInSingleBuffer);
  return reader;
}

void HDFSReader::emitCurrentBuffer(bool deleteEmptyBuffers) {
  if (deleteEmptyBuffers && currentBuffer->empty()) {
    delete currentBuffer;
  } else {
    if (wholeFileInSingleBuffer) {
      KVPairBuffer* kvPairBuffer = static_cast<KVPairBuffer*>(currentBuffer);

      kvPairBuffer->setSourceName(path);

      // Use PartitionFile to extract logical disk ID from path
      PartitionFile file(path);
      kvPairBuffer->setLogicalDiskID(file.getPartitionID());
      kvPairBuffer->addJobID(file.getJobID());
    } else {
      ByteStreamBuffer* byteStreamBuffer = static_cast<ByteStreamBuffer*>(
        currentBuffer);

      const StreamInfo& streamInfo = filenameToStreamIDMap->getStreamInfo(path);

      if (filenameToStreamIDMap != NULL) {
        byteStreamBuffer->setStreamID(streamInfo.getStreamID());
      }
    }

    emitWorkUnit(currentBuffer);
  }

  currentBuffer = NULL;
}
