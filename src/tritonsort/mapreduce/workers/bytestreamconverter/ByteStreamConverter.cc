#include <algorithm>

#include "common/buffers/ByteStreamBuffer.h"
#include "mapreduce/common/StreamInfo.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/bytestreamconverter/ByteStreamConverter.h"
#include "mapreduce/workers/bytestreamconverter/FormatReaderInterface.h"

ByteStreamConverter::ByteStreamConverter(
  uint64_t id, const std::string& stageName,
  MemoryAllocatorInterface& memoryAllocator, uint64_t defaultBufferSize,
  uint64_t alignmentSize, FilenameToStreamIDMap& _filenameToStreamIDMap,
  const std::string& formatReaderImpl, const Params& params,
  const std::string& phaseName)
  : SingleUnitRunnable<ByteStreamBuffer>(id, stageName),
    bufferFactory(*this, memoryAllocator, defaultBufferSize, alignmentSize),
    filenameToStreamIDMap(_filenameToStreamIDMap),
    formatReaderFactory(*this, formatReaderImpl, params, phaseName) {
}

void ByteStreamConverter::teardown() {
  TRITONSORT_ASSERT(formatReaders.empty(),
         "%llu streams were open during teardown.", formatReaders.size());
}

void ByteStreamConverter::run(ByteStreamBuffer* buffer) {
  uint64_t streamID = buffer->getStreamID();

  // Locate the format reader for this stream, or create if one does not exist.
  FormatReaderMap::iterator iter = formatReaders.find(streamID);
  FormatReaderInterface* formatReader = NULL;

  if (iter == formatReaders.end()) {
    formatReader = formatReaderFactory.newFormatReader(
      filenameToStreamIDMap.getStreamInfo(streamID));
    formatReaders[streamID] = formatReader;
  } else {
    formatReader = iter->second;
  }

  if (buffer->empty()) {
    // Empty buffer signals a closed stream.
    delete formatReader;
    formatReaders.erase(streamID);
  } else {
    formatReader->readByteStream(*buffer);
  }

  delete buffer;
}

KVPairBuffer* ByteStreamConverter::newBuffer(uint64_t size) {
  return bufferFactory.newInstance(size);
}

KVPairBuffer* ByteStreamConverter::newBufferAtLeastAsLargeAs(uint64_t size) {
  return bufferFactory.newInstance(
    std::max<uint64_t>(bufferFactory.getDefaultSize(), size));
}

void ByteStreamConverter::emitBuffer(
  KVPairBuffer& buffer, const StreamInfo& streamInfo) {

  // Set source and job ID.
  buffer.setSourceName(streamInfo.getFilename());
  buffer.addJobIDSet(streamInfo.getJobIDs());

  // If this was a partition file, also set the partition ID.
  uint64_t partitionID;
  uint64_t jobID;
  uint64_t chunkID;
  bool isPartitionFile = PartitionFile::matchFilename(
    streamInfo.getFilename(), partitionID, jobID, chunkID);

  if (isPartitionFile) {
    buffer.setLogicalDiskID(partitionID);
    buffer.setChunkID(chunkID);
  }

  emitWorkUnit(&buffer);
}

BaseWorker* ByteStreamConverter::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t defaultBufferSize =
    params.get<uint64_t>("DEFAULT_BUFFER_SIZE." + phaseName + "." + stageName);

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  FilenameToStreamIDMap* filenameToStreamIDMap =
    dependencies.get<FilenameToStreamIDMap>(
      stageName, "filename_to_stream_id_map");

  const std::string& formatReaderImpl =
    params.get<std::string>("FORMAT_READER." + phaseName);

  ByteStreamConverter* converter = new ByteStreamConverter(
    id, stageName, memoryAllocator, defaultBufferSize, alignmentSize,
    *filenameToStreamIDMap, formatReaderImpl, params, phaseName);

  return converter;
}
