#include "core/File.h"
#include "mapreduce/common/FilenameToStreamIDMap.h"
#include "mapreduce/common/StreamInfo.h"
#include "mapreduce/workers/reader/ByteStreamReader.h"

ByteStreamReader::ByteStreamReader(
  uint64_t id, const std::string& name, uint64_t _maxReadSize,
  uint64_t _alignmentSize, bool _directIO,
  FilenameToStreamIDMap& _filenameToStreamIDMap,
  MemoryAllocatorInterface& memoryAllocator, uint64_t bufferSize,
  bool _deleteAfterRead, bool _setStreamSize)
  : SingleUnitRunnable<ReadRequest>(id, name),
    maxReadSize(_maxReadSize),
    alignmentSize(_alignmentSize),
    directIO(_directIO),
    deleteAfterRead(_deleteAfterRead),
    setStreamSize(_setStreamSize),
    alignedBytesRead(0),
    logger(name, id),
    filenameToStreamIDMap(_filenameToStreamIDMap),
    bufferFactory(*this, memoryAllocator, bufferSize, alignmentSize) {

  readTimeStatID = logger.registerHistogramStat("read_time", 100);
  readSizeStatID = logger.registerHistogramStat("read_size", 100);
}

void ByteStreamReader::run(ReadRequest* readRequest) {
  File file(readRequest->path);
  const std::string& filename = file.getFilename();
  TRITONSORT_ASSERT(readRequest->jobIDs.size() > 0, "Expected read request to have at "
         "least one job ID");

  if (setStreamSize) {
    filenameToStreamIDMap.addFilenameWithSize(
      filename, readRequest->jobIDs, readRequest->length, readRequest->offset);
  } else {
    filenameToStreamIDMap.addFilename(
      filename, readRequest->jobIDs, readRequest->offset);
  }

  uint64_t fileSize = file.getCurrentSize();

  // Don't process zero-length files
  if (fileSize > 0) {
    logger.logDatum("input_filename", filename);
    file.open(File::READ);

    if (directIO > 0) {
      // Read with O_DIRECT
      file.enableDirectIO();
    }

    // Seek to the appropriate offset.
    if (readRequest->offset != 0) {
      TRITONSORT_ASSERT(readRequest->offset <= static_cast<uint64_t>(
               std::numeric_limits<int64_t>::max()),
             "Can't seek more than %lld bytes into the file (wanted to seek "
             "%llu bytes)", std::numeric_limits<int64_t>::max(),
             readRequest->offset);
      file.seek(readRequest->offset, File::FROM_BEGINNING);
    }

    // Read the file into fixed size buffers.
    uint64_t bytesRemaining = std::min<uint64_t>(
      readRequest->length, fileSize - readRequest->offset);

    while (bytesRemaining > 0) {
      // Get a new buffer to hold bytes.
      ByteStreamBuffer* buffer = getNewConfiguredBuffer(*readRequest);

      // Issue a read to fill the buffer.
      uint64_t readSize = std::min<uint64_t>(
        buffer->getCapacity(), bytesRemaining);
      const uint8_t* appendPtr = buffer->setupAppend(readSize);

      readTimer.start();
      file.read(
        const_cast<uint8_t*>(appendPtr), readSize, maxReadSize, alignmentSize);
      readTimer.stop();

      buffer->commitAppend(appendPtr, readSize);

      emitWorkUnit(buffer);

      logger.add(readTimeStatID, readTimer);
      logger.add(readSizeStatID, readSize);

      bytesRemaining -= readSize;
    }

    alignedBytesRead += file.getAlignedBytesRead();

    file.close();

    if (deleteAfterRead) {
      file.unlink();
    }

    // Notify the downstream stage that the file has been closed by sending it
    // an empty buffer
    ByteStreamBuffer* buffer = getNewConfiguredBuffer(*readRequest);
    emitWorkUnit(buffer);
  }

  // Reader is responsible for freeing dynamically created work units.
  delete readRequest;
}

void ByteStreamReader::teardown() {
  logger.logDatum("direct_io_bytes_read", alignedBytesRead);
}

ByteStreamBuffer* ByteStreamReader::getNewConfiguredBuffer(
  ReadRequest& readRequest) {
  ByteStreamBuffer* buffer = bufferFactory.newInstance();
  buffer->clear();

  const StreamInfo& streamInfo = filenameToStreamIDMap.getStreamInfo(
    readRequest.path, readRequest.offset);

  buffer->setStreamID(streamInfo.getStreamID());

  return buffer;
}

BaseWorker* ByteStreamReader::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  FilenameToStreamIDMap* filenameToStreamIDMap =
    dependencies.get<FilenameToStreamIDMap>(
      stageName, "filename_to_stream_id_map");

  // Read size is unlimited, unless specified.
  uint64_t maxReadSize = std::numeric_limits<uint64_t>::max();
  if (params.contains("MAX_READ_SIZE." + phaseName)) {
    maxReadSize = params.get<uint64_t>("MAX_READ_SIZE." + phaseName);
  }

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  bool directIO = params.getv<bool>(
    "DIRECT_IO.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t bufferSize = params.getv<uint64_t>(
    "DEFAULT_BUFFER_SIZE.%s.%s", phaseName.c_str(), stageName.c_str());
  ABORT_IF(alignmentSize > 0 && bufferSize % alignmentSize != 0,
           "Read buffer size %llu is not a multiple of alignment size %llu.",
           bufferSize, alignmentSize);

  bool deleteAfterRead = params.get<bool>("DELETE_AFTER_READ." + phaseName);

  // Sanity check
  ABORT_IF(!params.contains("FORMAT_READER." + phaseName),
           "Should not be reading into byte stream buffers in phase %s",
           phaseName.c_str());

  bool setStreamSize = (phaseName == "phase_two");

  ByteStreamReader* reader = new ByteStreamReader(
    id, stageName, maxReadSize, alignmentSize, directIO, *filenameToStreamIDMap,
    memoryAllocator, bufferSize, deleteAfterRead, setStreamSize);

  return reader;
}
