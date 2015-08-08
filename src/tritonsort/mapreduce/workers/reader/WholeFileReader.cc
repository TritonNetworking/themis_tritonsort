#include "common/PartitionFile.h"
#include "mapreduce/common/ReadRequest.h"
#include "mapreduce/workers/reader/WholeFileReader.h"

WholeFileReader::WholeFileReader(
  uint64_t id, const std::string& name, uint64_t _maxReadSize,
  uint64_t _alignmentSize, bool _directIO,
  MemoryAllocatorInterface& memoryAllocator, bool _deleteAfterRead)
  : SingleUnitRunnable<ReadRequest>(id, name),
    maxReadSize(_maxReadSize),
    alignmentSize(_alignmentSize),
    directIO(_directIO),
    deleteAfterRead(_deleteAfterRead),
    alignedBytesRead(0),
    logger(name, id),
    bufferFactory(*this, memoryAllocator, 0, alignmentSize) {

  readTimeStatID = logger.registerHistogramStat("read_time", 100);
  readSizeStatID = logger.registerHistogramStat("read_size", 100);
}

void WholeFileReader::run(ReadRequest* readRequest) {
  PartitionFile file(readRequest->path);
  const std::string& filename = file.getFilename();

  uint64_t fileSize = file.getCurrentSize();

  // Don't process zero-length files
  if (fileSize > 0) {
    logger.logDatum("input_filename", filename);
    file.open(File::READ);

    if (directIO) {
      // Read with O_DIRECT
      file.enableDirectIO();
    }

    // Get a buffer to hold the whole file.
    KVPairBuffer* buffer = bufferFactory.newInstance(fileSize);
    buffer->setSourceName(filename);
    buffer->setLogicalDiskID(file.getPartitionID());
    buffer->addJobID(file.getJobID());

    // Read the whole file into the buffer.
    const uint8_t* appendPtr = buffer->setupAppend(fileSize);

    readTimer.start();
    file.read(
      const_cast<uint8_t*>(appendPtr), fileSize, maxReadSize, alignmentSize);
    readTimer.stop();

    buffer->commitAppend(appendPtr, fileSize);

    emitWorkUnit(buffer);

    logger.add(readTimeStatID, readTimer);
    logger.add(readSizeStatID, fileSize);

    alignedBytesRead += file.getAlignedBytesRead();

    file.close();

    if (deleteAfterRead) {
      file.unlink();
    }
  }

  delete readRequest;
}

void WholeFileReader::teardown() {
  logger.logDatum("direct_io_bytes_read", alignedBytesRead);
}

BaseWorker* WholeFileReader::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  // Read size is unlimited, unless specified.
  uint64_t maxReadSize = std::numeric_limits<uint64_t>::max();
  if (params.contains("MAX_READ_SIZE." + phaseName)) {
    maxReadSize = params.get<uint64_t>("MAX_READ_SIZE." + phaseName);
  }

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  bool directIO =
    params.getv<bool>("DIRECT_IO.%s.%s", phaseName.c_str(), stageName.c_str());

  bool deleteAfterRead = params.get<bool>("DELETE_AFTER_READ." + phaseName);

  // Sanity check
  ABORT_IF(params.contains("FORMAT_READER." + phaseName),
           "Should be reading into byte stream buffers in phase %s",
           phaseName.c_str());

  WholeFileReader* reader = new WholeFileReader(
    id, stageName, maxReadSize, alignmentSize, directIO, memoryAllocator,
    deleteAfterRead);

  return reader;
}
