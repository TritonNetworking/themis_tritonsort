#include "core/MemoryUtils.h"
#include "mapreduce/workers/reader/LibAIOReader.h"

LibAIOReader::LibAIOReader(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  Params& params, NamedObjectCollection& dependencies, uint64_t _alignmentSize,
  bool _directIO, uint64_t asynchronousIODepth, uint64_t _maxReadSize,
  uint64_t defaultBufferSize, FilenameToStreamIDMap* filenameToStreamIDMap,
  MemoryAllocatorInterface& memoryAllocator, bool deleteAfterRead,
  bool useByteStreamBuffers, WriteTokenPool* tokenPool, ChunkMap* chunkMap)
  : AsynchronousReader(
      phaseName, stageName, id, asynchronousIODepth, defaultBufferSize,
      _alignmentSize, filenameToStreamIDMap, memoryAllocator, deleteAfterRead,
      useByteStreamBuffers, tokenPool, chunkMap),
    alignmentSize(_alignmentSize),
    directIO(_directIO),
    maxReadSize(_maxReadSize),
    events(new (themis::memcheck) io_event[asynchronousIODepth]) {
  // Initialize the libaio context
  memset(&context, 0, sizeof(context));
  int status = io_setup(asynchronousIODepth, &context);
  ABORT_IF(status != 0, "io_setup() failed with error %d", status);
}

LibAIOReader::~LibAIOReader() {
  delete[] events;
}

void LibAIOReader::teardown() {
  // Clean up the AIO context.
  int status = io_destroy(context);
  ABORT_IF(status != 0, "io_destroy() failed with error %d", status);

  // Make sure the underlying asynchronous reader tears down.
  AsynchronousReader::teardown();
}

void LibAIOReader::prepareRead(
  const uint8_t* buffer, File& file, uint64_t readSize) {
  file.prepareLibAIORead(
    const_cast<uint8_t*>(buffer), readSize, maxReadSize);
}

bool LibAIOReader::issueNextRead(const uint8_t* buffer) {
  // Issue the read.
  File* file = (reads.at(buffer))->file;
  struct iocb* controlBlock;
  bool disableDirectIORequired = false;
  bool submitted = file->submitNextLibAIORead(
        const_cast<uint8_t*>(buffer), alignmentSize, &context, controlBlock,
        disableDirectIORequired);
  if (!submitted && disableDirectIORequired) {
    // The read couldn't be submitted because we need to disable direct IO.
    // Force all in-flight IOs to complete.
    waitForReadsToComplete(numReadsInProgress());
    // Now disable direct IO.
    file->disableDirectIO();
    // Resume concurrent IOs.
    serviceIdleBuffers();
    emitFullBuffers();
    // Then repeat this request. We should be guaranteed that this IO submits.
    disableDirectIORequired = false;
    submitted = file->submitNextLibAIORead(
      const_cast<uint8_t*>(buffer), alignmentSize, &context, controlBlock,
      disableDirectIORequired);

    ABORT_IF(!submitted, "Request failed after disabling direct IO.");
    ABORT_IF(disableDirectIORequired,
             "Direct IO still on after direct IO disable");
  }

  if (submitted) {
    // Record the read so we can check it later for completion.
    outstandingReadBuffers[controlBlock] = buffer;
    return true;
  }

  // No reads left for this buffer.
  return false;
}

void LibAIOReader::waitForReadsToComplete(uint64_t numReads) {
  uint64_t inFlightIOs = numReadsInProgress();
  TRITONSORT_ASSERT(numReads <= inFlightIOs,
         "Blocking until %llu IOs complete but there are only %llu IOs",
         numReads, inFlightIOs);

  // Wait for at least numReads reads to complete.
  int status = io_getevents(
    context, numReads, inFlightIOs, events, NULL);
  ABORT_IF(status < static_cast<int64_t>(numReads),
           "Should have at least %llu completed I/Os, but got %d",
           numReads, status);

  // The events array contains information about completed reads, so iterate
  // over it, handling each read that completed.
  for (int i = 0; i < status; i++) {
    struct iocb* controlBlock = events[i].obj;
    int bytesRead = events[i].res;
    int readStatus = events[i].res2;

    // Verify that the read completed successfully.
    ABORT_IF(readStatus != 0,
             "libaio read failed with error %d.", readStatus);

    // Look up the buffer corresponding to this read.
    BufferMap::iterator iter = outstandingReadBuffers.find(controlBlock);
    TRITONSORT_ASSERT(iter != outstandingReadBuffers.end(), "Missing read map entry.");
    const uint8_t* buffer = iter->second;

    // Make sure the number of bytes read matches the size of read call.
    uint64_t readSize = controlBlock->u.c.nbytes;
    ABORT_IF(static_cast<uint64_t>(bytesRead) != readSize,
             "Supposed to read %llu bytes but only read %d (negative "
             "indicates an error: check /usr/include/asm-generic/errno-base.h "
             "or equivalent for error code)", readSize, bytesRead);

    // Update the map and delete the control block.
    outstandingReadBuffers.erase(iter);
    delete controlBlock;

    idleBuffers.push(buffer);
  }
}

uint64_t LibAIOReader::numReadsInProgress() {
  return outstandingReadBuffers.size();
}

void LibAIOReader::openFile(File& file) {
  file.open(File::READ_LIBAIO);
  if (directIO > 0) {
    file.enableDirectIO();
  }
}

BaseWorker* LibAIOReader::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  bool directIO = params.getv<bool>(
    "DIRECT_IO.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t bufferSize = params.getv<uint64_t>(
    "DEFAULT_BUFFER_SIZE.%s.%s", phaseName.c_str(), stageName.c_str());
  ABORT_IF(alignmentSize > 0 && bufferSize % alignmentSize != 0,
           "Read buffer size %llu is not a multiple of alignment size %llu.",
           bufferSize, alignmentSize);

  uint64_t asynchronousIODepth = params.get<uint64_t>(
    "ASYNCHRONOUS_IO_DEPTH." + phaseName + "." + stageName);

  // Read size is unlimited, unless specified.
  uint64_t maxReadSize = std::numeric_limits<uint64_t>::max();
  if (params.contains("MAX_READ_SIZE." + phaseName)) {
    maxReadSize = params.get<uint64_t>("MAX_READ_SIZE." + phaseName);
  }

  FilenameToStreamIDMap* filenameToStreamIDMap = NULL;
  if (dependencies.contains<FilenameToStreamIDMap>(
        stageName, "filename_to_stream_id_map")) {
    filenameToStreamIDMap = dependencies.get<FilenameToStreamIDMap>(
      stageName, "filename_to_stream_id_map");
  }

  bool deleteAfterRead = params.get<bool>("DELETE_AFTER_READ." + phaseName);

  bool useByteStreamBuffers = params.contains("FORMAT_READER." + phaseName);

  WriteTokenPool* tokenPool = NULL;
  ChunkMap* chunkMap = NULL;
  if (dependencies.contains<WriteTokenPool>("read_token_pool")) {
    tokenPool = dependencies.get<WriteTokenPool>("read_token_pool");
    chunkMap = dependencies.get<ChunkMap>("chunk_map");
  }

  LibAIOReader* reader = new LibAIOReader(
    phaseName, stageName, id, params, dependencies, alignmentSize, directIO,
    asynchronousIODepth, maxReadSize, bufferSize, filenameToStreamIDMap,
    memoryAllocator, deleteAfterRead, useByteStreamBuffers, tokenPool,
    chunkMap);

  return reader;
}
