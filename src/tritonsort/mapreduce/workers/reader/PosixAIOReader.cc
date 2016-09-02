#include <errno.h>

#include "core/MemoryUtils.h"
#include "mapreduce/workers/reader/PosixAIOReader.h"

PosixAIOReader::PosixAIOReader(
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
    aiocbList(new (themis::memcheck) aiocb*[asynchronousIODepth]) {
  // Initialize the aiocbList to NULLs
  for (uint64_t i = 0; i < asynchronousIODepth; i++) {
    aiocbList[i] = NULL;
  }
}

PosixAIOReader::~PosixAIOReader() {
  delete[] aiocbList;
}

void PosixAIOReader::prepareRead(
  const uint8_t* buffer, File& file, uint64_t readSize) {
  file.preparePosixAIORead(
    const_cast<uint8_t*>(buffer), readSize, maxReadSize);
}

bool PosixAIOReader::issueNextRead(const uint8_t* buffer) {
  // Issue the read.
  File* file = (reads.at(buffer))->file;
  struct aiocb* controlBlock;
  bool disableDirectIORequired = false;
  bool submitted = file->submitNextPosixAIORead(
    const_cast<uint8_t*>(buffer), alignmentSize, controlBlock,
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
    submitted = file->submitNextPosixAIORead(
      const_cast<uint8_t*>(buffer), alignmentSize, controlBlock,
      disableDirectIORequired);

    ABORT_IF(!submitted, "Request failed after disabling direct IO.");
    ABORT_IF(disableDirectIORequired,
             "Direct IO still on after direct IO disable");
  }

  if (submitted) {
    // Record the read so we can check it later for completion.
    outstandingReadBuffers[controlBlock] = buffer;

    // Update the aiocb list for future aio_suspend() calls by filling the first
    // NULL slot
    uint64_t i = 0;
    for (; i < asynchronousIODepth; i++) {
      if (aiocbList[i] == NULL) {
        aiocbList[i] = controlBlock;
        break;
      }
    }
    TRITONSORT_ASSERT(i < asynchronousIODepth,
           "Could not find a free slot in the aiocb list.");
    return true;
  }

  // No reads left for this buffer.
  return false;
}

void PosixAIOReader::waitForReadsToComplete(uint64_t numReads) {
  TRITONSORT_ASSERT(numReads <= numReadsInProgress(),
         "Blocking until %llu IOs complete but there are only %llu IOs",
         numReads, numReadsInProgress());
  uint64_t completedReads = 0;

  // Posix AIO interface does not let us block until N reads complete so keep
  // looping until we've done numReads
  while (completedReads < numReads) {
    // Wait for some reads to complete.
    int status = aio_suspend(aiocbList, numReadsInProgress(), NULL);
    ABORT_IF(status != 0,
             "aio_suspend() failed %d: %s", errno, strerror(errno));

    for (BufferMap::iterator iter = outstandingReadBuffers.begin();
         iter != outstandingReadBuffers.end(); iter++) {
      struct aiocb* controlBlock = iter->first;
      const uint8_t* buffer = iter->second;

      // Check if this read has completed.
      status = aio_error(controlBlock);
      if (status == 0) {
        // The read completed successfully.
        uint64_t readStatus = aio_return(controlBlock);
        uint64_t readSize = controlBlock->aio_nbytes;
        ABORT_IF(readStatus != readSize,
                 "aio_read() should have read %llu bytes but read %llu",
                 readSize, readStatus);
        // Remember this buffer so we can issue its next read later.
        idleBuffers.push(buffer);
        completedReads++;

        // Remove the request from the list of outstanding reads.
        uint64_t i = 0;
        for (; i < asynchronousIODepth; i++) {
          if (aiocbList[i] == controlBlock) {
            aiocbList[i] = NULL;
            break;
          }
        }
        TRITONSORT_ASSERT(i < asynchronousIODepth, "Could not find the control block "
               "associated with a completed read in the aiocb list.");

        delete controlBlock;

        // Remove this controlBlock from the read map, but make sure to set the
        // iterator just before it so incrementing the iterator at the end of
        // the loop will land on the next member of the map.
        BufferMap::iterator previousIterator = iter;
        previousIterator--;
        outstandingReadBuffers.erase(iter);
        iter = previousIterator;
      } else if (status != EINPROGRESS) {
        ABORT("aio_error() failed with status %llu");
      }
    }
  }
}

uint64_t PosixAIOReader::numReadsInProgress() {
  return outstandingReadBuffers.size();
}

void PosixAIOReader::openFile(File& file) {
  file.open(File::READ_POSIXAIO);
  if (directIO > 0) {
    file.enableDirectIO();
  }
}

BaseWorker* PosixAIOReader::newInstance(
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

  PosixAIOReader* reader = new PosixAIOReader(
    phaseName, stageName, id, params, dependencies, alignmentSize, directIO,
    asynchronousIODepth, maxReadSize, bufferSize, filenameToStreamIDMap,
    memoryAllocator, deleteAfterRead, useByteStreamBuffers, tokenPool,
    chunkMap);

  return reader;
}
