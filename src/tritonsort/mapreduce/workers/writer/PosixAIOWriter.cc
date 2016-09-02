#include <errno.h>

#include "core/MemoryUtils.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/writer/BaseWriter.h"
#include "mapreduce/workers/writer/PosixAIOWriter.h"

PosixAIOWriter::PosixAIOWriter(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  Params& params, NamedObjectCollection& dependencies, uint64_t _alignmentSize,
  uint64_t asynchronousIODepth, uint64_t _maxWriteSize)
  : AsynchronousWriter(
      phaseName, stageName, id, params, dependencies, File::WRITE_POSIXAIO,
      asynchronousIODepth),
    alignmentSize(_alignmentSize),
    maxWriteSize(_maxWriteSize),
    aiocbList(new (themis::memcheck) aiocb*[asynchronousIODepth]) {
  // Initialize the aiocbList to NULLs
  for (uint64_t i = 0; i < asynchronousIODepth; i++) {
    aiocbList[i] = NULL;
  }
}

PosixAIOWriter::~PosixAIOWriter() {
  delete[] aiocbList;
}

void PosixAIOWriter::prepareWrite(KVPairBuffer* buffer) {
  File* file = writer.getFile(buffer);
  file->preparePosixAIOWrite(
    const_cast<uint8_t*>(buffer->getRawBuffer()), buffer->getCurrentSize(),
    maxWriteSize);
}

bool PosixAIOWriter::issueNextWrite(KVPairBuffer* buffer) {
  // Grab the file for this buffer.
  File* file = writer.getFile(buffer);

  // Issue the write.
  struct aiocb* controlBlock;
  bool disableDirectIORequired = false;
  bool submitted = file->submitNextPosixAIOWrite(
    const_cast<uint8_t*>(buffer->getRawBuffer()), alignmentSize,
    controlBlock, disableDirectIORequired);
  if (!submitted && disableDirectIORequired) {
    // The write couldn't be submitted because we need to disable direct IO.
    // Force all in-flight IOs to complete.
    waitForWritesToComplete(numWritesInProgress());
    file->sync();
    // Now disable direct IO.
    file->disableDirectIO();
    // Resume concurrent IOs.
    serviceIdleBuffers();
    // Then repeat this request. We should be guaranteed that this IO submits.
    disableDirectIORequired = false;
    submitted = file->submitNextPosixAIOWrite(
      const_cast<uint8_t*>(buffer->getRawBuffer()), alignmentSize,
      controlBlock, disableDirectIORequired);

    ABORT_IF(!submitted, "Request failed after disabling direct IO.");
    ABORT_IF(disableDirectIORequired,
             "Direct IO still on after direct IO disable");
  }

  if (submitted) {
    // Record the write so we can check it later for completion.
    outstandingWriteBuffers[controlBlock] = buffer;

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

  // No writes left for this buffer.
  return false;
}

void PosixAIOWriter::waitForWritesToComplete(uint64_t numWrites) {
  TRITONSORT_ASSERT(numWrites <= numWritesInProgress(),
         "Blocking until %llu IOs complete but there are only %llu IOs",
         numWrites, numWritesInProgress());
  uint64_t completedWrites = 0;

  // Posix AIO interface does not let us block until N writes complete so keep
  // looping until we've done numWrites
  while (completedWrites < numWrites) {
    // Wait for some writes to complete.
    int status = aio_suspend(aiocbList, numWritesInProgress(), NULL);
    ABORT_IF(status != 0,
             "aio_suspend() failed %d: %s", errno, strerror(errno));

    // Iterate through the list of outstanding writes and remove any that have
    // completed.
    for (WriteMap::iterator iter = outstandingWriteBuffers.begin();
         iter != outstandingWriteBuffers.end(); iter++) {
      struct aiocb* controlBlock = iter->first;
      KVPairBuffer* buffer = iter->second;

      // Check if this write has completed.
      status = aio_error(controlBlock);
      if (status == 0) {
        // The write completed successfully.
        uint64_t writeStatus = aio_return(controlBlock);
        uint64_t writeSize = controlBlock->aio_nbytes;
        ABORT_IF(writeStatus != writeSize,
                 "aio_write() should have written %llu bytes but wrote %llu",
                 writeSize, writeStatus);
        // Remember this buffer so we can issue its next write later.
        idleBuffers.push(buffer);
        completedWrites++;

        // Remove the request from the list of outstanding writes.
        uint64_t i = 0;
        for (; i < asynchronousIODepth; i++) {
          if (aiocbList[i] == controlBlock) {
            aiocbList[i] = NULL;
            break;
          }
        }
        TRITONSORT_ASSERT(i < asynchronousIODepth, "Could not find the control block "
               "associated with a completed write in the aiocb list.");

        delete controlBlock;

        // Remove this controlBlock from the write map, but make sure to set the
        // iterator just before it so incrementing the iterator at the end of the
        // loop will land on the next member of the map.
        WriteMap::iterator previousIterator = iter;
        previousIterator--;
        outstandingWriteBuffers.erase(iter);
        iter = previousIterator;
      } else if (status != EINPROGRESS) {
        ABORT("aio_error() failed with status %d", status);
      }
    }
  }
}

uint64_t PosixAIOWriter::numWritesInProgress() {
  return outstandingWriteBuffers.size();
}

BaseWorker* PosixAIOWriter::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  // If this worker was spawned by a MultiProtocolWriter, want to make sure it
  // gets configured according to the parameters in use by its parent stage
  std::string parentStageName(stageName, 0, stageName.find_first_of(':'));

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), parentStageName.c_str());

  uint64_t asynchronousIODepth = params.get<uint64_t>(
    "ASYNCHRONOUS_IO_DEPTH." + phaseName + "." + parentStageName);

  // Write size is unlimited, unless specified.
  uint64_t maxWriteSize = std::numeric_limits<uint64_t>::max();
  if (params.contains("MAX_WRITE_SIZE." + phaseName)) {
    maxWriteSize = params.get<uint64_t>("MAX_WRITE_SIZE." + phaseName);
  }

  PosixAIOWriter* writer = new PosixAIOWriter(
    phaseName, stageName, id, params, dependencies, alignmentSize,
    asynchronousIODepth, maxWriteSize);

  return writer;
}
