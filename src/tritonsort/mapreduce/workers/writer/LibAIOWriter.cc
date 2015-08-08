#include "core/MemoryUtils.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/writer/BaseWriter.h"
#include "mapreduce/workers/writer/LibAIOWriter.h"

LibAIOWriter::LibAIOWriter(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  Params& params, NamedObjectCollection& dependencies, uint64_t _alignmentSize,
  uint64_t asynchronousIODepth, uint64_t _maxWriteSize)
  : AsynchronousWriter(
      phaseName, stageName, id, params, dependencies, File::WRITE_LIBAIO,
      asynchronousIODepth),
    alignmentSize(_alignmentSize),
    maxWriteSize(_maxWriteSize),
    events(new (themis::memcheck) io_event[asynchronousIODepth]) {
  // Initialize the libaio context
  memset(&context, 0, sizeof(context));
  int status = io_setup(asynchronousIODepth, &context);
  ABORT_IF(status != 0, "io_setup() failed with error %d", status);
}

LibAIOWriter::~LibAIOWriter() {
  delete[] events;
}

void LibAIOWriter::prepareWrite(KVPairBuffer* buffer) {
  File* file = writer.getFile(buffer);
  file->prepareLibAIOWrite(
    const_cast<uint8_t*>(buffer->getRawBuffer()), buffer->getCurrentSize(),
    maxWriteSize);
}

void LibAIOWriter::teardown() {
  // Clean up the AIO context.
  int status = io_destroy(context);
  ABORT_IF(status != 0, "io_destroy() failed with error %d", status);

  AsynchronousWriter::teardown();
}

bool LibAIOWriter::issueNextWrite(KVPairBuffer* buffer) {
  // Grab the file for this buffer.
  File* file = writer.getFile(buffer);

  // Issue the write.
  struct iocb* controlBlock;
  bool disableDirectIORequired = false;
  bool submitted = file->submitNextLibAIOWrite(
    const_cast<uint8_t*>(buffer->getRawBuffer()), alignmentSize, &context,
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
    submitted = file->submitNextLibAIOWrite(
      const_cast<uint8_t*>(buffer->getRawBuffer()), alignmentSize, &context,
      controlBlock, disableDirectIORequired);

    ABORT_IF(!submitted, "Request failed after disabling direct IO.");
    ABORT_IF(disableDirectIORequired,
             "Direct IO still on after direct IO disable");
  }

  if (submitted) {
    // Record the write so we can check it later for completion.
    outstandingWriteBuffers[controlBlock] = buffer;
    return true;
  }

  // No writes left for this buffer.
  return false;
}

void LibAIOWriter::waitForWritesToComplete(uint64_t numWrites) {
  uint64_t inFlightIOs = numWritesInProgress();
  ASSERT(numWrites <= inFlightIOs,
         "Blocking until %llu IOs complete but there are only %llu IOs",
         numWrites, inFlightIOs);

  // Wait for at least numWrites writes to complete.
  int status = io_getevents(context, numWrites, inFlightIOs, events, NULL);
  ABORT_IF(status < static_cast<int64_t>(numWrites),
           "Should have at least %llu completed I/Os, but got %d",
           numWrites, status);

  // The events array contains information about completed writes, so iterate
  // over it, handling each write that completed.
  for (int i = 0; i < status; i++) {
    struct iocb* controlBlock = events[i].obj;
    long bytesWritten = events[i].res;
    long writeStatus = events[i].res2;

    // Verify that the write completed successfully.
    ABORT_IF(writeStatus != 0,
             "libaio write failed with error %d.", writeStatus);

    // Look up the buffer corresponding to this write.
    WriteMap::iterator iter = outstandingWriteBuffers.find(controlBlock);
    ASSERT(iter != outstandingWriteBuffers.end(), "Missing write map entry.");
    KVPairBuffer* buffer = iter->second;

    // Make sure the number of bytes written matches the size of write call.
    uint64_t writeSize = controlBlock->u.c.nbytes;
    ABORT_IF(static_cast<uint64_t>(bytesWritten) != writeSize,
             "Supposed to write %llu bytes but only wrote %li.",
             writeSize, bytesWritten);

    // Update the map and delete the control block.
    outstandingWriteBuffers.erase(iter);
    delete controlBlock;

    idleBuffers.push(buffer);
  }
}

uint64_t LibAIOWriter::numWritesInProgress() {
  return outstandingWriteBuffers.size();
}

BaseWorker* LibAIOWriter::newInstance(
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

  LibAIOWriter* writer = new LibAIOWriter(
    phaseName, stageName, id, params, dependencies, alignmentSize,
    asynchronousIODepth, maxWriteSize);

  return writer;
}
