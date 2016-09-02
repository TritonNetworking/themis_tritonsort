#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/writer/AsynchronousWriter.h"
#include "mapreduce/workers/writer/BaseWriter.h"

AsynchronousWriter::AsynchronousWriter(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  Params& params, NamedObjectCollection& dependencies,
  File::AccessMode asyncMode, uint64_t _asynchronousIODepth)
  : MultiQueueRunnable(id, stageName),
    asynchronousIODepth(_asynchronousIODepth),
    logger(stageName, id),
    writer(*(BaseWriter::newBaseWriter(
               phaseName, stageName, id, params, dependencies,
               asyncMode, logger))),
    asynchronousIODepthStatID(
      logger.registerHistogramStat("asynchronous_io_depth", 4)) {
}

AsynchronousWriter::~AsynchronousWriter() {
  delete &writer;
}

void AsynchronousWriter::run() {
  bool done = false;
  while (!done) {
    logger.add(asynchronousIODepthStatID, numWritesInProgress());

    KVPairBuffer* buffer = NULL;
    bool gotNewWork = attemptGetNewWork(getID(), buffer);

    if (gotNewWork) {
      if (buffer == NULL) {
        done = true;

        // Finish writing all existing buffers so we don't have an artificially
        // long teardown time.
        while (numWritesInProgress() > 0) {
          waitForWriteToComplete();
        }
      } else {
        // Wait until we have a free I/O slot, and then service a new buffer.
        while (numWritesInProgress() >= asynchronousIODepth) {
          // The maximum number of I/Os are in flight, so wait until at least
          // one completes in the hope that it frees up a slot.
          waitForWriteToComplete();
        }

        // Prepare the buffer for writing and issue the first write.
        prepareWrite(buffer);
        issueNextWrite(buffer);
      }
    } else {
      // No new writes to issue, so process our existing buffers until it's time
      // to go back to the tracker.
      Timer processTimer;
      processTimer.start();
      while (processTimer.getRunningTime() < 1000) {
        if (numWritesInProgress() == 0) {
          // There's no work, and no buffers to write, so sleep.
          uint64_t elapsed = processTimer.getRunningTime();
          if (elapsed < 1000) {
            // Sleep a little extra to make sure we don't have to do this twice.
            startWaitForWorkTimer();
            usleep(1100 - elapsed);
            stopWaitForWorkTimer();
          }
        } else {
          // Wait until some write completes.
          waitForWriteToComplete();
        }
      }
    }
  }
}

void AsynchronousWriter::teardown() {
  TRITONSORT_ASSERT(numWritesInProgress() == 0,
         "Should have finished all writes before tearing down.");

  // Teardown the BaseWriter which will fsync and close files.
  writer.teardown();
}

void AsynchronousWriter::serviceIdleBuffers() {
  while (!idleBuffers.empty()) {
    KVPairBuffer* buffer = idleBuffers.front();
    idleBuffers.pop();

    // If we still have part of this buffer left to write, issue another
    // asynchronous write.
    if (!issueNextWrite(buffer)) {
      // No more writes for this buffer.
      writer.logBufferWritten(buffer);
      delete buffer;
    }
  }
}

void AsynchronousWriter::waitForWriteToComplete() {
  // Wait for at least one write to complete
  waitForWritesToComplete(1);
  // Issue more writes for any buffers that had a completed write.
  serviceIdleBuffers();
}
