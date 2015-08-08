#ifndef TRITONSORT_COALESCER_H
#define TRITONSORT_COALESCER_H

#include <limits.h>

#include "common/BufferListContainer.h"
#include "common/buffers/BufferAligner.h"
#include "common/buffers/BufferList.h"
#include "core/MemoryUtils.h"
#include "core/SingleUnitRunnable.h"

/**
   The coalescer's responibility is to chain a bunch of small logical disk
   buffers together into a single contiguous buffer.
*/
template <typename In, typename Out, typename OutFactory> class Coalescer
  : public SingleUnitRunnable< BufferListContainer<In> > {
WORKER_IMPL

  public:
  /// Shorthand for a BufferListContainer that contains lists of the
  /// appropriate logical disk buffer type
  typedef BufferListContainer<In> IBLContainer;

  /// Constructor
  /**
     \param id the worker ID of this worker within its stage

     \param name the name of the stage for which the worker is working
  */
  Coalescer(uint64_t id, const std::string& name,
            MemoryAllocatorInterface& memoryAllocator,
            uint64_t defaultBufferSize, uint64_t alignmentSize,
            uint64_t _writeSizeMultiple)
    : SingleUnitRunnable<IBLContainer>(id, name),
      writeSizeMultiple(_writeSizeMultiple),
      bufferFactory(
        *this, memoryAllocator, defaultBufferSize, alignmentSize) {
  }

  /**
     Transfers the write token contained in the given IBLContainer to a writer
     buffer, then appends each of the container's buffers to that writer buffer
     in turn. Once all the container's buffers have been appended, they are
     returned to the appropriate buffer pool and the writer buffer is passed to
     the writer.

     \param container the container to process
     \sa SingleUnitRunnable::run
  */
  void run(IBLContainer* container) {
    BufferList<In>& list = container->getList();
    uint64_t logicalDiskID = list.getLogicalDiskID();
    uint64_t jobID = container->getJobID();

    // Find the buffer aligner for this job, creating it if it doesn't exist
    BufferAligner<Out>* aligner = NULL;
    typename BufferAlignerMap::iterator iter = aligners.find(jobID);

    if (iter != aligners.end()) {
      aligner = iter->second;
    } else {
      aligner = new (themis::memcheck) BufferAligner<Out>(writeSizeMultiple);
      aligners.insert(std::make_pair(jobID, aligner));
    }

    ASSERT(aligner != NULL, "Should have constructed an aligner by now");

    // Record this job ID and logical disk ID for teardown.
    logicalDiskIDs[jobID].insert(logicalDiskID);

    // Create a buffer large enough to contain the entire list's buffers and
    // any bytes left over from a previous unaligned write
    Out* buffer = getNewBuffer(
      logicalDiskID,
      list.getTotalDataSize() + aligner->getRemainingBytes(logicalDiskID),
      aligner);
    buffer->setToken(container->getToken());
    buffer->addJobID(jobID);

    In* head = list.removeHead();

    // Append the entire list onto the buffer one element at a time
    while (head != NULL) {
      ASSERT((head->getJobIDs()).size() == 1, "Expected buffers being "
             "coalesced to have exactly one job ID; this one had %llu",
             (head->getJobIDs()).size());

      ASSERT(*(head->getJobIDs().begin()) == jobID, "Expected all buffers "
             "being coalesced to have the same job ID");

      buffer->append(head->getRawBuffer(), head->getCurrentSize());

      // Delete the head of the list and grab the next buffer
      delete head;
      head = list.removeHead();
    }

    ASSERT(list.getSize() == 0, "List is not empty at the end of append loop");

    // Store any unaligned bytes in the last buffer so that the next buffer (or
    // teardown) can write them out later.
    aligner->finish(buffer, logicalDiskID);

    this->emitWorkUnit(buffer);

    delete container;
  }

  /**
     Cleanup method for the Coalescer. For each logical disk ID seen by this
     coalescer, check if the aligner has left over bytes for this LD and if so,
     emit a final, unaligned buffer for that LD. These final buffers have NULL
     write tokens, and therefore will be suboptimally placed in queues, but this
     is simpler than having the Coalescer check out tokens from the pool.
  */
  void teardown() {
    for (LogicalDiskIDMap::iterator jobIter = logicalDiskIDs.begin();
         jobIter != logicalDiskIDs.end(); jobIter++) {
      uint64_t jobID = jobIter->first;
      LogicalDiskIDSet& logicalDiskIDsForJob = jobIter->second;

      typename BufferAlignerMap::iterator alignerIter = aligners.find(jobID);

      ASSERT(alignerIter != aligners.end(), "Can't find a buffer aligner for "
             "job %llu", jobID);

      BufferAligner<Out>* aligner = alignerIter->second;

      for (LogicalDiskIDSet::iterator iter = logicalDiskIDsForJob.begin();
           iter != logicalDiskIDsForJob.end(); iter++) {
        uint64_t logicalDiskID = *iter;

        // If the aligner has extra bytes, emit a final buffer for this LD
        if (aligner->hasRemainingBytes(logicalDiskID)) {
          Out* buffer = getNewBuffer(
            logicalDiskID, aligner->getRemainingBytes(logicalDiskID), aligner);

          // Add the most recent job ID to this buffer's list of job IDs
          ASSERT(jobID > 0, "Job ID found to be unset during teardown");
          buffer->addJobID(jobID);

          // Let the aligner know that we are done with this logical disk
          aligner->finishLastBuffer(logicalDiskID);

          // Emit the buffer
          this->emitWorkUnit(buffer);
        }
      }

      delete aligner;
    }

    aligners.clear();
  }
private:
  typedef std::set<uint64_t> LogicalDiskIDSet;
  typedef std::map<uint64_t, LogicalDiskIDSet> LogicalDiskIDMap;
  typedef std::map< uint64_t, BufferAligner<Out>* > BufferAlignerMap;

  Out* getNewBuffer(
    uint64_t logicalDiskID, uint64_t bufferSize, BufferAligner<Out>* aligner) {
    Out* buffer;

    buffer = bufferFactory.newInstance(bufferSize);

    buffer->clear();
    buffer->setLogicalDiskID(logicalDiskID);
    buffer->setToken(NULL);

    // Add any data left over from the previous buffer
    aligner->prepare(buffer, logicalDiskID);

    return buffer;
  }

  const uint64_t writeSizeMultiple;

  OutFactory bufferFactory;
  BufferAlignerMap aligners;
  LogicalDiskIDMap logicalDiskIDs;
};

template <typename In, typename Out, typename OutFactory>
BaseWorker* Coalescer<In, Out, OutFactory>::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t defaultBufferSize = params.get<uint64_t>(
    "DEFAULT_BUFFER_SIZE." + phaseName + "." + stageName);

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  uint64_t writeSizeMultiple =
    params.getv<uint64_t>("WRITE_SIZE_MULTIPLE.%s", phaseName.c_str());

  Coalescer<In, Out, OutFactory>* coalescer =
    new Coalescer<In, Out, OutFactory>(
      id, stageName, memoryAllocator, defaultBufferSize, alignmentSize,
      writeSizeMultiple);

  return coalescer;
}

#endif // TRITONSORT_COALESCER_H
