#ifndef THEMIS_WHOLE_FILE_READER_H
#define THEMIS_WHOLE_FILE_READER_H

#include <stdint.h>

#include "core/SingleUnitRunnable.h"
#include "core/StatLogger.h"
#include "mapreduce/common/KVPairBufferFactory.h"

/**
   A WholeFileReader will read an entire file into a single KVPairBuffer.
 */
class WholeFileReader : public SingleUnitRunnable<ReadRequest> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the unique ID of this worker within its parent stage

     \param name the name of the worker's parent stage

     \param maxReadSize the maximum size of a read() syscall

     \param alignmentSize the alignment to use for direct IO, or 0 if not
     reading with direct IO

     \param directIO if true, read with O_DIRECT

     \param memoryAllocator a memory allocator that the worker will use to
     allocate memory for buffers

     \param deleteAfterRead whether or not the reader should delete the file
     after reading and closing the file
   */
  WholeFileReader(uint64_t id, const std::string& name, uint64_t maxReadSize,
                  uint64_t alignmentSize, bool directIO,
                  MemoryAllocatorInterface& memoryAllocator,
                  bool deleteAfterRead);

  /// Read the given file entirely into a single KVPairBuffer
  /**
     \param readRequest a data structure with information about the current
     file
   */
  void run(ReadRequest* readRequest);

  void teardown();

private:
  const uint64_t maxReadSize;
  const uint64_t alignmentSize;
  const bool directIO;
  const bool deleteAfterRead;

  uint64_t readTimeStatID;
  uint64_t readSizeStatID;
  uint64_t alignedBytesRead;

  Timer readTimer;
  StatLogger logger;

  KVPairBufferFactory bufferFactory;
};

#endif // THEMIS_WHOLE_FILE_READER_H
