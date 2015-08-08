#ifndef MAPRED_BYTE_STREAM_READER_H
#define MAPRED_BYTE_STREAM_READER_H

#include <stdint.h>

#include "common/ByteStreamBufferFactory.h"
#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/ReadRequest.h"

class FilenameToStreamIDMap;

/**
   ByteStreamReader reads a stream of bytes from a file into a collection of
   ByteStreamBuffers.
 */
class ByteStreamReader : public SingleUnitRunnable<ReadRequest> {
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

     \param filenameToStreamIDMap two-way mapping between filenames and stream
     IDs, used to tag buffers with an appropriate stream ID

     \param memoryAllocator a memory allocator that the worker will use to
     allocate memory for buffers

     \param bufferSize the size of the buffers that this worker will allocate
     in bytes

     \param deleteAfterRead whether or not the reader should delete the file
     after reading and closing the file

     \param setStreamSize whether not the reader should set the size of new
     streams it creates for input files
   */
  ByteStreamReader(
    uint64_t id, const std::string& name, uint64_t maxReadSize,
    uint64_t alignmentSize, bool directIO,
    FilenameToStreamIDMap& filenameToStreamIDMap,
    MemoryAllocatorInterface& memoryAllocator, uint64_t bufferSize,
    bool deleteAfterRead, bool setStreamSize);

  /**
     Read data from a file into buffers.

     \param inputWorkUnit a ReadRequest containing information about the
     file being read
   */
  void run(ReadRequest* inputWorkUnit);

  void teardown();

private:
  /// Create a buffer and configure it for proper emission
  /**
     \param inputWorkUnit the ReadRequest containing information about the
     file being read. Information from this object is used to configure the
     buffer's metadata
   */
  ByteStreamBuffer* getNewConfiguredBuffer(ReadRequest& inputWorkUnit);

  const uint64_t maxReadSize;
  const uint64_t alignmentSize;
  const bool directIO;
  const bool deleteAfterRead;
  const bool setStreamSize;

  uint64_t readTimeStatID;
  uint64_t readSizeStatID;
  uint64_t alignedBytesRead;

  Timer readTimer;
  StatLogger logger;

  FilenameToStreamIDMap& filenameToStreamIDMap;
  ByteStreamBufferFactory bufferFactory;
};

#endif // MAPRED_BYTE_STREAM_READER_H
