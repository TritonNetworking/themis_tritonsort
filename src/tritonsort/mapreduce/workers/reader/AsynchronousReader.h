#ifndef MAPRED_ASYNCHRONOUS_READER_H
#define MAPRED_ASYNCHRONOUS_READER_H

#include <map>

#include "common/ByteStreamBufferFactory.h"
#include "common/WriteTokenPool.h"
#include "core/MultiQueueRunnable.h"
#include "mapreduce/common/ChunkMap.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/ReadRequest.h"

class File;
class FilenameToStreamIDMap;

/**
   AsynchronousReader is a base class for PosixAIOReader and LibAIOReader. It
   implements the structural logic for an AIO reader as a MultiQueueRunnable. It
   issues reads asynchronously rather than blocking until reads complete. The
   parameter ASYNCHRONOUS_IO_DEPTH.phase_name.reader determines how many reads
   will be in flight at any given time. After this many reads have been issued,
   the reader waits until at least one read completes before issuing more.

   If there are no more work units in the queue, AsynchronousReader continues to
   process its existing files while checking in with the tracker at 10ms
   intervals. If the reader has no files to read and there are no buffers in
   the queue, it sleeps, while again checking in at 10ms intervals.
 */
class AsynchronousReader : public MultiQueueRunnable<ReadRequest> {

public:
  /// Constructor
  /**
     \param phaseName the name of the phase

     \param stageName the name of the reader stage

     \param id the id of the reader worker

     \param asynchronousIODepth the maximum number of in-flight read operations

     \param defaultBufferSize the default size of byte stream buffers

     \param alignmentSize the direct IO alignment size

     \param filenameToStreamIDMap the global mapping from filename to stream ID

     \param memoryAllocator the allocator to use for creating buffers

     \param deleteAfterRead whether or not the reader should delete the file
     after reading and closing the file

     \param useByteStreamBuffers whether or not the reader should read into byte
     stream buffers

     \param tokenPool a token pool for mergereduce phase three

     \param chunkMap a chunk map for mergereduce phase three
   */
  AsynchronousReader(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    uint64_t asynchronousIODepth, uint64_t defaultBufferSize,
    uint64_t alignmentSize, FilenameToStreamIDMap* filenameToStreamIDMap,
    MemoryAllocatorInterface& memoryAllocator, bool deleteAfterRead,
    bool useByteStreamBuffers, WriteTokenPool* tokenPool, ChunkMap* chunkMap);

  /// Destructor
  virtual ~AsynchronousReader() {}

  /// Issue reads asynchronously up to the given IO depth.
  void run();

  /// Checks that reads have been completed successfully.
  virtual void teardown();

protected:
  /// Data structure used for keeping track of in-flight reads.
  struct ReadInfo {
    BaseBuffer* buffer;
    File* file;
    ReadRequest* request;
    uint64_t size;
    WriteToken* token;
    uint64_t tokenID;
    ReadInfo(
      BaseBuffer* _buffer, File* _file, ReadRequest* _request, uint64_t _size,
      uint64_t _tokenID)
      : buffer(_buffer),
        file(_file),
        request(_request),
        size(_size),
        token(NULL),
        tokenID(_tokenID) {}
  };

  typedef std::map<const uint8_t*, ReadInfo*> ReadMap;

  /**
     Issue the next read into a given buffer.

     \param buffer the buffer into which to read

     \return true if a read was issued, and false if the buffer has been
     filled
   */
  virtual bool issueNextRead(const uint8_t* appendPointer) = 0;

  /// Wait for some read to complete.
  /**
     \param numReads the number of reads that must complete before this returns.
   */
  virtual void waitForReadsToComplete(uint64_t numReads) = 0;

  /// Issue new reads for any buffers that are idle but still not complete.
  void serviceIdleBuffers();

  /// Emit buffers whose reads have been completely executed.
  void emitFullBuffers();

  const uint64_t asynchronousIODepth;

  ReadMap reads;

  /// Buffers with completed reads that need to be re-scheduled for more reads.
  std::queue<const uint8_t*> idleBuffers;

private:
  typedef std::map<ReadRequest*, uint64_t> BytesReadMap;

  /// Prepare a buffer for asynchronous reading.
  /**
     \param appendPointer the raw location to read into

     \param file the file to read

     \param readSize the number of bytes to read
   */
  virtual void prepareRead(
    const uint8_t* appendPointer, File& file, uint64_t readSize) = 0;


  /// \return the number of reads that are in-progress
  virtual uint64_t numReadsInProgress() = 0;

  /// Open a file with the appropriate access mode.
  /**
     \param file the file to open
   */
  virtual void openFile(File& file) = 0;

  /// If the read request points to a non-empty file, set up internal
  /// data structures.
  /**
     \param readRequest the read request to process next

     \param[out] readInfo a data structure corresponding to the read

     \return true if the file is non-empty, false otherwise
   */
  bool processNewReadRequest(ReadRequest* readRequest, ReadInfo*& readInfo);

  /// Check out a new buffer and start reading.
  /**
     \param readInfo a data structure describing the read

     \param returnEmptyBuffer if true, this function just creates an empty
     buffer with the appropriate fields set and returns

     \return a pointer to the new buffer
   */
  BaseBuffer* startNextReadBuffer(
    ReadInfo* readInfo, bool returnEmptyBuffer=false);

  /// Wait for a read to complete and emit the associated read buffer, and
  /// possibly start reading into a new buffer if we're not done with the
  /// request yet.
  void waitForReadCompletionAndEmitBuffers();

  /// Try any paused reads by checking the token pool for available read tokens.
  void checkForReadTokens();

  StatLogger logger;

  const uint64_t readSizeStatID;
  const uint64_t asynchronousIODepthStatID;
  const bool deleteAfterRead;
  const bool useByteStreamBuffers;
  const bool setStreamSize;

  uint64_t alignedBytesRead;

  FilenameToStreamIDMap* const filenameToStreamIDMap;

  ByteStreamBufferFactory byteStreamBufferFactory;
  KVPairBufferFactory kvPairBufferFactory;

  BytesReadMap bytesRead;

  /// Buffers that are full and can be emitted.
  std::queue<const uint8_t*> fullBuffers;

  // Used for phase three.
  WriteTokenPool* tokenPool;
  std::map<uint64_t, uint64_t> offsetMap;
  std::map<uint64_t, ReadInfo*> waitingForToken;
  std::set<uint64_t> tokenIDSet;
};

#endif // MAPRED_ASYNCHRONOUS_READER_H
