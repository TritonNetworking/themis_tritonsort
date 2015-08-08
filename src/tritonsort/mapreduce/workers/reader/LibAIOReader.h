#ifndef MAPRED_LIBAIO_READER_H
#define MAPRED_LIBAIO_READER_H

#include <libaio.h>
#include <map>

#include "mapreduce/workers/reader/AsynchronousReader.h"

/**
   LibAIOReader is an AsynchronousReader that performs reads with the
   Linux native AIO library, libaio.
 */
class LibAIOReader : public AsynchronousReader {
  WORKER_IMPL

public:
  /// Constructor
  /**
     \param phaseName the name of the phase

     \param stageName the name of the reader stage

     \param id the id of the reader worker

     \param params the global Params object for the application

     \param dependencies the injected dependencies for this stage

     \param alignmentSize the alignment size for direct IO

     \param directIO if true, read with O_DIRECT

     \param asynchronousIODepth the maximum number of in-flight read operations

     \param maxReadSize the maximum size of an asynchronous read() call

     \param defaultBufferSize the default size of byte stream buffers

     \param filenameToStreamIDMap the global mapping from filename to stream ID

     \param memoryAllocator the allocator to use for creating buffers

     \param deleteAfterRead whether or not the reader should delete the file
     after reading and closing the file

     \param useByteStreamBuffers whether or not the reader should read into byte
     stream buffers

     \param tokenPool a token pool for mergereduce phase three

     \param chunkMap a chunk map for mergereduce phase three
   */
  LibAIOReader(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    Params& params, NamedObjectCollection& dependencies,
    uint64_t alignmentSize, bool directIO, uint64_t asynchronousIODepth,
    uint64_t maxReadSize, uint64_t defaultBufferSize,
    FilenameToStreamIDMap* filenameToStreamIDMap,
    MemoryAllocatorInterface& memoryAllocator, bool deleteAfterRead,
    bool useByteStreamBuffers, WriteTokenPool* tokenPool, ChunkMap* chunkMap);

  /// Destructor
  virtual ~LibAIOReader();

  /// Destroy the AIO context.
  void teardown();

private:
  typedef std::map<struct iocb*, const uint8_t*> BufferMap;

  /// \sa AsynchronousReader::prepareRead
  void prepareRead(const uint8_t* buffer, File& file, uint64_t readSize);

  /// \sa AsynchronousReader::issueNextRead
  bool issueNextRead(const uint8_t* buffer);

  /// \sa AsynchronousReader::WaitForReadsToComplete
  void waitForReadsToComplete(uint64_t numReads);

  /// \sa AsynchronousReader::numReadsInProgress
  uint64_t numReadsInProgress();

  std::queue<const uint8_t*> idleQueue;

  /// \sa AsynchronousReader::openFile
  void openFile(File& file);

  const uint64_t alignmentSize;
  const bool directIO;
  const uint64_t maxReadSize;

  BufferMap outstandingReadBuffers;

  io_context_t context;
  struct io_event* events;
};

#endif // MAPRED_LIBAIO_READER_H
