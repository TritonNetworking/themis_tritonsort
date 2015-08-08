#ifndef THEMIS_MAPRED_HDFS_READER_H
#define THEMIS_MAPRED_HDFS_READER_H

#include <stdint.h>

#include "common/ByteStreamBufferFactory.h"
#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/ReadRequest.h"

class FilenameToStreamIDMap;

/**
   HDFSReader reads file data from HDFS
 */
class HDFSReader : public SingleUnitRunnable<ReadRequest> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the unique ID of this worker within its parent stage

     \param name the name of the worker's parent stage

     \param filenameToStreamIDMap two-way mapping between filenames and stream
     IDs, used to tag buffers with an appropriate stream ID

     \param numDownstreamConverters the number of ByteStreamConverter workers
     in the next stage; required so that this worker can deterministically
     route its output to a single ByteStreamConverter

     \param memoryAllocator a memory allocator that the worker will use to
     allocate memory for buffers

     \param byteStreamBufferSize the size of the buffers that this worker will
     allocate in bytes

     \param wholeFileInSingleBuffer files should be received to a single buffer
   */
  HDFSReader(
    uint64_t id, const std::string& name,
    FilenameToStreamIDMap* filenameToStreamIDMap,
    uint64_t numDownstreamConverters, MemoryAllocatorInterface& memoryAllocator,
    uint64_t byteStreamBufferSize, bool wholeFileInSingleBuffer);

  /// Destructor
  virtual ~HDFSReader();

  /// Give the resource monitor information about the number of bytes and work
  /// units produced
  void resourceMonitorOutput(Json::Value& obj);

  /// Read data from HDFS into buffers
  void run(ReadRequest* request);

  /**
     Handle a read by the HDFS client

     \param resultBytes a pointer to the HDFS client's receive buffer
     containing the bytes to be read

     \param size the size of the receive buffer in bytes

     \return the number of receive buffer bytes that this method was able to
     handle immediately
   */
  uint64_t handleHDFSRead(void* resultBytes, uint64_t size);

private:
  void emitCurrentBuffer(bool deleteEmptyBuffers = true);


  const uint64_t numDownstreamConverters;
  const bool wholeFileInSingleBuffer;

  uint64_t totalBytesProduced;
  uint64_t currentBytesProduced;

  std::string path;
  uint64_t fileSize;

  FilenameToStreamIDMap* filenameToStreamIDMap;
  ByteStreamBufferFactory byteStreamBufferFactory;
  KVPairBufferFactory kvPairBufferFactory;

  BaseBuffer* currentBuffer;
};

#endif // THEMIS_MAPRED_HDFS_READER_H
