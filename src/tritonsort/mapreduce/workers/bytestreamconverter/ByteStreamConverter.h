#ifndef MAPRED_BYTE_STREAM_CONVERTER_H
#define MAPRED_BYTE_STREAM_CONVERTER_H

#include "common/PartitionFile.h"
#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/FilenameToStreamIDMap.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/workers/bytestreamconverter/FormatReaderFactory.h"

class ByteStreamBuffer;
class KVPairBuffer;
class StreamInfo;

/**
   ByteStreamConverter is a worker that processes a raw stream of bytes and
   converts it to key/value pairs that can be understood by the rest of the
   MapReduce pipeline. As such, it should be placed after a Reader stage to read
   various file formats.
 */
class ByteStreamConverter : public SingleUnitRunnable<ByteStreamBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker id

     \param stageName the worker stage name

     \param memoryAllocator the memory allocator to use for buffer creation

     \param defaultBufferSize the default buffer size

     \param alignmentSize if non-zero, buffers will be aligned to be a multiple
     of this size

     \param filenameToStreamIDMap a map linking files to streams

     \param formatReaderImpl the format reader implementation to use

     \param params the global params object

     \param phaseName the name of the phase
   */
  ByteStreamConverter(
    uint64_t id, const std::string& stageName,
    MemoryAllocatorInterface& memoryAllocator, uint64_t defaultBufferSize,
    uint64_t alignmentSize, FilenameToStreamIDMap& filenameToStreamIDMap,
    const std::string& formatReaderImpl, const Params& params,
    const std::string& phaseName);

  /**
     Verifies that all streams have been closed properly.
   */
  void teardown();

  /**
     Read a buffer of raw bytes and convert it to key/value pairs using a format
     reader.

     \param buffer the byte stream buffer to convert
   */
  void run(ByteStreamBuffer* buffer);

  /**
     Callback for format readers that gets a new buffer of a given size.

     \param the size of buffer to create

     \return a new buffer of the specified size
   */
  KVPairBuffer* newBuffer(uint64_t size);

  /**
     Callback for format readers that gets a new buffer of at least a given
     size.

     \param size the minimum size buffer to return

     \return a buffer whose size is the maximum of the requested size and the
     default buffer size
   */
  KVPairBuffer* newBufferAtLeastAsLargeAs(uint64_t size);

  /**
     Callback for format readers that emits a buffer downstream

     \param buffer the buffer to emit

     \param streamInfo the stream that this buffer belongs to
   */
  void emitBuffer(KVPairBuffer& buffer, const StreamInfo& streamInfo);

private:
  typedef std::map<uint64_t, FormatReaderInterface*> FormatReaderMap;

  KVPairBufferFactory bufferFactory;

  FilenameToStreamIDMap& filenameToStreamIDMap;

  FormatReaderFactory formatReaderFactory;

  FormatReaderMap formatReaders;
};

#endif // MAPRED_BYTE_STREAM_CONVERTER_H
