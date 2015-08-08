#ifndef MAPRED_KVPAIR_FORMAT_READER_H
#define MAPRED_KVPAIR_FORMAT_READER_H

#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/workers/bytestreamconverter/FormatReaderInterface.h"

class ByteStreamConverter;
class KVPairBuffer;
class StreamInfo;

/**
   KVPairFormatReader interprets bytes as key value pairs with 8 byte headers.
   It is optimized to reduce the number of memcpy() calls. Partial tuples on
   either end of an input buffer are copied to a separate overflow buffer that
   contains only a single tuple. All of the complete tuples in the middle of the
   buffer are emitted without copying using seekForward() and setCurrentSize()
   to chop the front and the end off of the input buffer.

   Buffers are processed according to the following steps:
     1.  Check if we were in the middle of reading a header. If so, finish
         reading the header and create out a new overflow buffer of the right
         size.
     2.  Check if we were in the middle of a partial tuple, and if so copy it to
         the overflow buffer. Emit the overflow buffer if we can fill it up.
     3.  Scan over all of the complete tuples in the buffer.
     4a. If there is a partial tuple left in the buffer, create a new overflow
         buffer of the right size and copy the remaining bytes.
     4b. If there is only a partial header left in the buffer, copy it to a
         separate header buffer, which we will fill in step 1 of the next buffer
         in this stream.
     5.  Emit the input ByteStreamBuffer, which now contains only complete
         tuples, as a KVPairBuffer using KVPairBuffer's copy constructor, which
         passes the underlying memory region without copying.
 */
class KVPairFormatReader : public FormatReaderInterface {
public:
  /// Constructor
  /**
     \param streamInfo the stream that this format reader is associated with

     \param parentConverter the ByteStreamConverter worker
   */
  KVPairFormatReader(
    const StreamInfo& streamInfo, ByteStreamConverter& parentConverter);

  /// Destructor
  virtual ~KVPairFormatReader();

  /**
     Convert a stream of bytes formatted as key/value pairs into buffers
     containing only complete tuples.

     \param buffer the byte stream buffer to read as key/value pairs
   */
  void readByteStream(ByteStreamBuffer& buffer);

private:
  inline KVPairBuffer* newOverflowBuffer(uint64_t size) {
    overflowBuffer = parentConverter.newBuffer(size);

    return overflowBuffer;
  }

  const StreamInfo& streamInfo;

  ByteStreamConverter& parentConverter;

  // Stream state
  uint8_t header[KeyValuePair::HEADER_SIZE];
  uint64_t headerBytesNeeded;
  KVPairBuffer* overflowBuffer;
};

#endif // MAPRED_KVPAIR_FORMAT_READER_H
