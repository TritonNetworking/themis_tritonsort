#ifndef MAPRED_FIXED_SIZE_KV_PAIR_FORMAT_READER_H
#define MAPRED_FIXED_SIZE_KV_PAIR_FORMAT_READER_H

#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/workers/bytestreamconverter/FormatReaderInterface.h"

class ByteStreamConverter;
class KVPairBuffer;
class StreamInfo;

/**
   FixedSizeKVPairFormatReader reads tuples of a fixed key and value length into
   MapReduce-style kv pairs that contain length headers. One use case of this
   format reader is Graysort, where we have 10 byte keys and 90 byte values.
   Tuple headers can be created on the fly in the format reader, and removed
   right before the writer stage to save storage capacity and bandwidth.

   This format reader has two modes of operation depending on whether or not
   StreamInfo::setSize() was called on the input stream. If no size was set,
   then the format reader creates output buffers large enough to hold all the
   complete tuples in an input buffer, and emits these output buffers
   immediately. This is the expected phase one behavior. However, if the stream
   size has been set, the format reader allocates a single buffer on
   construction that will hold the entire stream. This behavior is targeted at
   phase two, where we will need to use a byte stream reader to read the file,
   but we still want buffers containing the entire file.
 */
class FixedSizeKVPairFormatReader : public FormatReaderInterface {
public:
  /// Constructor
  /**
     \param streamInfo information about the stream we're reading

     \param parentConverter the converter that created this format reader

     \param keyLength the length of keys to read from the buffer

     \param valueLength the length of values to read from the buffer
   */
  FixedSizeKVPairFormatReader(
    const StreamInfo& streamInfo, ByteStreamConverter& parentConverter,
    uint32_t keyLength, uint32_t valueLength);

  /// Destructor
  /**
     If the stream size has been set, the single buffer for the stream will be
     emitted here.
   */
  virtual ~FixedSizeKVPairFormatReader();

  /**
     Convert a byte stream buffer of fixed-size tuples into a key/value pair
     buffer of MapReduce tuples. If the stream size has been set, then this
     function doesn't emit any buffers. If the stream size has not been set,
     one output buffer is emitted per function invocation.

     \param buffer the input buffer containing fixed-size tuples, including
     including partial tuples on either end
   */
  void readByteStream(ByteStreamBuffer& buffer);

private:
  /**
     Get a new output buffer large enough to hold one MapReduce tuple per
     fixed-size tuple in the input buffer.

     \param inputBufferSize the size of the buffer containing fixed-size tuples
   */
  void getNewBuffer(uint64_t inputBufferSize);

  // Emit the current output buffer.
  void emitFullBuffer();

  const StreamInfo& streamInfo;
  const bool emitSingleBuffer;
  const uint64_t tupleSize;

  ByteStreamConverter& parentConverter;

  // Byte array that after construction contains a valid MapReduce tuple header
  // for a fixed-size tuple of the user-configured key and value lengths.
  uint8_t header[KeyValuePair::HEADER_SIZE];

  // Stream state:
  // Output buffer state
  KVPairBuffer* outputBuffer;
  const uint8_t* appendPtr;
  uint8_t* outputPtr;
  uint64_t outputBytes;

  // Partial tuple state.
  uint64_t partialBytes;
  // We'll need to dynamically create this array because we don't yet know how
  // big the tuples are.
  uint8_t* partialTuple;
};

#endif // MAPRED_FIXED_SIZE_KV_PAIR_FORMAT_READER_H
