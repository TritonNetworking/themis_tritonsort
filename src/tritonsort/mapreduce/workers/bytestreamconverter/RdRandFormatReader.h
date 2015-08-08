#ifndef MAPRED_RDRAND_FORMAT_READER_H
#define MAPRED_RDRAND_FORMAT_READER_H

#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/workers/bytestreamconverter/FormatReaderInterface.h"

#define FRAGMENT_SIZE sizeof(uint64_t)

class ByteStreamConverter;
class StreamInfo;

/**
   RdRandFormatReader reads files consisting of pairs 64-bit integers
   generated as fragments of a RDRAND instruction. Because we can't be sure
   which two integers make up the full 128-bit random number, this format reader
   emits all consecutive pairs.

   For simplicity, this format reader requires that all input buffers be 64-bit
   aligned, so that every buffer starts and ends on a 64-bit fragment boundary.
   In practice this requirement is not a problem because we're likely going to
   use 512-byte aligned direct I/O for input buffers anyway.
 */
class RdRandFormatReader : public FormatReaderInterface {
public:
  /// Constructor
  RdRandFormatReader(
    const StreamInfo& streamInfo, ByteStreamConverter& parentConverter);

  /// Destructor
  virtual ~RdRandFormatReader() {}

  /**
     Read a buffer consisting of 64-bit fragments and emit every consecutive
     128-bit pair.

     \param buffer the input buffer containing 64-bit fragments
   */
  void readByteStream(ByteStreamBuffer& buffer);

private:
  const StreamInfo& streamInfo;

  bool firstBuffer;
  ByteStreamConverter& parentConverter;

  uint8_t header[KeyValuePair::HEADER_SIZE];
  uint8_t previousFragment[FRAGMENT_SIZE];
};

#endif // MAPRED_RDRAND_FORMAT_READER_H
