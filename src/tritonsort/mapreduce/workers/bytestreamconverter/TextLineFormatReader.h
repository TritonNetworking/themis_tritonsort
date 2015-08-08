#ifndef MAPRED_TEXT_LINE_FORMAT_READER_H
#define MAPRED_TEXT_LINE_FORMAT_READER_H

#include <stdint.h>
#include <string>
#include <vector>

#include "mapreduce/workers/bytestreamconverter/FormatReaderInterface.h"

class KVPairBuffer;
class ByteStreamConverter;
class StreamInfo;

/**
   TextLineFormatReader interprets bytes as ASCII characters. It creates buffers
   containing key/value pairs where the key is the name of the source file and
   the value is a line of text from the stream delimited by a newline character.
   Emitted lines do NOT contain trailing newline characters. Both
   Unix-style (\n) and Windows-style (\r\n) newlines are supported, and lines
   may span input buffers. Empty lines are skipped. When the stream is closed,
   any remaining characters are emitted as line even if there is no trailing
   newline character.
 */
class TextLineFormatReader : public FormatReaderInterface {
public:
  /// Constructor
  /**
     \param streamInfo the stream that this format reader is associated with

     \param parentConverter the ByteStreamConverter worker
   */
  TextLineFormatReader(
    const StreamInfo& streamInfo, ByteStreamConverter& parentConverter);

  /// Destructor
  /**
     Emits any remaining text as a line even if there was not trailing newline.
   */
  ~TextLineFormatReader();

  /**
     Convert a stream of bytes formatted as ASCII characters into buffers
     containing key/value pairs where the key is the source filename and the
     value is a line of text WITHOUT newline characters.

     \param buffer the byte stream buffer to read as ASCII text
   */
  void readByteStream(ByteStreamBuffer& buffer);

private:
  /**
     Write a line of text accumulated in the line buffer to a KVPairBuffer.
   */
  void writeLine();

  const StreamInfo& streamInfo;
  const std::string& filename;

  ByteStreamConverter& parentConverter;

  KVPairBuffer* outputBuffer;

  std::vector<uint8_t> lineBuffer;
};

#endif // MAPRED_TEXT_LINE_FORMAT_READER_H
