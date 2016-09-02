#include "common/buffers/ByteStreamBuffer.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/StreamInfo.h"
#include "mapreduce/workers/bytestreamconverter/ByteStreamConverter.h"
#include "mapreduce/workers/bytestreamconverter/TextLineFormatReader.h"

TextLineFormatReader::TextLineFormatReader(
  const StreamInfo& _streamInfo, ByteStreamConverter& _parentConverter)
  : streamInfo(_streamInfo),
    filename(_streamInfo.getFilename()),
    parentConverter(_parentConverter),
    outputBuffer(NULL) {
}

TextLineFormatReader::~TextLineFormatReader() {
  // If the stream terminated without a newline, write any remaining characters.
  writeLine();

  // If we still have a buffer, emit it.
  if (outputBuffer != NULL) {
    parentConverter.emitBuffer(*outputBuffer, streamInfo);
    outputBuffer = NULL;
  }
}

void TextLineFormatReader::readByteStream(ByteStreamBuffer& buffer) {
  const uint8_t* rawBuffer = buffer.getRawBuffer();

  for (uint64_t i = 0; i < buffer.getCurrentSize(); i++) {
    // Check for line delimiter.
    if (rawBuffer[i] == '\n' ) {
      // We hit a newline.
      // Support Windows-style line delimiters by stripping a previous '\r'.
      if (!lineBuffer.empty() && lineBuffer.back() == '\r') {
        lineBuffer.pop_back();
      }

      writeLine();
    } else {
      // Not a newline. Copy character to line buffer.
      lineBuffer.push_back(rawBuffer[i]);
    }
  }
}

void TextLineFormatReader::writeLine() {
  if (!lineBuffer.empty()) {
    // Line is non-empty. Create a key/value pair with the filename as the
    // key and the line as the value.
    uint64_t tupleSize =
      KeyValuePair::tupleSize(filename.size(), lineBuffer.size());

    if (outputBuffer != NULL &&
        outputBuffer->getCapacity() - outputBuffer->getCurrentSize() <
        tupleSize) {
      // Output buffer isn't large enough for this tuple, so emit it.
      parentConverter.emitBuffer(*outputBuffer, streamInfo);
      outputBuffer = NULL;
    }

    if (outputBuffer == NULL) {
      // We don't have an output buffer, so get one at least as large as the
      // tuple size.
      outputBuffer = parentConverter.newBufferAtLeastAsLargeAs(tupleSize);
    }


    uint8_t* lineKey = NULL;
    uint8_t* lineValue = NULL;

    outputBuffer->setupAppendKVPair(
      filename.size(), lineBuffer.size(), lineKey, lineValue);

    // Write key
    memcpy(lineKey, filename.c_str(), filename.size());
    // Write value
    memcpy(lineValue, lineBuffer.data(), lineBuffer.size());

    outputBuffer->commitAppendKVPair(
      lineKey, lineValue, lineBuffer.size());

    lineBuffer.clear();
  }
}
