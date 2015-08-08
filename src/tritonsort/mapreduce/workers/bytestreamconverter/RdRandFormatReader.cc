#include <string.h>

#include "common/buffers/ByteStreamBuffer.h"
#include "mapreduce/common/StreamInfo.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/bytestreamconverter/ByteStreamConverter.h"
#include "mapreduce/workers/bytestreamconverter/RdRandFormatReader.h"

RdRandFormatReader::RdRandFormatReader(
  const StreamInfo& _streamInfo, ByteStreamConverter& _parentConverter)
  : streamInfo(_streamInfo),
    firstBuffer(true),
    parentConverter(_parentConverter) {
  // Set up the header.
  KeyValuePair::setKeyLength(header, 2 * FRAGMENT_SIZE);
  KeyValuePair::setValueLength(header, 0);
}

void RdRandFormatReader::readByteStream(ByteStreamBuffer& buffer) {
  uint64_t inputBufferSize = buffer.getCurrentSize();

  // We require that each buffer be 64-bit aligned for simplicity. This is going
  // to be the case anyway if direct I/O is used.
  ABORT_IF(inputBufferSize % FRAGMENT_SIZE != 0,
           "Buffer must be %llu byte aligned but has size %llu",
           FRAGMENT_SIZE, inputBufferSize);

  // Input buffer consists of 64-bit fragments.
  uint64_t numFragments = inputBufferSize / FRAGMENT_SIZE;

  // If we've seen at least one buffer before, then we've kept its last fragment
  // around, so use that for this buffer.
  if (!firstBuffer) {
    numFragments++;
  }

  // Output consecutive pairs. Every fragment except for the last one will be
  // the first half of a pair.
  uint64_t outputBufferSize =
    (numFragments - 1) * (KeyValuePair::HEADER_SIZE + (2 * FRAGMENT_SIZE));

  KVPairBuffer* outputBuffer = parentConverter.newBuffer(outputBufferSize);
  const uint8_t* appendPtr = outputBuffer->setupAppend(outputBufferSize);
  uint8_t* outputPtr = const_cast<uint8_t*>(appendPtr);

  const uint8_t* inputPtr = buffer.getRawBuffer();
  for (uint64_t i = 0; i < numFragments - 1; i++) {
    // Copy header to output buffer.
    outputPtr = reinterpret_cast<uint8_t*>(
      mempcpy(outputPtr, header, KeyValuePair::HEADER_SIZE));

    if (i == 0 && !firstBuffer) {
      // Use the previous fragment as the first half of the pair.
      outputPtr = reinterpret_cast<uint8_t*>(
        mempcpy(outputPtr, previousFragment, FRAGMENT_SIZE));
      // Use the first fragment from the input buffer as the second half of the
      // pair.
      outputPtr = reinterpret_cast<uint8_t*>(
        mempcpy(outputPtr, inputPtr, FRAGMENT_SIZE));
    } else {
      // Use two consecutive fragments from the input buffer.
      outputPtr = reinterpret_cast<uint8_t*>(
        mempcpy(outputPtr, inputPtr, 2 * FRAGMENT_SIZE));
      // Only advance the input buffer by 1 fragment so we capture every
      // consecutive pair.
      inputPtr += FRAGMENT_SIZE;
    }
  }

  // Copy the last fragment to the previousFragment buffer.
  memcpy(previousFragment, inputPtr, FRAGMENT_SIZE);
  firstBuffer = false;

  // Commit the copied bytes and emit the buffer.
  outputBuffer->commitAppend(appendPtr, outputBufferSize);
  parentConverter.emitBuffer(*outputBuffer, streamInfo);
}

