#include <string.h>

#include "common/PartitionFile.h"
#include "common/buffers/ByteStreamBuffer.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/StreamInfo.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/bytestreamconverter/ByteStreamConverter.h"
#include "mapreduce/workers/bytestreamconverter/FixedSizeKVPairFormatReader.h"

FixedSizeKVPairFormatReader::FixedSizeKVPairFormatReader(
  const StreamInfo& _streamInfo, ByteStreamConverter& _parentConverter,
  uint32_t keyLength, uint32_t valueLength)
  : streamInfo(_streamInfo),
    emitSingleBuffer(
      streamInfo.getSize() != std::numeric_limits<uint64_t>::max()),
    tupleSize(keyLength + valueLength),
    parentConverter(_parentConverter),
    outputBuffer(NULL),
    partialBytes(0) {
  // Set up the header.
  KeyValuePair::setKeyLength(header, keyLength);
  KeyValuePair::setValueLength(header, valueLength);

  // Set up the partial tuple array.
  partialTuple = new (themis::memcheck) uint8_t[tupleSize];

  // If the stream has a size associated with it, allocate a single buffer to
  // hold the entire stream.
  if (emitSingleBuffer) {
    ASSERT(streamInfo.getSize() % tupleSize == 0,
           "Stream should only contain fixed-size tuples which are %llu bytes "
           "but stream has %llu bytes (not divisible).", tupleSize,
           streamInfo.getSize());

    getNewBuffer(streamInfo.getSize());
  }
}

FixedSizeKVPairFormatReader::~FixedSizeKVPairFormatReader() {
  if (emitSingleBuffer) {
    ASSERT(outputBytes == outputBuffer->getCapacity(),
           "Allocated a single buffer to emit of size %llu but only copied "
           "%llu bytes", outputBuffer->getCapacity(), outputBytes);
    emitFullBuffer();
  }

  // Sanity check
  ASSERT(outputBuffer == NULL, "Still have an output buffer after teardown.");

  if (partialTuple != NULL) {
    delete[] partialTuple;
    partialTuple = NULL;
  }
}

void FixedSizeKVPairFormatReader::readByteStream(ByteStreamBuffer& buffer) {
  uint64_t remainingInputBytes = buffer.getCurrentSize();
  const uint8_t* inputPtr = buffer.getRawBuffer();

  if (!emitSingleBuffer) {
    getNewBuffer(remainingInputBytes);
  }

  // Handle any partial bytes we had from the previous buffer.
  if (partialBytes > 0) {
    uint64_t partialInputBytes = tupleSize - partialBytes;
    outputPtr = reinterpret_cast<uint8_t*>(
      mempcpy(outputPtr, header, KeyValuePair::HEADER_SIZE));
    outputPtr = reinterpret_cast<uint8_t*>(
      mempcpy(outputPtr, partialTuple, partialBytes));
    outputPtr = reinterpret_cast<uint8_t*>(
      mempcpy(outputPtr, inputPtr, partialInputBytes));

    outputBytes += tupleSize + KeyValuePair::HEADER_SIZE;
    partialBytes = 0;
    inputPtr += partialInputBytes;
    remainingInputBytes -= partialInputBytes;
  }

  // Now handle all the complete tuples in the remaining part of the input
  //buffer.
  while (remainingInputBytes >= tupleSize) {
    outputPtr = reinterpret_cast<uint8_t*>(
      mempcpy(outputPtr, header, KeyValuePair::HEADER_SIZE));
    outputPtr = reinterpret_cast<uint8_t*>(
      mempcpy(outputPtr, inputPtr, tupleSize));
    inputPtr += tupleSize;
    outputBytes += tupleSize + KeyValuePair::HEADER_SIZE;
    remainingInputBytes -= tupleSize;
  }

  // Finally copy any remaining bytes to the partial tuple buffer.
  memcpy(partialTuple, inputPtr, remainingInputBytes);
  partialBytes = remainingInputBytes;

  if (!emitSingleBuffer) {
    emitFullBuffer();
  }
}

void FixedSizeKVPairFormatReader::getNewBuffer(uint64_t inputBufferSize) {
  ASSERT(outputBuffer == NULL,
         "Tried to get a new buffer when stream %llu already has one",
         streamInfo.getStreamID());

  uint64_t outputBufferSize =
    ((inputBufferSize + partialBytes) / tupleSize) *
    (tupleSize + KeyValuePair::HEADER_SIZE);
  outputBuffer = parentConverter.newBuffer(outputBufferSize);
  appendPtr = outputBuffer->setupAppend(outputBufferSize);
  outputPtr = const_cast<uint8_t*>(appendPtr);
  outputBytes = 0;
}

void FixedSizeKVPairFormatReader::emitFullBuffer() {
  // Commit the append to the output buffer which now contains only complete
  // MapReduce tuples.
  outputBuffer->commitAppend(appendPtr, outputBytes);
  outputBytes = 0;
  // Emit the output buffer. The converter will delete the input buffer for us.
  parentConverter.emitBuffer(*outputBuffer, streamInfo);
  outputBuffer = NULL;
}
