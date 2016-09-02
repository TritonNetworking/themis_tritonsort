#include "common/buffers/ByteStreamBuffer.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/StreamInfo.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/bytestreamconverter/ByteStreamConverter.h"
#include "mapreduce/workers/bytestreamconverter/KVPairFormatReader.h"

KVPairFormatReader::KVPairFormatReader(
  const StreamInfo& _streamInfo, ByteStreamConverter& _parentConverter)
  : streamInfo(_streamInfo),
    parentConverter(_parentConverter),
    headerBytesNeeded(0),
    overflowBuffer(NULL) {
}

KVPairFormatReader::~KVPairFormatReader() {
  if (overflowBuffer != NULL) {
    delete overflowBuffer;
    overflowBuffer = NULL;
  }
}

void KVPairFormatReader::readByteStream(ByteStreamBuffer& buffer) {
  // Check to see if we were in the middle of reading a header at the end of
  // the last buffer for this stream.
  if (headerBytesNeeded > 0) {
    TRITONSORT_ASSERT(overflowBuffer == NULL,
           "Should not have overflow buffer when reading header.");

    // Try to read header bytes.
    uint64_t headerBytes = std::min<uint64_t>(
      headerBytesNeeded, buffer.getCurrentSize());
    memcpy(
      header + KeyValuePair::HEADER_SIZE - headerBytesNeeded,
      buffer.getRawBuffer(), headerBytes);
    headerBytesNeeded -= headerBytes;

    if (headerBytesNeeded == 0) {
      // Complete header. Get a new overflow buffer and copy the header.
      uint64_t tupleSize = KeyValuePair::tupleSize(header);
      newOverflowBuffer(tupleSize);

      const uint8_t* appendPtr =
        overflowBuffer->setupAppend(KeyValuePair::HEADER_SIZE);
      memcpy(
        const_cast<uint8_t*>(appendPtr), header,
        KeyValuePair::HEADER_SIZE);
      overflowBuffer->commitAppend(
        appendPtr, KeyValuePair::HEADER_SIZE);
    }

    // Seek past the partial header.
    buffer.seekForward(headerBytes);
    if (buffer.empty()) {
      return;
    }
  }

  // Copy overflow bytes.
  if (overflowBuffer != NULL) {
    // Copy up to the end of the overflow buffer
    uint64_t overflowBytes = overflowBuffer->getCapacity() -
      overflowBuffer->getCurrentSize();
    // Don't copy beyond the input buffer.
    overflowBytes = std::min<uint64_t>(overflowBytes, buffer.getCurrentSize());

    const uint8_t* appendPtr = overflowBuffer->setupAppend(overflowBytes);
    memcpy(
      const_cast<uint8_t*>(appendPtr), buffer.getRawBuffer(), overflowBytes);
    overflowBuffer->commitAppend(appendPtr, overflowBytes);

    // If we filled the overflow buffer, emit it.
    if (overflowBuffer->full()) {
      parentConverter.emitBuffer(*overflowBuffer, streamInfo);
      overflowBuffer = NULL;
    }

    // Seek past the overflow bytes.
    buffer.seekForward(overflowBytes);
    if (buffer.empty()) {
      return;
    }
  }

  // At this point we should not have an overflow buffer, since we either filled
  // it and emitted it, or we used up the entire buffer and returned.
  TRITONSORT_ASSERT(overflowBuffer == NULL, "Should not have an overflow buffer.");

  // Scan to the end of the last complete tuple.
  uint8_t* rawBuffer = const_cast<uint8_t*>(buffer.getRawBuffer());
  uint64_t bufferSize = buffer.getCurrentSize();
  uint64_t completeBytes = 0;
  uint64_t tupleSize = 0;
  // Scan until we can no longer read a header.
  while (completeBytes + KeyValuePair::HEADER_SIZE <= bufferSize) {
    tupleSize = KeyValuePair::tupleSize(rawBuffer + completeBytes);

    if (completeBytes + tupleSize > bufferSize) {
      // Partial tuple
      break;
    }

    // Complete tuple
    completeBytes += tupleSize;
  }

  uint64_t bytesRemaining = bufferSize - completeBytes;
  uint8_t* partialTuple = rawBuffer + completeBytes;

  if (bytesRemaining > 0) {
    if (bytesRemaining < KeyValuePair::HEADER_SIZE) {
      // We only have a partial header.
      memcpy(header, partialTuple, bytesRemaining);
      headerBytesNeeded = KeyValuePair::HEADER_SIZE - bytesRemaining;
    } else {
      // There's a partial tuple at the end. Get a buffer of the appropriate
      // size and fill it with the partial tuple.
      newOverflowBuffer(tupleSize);

      const uint8_t* appendPtr = overflowBuffer->setupAppend(bytesRemaining);
      memcpy(const_cast<uint8_t*>(appendPtr), partialTuple, bytesRemaining);
      overflowBuffer->commitAppend(appendPtr, bytesRemaining);
    }
  }

  // Truncate the buffer to contain only complete bytes.
  buffer.setCurrentSize(completeBytes);

  // Create a KVPairBuffer to emit using the same underlying memory region.
  KVPairBuffer* kvPairBuffer = new (themis::memcheck) KVPairBuffer(NULL, 0, 0);
  kvPairBuffer->stealMemory(buffer);

  // Emit the new buffer.
  parentConverter.emitBuffer(*kvPairBuffer, streamInfo);
}
