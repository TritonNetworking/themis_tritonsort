#include "core/MemoryUtils.h"
#include "mapreduce/common/PartialKVPairWriter.h"
#include "mapreduce/common/PartitionFunctionInterface.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

PartialKVPairWriter::PartialKVPairWriter(
  EmitBufferCallback _emitBufferCallback, GetBufferCallback _getBufferCallback,
  bool _writeWithoutHeaders, bool _partialSerialize)
  : emitBufferCallback(_emitBufferCallback),
    getBufferCallback(_getBufferCallback),
    writeWithoutHeaders(_writeWithoutHeaders),
    partialSerialize(_partialSerialize),
    partitionFunction(NULL),
    localPartitioning(false),
    partitionGroup(0),
    partitionOffset(0),
    setupKeyLength(0),
    setupMaxValueLength(0),
    setupPartition(0),
    setupTuple(NULL),
    temporaryBuffer(NULL),
    tuplesWritten(0),
    bytesWritten(0) {
  // Initially assume 1 partition.
  appendInfos.push_back(new AppendInfo());
}

PartialKVPairWriter::~PartialKVPairWriter() {
  for (std::vector<AppendInfo*>::iterator iter = appendInfos.begin();
       iter != appendInfos.end(); iter++) {
    // We should have flushed all buffers.
    ASSERT((*iter)->buffer == NULL,
           "Should have flushed all buffers before deleting the writer.");
    delete *iter;
  }
}

void PartialKVPairWriter::setPartitionFunction(
  PartitionFunctionInterface& _partitionFunction, uint64_t numPartitions,
  bool _localPartitioning, uint64_t _partitionGroup,
  uint64_t _partitionOffset) {
  partitionFunction = &_partitionFunction;
  localPartitioning = _localPartitioning;
  partitionGroup = _partitionGroup;
  partitionOffset = _partitionOffset;

  // Extend appendInfos up to the new number of partitions.
  for (uint64_t i = appendInfos.size(); i < numPartitions; i++) {
    appendInfos.push_back(new AppendInfo());
  }
}

void PartialKVPairWriter::write(KeyValuePair& kvPair) {
  kvPair.setWriteWithoutHeader(writeWithoutHeaders);

  // Compute the key's partition.
  uint64_t partition = 0;
  if (partitionFunction != NULL) {
    if (localPartitioning) {
      partition = partitionFunction->localPartition(
        kvPair.getKey(), kvPair.getKeyLength(), partitionGroup) -
        partitionOffset;
    } else {
      partition = partitionFunction->globalPartition(
        kvPair.getKey(), kvPair.getKeyLength()) - partitionOffset;
    }
  }

  // Get fields associated with this partition.
  AppendInfo*& appendInfo = appendInfos.at(partition);
  KVPairBuffer*& buffer = appendInfo->buffer;
  uint8_t*& appendPointer = appendInfo->pointer;
  uint64_t& appendLength = appendInfo->length;

  if (buffer == NULL) {
    // Get a new buffer.
    getBuffer(partition);
  }

  // Try to append the whole tuple
  uint64_t tupleSize = kvPair.getWriteSize();
  if (appendLength + tupleSize <= buffer->getCapacity()) {
    // This tuple will fit entirely in the buffer, so copy it.
    kvPair.serialize(appendPointer + appendLength);
    appendLength += tupleSize;
  } else if (partialSerialize) {
    // The tuple is too large for the buffer.
    // Copy whatever bit of the tuple we can before getting a new buffer.
    uint64_t bytesSerialized = 0;
    do {
      // Perform a partial serialize up to the length of the current buffer.
      uint64_t serializeLength = std::min(
        tupleSize - bytesSerialized,
        buffer->getCapacity() - appendLength);
      kvPair.partialSerialize(
        appendPointer + appendLength, bytesSerialized, serializeLength);
      bytesSerialized += serializeLength;
      appendLength += serializeLength;

      // Only emit if the buffer is exactly full.
      if (appendLength == buffer->getCapacity()) {
        emitBuffer(partition);
        // Get a new default sized buffer.
        getBuffer(partition);
      }
    } while (bytesSerialized < tupleSize);
  } else {
    // Partial serialization is disabled. Emit the buffer, get a new one.
    emitBuffer(partition);
    // Get a new default sized buffer.
    getBuffer(partition);

    ASSERT(appendLength + tupleSize <= buffer->getCapacity(),
           "New buffer is only %llu bytes but need %llu",
           buffer->getCapacity(), tupleSize);

    kvPair.serialize(appendPointer + appendLength);
    appendLength += tupleSize;
  }

  ++tuplesWritten;
  bytesWritten += tupleSize;
}

uint8_t* PartialKVPairWriter::setupWrite(
  const uint8_t* key, uint32_t keyLength, uint32_t maxValueLength) {

  ASSERT(temporaryBuffer == NULL,
         "We should not have allocated a temporary buffer before setupWrite()");

  // Compute the key's partition.
  setupPartition = 0;
  if (partitionFunction != NULL) {
    if (localPartitioning) {
      setupPartition = partitionFunction->localPartition(
        key, keyLength, partitionGroup) - partitionOffset;
    } else {
      setupPartition = partitionFunction->globalPartition(key, keyLength) -
        partitionOffset;
    }
  }

  // Get fields associated with this partition.
  AppendInfo*& appendInfo = appendInfos.at(setupPartition);
  KVPairBuffer*& buffer = appendInfo->buffer;
  uint8_t*& appendPointer = appendInfo->pointer;
  uint64_t& appendLength = appendInfo->length;

  if (buffer == NULL) {
    // Get a new buffer.
    getBuffer(setupPartition);
  }

  uint64_t tupleSize = KeyValuePair::tupleSize(
    keyLength, maxValueLength, !writeWithoutHeaders);

  // Determine where in memory to write this tuple. If the tuple will fit in the
  // current buffer, just write it there.
  setupTuple = appendPointer + appendLength;
  if (appendLength + tupleSize > buffer->getCapacity()) {
    // This tuple is going to overflow the buffer if the user writes up to the
    // maximum value length, so we can't just hand the user a pointer into the
    // buffer.
    //
    // Instead, malloc a region of memory large enough to store the entire
    // tuple, and then we'll deal with the buffer when the user commits the
    // tuple. This might be slow, but we're hoping that this won't be the common
    // case.
    temporaryBuffer = new (themis::memcheck) uint8_t[tupleSize];
    setupTuple = temporaryBuffer;
  }

  // Fetch key and value pointers from wherever we decided to write the tuple.
  uint8_t* keyPointer;
  uint8_t* valuePointer;
  KeyValuePair::getKeyValuePointers(
    setupTuple, keyLength, keyPointer, valuePointer, writeWithoutHeaders);

  // Write the key.
  memcpy(keyPointer, key, keyLength);

  // Store information about this setupWrite() call
  setupKeyLength = keyLength;
  setupMaxValueLength = maxValueLength;

  // Return the value.
  return valuePointer;
}

void PartialKVPairWriter::commitWrite(uint32_t valueLength) {
  // Sanity checks
  ASSERT(setupTuple != NULL, "Cannot call commitWrite before setupWrite()");
  ASSERT(valueLength <= setupMaxValueLength,
         "Got value length %llu but max value length is %llu",
         valueLength, setupMaxValueLength);

  if (!writeWithoutHeaders) {
    // Update the header for the new value length.
    KeyValuePair::setValueLength(setupTuple, valueLength);
  }

  uint64_t tupleSize = KeyValuePair::tupleSize(
    setupKeyLength, valueLength, !writeWithoutHeaders);

  // Get fields associated with this partition.
  AppendInfo*& appendInfo = appendInfos.at(setupPartition);
  KVPairBuffer*& buffer = appendInfo->buffer;
  uint8_t*& appendPointer = appendInfo->pointer;
  uint64_t& appendLength = appendInfo->length;

  if (temporaryBuffer != NULL) {
    // The user ended up writing data to a temporary buffer because the tuple
    // was too large to fit in the current buffer. Repeatedly copy as much of
    // tuple as we can into buffers until we run out of tuple bytes.
    uint64_t bytesCopied = 0;
    do {
      uint64_t bytesToCopy = std::min(
        tupleSize - bytesCopied, buffer->getCapacity() - appendLength);

      // Copy this many bytes to the buffer.
      memcpy(
        appendPointer + appendLength, temporaryBuffer + bytesCopied,
        bytesToCopy);
      bytesCopied += bytesToCopy;
      appendLength += bytesToCopy;

      // If we filled up the buffer, get a new one.
      if (appendLength == buffer->getCapacity()) {
        emitBuffer(setupPartition);
        // Get a new default sized buffer.
        getBuffer(setupPartition);
      }
    } while (bytesCopied < tupleSize);

    delete[] temporaryBuffer;
  } else {
    // The tuple was small enough to fit in the buffer, so that means all we
    // have to do is update the current position since the user did the copying
    // for us.
    appendLength += tupleSize;

    // If we filled up the buffer, get a new one.
    if (appendLength == buffer->getCapacity()) {
      emitBuffer(setupPartition);
      // Get a new default sized buffer.
      getBuffer(setupPartition);
    }
  }

  bytesWritten += tupleSize;
  tuplesWritten++;

  setupKeyLength = 0;
  setupMaxValueLength = 0;
  setupPartition = 0;
  setupTuple = NULL;
  temporaryBuffer = NULL;
}

void PartialKVPairWriter::flushBuffers() {
  for (uint64_t i = 0; i < appendInfos.size(); i++) {
    AppendInfo*& appendInfo = appendInfos.at(i);
    if (appendInfo->buffer != NULL) {
      // Send nonempty buffers downstream
      if (appendInfo->length > 0) {
        emitBuffer(i);
      } else {
        delete appendInfo->buffer;
        appendInfo->buffer = NULL;
        appendInfo->pointer = NULL;
        appendInfo->length = 0;
      }
    }
  }
}

uint64_t PartialKVPairWriter::getNumBytesCallerTriedToWrite() const {
  // PartialKVPairWriter writes all tuples, so this value is just bytesWritten
  return bytesWritten;
}

uint64_t PartialKVPairWriter::getNumBytesWritten() const {
  return bytesWritten;
}

uint64_t PartialKVPairWriter::getNumTuplesWritten() const {
  return tuplesWritten;
}

void PartialKVPairWriter::getBuffer(uint64_t partition) {
  AppendInfo*& appendInfo = appendInfos.at(partition);
  KVPairBuffer* buffer = getBufferCallback();
  appendInfo->buffer = buffer;

  // The partial writer is going to fill up the buffer to the brim, so set up an
  // append for the entire length of the buffer.
  appendInfo->pointer = const_cast<uint8_t*>(
    buffer->setupAppend(buffer->getCapacity()));
  appendInfo->length = 0;
}

void PartialKVPairWriter::emitBuffer(uint64_t partition) {
  AppendInfo*& appendInfo = appendInfos.at(partition);

  // Commit the outstanding append.
  appendInfo->buffer->commitAppend(
    appendInfo->pointer, appendInfo->length);
  appendInfo->pointer = NULL;
  appendInfo->length = 0;

  // Emit the buffer downstream
  emitBufferCallback(appendInfo->buffer, partition);

  // Clear the buffer from our internal data structures.
  appendInfo->buffer = NULL;
}
