#include "mapreduce/common/FastKVPairWriter.h"
#include "mapreduce/common/PartitionFunctionInterface.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

FastKVPairWriter::FastKVPairWriter(
  const PartitionFunctionInterface* _partitionFunction,
  uint64_t _outputTupleSampleRate, EmitBufferCallback _emitBufferCallback,
  GetBufferCallback _getBufferCallback, PutBufferCallback _putBufferCallback,
  LogSampleCallback _logSampleCallback,
  LogWriteStatsCallback _logWriteStatsCallback,
  bool _garbageCollectPartitionFunction, bool _writeWithoutHeaders)
  : emitBufferCallback(_emitBufferCallback),
    getBufferCallback(_getBufferCallback),
    putBufferCallback(_putBufferCallback),
    logSampleCallback(_logSampleCallback),
    logWriteStatsCallback(_logWriteStatsCallback),
    outputTupleSampleRate(_outputTupleSampleRate),
    garbageCollectPartitionFunction(_garbageCollectPartitionFunction),
    writeWithoutHeaders(_writeWithoutHeaders),
    partitionFunction(_partitionFunction),
    appendKeyPointer(NULL),
    appendValuePointer(NULL),
    appendKeyLength(0),
    appendPartition(0),
    tuplesWritten(0),
    bytesWritten(0) {
  // Use one buffer per global partition.
  uint64_t numBuffers = 1;
  if (partitionFunction != NULL) {
    numBuffers = partitionFunction->numGlobalPartitions();
  }
  buffers.resize(numBuffers);
}

FastKVPairWriter::~FastKVPairWriter() {
  if (logWriteStatsCallback) {
    logWriteStatsCallback(bytesWritten, tuplesWritten);
  }

  for (uint64_t i = 0; i < buffers.size(); i++) {
    ASSERT(buffers[i] == NULL,
           "Should have flushed all buffers before deleting the writer");
  }
  buffers.clear();

  if (garbageCollectPartitionFunction) {
    delete partitionFunction;
    partitionFunction = NULL;
  }
}

void FastKVPairWriter::write(KeyValuePair& kvPair) {
  kvPair.setWriteWithoutHeader(writeWithoutHeaders);

  if (outputTupleSampleRate > 0 && logSampleCallback &&
      tuplesWritten % outputTupleSampleRate == 0) {
    logSampleCallback(kvPair);
  }

  // Compute partition and grab the associated buffer
  uint64_t partition = 0;
  if (partitionFunction != NULL) {
  partition = partitionFunction->globalPartition(
    kvPair.getKey(), kvPair.getKeyLength());
  }
  KVPairBuffer*& buffer = buffers[partition];

  // Make sure the buffer is large enough for this tuple.
  uint64_t tupleSize = kvPair.getWriteSize();
  if (buffer == NULL ||
      buffer->getCurrentSize() + tupleSize > buffer->getCapacity()) {
    // Buffer not large enough to hold the tuple. Emit and get a new one.
    if (buffer != NULL) {
      emitBufferCallback(buffer, partition);
    }
    buffer = getBufferCallback(tupleSize);
    buffer->setNode(partition);
  }

  // Write the tuple.
  buffer->addKVPair(kvPair);

  // Update statistics.
  bytesWritten += tupleSize;
  tuplesWritten++;
}

uint8_t* FastKVPairWriter::setupWrite(
  const uint8_t* key, uint32_t keyLength, uint32_t maxValueLength) {
  ASSERT(appendKeyPointer == NULL,
         "Uncommited write is already in progress; can't start another one");

  // Compute partition and grab the associated buffer
  appendPartition = 0;
  if (partitionFunction != NULL) {
    appendPartition = partitionFunction->globalPartition(key, keyLength);
  }
  KVPairBuffer*& buffer = buffers[appendPartition];

  uint64_t tupleSize = KeyValuePair::tupleSize(
    keyLength, maxValueLength, !writeWithoutHeaders);
  if (buffer == NULL ||
      buffer->getCurrentSize() + tupleSize > buffer->getCapacity()) {
    // Buffer not large enough to hold the tuple. Emit and get a new one.
    if (buffer != NULL) {
      emitBufferCallback(buffer, appendPartition);
    }
    buffer = getBufferCallback(tupleSize);
    buffer->setNode(appendPartition);
  }

  // Get append pointers
  buffer->setupAppendKVPair(
    keyLength, maxValueLength, appendKeyPointer, appendValuePointer,
    writeWithoutHeaders);

  // Store the key length so we can accurately update statistics.
  appendKeyLength = keyLength;

  // Write the key.
  memcpy(appendKeyPointer, key, keyLength);

  return appendValuePointer;
}

void FastKVPairWriter::commitWrite(uint32_t valueLength) {
  ASSERT(appendKeyPointer != NULL,
         "Must have previously set up a tuple to append with setupWrite()");

  // Commit the append.
  KVPairBuffer*& buffer = buffers[appendPartition];
  buffer->commitAppendKVPair(
    appendKeyPointer, appendValuePointer, valueLength, writeWithoutHeaders);

  // Update statistics.
  bytesWritten += KeyValuePair::tupleSize(
    appendKeyLength, valueLength, !writeWithoutHeaders);
  tuplesWritten++;

  // Reset append state.
  appendKeyPointer = NULL;
}

// Send all buffers in the array to the downstream tracker.
void FastKVPairWriter::flushBuffers() {
  for (uint64_t i = 0; i < buffers.size(); i++) {
    if (buffers[i] != NULL) {

      // Send nonempty buffers downstream
      if (!buffers[i]->empty()) {
        emitBufferCallback(buffers[i], i);
      } else {
        putBufferCallback(buffers[i]);
      }

      // Mark this bucket as empty for future write() calls
      buffers[i] = NULL;
    }
  }
}

uint64_t FastKVPairWriter::getNumBytesCallerTriedToWrite() const {
  // FastKVPairWriter writes all tuples, so this value is just bytesWritten
  return bytesWritten;
}

uint64_t FastKVPairWriter::getNumBytesWritten() const {
  return bytesWritten;
}

uint64_t FastKVPairWriter::getNumTuplesWritten() const {
  return tuplesWritten;
}
