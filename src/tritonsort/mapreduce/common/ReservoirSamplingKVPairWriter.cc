#include <algorithm>
#include <limits>
#include <stdlib.h>

#include "ReservoirSamplingKVPairWriter.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/HashedPhaseZeroKVPairWriteStrategy.h"
#include "mapreduce/common/PhaseZeroKVPairWriteStrategy.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/filter/RecordFilter.h"

ReservoirSamplingKVPairWriter::ReservoirSamplingKVPairWriter(
  uint64_t _outputTupleSampleRate, const RecordFilter* _recordFilter,
  EmitBufferCallback _emitBufferCallback, GetBufferCallback _getBufferCallback,
  PutBufferCallback _putBufferCallback, LogSampleCallback _logSampleCallback,
  LogWriteStatsCallback _logWriteStatsCallback,
  KVPairWriteStrategyInterface* _writeStrategy)
  : recordFilter(_recordFilter),
    outputTupleSampleRate(_outputTupleSampleRate),
    emitBufferCallback(_emitBufferCallback),
    getBufferCallback(_getBufferCallback),
    putBufferCallback(_putBufferCallback),
    logSampleCallback(_logSampleCallback),
    logWriteStatsCallback(_logWriteStatsCallback),
    writeStrategy(_writeStrategy),
    buffer(getBufferCallback(0)),
    maxSamples(std::numeric_limits<uint64_t>::max()),
    sampleSize(buffer->getCapacity() / 2), // Use half of the buffer for samples
    tuplesSeen(0),
    tuplesWritten(0),
    bytesSeen(0),
    bytesWritten(0),
    setupWriteValue(NULL) {
  ASSERT(writeStrategy != NULL,
         "Cannot pass NULL write strategy to reservoir sampling writer");

  resetPendingWriteState();
}

ReservoirSamplingKVPairWriter::~ReservoirSamplingKVPairWriter() {
  logWriteStatsCallback(bytesWritten, bytesSeen, tuplesWritten, tuplesSeen);

  ASSERT(buffer == NULL,
         "Should have flushed the buffer before deleting the writer");

  if (setupWriteValue != NULL) {
    delete[] setupWriteValue;
    setupWriteValue = NULL;
  }

  if (writeStrategy != NULL) {
    delete writeStrategy;
    writeStrategy = NULL;
  }
}

void ReservoirSamplingKVPairWriter::write(KeyValuePair& kvPair) {
  if (recordFilter != NULL &&
      !recordFilter->pass(
        kvPair.getKey(), kvPair.getKeyLength())) {
    // Filter has rejected the record and we shouldn't write it
    return;
  }

  if (tuplesSeen % outputTupleSampleRate == 0) {
    logSampleCallback(kvPair);
  }

  writeSampleRecordToBuffer(
    kvPair.getKey(), kvPair.getKeyLength(), kvPair.getValue(),
    kvPair.getValueLength());
}

uint8_t* ReservoirSamplingKVPairWriter::setupWrite(
  const uint8_t* key, uint32_t keyLength, uint32_t maxValueLength) {
  ABORT_IF(setupWriteKey != NULL, "It looks like setupWrite has been called "
           "before without a subsequent call to commitWrite");

  setupWriteKey = key;
  setupWriteKeyLength = keyLength;
  setupWriteMaxValueLength = maxValueLength;

  setupWriteValue = new (themis::memcheck) uint8_t[maxValueLength];

  return setupWriteValue;
}

void ReservoirSamplingKVPairWriter::commitWrite(uint32_t valueLength) {
  ASSERT(setupWriteKey != NULL, "Have to call setupWrite before you can "
         "call commitWrite");
  ASSERT(valueLength <= setupWriteMaxValueLength, "Can't write more than you "
         "promised you would in setupWrite");

  if (recordFilter == NULL ||
      recordFilter->pass(setupWriteKey, setupWriteKeyLength)) {

    writeSampleRecordToBuffer(
      setupWriteKey, setupWriteKeyLength, setupWriteValue, valueLength);
  }

  resetPendingWriteState();
}

void ReservoirSamplingKVPairWriter::flushBuffers() {
  // Perform compaction to remove all invalid tuples.
  compactSampleBuffer();

  // Number of tuples written is at most the maximum number of samples.
  tuplesWritten = std::min<uint64_t>(tuplesSeen, maxSamples);

  // Calculate the number of bytes written by scanning the buffer's values.
  buffer->resetIterator();
  KeyValuePair kvPair;
  while (buffer->getNextKVPair(kvPair)) {
    // Value is the number of bytes in the tuple as a uint64_t.
    bytesWritten += *(reinterpret_cast<const uint64_t*>(kvPair.getValue()));
  }

  // If the buffer is non-empty, emit it.
  if (!buffer->empty()) {
    buffer->setNode(0);
    emitBufferCallback(buffer, 0);
  } else {
    putBufferCallback(buffer);
  }
  buffer = NULL;
}

void ReservoirSamplingKVPairWriter::compactSampleBuffer() {
  // Perform compaction by collecting all valid tuples into a new buffer.
  // Get a new default-sized buffer to hold valid tuples.
  KVPairBuffer* newBuffer = getBufferCallback(0);

  // Copy all valid tuples from the old buffer to the new buffer.
  std::sort(validTuples.begin(), validTuples.end());
  buffer->resetIterator();
  KeyValuePair kvPair;
  uint64_t tupleIndex = 0;
  std::vector<uint64_t>::iterator validIterator = validTuples.begin();
  while (buffer->getNextKVPair(kvPair) &&
         validIterator != validTuples.end()) {
    if (tupleIndex == *validIterator) {
      // This tuple is valid, copy it to the new buffer.
      newBuffer->addKVPair(kvPair);
      // Advance to the next valid tuple.
      validIterator++;
    }
    tupleIndex++;
  }

  // All valid tuples have been copied. Re-index the list of valid tuples.
  tupleIndex = 0;
  for (validIterator = validTuples.begin();
       validIterator != validTuples.end(); validIterator++, tupleIndex++) {
    *validIterator = tupleIndex;
  }

  // Return the old buffer and start using the new buffer.
  putBufferCallback(buffer);
  buffer = newBuffer;
}

void ReservoirSamplingKVPairWriter::writeSampleRecordToBuffer(
  const uint8_t* key, uint32_t keyLength, const uint8_t* value,
  uint32_t valueLength) {

  // Record samples sequentially until the buffer fills up.
  uint64_t writeIndex = tuplesSeen;
  tuplesSeen++;
  bytesSeen += KeyValuePair::tupleSize(keyLength, valueLength);

  // If the buffer is already full, then randomly replace a tuple with
  // probability (maxSamples / tuplesSeen)
  if (writeIndex >= maxSamples) {
    writeIndex = lrand48() % tuplesSeen;
  }

  if (writeIndex < maxSamples) {
    // This tuple should be sampled and will replace position: writeIndex

    // Step 1: First check if we are going to overflow the buffer and need to
    // perform a compaction step.
    uint64_t writeKeyLength = writeStrategy->getOutputKeyLength(keyLength);
    uint64_t writeValueLength =
      writeStrategy->getOutputValueLength(valueLength);
    uint64_t tupleSize =
      KeyValuePair::tupleSize(writeKeyLength, writeValueLength);

    if (buffer->getCapacity() - buffer->getCurrentSize() < tupleSize) {
      compactSampleBuffer();

      ABORT_IF(buffer->getCapacity() - buffer->getCurrentSize() < tupleSize,
               "After compaction, buffer still isn't large enough to handle "
               "tuple of siez %llu", tupleSize);
    }

    // Step 2: Write the tuple.
    uint8_t* outputKey = NULL;
    uint8_t* outputValue = NULL;

    buffer->setupAppendKVPair(
      writeKeyLength, writeValueLength, outputKey, outputValue);

    writeStrategy->writeKey(key, keyLength, outputKey);
    writeStrategy->writeValue(value, valueLength, keyLength, outputValue);

    buffer->commitAppendKVPair(outputKey, outputValue, writeValueLength);

    if (tuplesSeen < maxSamples && buffer->getCurrentSize() > sampleSize) {
      // We've hit the maximum sample size, so start replacing tuples from here
      // on.
      maxSamples = tuplesSeen;
    }

    // Step 3: Update the list of valid tuples. This will simultaneously
    // invalidate the replaced tuple and make the new one valid.
    if (writeIndex >= validTuples.size()) {
      // Make sure we have enough room in the vector.
      validTuples.resize(writeIndex + 1);
    }
    validTuples[writeIndex] = buffer->getNumTuples() - 1;
  }
}

uint64_t ReservoirSamplingKVPairWriter::getNumBytesCallerTriedToWrite() const {
  // Report all bytes seen to get the correct intermediate ratio.
  return bytesSeen;
}

uint64_t ReservoirSamplingKVPairWriter::getNumBytesWritten() const {
  // Will return 0 if called before flushBuffers()
  return bytesWritten;
}

uint64_t ReservoirSamplingKVPairWriter::getNumTuplesWritten() const {
  // Will return 0 if called before flushBuffers()
  return tuplesWritten;
}

