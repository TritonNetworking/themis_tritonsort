#include "core/BaseWorker.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/DefaultKVPairWriteStrategy.h"
#include "mapreduce/common/KVPairWriteStrategyInterface.h"
#include "mapreduce/common/KVPairWriter.h"
#include "mapreduce/common/PartitionFunctionInterface.h"

KVPairWriter::KVPairWriter(
  uint64_t _numBuffers, const PartitionFunctionInterface& _partitionFunction,
  KVPairWriteStrategyInterface* _writeStrategy, uint64_t _outputTupleSampleRate,
  const RecordFilter* _recordFilter, EmitBufferCallback _emitBufferCallback,
  GetBufferCallback _getBufferCallback, PutBufferCallback _putBufferCallback,
  LogSampleCallback _logSampleCallback,
  LogWriteStatsCallback _logWriteStatsCallback,
  bool _garbageCollectPartitionFunction)
  : emitBufferCallback(_emitBufferCallback),
    getBufferCallback(_getBufferCallback),
    putBufferCallback(_putBufferCallback),
    logSampleCallback(_logSampleCallback),
    logWriteStatsCallback(_logWriteStatsCallback),
    numBuffers(_numBuffers),
    outputTupleSampleRate(_outputTupleSampleRate),
    garbageCollectPartitionFunction(_garbageCollectPartitionFunction),
    partitionFunction(_partitionFunction),
    recordFilter(_recordFilter),
    writeStrategy(_writeStrategy),
    tuplesWritten(0),
    bytesWritten(0) {

  TRITONSORT_ASSERT(numBuffers > 0, "KVPairWriter must write to at least one buffer");

  if (writeStrategy == NULL) {
    this->writeStrategy = new DefaultKVPairWriteStrategy();
  } else {
    this->writeStrategy = writeStrategy;
  }


  buffers.resize(numBuffers, NULL);

  resetPendingWriteState();
}

KVPairWriter::~KVPairWriter() {
  logWriteStatsCallback(bytesWritten, tuplesWritten);

  for (uint64_t i = 0; i < numBuffers; i++) {
    TRITONSORT_ASSERT(buffers[i] == NULL, "Should have flushed all buffers before "
           "deleting the writer");
  }
  buffers.clear();

  if (writeStrategy != NULL) {
    delete writeStrategy;
    writeStrategy = NULL;
  }

  if (garbageCollectPartitionFunction) {
    delete &partitionFunction;
  }
}

KVPairBuffer* KVPairWriter::getBufferForKey(
  const uint8_t* key, uint32_t keyLength, uint64_t tupleSize,
  uint64_t& bufferNumber) {

  // In the case of the mapper, use the global partition to pick the buffer.
  bufferNumber = partitionFunction.globalPartition(key, keyLength);
  TRITONSORT_ASSERT(bufferNumber < numBuffers, "Invalid buffer number provided by "
         "partition function");

  KVPairBuffer* buffer = buffers[bufferNumber];

  if (buffer == NULL) {
    getNewBuffer(buffer, bufferNumber, tupleSize);
  }

  if (buffer->getCurrentSize() + tupleSize > buffer->getCapacity()) {
    emitBufferCallback(buffer, bufferNumber);
    getNewBuffer(buffer, bufferNumber, tupleSize);
  }

  ABORT_IF(buffer == NULL, "Should have retrieved a buffer at this point");
  return buffer;
}

void KVPairWriter::write(KeyValuePair& kvPair) {
  const uint8_t* key = kvPair.getKey();
  uint32_t keyLength = kvPair.getKeyLength();

  if (recordFilter != NULL &&
      !partitionFunction.acceptedByFilter(key, keyLength, *recordFilter)) {
    // Filter has rejected the record, so we shouldn't write it
    return;
  }

  uint64_t bufNo = 0;

  if (tuplesWritten % outputTupleSampleRate == 0) {
    logSampleCallback(kvPair);
  }

  uint32_t outputKeyLength = writeStrategy->getOutputKeyLength(
    kvPair.getKeyLength());

  uint32_t outputValueLength = writeStrategy->getOutputValueLength(
    kvPair.getValueLength());

  KVPairBuffer* buffer = getBufferForKey(
    key, keyLength, KeyValuePair::tupleSize(outputKeyLength, outputValueLength),
    bufNo);

  // Record the total number of bytes written, even if we're just writing keys.
  bytesWritten += kvPair.getWriteSize();

  bool altersKey = writeStrategy->altersKey();
  bool altersValue = writeStrategy->altersValue();

  if (altersKey || altersValue) {
    // Either of both of the key and value for this record need to be modified
    // before they're written; any unmodified portion can just be copied.

    uint8_t* outputKeyPtr;
    uint8_t* outputValuePtr;

    buffer->setupAppendKVPair(
      outputKeyLength, outputValueLength, outputKeyPtr, outputValuePtr);

    writeStrategy->writeKey(
      kvPair.getKey(), kvPair.getKeyLength(), outputKeyPtr);

    writeStrategy->writeValue(
      kvPair.getValue(), kvPair.getValueLength(), kvPair.getKeyLength(),
      outputValuePtr);

    buffer->commitAppendKVPair(outputKeyPtr, outputValuePtr, outputValueLength);
  } else {
    // Record can be added to the buffer unmodified
    buffer->addKVPair(kvPair);
  }

  tuplesWritten++;

  if (buffer->full()) {
    emitBufferCallback(buffer, bufNo);
    buffers[bufNo] = NULL;
  }
}

uint8_t* KVPairWriter::setupWrite(const uint8_t* key, uint32_t keyLength,
                    uint32_t maxValueLength) {
  TRITONSORT_ASSERT(tupleAppendBuffer == NULL, "Uncommited write is already in progress; "
         "can't start another one");

  // If the transformed version of the value will be bigger than the maximum
  // value length, increase the maximum value length accordingly
  uint64_t outputMaxValueLength = writeStrategy->getOutputValueLength(
    maxValueLength);

  uint32_t outputKeyLength = writeStrategy->getOutputKeyLength(keyLength);

  // Make sure we've got a buffer big enough to hold the transformed value
  tupleAppendBuffer = getBufferForKey(
    key, keyLength,
    KeyValuePair::tupleSize(outputKeyLength, outputMaxValueLength),
    tupleAppendBufferNumber);

  tupleAppendKeyLength = keyLength;

  tupleAppendBuffer->setupAppendKVPair(
    outputKeyLength, outputMaxValueLength, tupleAppendKeyPtr,
    tupleAppendValuePtr);

  // Copy the key (transformed or not) into the appropriate place in the
  // pending portion of buffer memory
  writeStrategy->writeKey(key, keyLength, tupleAppendKeyPtr);

  tupleAppendTmpValue = NULL;

  if (writeStrategy->altersValue()) {
    // We will be transforming the value; instead of returning a pointer to the
    // buffer's memory, we'll allocate some internal memory for the caller to
    // write to and write the transformed version of the value into the right
    // place on commit
    tupleAppendTmpValue = new (themis::memcheck) uint8_t[maxValueLength];
    return tupleAppendTmpValue;
  } else {
    // The value will be written unmodified, so the caller can just write
    // directly into buffer memory
    return tupleAppendValuePtr;
  }
}

void KVPairWriter::commitWrite(uint32_t valueLength) {
  TRITONSORT_ASSERT(tupleAppendBuffer != NULL, "Must have previously set up a tuple to "
         "append with setupWrite()");

  if (recordFilter != NULL &&
      !partitionFunction.acceptedByFilter(
        tupleAppendKeyPtr, tupleAppendKeyLength, *recordFilter)) {
    // Filter has rejected the write, so we shouldn't write it
    tupleAppendBuffer->abortAppendKVPair(
      tupleAppendKeyPtr, tupleAppendValuePtr);
  } else {
    if (writeStrategy->altersValue()) {
      // The writer has written the value to the temporary memory we gave it in
      // setupWrite; we need to transform that value and write it in before
      // committing

      writeStrategy->writeValue(
        tupleAppendTmpValue, valueLength, tupleAppendKeyLength,
        tupleAppendValuePtr);
    }

    // If the write strategy isn't going to alter the value, we can just
    // commit the append, because the caller has already written its value to
    // the right place

    tupleAppendBuffer->commitAppendKVPair(
      tupleAppendKeyPtr, tupleAppendValuePtr,
      writeStrategy->getOutputValueLength(valueLength));

    // If the buffer is full, emit it
    if (tupleAppendBuffer->full()) {
      emitBufferCallback(tupleAppendBuffer, tupleAppendBufferNumber);
      buffers[tupleAppendBufferNumber] = NULL;
    }

    // Record the total number of bytes written, even if we're just writing
    // keys.
    bytesWritten += KeyValuePair::tupleSize(
      tupleAppendKeyLength, valueLength);
  }

  // Reset values for the next time we append
  resetPendingWriteState();

  // If we allocated temporary value memory, we need to delete it
  if (writeStrategy->altersValue()) {
    delete[] tupleAppendValuePtr;
  }

  tupleAppendValuePtr = NULL;
}

void KVPairWriter::getNewBuffer(
  KVPairBuffer*& buffer, uint64_t bufNo, uint64_t minimumCapacity) {

  buffer = getBufferCallback(minimumCapacity);
  buffer->setNode(bufNo);
  buffers[bufNo] = buffer;
}

// Send all buffers in the array to the downstream tracker.
void KVPairWriter::flushBuffers() {
  for (uint64_t i = 0; i < numBuffers; i++) {
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

uint64_t KVPairWriter::getNumBytesCallerTriedToWrite() const {
  // Since the KVPairWriter writes all tuples, this value is just bytesWritten
  return bytesWritten;
}

uint64_t KVPairWriter::getNumBytesWritten() const {
  return bytesWritten;
}

uint64_t KVPairWriter::getNumTuplesWritten() const {
  return tuplesWritten;
}
