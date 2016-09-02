#include "mapreduce/common/SimpleKVPairWriter.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

SimpleKVPairWriter::SimpleKVPairWriter(
  EmitBufferCallback _emitBufferCallback, GetBufferCallback _getBufferCallback)
  : emitBufferCallback(_emitBufferCallback),
    getBufferCallback(_getBufferCallback),
    buffer(NULL),
    appendPointer(NULL),
    appendLength(0),
    tuplesWritten(0),
    bytesWritten(0) {
}

SimpleKVPairWriter::~SimpleKVPairWriter() {
  TRITONSORT_ASSERT(buffer == NULL,
         "Should have flushed buffer before deleting the writer.");
}

void SimpleKVPairWriter::write(KeyValuePair& kvPair) {
  uint64_t tupleSize = kvPair.getWriteSize();
  if (buffer != NULL &&
      buffer->getCapacity() - appendLength < tupleSize) {
    // The existing buffer isn't large enough, so emit it.
    emitBuffer();
  }

  if (buffer == NULL) {
    // Get a new buffer at least large enough to hold this tuple.
    buffer = getBufferCallback(tupleSize);

    // Begin an append for the entire buffer
    appendPointer = const_cast<uint8_t*>(
      buffer->setupAppend(buffer->getCapacity()));
    appendLength = 0;
  }

  // Append the tuple.
  kvPair.serialize(appendPointer + appendLength);
  appendLength += tupleSize;

  bytesWritten += tupleSize;
  tuplesWritten++;
}

uint8_t* SimpleKVPairWriter::setupWrite(
  const uint8_t* key, uint32_t keyLength, uint32_t maxValueLength) {
  ABORT("setupWrite() not implemented.");

  return NULL;
}

void SimpleKVPairWriter::commitWrite(uint32_t valueLength) {
  ABORT("commitWrite() not implemented.");
}

void SimpleKVPairWriter::flushBuffers() {
  if (buffer != NULL) {
    if (appendLength > 0) {
      emitBuffer();
    } else {
      delete buffer;
      buffer = NULL;
      appendPointer = NULL;
      appendLength = 0;
    }
  }
}

uint64_t SimpleKVPairWriter::getNumBytesCallerTriedToWrite() const {
  // SimpleKVPairWriter writes all tuples, so this value is just bytesWritten
  return bytesWritten;
}

uint64_t SimpleKVPairWriter::getNumBytesWritten() const {
  return bytesWritten;
}

uint64_t SimpleKVPairWriter::getNumTuplesWritten() const {
  return tuplesWritten;
}

void SimpleKVPairWriter::emitBuffer() {
  // Commit the outstanding append.
  buffer->commitAppend(appendPointer, appendLength);

  // Emit buffer downstream
  emitBufferCallback(buffer);

  // Forget this buffer
  buffer = NULL;
}
