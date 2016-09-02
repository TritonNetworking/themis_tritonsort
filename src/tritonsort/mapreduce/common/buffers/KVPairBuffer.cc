#include <limits>

#include "KVPairBuffer.h"
#include "core/ByteOrder.h"
#include "mapreduce/common/KeyValuePair.h"

KVPairBuffer::KVPairBuffer(
  MemoryAllocatorInterface& memoryAllocator, uint64_t callerID,
  uint64_t capacity, uint64_t alignmentMultiple)
  : BaseBuffer(memoryAllocator, callerID, capacity, alignmentMultiple) {
  init();
}


KVPairBuffer::KVPairBuffer(
  MemoryAllocatorInterface& memoryAllocator, uint8_t* memoryRegion,
  uint64_t capacity, uint64_t alignmentMultiple)
  : BaseBuffer(memoryAllocator, memoryRegion, capacity, alignmentMultiple) {
  init();
}


KVPairBuffer::KVPairBuffer(
  uint8_t* memoryRegion, uint64_t capacity, uint64_t alignmentMultiple)
  : BaseBuffer(memoryRegion, capacity, alignmentMultiple) {
  init();
}

KVPairBuffer::KVPairBuffer(uint64_t capacity, uint64_t alignmentMultiple)
  : BaseBuffer(capacity, alignmentMultiple) {
  init();
}

void KVPairBuffer::init() {
  pendingAppendPtr = NULL;
  pendingKeyLength = 0;
  pendingMaxValueLength = 0;

  partitionGroup = std::numeric_limits<uint64_t>::max();
  // poolID should persist across clear() so initialize it here
  poolID = std::numeric_limits<uint64_t>::max();
  clear();
}

void KVPairBuffer::addKVPair(KeyValuePair& kvPair) {
  uint64_t kvPairSize = kvPair.getWriteSize();

  bool cachedBeforeCommitAppend = cached;

  const uint8_t* bufPtr = setupAppend(kvPairSize);
  kvPair.serialize(const_cast<uint8_t*>(bufPtr));
  commitAppend(bufPtr, kvPairSize);

  // Update tuple metadata.
  if (cachedBeforeCommitAppend) {
    cached = true;

    ++numTuples;
    uint32_t keyLength = kvPair.getKeyLength();
    minKeyLength = std::min(minKeyLength, keyLength);
    maxKeyLength = std::max(maxKeyLength, keyLength);
  }
}

bool KVPairBuffer::getNextKVPair(KeyValuePair& kvPair) {
  if (offset == currentSize) {
    return false;
  } else {
    uint8_t* recordPtr = const_cast<uint8_t*>(getRawBuffer()) + offset;
    uint32_t recordSize = 0;

    kvPair.deserialize(recordPtr);
    recordSize = kvPair.getReadSize();

    TRITONSORT_ASSERT(offset + recordSize <= currentSize, "Deserialized a tuple ("
           "%llu bytes) that is too large to be where it is in the buffer ("
           "started at offset %llu with max size %llu).", kvPair.getReadSize(),
           offset, currentSize);
    offset += recordSize;

    return true;
  }
}

void KVPairBuffer::resetIterator() {
  offset = 0;
}

void KVPairBuffer::setIteratorPosition(uint64_t position) {
  offset = position;
}

uint64_t KVPairBuffer::getIteratorPosition() {
  return offset;
}

void KVPairBuffer::setupAppendKVPair(
  uint32_t keyLength, uint32_t maxValueLength, uint8_t*& key, uint8_t*& value,
  bool writeWithoutHeader) {

  TRITONSORT_ASSERT(pendingAppendPtr == NULL, "More than one setupAppendKVPair cannot be "
         "called without a corresponding commitAppendKVPair");

  uint64_t tupleSize = KeyValuePair::tupleSize(
    keyLength, maxValueLength, !writeWithoutHeader);

  pendingAppendPtr = const_cast<uint8_t*>(setupAppend(tupleSize));
  pendingKeyLength = keyLength;
  pendingMaxValueLength = maxValueLength;

  KeyValuePair::getKeyValuePointers(
    pendingAppendPtr, keyLength, key, value, writeWithoutHeader);
}

void KVPairBuffer::commitAppendKVPair(
  const uint8_t* key, const uint8_t* value, uint32_t valueLength,
  bool writeWithoutHeader) {

  // Sanity check that the key and value are the same as before.
  TRITONSORT_ASSERT((writeWithoutHeader && key == KeyValuePair::keyWithoutHeader(
            pendingAppendPtr)) ||
         (!writeWithoutHeader && key == KeyValuePair::key(pendingAppendPtr)),
         "Can't alter the address of the key between setupAppendKVPair and "
         "commitAppendKVPair");

  TRITONSORT_ASSERT((writeWithoutHeader && value == KeyValuePair::valueWithoutHeader(
            pendingAppendPtr, pendingKeyLength)) ||
         (!writeWithoutHeader && value == KeyValuePair::value(
           pendingAppendPtr)),
         "Can't alter the address of the value between setupAppendKVPair and "
         "commitAppendKVPair");

  // Sanity check that the value didn't increase beyond the max.
  TRITONSORT_ASSERT(valueLength <= pendingMaxValueLength,
         "Value length %llu cannot be larger than max value length %llu",
         valueLength, pendingMaxValueLength);

  bool cachedBeforeCommitAppend = cached;

  if (!writeWithoutHeader) {
    // Update the header for the new value length.
    KeyValuePair::setValueLength(pendingAppendPtr, valueLength);
  }

  commitAppend(
    pendingAppendPtr, KeyValuePair::tupleSize(
      pendingKeyLength, valueLength, !writeWithoutHeader));

  // Update tuple metadata.
  if (cachedBeforeCommitAppend) {
    cached = true;

    ++numTuples;
    minKeyLength = std::min(minKeyLength, pendingKeyLength);
    maxKeyLength = std::max(maxKeyLength, pendingKeyLength);
  }

  pendingAppendPtr = NULL;
  pendingKeyLength = 0;
  pendingMaxValueLength = 0;
}

void KVPairBuffer::abortAppendKVPair(
  const uint8_t* key, const uint8_t* value, bool writeWithoutHeader) {
  // Sanity check that the key and value are the same as before.
  TRITONSORT_ASSERT((writeWithoutHeader && key == KeyValuePair::keyWithoutHeader(
            pendingAppendPtr)) ||
         (!writeWithoutHeader && key == KeyValuePair::key(pendingAppendPtr)),
         "Can't alter the address of the key between setupAppendKVPair and "
         "commitAppendKVPair");

  TRITONSORT_ASSERT((writeWithoutHeader && value == KeyValuePair::valueWithoutHeader(
            pendingAppendPtr, pendingKeyLength)) ||
         (!writeWithoutHeader && value == KeyValuePair::value(
           pendingAppendPtr)),
         "Can't alter the address of the value between setupAppendKVPair and "
         "commitAppendKVPair");

  abortAppend(pendingAppendPtr);

  pendingAppendPtr = NULL;
  pendingKeyLength = 0;
  pendingMaxValueLength = 0;
}

void KVPairBuffer::clear() {
  BaseBuffer::clear();
  offset = 0;
  cached = false;
  node = std::numeric_limits<uint64_t>::max();
  logicalDiskID = std::numeric_limits<uint64_t>::max();
  chunkID = std::numeric_limits<uint64_t>::max();
  sourceName.clear();
  jobIDSet.clear();
}

uint64_t KVPairBuffer::getNumTuples() {
  if (!cached) {
    calculateTupleMetadata();
  }
  TRITONSORT_ASSERT(cached);
  return numTuples;
}

uint32_t KVPairBuffer::getMinKeyLength() {
  if (!cached) {
    calculateTupleMetadata();
  }
  TRITONSORT_ASSERT(cached);
  return minKeyLength;
}

uint32_t KVPairBuffer::getMaxKeyLength() {
  if (!cached) {
    calculateTupleMetadata();
  }
  TRITONSORT_ASSERT(cached);
  return maxKeyLength;
}

void KVPairBuffer::calculateTupleMetadata() {
  uint8_t* iterator = const_cast<uint8_t*>(getRawBuffer());
  uint8_t* endOfBuffer = iterator + currentSize;
  numTuples = 0;
  minKeyLength = std::numeric_limits<uint32_t>::max();
  maxKeyLength = 0;

  // Iterate through the buffer counting tuples and calculating maximum key
  // length.
  while (iterator < endOfBuffer) {
    ++numTuples;
    uint32_t keyLength = KeyValuePair::keyLength(iterator);
    minKeyLength = std::min(minKeyLength, keyLength);
    maxKeyLength = std::max(maxKeyLength, keyLength);
    iterator = KeyValuePair::nextTuple(iterator);
  }

  TRITONSORT_ASSERT(iterator == endOfBuffer,
         "KVPairBuffer must end on clean tuple boundary, but found %llu extra "
         "bytes", iterator - endOfBuffer);

  cached = true;
}

KVPairBuffer::NetworkMetadata* KVPairBuffer::getNetworkMetadata() {
  ABORT_IF(jobIDSet.size() != 1,
           "Expected send buffer to have exactly 1 job ID, but found %llu",
           jobIDSet.size());

  NetworkMetadata* metadata = new NetworkMetadata();
  metadata->bufferLength = hostToBigEndian64(currentSize);
  metadata->jobID = hostToBigEndian64(*(jobIDSet.begin()));
  metadata->partitionGroup = hostToBigEndian64(partitionGroup);
  metadata->partitionID = hostToBigEndian64(logicalDiskID);

  return metadata;
}
