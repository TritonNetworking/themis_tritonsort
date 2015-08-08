#include "mapreduce/workers/reducer/ReduceKVPairIterator.h"

ReduceKVPairIterator::ReduceKVPairIterator(KVPairBuffer& _buffer)
  : currentKey(NULL),
    currentKeyLength(0),
    currentKeyStartPosition(0),
    nextKey(NULL),
    nextKeyLength(0),
    nextKeyStartPosition(0),
    nextKeyStartPositionKnown(true),
    doneWithKeyGroup(false),
    noMoreTuples(false),
    buffer(_buffer) {
}

bool ReduceKVPairIterator::startNextKey(
  const uint8_t*& key, uint32_t& keyLength) {
  if (noMoreTuples) {
    return false;
  }

  // If, for whatever reason, the caller didn't iterate to the end before
  // ending reduce, make sure that we're advanced to the next key by iterating
  // forward until you hit the next key
  if (!nextKeyStartPositionKnown) {
    KeyValuePair kvPair;

    while (next(kvPair)) {
    }

    ASSERT(nextKeyStartPositionKnown, "After iterating to the end of a key, we "
           "don't know the start position for the next key; this indicates a "
           "problem with the iterator logic somewhere");
  }
  // However now that we've hit the next key group, we don't know the start
  // of the next group
  nextKeyStartPositionKnown = false;
  doneWithKeyGroup = false;

  buffer.setIteratorPosition(nextKeyStartPosition);

  if (nextKey == NULL) {
    // Read the first key/value pair so you know which key will be the first key
    // being reduced
    KeyValuePair kvPair;

    if (!buffer.getNextKVPair(kvPair)) {
      // The buffer was empty, so just return.
      noMoreTuples = true;
      return false;
    } else {
      nextKey = kvPair.getKey();
      nextKeyLength = kvPair.getKeyLength();

      // Move the iterator backwards so that we can reduce the k/v pair
      buffer.setIteratorPosition(nextKeyStartPosition);
    }
  }

  // At this point we know the next key, so update current key, write output
  // parameters, and return.
  currentKeyStartPosition = nextKeyStartPosition;
  currentKey = nextKey;
  currentKeyLength = nextKeyLength;
  key = currentKey;
  keyLength = currentKeyLength;

  return true;
}

bool ReduceKVPairIterator::next(KeyValuePair& kvPair) {
  ASSERT(!doneWithKeyGroup,
         "Should not be calling next() after the end of a key group.");

  uint64_t recordStartIteratorPosition = buffer.getIteratorPosition();
  if (!buffer.getNextKVPair(kvPair)) {
    doneWithKeyGroup = true;
    noMoreTuples = true;
    return false;
  }

  if (compare(
        currentKey, currentKeyLength, kvPair.getKey(), kvPair.getKeyLength())
      != 0) {
    // Found a key different from the key currently being reduced
    nextKeyStartPosition = recordStartIteratorPosition;
    nextKeyStartPositionKnown = true;
    nextKey = kvPair.getKey();
    nextKeyLength = kvPair.getKeyLength();

    doneWithKeyGroup = true;
    return false;
  } else {
    // Same key as before, so we can keep on reducing for it
    return true;
  }
}

void ReduceKVPairIterator::reset() {
  doneWithKeyGroup = false;
  buffer.setIteratorPosition(currentKeyStartPosition);
}
