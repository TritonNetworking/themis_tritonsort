#include <arpa/inet.h>
#include <string.h>

#include "core/ByteOrder.h"
#include "core/Comparison.h"
#include "core/File.h"
#include "core/MemoryUtils.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/boundary/KeyList.h"

KeyList::KeyList(
  uint64_t _numKeys, uint64_t _numBytes, uint64_t _lowerBoundOffset)
  : numKeys(_numKeys),
    numBytes(_numBytes),
    lowerBoundOffset(_lowerBoundOffset),
    keyBuffer(new (themis::memcheck) uint8_t[numBytes]),
    nextKeyOffset(0),
    nextByteOffset(0) {
  memset(keyBuffer, 0, numBytes);
  keyInfos.resize(numKeys);
}

KeyList* KeyList::newKeyListFromFile(File& file) {
  // Read number of keys, number of bytes, and lower bound offset.
  uint64_t numKeys;
  file.read(reinterpret_cast<uint8_t*>(&numKeys), sizeof(numKeys));
  numKeys = bigEndianToHost64(numKeys);

  uint64_t numBytes;
  file.read(reinterpret_cast<uint8_t*>(&numBytes), sizeof(numBytes));
  numBytes = bigEndianToHost64(numBytes);

  uint64_t lowerBoundOffset;
  file.read(
    reinterpret_cast<uint8_t*>(&lowerBoundOffset), sizeof(lowerBoundOffset));
  lowerBoundOffset = bigEndianToHost64(lowerBoundOffset);

  // Create a new KeyList of the right size.
  KeyList* keyList =
    new (themis::memcheck) KeyList(numKeys, numBytes, lowerBoundOffset);

  // Read in the key buffer.
  uint8_t* keyBuffer = new (themis::memcheck) uint8_t[numBytes];
  file.read(reinterpret_cast<uint8_t*>(keyBuffer), numBytes);

  // Read each key length and add each key to the key list.
  uint32_t keyLength;
  uint8_t* keyPointer = keyBuffer;
  for (uint64_t i = 0; i < numKeys; i++) {
    file.read(reinterpret_cast<uint8_t*>(&keyLength), sizeof(keyLength));
    keyLength = ntohl(keyLength);

    // Add the key. Unfortunately this incurs an additional memory copy but
    // this should be an infrequent operation.
    keyList->addKey(keyPointer, keyLength);
    keyPointer += keyLength;
  }

  // Don't leak the temporary key buffer.
  delete[] keyBuffer;

  return keyList;
}

KeyList::~KeyList() {
  // Don't leak memory from the key buffer.
  if (keyBuffer != NULL) {
    delete[] keyBuffer;
    keyBuffer = NULL;
  }
}

void KeyList::addKey(const uint8_t* key, uint32_t keyLength) {
  ASSERT(nextKeyOffset < numKeys,
         "Added more keys than the KeyList can accept (%llu)", numKeys);
  ASSERT(nextByteOffset + keyLength <= numBytes,
         "Tried to add %llu bytes to KeyList at capacity (%llu / %llu)",
         keyLength, nextByteOffset, numBytes);

  // Copy the key to the contiguous memory buffer for better cache performance.
  memcpy(keyBuffer + nextByteOffset, key, keyLength);

  // Store the offset in the keyInfos vector.
  keyInfos[nextKeyOffset].keyLength = keyLength;
  keyInfos[nextKeyOffset].keyPtr = keyBuffer + nextByteOffset;

  // Increment offsets.
  nextKeyOffset++;
  nextByteOffset += keyLength;
}

uint64_t KeyList::findLowerBound(const uint8_t* key, uint32_t keyLength) const {
  ASSERT(nextKeyOffset == numKeys,
         "Tried to search partially-empty KeyList (%llu / %llu)",
         nextKeyOffset, numKeys);

  uint64_t lowerBound = 0;
  uint64_t upperBound = numKeys - 1;

  while (lowerBound < upperBound) {
    // Round midpoint up if non-integer (+1)
    uint64_t midpoint = (upperBound + lowerBound + 1) / 2;
    const KeyInfo& midpointKeyInfo = keyInfos.at(midpoint);
    if (compare(
      key, keyLength, midpointKeyInfo.keyPtr, midpointKeyInfo.keyLength) < 0) {
      // If < midpoint, set upper = m - 1, but force upper >= 0
      upperBound = midpoint == 0 ? 0 : midpoint - 1;
    } else {
      // If >= midpoint, set lower = m
      lowerBound = midpoint;
    }
  }

  return lowerBound + lowerBoundOffset;
}

void KeyList::writeToFile(File& file) const {
  // Write number of keys, number of bytes, and lower bound offset.
  uint64_t tmpNumKeys = hostToBigEndian64(numKeys);
  file.write(reinterpret_cast<uint8_t*>(&tmpNumKeys), sizeof(tmpNumKeys));

  uint64_t tmpNumBytes = hostToBigEndian64(numBytes);
  file.write(reinterpret_cast<uint8_t*>(&tmpNumBytes), sizeof(tmpNumBytes));

  uint64_t tmpLowerBoundOffset = hostToBigEndian64(lowerBoundOffset);
  file.write(
    reinterpret_cast<uint8_t*>(&tmpLowerBoundOffset),
    sizeof(tmpLowerBoundOffset));

  // Write the key buffer.
  file.write(reinterpret_cast<uint8_t*>(keyBuffer), numBytes);

  // Write the key infos.
  for (uint64_t i = 0; i < keyInfos.size(); i++) {
    // Only need to write key length since we can derive pointer at read time.
    uint32_t tmpKeyLength = htonl(keyInfos[i].keyLength);
    file.write(reinterpret_cast<uint8_t*>(&tmpKeyLength), sizeof(tmpKeyLength));
  }
}

bool KeyList::equals(const KeyList& other) const {
  // Compare integer fields.
  if (numKeys != other.numKeys) {
    return false;
  }

  if (numBytes != other.numBytes) {
    return false;
  }

  if (lowerBoundOffset != other.lowerBoundOffset) {
    return false;
  }

  if (nextKeyOffset != other.nextKeyOffset) {
    return false;
  }

  if (nextByteOffset != other.nextByteOffset) {
    return false;
  }

  // Compare keys.
  if (memcmp(keyBuffer, other.keyBuffer, numBytes) != 0) {
    return false;
  }

  // Compare key lengths.
  for (uint64_t i = 0; i < numKeys; i++) {
    if (keyInfos[i].keyLength != other.keyInfos[i].keyLength) {
      return false;
    }
  }

  return true;
}

uint64_t KeyList::getCurrentSize() const {
  // Just count the large data structures because this value is used mainly for
  // monitoring purposes and doesn't have to be exact.
  return numBytes + (numKeys * sizeof(KeyInfo));
}

uint64_t KeyList::getNumKeys() const {
  return numKeys;
}
