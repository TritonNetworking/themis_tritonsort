#include "TestMemoryBackedKVPair.h"
#include "core/TritonSortAssert.h"

TestMemoryBackedKVPair::TestMemoryBackedKVPair(uint64_t keySize,
                                               uint64_t valueSize) {
  rawKey = new uint8_t[keySize];
  rawValue = new uint8_t[valueSize];

  setKey(rawKey, keySize);
  setValue(rawValue, valueSize);
}

TestMemoryBackedKVPair::~TestMemoryBackedKVPair() {
  delete[] rawKey;
  delete[] rawValue;
}
