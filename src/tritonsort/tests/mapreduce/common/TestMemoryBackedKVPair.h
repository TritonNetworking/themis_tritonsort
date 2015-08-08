#ifndef MAPRED_TEST_MEMORY_BACKED_KV_PAIR_H
#define MAPRED_TEST_MEMORY_BACKED_KV_PAIR_H

/**
   To simplify the testing of certain functions relating to the KeyValuePair
class, it's convenient to have a KeyValuePair whose memory is initialized on
construction and freed on destruction. The TestMemoryBackedKVPair class
provides that functionality.
 */

#include "mapreduce/common/KeyValuePair.h"

class TestMemoryBackedKVPair : public KeyValuePair {
public:
  TestMemoryBackedKVPair(uint64_t keySize, uint64_t valueSize);
  virtual ~TestMemoryBackedKVPair();
private:
  TestMemoryBackedKVPair() : KeyValuePair() {}
  uint8_t* rawKey;
  uint8_t* rawValue;
};

#endif // MAPRED_TEST_MEMORY_BACKED_KV_PAIR_H
