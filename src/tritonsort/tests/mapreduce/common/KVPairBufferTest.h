#ifndef MAPRED_KV_PAIR_BUFFER_TEST_H
#define MAPRED_KV_PAIR_BUFFER_TEST_H

#include <stdint.h>

#include "tritonsort/config.h"
#include "mapreduce/common/KeyValuePair.h"
#include "tests/mapreduce/common/MemoryAllocatingTestFixture.h"

class MemoryAllocatorInterface;

class KVPairBufferTest : public MemoryAllocatingTestFixture {
};

#endif // MAPRED_KV_PAIR_BUFFER_TEST_H
