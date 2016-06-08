#ifndef TRITONSORT_RESOURCE_QUEUE_TEST_H
#define TRITONSORT_RESOURCE_QUEUE_TEST_H

#include <stdint.h>
#include "gtest.h"

class ResourceQueueTest : public ::testing::Test {
protected:
  void testStealFrom(uint64_t sourceQueueSize, uint64_t destQueueSize,
                     uint64_t numSourceElements, uint64_t numDestElements,
                     uint64_t sourceOffset, uint64_t destOffset,
                     uint64_t numElementsToCopy);
};

#endif // TRITONSORT_RESOURCE_QUEUE_TEST_H
