#ifndef TRITONSORT_BUFFER_LIST_TEST_H
#define TRITONSORT_BUFFER_LIST_TEST_H

#include <stdint.h>
#include <vector>
#include "gtest/gtest.h"

class DummyListable;
template <typename T> class BufferList;

class BufferListTest : public ::testing::Test {

protected:
  void assertElementsFormAList(std::vector<DummyListable>& listables,
                               uint64_t startIndex,
                               uint64_t endIndex);
  void assertBufferListAndBufferArrayMatch(
    std::vector<DummyListable>& listables,
    BufferList<DummyListable>& bufferList);
  void assertBufferListEmpty(BufferList<DummyListable>& bufferList);
  void testBulkMove(uint64_t numListables, uint64_t numListablesToMove);
};

#endif // TRITONSORT_BUFFER_LIST_TEST_H
