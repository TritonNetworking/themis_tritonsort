#include "BufferListTest.h"
#include "common/buffers/BufferList.h"
#include "tests/common/DummyListable.h"

void BufferListTest::testBulkMove(uint64_t numListables,
                                  uint64_t numListablesToMove) {
  std::vector<DummyListable> listables;
  listables.resize(numListables);

  const uint64_t listableSize = 100;
  for (uint64_t i = 0; i < numListables; i++) {
    listables[i].setSize(listableSize);
  }

  BufferList<DummyListable> sourceBufferList;
  for (uint64_t i = 0; i < numListables; i++) {
    sourceBufferList.append(&(listables[i]));
  }
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Source list has the wrong number of buffers.",
                               numListables, sourceBufferList.getSize());
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Source list has the wrong number of bytes.",
                               numListables * listableSize,
                               sourceBufferList.getTotalDataSize());

  assertElementsFormAList(listables, 0, numListables);

  BufferList<DummyListable> destBufferList;

  sourceBufferList.bulkMoveBuffersTo(
    destBufferList, numListablesToMove * listableSize);

  uint64_t shouldHaveMoved = std::min<uint64_t>(numListablesToMove,
                                                numListables);

  CPPUNIT_ASSERT_EQUAL_MESSAGE("Source list has the wrong number of buffers "
                               "after bulkMove().",
                               numListables - shouldHaveMoved,
                               sourceBufferList.getSize());
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Source list has the wrong number of bytes "
                               "after bulkMove().",
                               (numListables - shouldHaveMoved) * listableSize,
                               sourceBufferList.getTotalDataSize());
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Destination list has the wrong number of "
                               "buffers after bulkMove().",
                               shouldHaveMoved, destBufferList.getSize());
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Destination list has the wrong number of "
                               "bytes after bulkMove().",
                               shouldHaveMoved * listableSize,
                               destBufferList.getTotalDataSize());

  // After the move, both the elements that have been moved and the elements
  // that have not been moved should form correct lists.

  assertElementsFormAList(listables, 0, shouldHaveMoved);
  if (shouldHaveMoved < numListables) {
    assertElementsFormAList(listables, shouldHaveMoved, numListables);
  }
}

void BufferListTest::testBulkMoveComplete() {
  for (uint64_t i = 0; i < 10; i++) {
    testBulkMove(i, i);
  }
}

void BufferListTest::testBulkMovePartial() {
  for (uint64_t i = 0; i < 10; i++) {
    for (uint64_t j = 0; j < i; j++) {
      testBulkMove(i, j);
    }
  }
}

void BufferListTest::testBulkMoveMoreThanExists() {
  testBulkMove(0, 3);
  testBulkMove(3, 7);
}

void BufferListTest::testBulkMoveWithNonMultipleBytes() {
  // Test that a bulk move with a byte cap that is not a whole number of buffers
  // will not exceed that byte limit.

  // Create 5 buffers with 100, 200, 300, 400, 500 bytes respectively.
  uint64_t numListables = 5;
  DummyListable listables[numListables];
  for (uint64_t i = 0; i < numListables; i++) {
    listables[i].setSize((i + 1) * 100);
  }

  BufferList<DummyListable> sourceBufferList;
  for (uint64_t i = 0; i < numListables; i++) {
    sourceBufferList.append(&(listables[i]));
  }

  CPPUNIT_ASSERT_EQUAL_MESSAGE("Source list has the wrong number of buffers.",
                               numListables, sourceBufferList.getSize());
  // Should be 1500 bytes in total.
  uint64_t totalBytes = 1500;
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Source list has the wrong number of bytes.",
                               totalBytes, sourceBufferList.getTotalDataSize());

  BufferList<DummyListable> destBufferList;

  // Move at most 766 bytes. This should move the first 3 buffers.
  uint64_t bytesMoved = sourceBufferList.bulkMoveBuffersTo(destBufferList, 766);
  // Should have moved the first 3 buffers for a total of 600 bytes.
  uint64_t bytesInFirstThreeBuffers = 600;
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Moved the wrong number of bytes.",
                               bytesInFirstThreeBuffers, bytesMoved);

  // 2 remaining buffers in the source list with total size 900.
  uint64_t twoBuffers = 2;
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Source list has the wrong number of buffers "
                               "after bulkMove().",
                               twoBuffers, sourceBufferList.getSize());
  uint64_t bytesInLastTwoBuffers = 900;
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Source list has the wrong number of bytes "
                               "after bulkMove().",
                               bytesInLastTwoBuffers,
                               sourceBufferList.getTotalDataSize());

  // 3 buffers in the destination list with total size 600.
  uint64_t threeBuffers = 3;
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Destination list has the wrong number of "
                               "buffers after bulkMove().",
                               threeBuffers, destBufferList.getSize());
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Destination list has the wrong number of "
                               "bytes after bulkMove().",
                               bytesInFirstThreeBuffers,
                               destBufferList.getTotalDataSize());
}

void BufferListTest::testMoveListContents() {
  uint64_t numListables = 10;
  std::vector<DummyListable> listables(numListables);
  std::vector<uint64_t> listableSizes;

  listableSizes.push_back(1500);
  listableSizes.push_back(1300);
  listableSizes.push_back(1000);
  listableSizes.push_back(2000);
  listableSizes.push_back(1750);
  listableSizes.push_back(1230);
  listableSizes.push_back(5402);
  listableSizes.push_back(1003);
  listableSizes.push_back(1540);
  listableSizes.push_back(2020);

  CPPUNIT_ASSERT_EQUAL_MESSAGE("Expected 10 listable buffer sizes; sure you "
                               "wrote this test correctly?",
                               static_cast<size_t>(10), listableSizes.size());

  for (uint64_t i = 0; i < numListables; i++) {
    listables[i].setSize(listableSizes[i]);
  }

  BufferList<DummyListable> sourceBufferList;

  for (uint64_t i = 0; i < numListables; i++) {
    sourceBufferList.append(&(listables[i]));
  }

  // Make sure that putting the listables into the BufferList has linked them
  // together into a doubly-linked list
  assertElementsFormAList(listables, 0, numListables);

  // Make sure that sourceBufferList is sane
  assertBufferListAndBufferArrayMatch(listables, sourceBufferList);

  BufferList<DummyListable> destBufferList;

  // Make sure the destination buffer is empty prior to the move
  assertBufferListEmpty(destBufferList);

  // Move buffers from the source to the destination list
  sourceBufferList.bulkMoveBuffersTo(destBufferList, 20000);

  // Source list should now be empty
  assertBufferListEmpty(sourceBufferList);

  // Elements should still form a list
  assertElementsFormAList(listables, 0, numListables);

  // Make sure that the destination buffer list is still sane
  assertBufferListAndBufferArrayMatch(listables, destBufferList);
}

void BufferListTest::assertBufferListAndBufferArrayMatch(
  std::vector<DummyListable>& listables,
  BufferList<DummyListable>& bufferList) {

  uint64_t totalListableSize = 0;
  uint64_t numListables = listables.size();

  for (std::vector<DummyListable>::iterator iter = listables.begin();
       iter != listables.end(); iter++) {
    totalListableSize += iter->getSize();
  }

  CPPUNIT_ASSERT_EQUAL_MESSAGE("Source list has the wrong number of buffers.",
                               numListables, bufferList.getSize());
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Source list has the wrong number of bytes.",
                               totalListableSize,
                               bufferList.getTotalDataSize());
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Expected source list's head to point to "
                               "first buffer in listables", &listables[0],
                               bufferList.getHead());
}

void BufferListTest::assertBufferListEmpty(
  BufferList<DummyListable>& bufferList) {

  DummyListable* nullBuffer = NULL;

  CPPUNIT_ASSERT_EQUAL_MESSAGE("Expected new buffer lists to have 0 size",
                               static_cast<uint64_t>(0),
                               bufferList.getSize());
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Expected new buffer lists to be have 0 total "
                               "data size",
                               static_cast<uint64_t>(0),
                               bufferList.getTotalDataSize());
  CPPUNIT_ASSERT_EQUAL_MESSAGE("Expected new buffer lists to be have NULL head",
                               nullBuffer, bufferList.getHead());
}

void BufferListTest::assertElementsFormAList(
  std::vector<DummyListable>& listables, uint64_t startIndex,
  uint64_t endIndex) {

  DummyListable* nullBuffer = NULL;

  for (uint64_t i = startIndex; i < endIndex; i++) {
    if (i == startIndex) {
      CPPUNIT_ASSERT_EQUAL_MESSAGE("Head should not have a predecessor.",
                                   nullBuffer, listables[i].getPrev());
    } else {
      CPPUNIT_ASSERT_EQUAL_MESSAGE("Predecessor should be the previous node.",
                                   &(listables[i - 1]), listables[i].getPrev());
    }

    if (i == endIndex - 1)  {
      CPPUNIT_ASSERT_EQUAL_MESSAGE("Tail should not have a successor.",
                                   nullBuffer, listables[i].getNext());
    } else {
      CPPUNIT_ASSERT_EQUAL_MESSAGE("Successor should be the next node.",
                                   &(listables[i + 1]), listables[i].getNext());
    }
  }
}
