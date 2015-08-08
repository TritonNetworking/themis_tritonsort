#ifndef TRITONSORT_BUFFER_LIST_TEST_H
#define TRITONSORT_BUFFER_LIST_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <stdint.h>
#include <vector>

class DummyListable;
template <typename T> class BufferList;

class BufferListTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( BufferListTest );
  CPPUNIT_TEST( testMoveListContents );
  CPPUNIT_TEST(testBulkMoveComplete);
  CPPUNIT_TEST(testBulkMovePartial);
  CPPUNIT_TEST(testBulkMoveMoreThanExists);
  CPPUNIT_TEST(testBulkMoveWithNonMultipleBytes);
  CPPUNIT_TEST_SUITE_END();
public:
  void testMoveListContents();
  void testBulkMoveComplete();
  void testBulkMoveMoreThanExists();
  void testBulkMovePartial();
  void testBulkMoveWithNonMultipleBytes();

private:
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
