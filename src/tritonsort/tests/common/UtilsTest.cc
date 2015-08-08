#include "UtilsTest.h"
#include "core/Utils.h"

void UtilsTest::setUp() {

}

void UtilsTest::tearDown() {

}

void UtilsTest::testParseCommaDelimitedList() {
  StringList testList;
  std::string testString1 = "  ";

  parseCommaDelimitedList<std::string, StringList>(testList, testString1);

  CPPUNIT_ASSERT_EQUAL((uint64_t) 0, testList.size());

  testList.clear();

  std::string testString2 = "/disk/1, /disk/2,/disk/3,,/disk/4,   ";

  parseCommaDelimitedList<std::string, StringList>(testList, testString2);

  CPPUNIT_ASSERT_EQUAL((uint64_t) 4, testList.size());

  uint64_t index = 1;

  for (StringList::iterator iter = testList.begin(); iter != testList.end();
       iter++) {
    char testDiskNameBuffer[255];
    sprintf(testDiskNameBuffer, "/disk/%lu", index++);
    std::string testDiskStr(testDiskNameBuffer);

    CPPUNIT_ASSERT_EQUAL(testDiskStr, *iter);
  }

  std::string testString3 = "42, 69, 8";

  std::vector<uint64_t> testVector;

  parseCommaDelimitedList< uint64_t, std::vector<uint64_t> >(
    testVector, testString3);

  CPPUNIT_ASSERT_EQUAL((uint64_t) 3, testVector.size());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 42, testVector[0]);
  CPPUNIT_ASSERT_EQUAL((uint64_t) 69, testVector[1]);
  CPPUNIT_ASSERT_EQUAL((uint64_t) 8, testVector[2]);

  std::string testString4 = "42";

  testVector.clear();

  parseCommaDelimitedList< uint64_t, std::vector<uint64_t> >(
    testVector, testString4);

  CPPUNIT_ASSERT_EQUAL((uint64_t) 1, testVector.size());
  CPPUNIT_ASSERT_EQUAL((uint64_t) 42, testVector[0]);

}
