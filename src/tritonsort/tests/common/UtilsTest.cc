#include "UtilsTest.h"
#include "core/Utils.h"

TEST_F(UtilsTest, testParseCommaDelimitedList) {
  StringList testList;
  std::string testString1 = "  ";

  parseCommaDelimitedList<std::string, StringList>(testList, testString1);

  EXPECT_EQ((uint64_t) 0, testList.size());

  testList.clear();

  std::string testString2 = "/disk/1, /disk/2,/disk/3,,/disk/4,   ";

  parseCommaDelimitedList<std::string, StringList>(testList, testString2);

  EXPECT_EQ((uint64_t) 4, testList.size());

  uint64_t index = 1;

  for (StringList::iterator iter = testList.begin(); iter != testList.end();
       iter++) {
    char testDiskNameBuffer[255];
    sprintf(testDiskNameBuffer, "/disk/%lu", index++);
    std::string testDiskStr(testDiskNameBuffer);

    EXPECT_EQ(testDiskStr, *iter);
  }

  std::string testString3 = "42, 69, 8";

  std::vector<uint64_t> testVector;

  parseCommaDelimitedList< uint64_t, std::vector<uint64_t> >(
    testVector, testString3);

  EXPECT_EQ((uint64_t) 3, testVector.size());
  EXPECT_EQ((uint64_t) 42, testVector[0]);
  EXPECT_EQ((uint64_t) 69, testVector[1]);
  EXPECT_EQ((uint64_t) 8, testVector[2]);

  std::string testString4 = "42";

  testVector.clear();

  parseCommaDelimitedList< uint64_t, std::vector<uint64_t> >(
    testVector, testString4);

  EXPECT_EQ((uint64_t) 1, testVector.size());
  EXPECT_EQ((uint64_t) 42, testVector[0]);

}
