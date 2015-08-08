#include <queue>

#include "common/WriteToken.h"
#include "common/WriteTokenPool.h"
#include "tests/common/WriteTokenPoolTest.h"

void WriteTokenPoolTest::testAttemptGetToExhaustion() {
  uint64_t tokensPerDisk = 8;
  uint64_t numDisks = 8;

  WriteTokenPool pool(tokensPerDisk, numDisks);

  std::set<uint64_t> diskIDSet;

  for (uint64_t i = 0; i < numDisks; i++) {
    diskIDSet.insert(i);
  }

  std::queue<WriteToken*> tokenQueue;

  for (uint64_t diskID = 0; diskID < numDisks; diskID++) {
    for (uint64_t tokenNumber = 0; tokenNumber < tokensPerDisk;
         tokenNumber++) {
      WriteToken* token = pool.attemptGetToken(diskIDSet);

      CPPUNIT_ASSERT_MESSAGE("Expected to get a token here", token != NULL);
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Got a token with the wrong ID", diskID, token->getDiskID());
      tokenQueue.push(token);
    }
  }

  WriteToken* missedToken = pool.attemptGetToken(diskIDSet);
  CPPUNIT_ASSERT_EQUAL_MESSAGE(
    "Expected to miss an attempt get on an empty pool",
    static_cast<WriteToken*>(NULL), missedToken);

  while (!tokenQueue.empty()) {
    pool.putToken(tokenQueue.front());
    tokenQueue.pop();
  }
}

