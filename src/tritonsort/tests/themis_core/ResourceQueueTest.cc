#include "ResourceQueueTest.h"
#include "common/DummyWorkUnit.h"
#include "core/ResourceQueue.h"

TEST_F(ResourceQueueTest, testEmpty) {
  ResourceQueue q(1);
  EXPECT_TRUE(q.empty());
  DummyWorkUnit* dummy = new DummyWorkUnit;
  q.push(dummy);
  EXPECT_TRUE(!q.empty());
  q.pop();
  EXPECT_TRUE(q.empty());
  delete dummy;
}

TEST_F(ResourceQueueTest, testSize) {
  DummyWorkUnit* dummy1 = new DummyWorkUnit();
  DummyWorkUnit* dummy2 = new DummyWorkUnit();
  DummyWorkUnit* dummy3 = new DummyWorkUnit();

  ResourceQueue q(3);

  EXPECT_EQ((uint64_t) 0, q.size());

  q.push(dummy1);
  q.push(dummy2);
  q.push(dummy3);

  EXPECT_EQ((uint64_t) 3, q.size());
  q.pop();
  EXPECT_EQ((uint64_t) 2, q.size());
  q.push(dummy1);
  EXPECT_EQ((uint64_t) 3, q.size());
  q.pop();
  q.pop();
  q.pop();
  EXPECT_EQ((uint64_t) 0, q.size());

  delete dummy1;
  delete dummy2;
  delete dummy3;
}

TEST_F(ResourceQueueTest, testFront) {
  DummyWorkUnit* dummy = new DummyWorkUnit();
  ResourceQueue q(1);
  q.push(dummy);
  EXPECT_EQ(dummy, dynamic_cast<DummyWorkUnit*>(q.front()));
  delete dummy;
}

TEST_F(ResourceQueueTest, testStealFrom) {
// Overkill is one of my many modes
  for (uint64_t destQueueSize = 1; destQueueSize < 5; destQueueSize++) {
    for (uint64_t sourceQueueSize = 1; sourceQueueSize <= destQueueSize;
         sourceQueueSize++) {
      for (uint64_t numSourceElements = 1; numSourceElements <= sourceQueueSize;
           numSourceElements++) {
        for (uint64_t numDestElements = 1; numDestElements <=
               destQueueSize - numSourceElements; numDestElements++) {
          for (uint64_t sourceOffset = 0; sourceOffset < sourceQueueSize;
               sourceOffset++) {
            for (uint64_t destOffset = 0; destOffset < destQueueSize;
              destOffset++) {
              for (uint64_t numElementsToSteal = 0; numElementsToSteal <=
                     numSourceElements; numElementsToSteal++) {
                testStealFrom(sourceQueueSize, destQueueSize, numSourceElements,
                              numDestElements, sourceOffset, destOffset,
                              numElementsToSteal);
              }
            }
          }
        }
      }
    }
  }
}

void ResourceQueueTest::testStealFrom(uint64_t sourceQueueSize,
                                      uint64_t destQueueSize,
                                      uint64_t numSourceElements,
                                      uint64_t numDestElements,
                                      uint64_t sourceOffset,
                                      uint64_t destOffset,
                                      uint64_t numElementsToSteal) {
  std::ostringstream messageTextStream;
  messageTextStream << "testStealFrom(sourceQueueSize=" << sourceQueueSize
                    << ", destQueueSize=" << destQueueSize
                    << ", numSourceElements=" << numSourceElements
                    << ", numDestElements=" << numDestElements
                    << ", sourceOffset=" << sourceOffset
                    << ", destOffset=" << destOffset
                    << ", numElementsToSteal=" << numElementsToSteal
                    << ")";

  std::string messageTextString(messageTextStream.str());
  const char* messageText = messageTextString.c_str();

  ResourceQueue sourceQueue(sourceQueueSize);
  ResourceQueue destQueue(destQueueSize);

  // Set up dummy resources for source and destination
  DummyWorkUnit* sourceDummies[numSourceElements];

  for (uint64_t i = 0; i < numSourceElements; i++) {
    sourceDummies[i] = new DummyWorkUnit();
  }

  DummyWorkUnit* destDummies[numDestElements];

  for (uint64_t i = 0; i < numDestElements; i++) {
    destDummies[i] = new DummyWorkUnit();
  }

// "Pad" the source and destination queues so that the resources in each queue
// will be offset within their respective arrays internally
  DummyWorkUnit* paddingDummy = new DummyWorkUnit();

  for (uint64_t i = 0; i < sourceOffset; i++) {
    sourceQueue.push(paddingDummy);
  }

  sourceQueue.push(sourceDummies[0]);

  for (uint64_t i = 0; i < sourceOffset; i++) {
    sourceQueue.pop();
  }

  for (uint64_t i = 0; i < destOffset; i++) {
    destQueue.push(paddingDummy);
  }

  destQueue.push(destDummies[0]);

  for (uint64_t i = 0; i < destOffset; i++) {
    destQueue.pop();
  }

  // Push elements onto each queue

  for (uint64_t i = 1; i < numSourceElements; i++) {
    sourceQueue.push(sourceDummies[i]);
  }

  for (uint64_t i = 1; i < numDestElements; i++) {
    destQueue.push(destDummies[i]);
  }

  // Do the actual operation that we're testing (finally!)
  destQueue.stealFrom(sourceQueue, numElementsToSteal);

  // Make sure sizes are right
  uint64_t resultingDestQueueSize = destQueue.size();
  uint64_t resultingSourceQueueSize = sourceQueue.size();

  EXPECT_EQ(numDestElements + numElementsToSteal, resultingDestQueueSize)
      << messageText;
  EXPECT_EQ(numSourceElements - numElementsToSteal, resultingSourceQueueSize)
      << messageText;

  // Verify the contents of the queues

  for (uint64_t i = 0; i < resultingDestQueueSize; i++) {
    if (i < numDestElements) {
      EXPECT_EQ(destDummies[i], dynamic_cast<DummyWorkUnit*>(
          destQueue.front())) << messageText;
    } else {
      EXPECT_EQ(sourceDummies[i - numDestElements],
                dynamic_cast<DummyWorkUnit*>(destQueue.front())) << messageText;
    }
    destQueue.pop();
  }

  for (uint64_t i = 0; i < resultingSourceQueueSize; i++) {
    EXPECT_EQ(sourceDummies[i + numElementsToSteal],
              dynamic_cast<DummyWorkUnit*>(sourceQueue.front()));
    sourceQueue.pop();
  }

  // Clean up
  delete paddingDummy;

  for (uint64_t i = 0; i < numSourceElements; i++) {
    delete sourceDummies[i];
  }

  for (uint64_t i = 0; i < numDestElements; i++) {
    delete destDummies[i];
  }
}
