#include <sstream>
#include <unistd.h>

#include "common/WriteToken.h"
#include "common/WriteTokenPool.h"
#include "core/MemoryUtils.h"
#include "core/ResourceMonitor.h"
#include "core/ThreadSafeQueue.h"
#include "core/TritonSortAssert.h"

WriteTokenPool::WriteTokenPool(uint64_t _tokensPerDisk, uint64_t _numDisks)
  : tokensPerDisk(_tokensPerDisk),
    numDisks(_numDisks) {

  tokenQueues = new ThreadSafeQueue<WriteToken*>[numDisks];

  for (uint64_t diskNumber = 0; diskNumber < numDisks; diskNumber++) {
    for (uint64_t tokenCount = 0; tokenCount < tokensPerDisk; tokenCount++) {
      tokenQueues[diskNumber].push(new WriteToken(diskNumber));
    }
  }

  ResourceMonitor::registerClient(this, "write_token_pool");
}

WriteTokenPool::~WriteTokenPool() {
  ResourceMonitor::unregisterClient(this);

  if (tokenQueues != NULL) {
    for (uint64_t diskNumber = 0; diskNumber < numDisks; diskNumber++) {
      ThreadSafeQueue<WriteToken*>& currentQueue = tokenQueues[diskNumber];

      uint64_t tokensPopped = 0;

      while (!currentQueue.empty()) {
        tokensPopped++;
        WriteToken* token = currentQueue.blockingPop();
        delete token;
      }

      ABORT_IF(tokensPopped != tokensPerDisk, "Not all tokens were returned "
               "to disk %llu in this token pool (%llu token(s) missing)",
               diskNumber, tokensPerDisk - tokensPopped);
    }

    delete[] tokenQueues;
    tokenQueues = NULL;
  }
}

void WriteTokenPool::resourceMonitorOutput(Json::Value& obj) {
  for (uint64_t diskNumber = 0; diskNumber < numDisks; diskNumber++) {
    std::ostringstream oss;
    oss << "tokens_in_pool_" << diskNumber;
    obj[oss.str()] = Json::UInt64(tokenQueues[diskNumber].size());
  }
}


WriteToken* WriteTokenPool::getToken(const std::set<uint64_t>& diskIDSet) {
  WriteToken* token = NULL;

  do {
    token = attemptGetToken(diskIDSet);

    if (token == NULL) {
      usleep(100);
    }
  } while (token == NULL);

  return token;
}

WriteToken* WriteTokenPool::attemptGetToken(
  const std::set<uint64_t>& diskIDSet) {

  bool successfulPop = false;
  WriteToken* token = NULL;

  for (std::set<uint64_t>::iterator iter = diskIDSet.begin();
       !successfulPop && iter != diskIDSet.end(); iter++) {
    uint64_t diskID = *iter;
    ASSERT(diskID < numDisks, "Disk ID out of bounds (%llu [received] > "
           "%llu [numDisks])", diskID, numDisks);
    successfulPop = tokenQueues[diskID].pop(token);
  }

  return token;
}

void WriteTokenPool::putToken(WriteToken* token) {
  uint64_t diskID = token->getDiskID();

  tokenQueues[diskID].push(token);
}
