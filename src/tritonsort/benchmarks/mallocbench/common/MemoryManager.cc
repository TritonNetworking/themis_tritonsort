#include <algorithm>

#include "MemoryManager.h"
#include "core/IntervalStatLogger.h"
#include "core/ScopedLock.h"
#include "core/StatusPrinter.h"
#include "core/Timer.h"
#include "core/TritonSortAssert.h"

MemoryManager::MemoryManager(uint64_t _capacity, bool _verbose,
                             WakePolicy _wakePolicy) :
  verbose(_verbose),
  wakePolicy(_wakePolicy),
  capacity(_capacity),
  remainingBytes(_capacity),
  averageBlockTime(0),
  numCompletedBlockedRequests(0),
  logger("MemoryManager"),
  numGets(0),
  numPuts(0),
  numCondWaitCalls(0) {

  pthread_mutex_init(&lock, NULL);

  blockTimeStatID = logger.registerStat("block_time");
  requestSizeStatID = logger.registerStat("request_size");
  runTimer.start();

  IntervalStatLogger::registerClient(this);
}

MemoryManager::~MemoryManager() {
  runTimer.stop();

  IntervalStatLogger::unregisterClient(this);

  logger.logDatum("capacity", capacity);
  logger.logDatum("num_gets", numGets);
  logger.logDatum("num_puts", numPuts);
  logger.logDatum("num_cond_wait_calls", numCondWaitCalls);
  logger.logDatum("run_time", runTimer.getElapsed());

  pthread_mutex_destroy(&lock);

  // Destroy and delete any CVs created dynamically during the run
  for (std::map<uint64_t, pthread_cond_t*>::iterator iter =
         conditionVariables.begin();
       iter != conditionVariables.end(); ++iter) {
    pthread_cond_destroy(iter->second);
    delete iter->second;
  }

  ABORT_IF(!lowPriorityQueue.empty(),
           "Somehow the MemoryManager was destroyed but there are still "
           "blocked requests outstanding in the low priority queue");
  ABORT_IF(!highPriorityQueue.empty(),
           "Somehow the MemoryManager was destroyed but there are still "
           "blocked requests outstanding in the high priority queue");
}

uint8_t* MemoryManager::get(uint64_t size, uint64_t workerID) {
  ScopedLock scopedLock(&lock);
  ++numGets;
  logger.add(requestSizeStatID, size);
  ABORT_IF(size > capacity,
           "Got request size %llu > capacity %llu", size, capacity);

  // Create a CV for this thread
  if (!conditionVariables.count(workerID)) {
    pthread_cond_t* newCV = new pthread_cond_t;
    pthread_cond_init(newCV, NULL);
    conditionVariables[workerID] = newCV;
  }

  // Create a pending request
  RequestInfo* thisRequest = new RequestInfo(workerID, size);
  lowPriorityQueue.push_back(thisRequest);

  uint8_t* buffer = NULL;

  if (!canService(thisRequest) || size > remainingBytes) {
    if (verbose) {
      StatusPrinter::add("[%llu] BLOCK! - request size %llu", workerID, size);
      StatusPrinter::add("MEMORY REMAINING [%llu / %llu]",
                         remainingBytes, capacity);
    }

    // Go to sleep until the request can be fulfilled.
    Timer blockedTimer;
    blockedTimer.start();
    // Wait until there are enough bytes and the chosen policy permits the
    // request to get service.
    while (!canService(thisRequest) || size > remainingBytes) {
      ++numCondWaitCalls;
      pthread_cond_wait(conditionVariables[workerID], &lock);
    }
    blockedTimer.stop();
    logger.add(blockTimeStatID, blockedTimer.getElapsed());
    updateAverageBlockTime(blockedTimer.getElapsed());
  }

  // Service the request
  buffer = new uint8_t[size];
  remainingBytes -= size;
  bufferSizes[buffer] = size;

  if (verbose) {
    StatusPrinter::add("[%llu] get() retrieved %llu byte buffer %p",
                       workerID, size, buffer);
    StatusPrinter::add("MEMORY REMAINING [%llu / %llu]",
                       remainingBytes, capacity);
  }

  // Delete the request struct
  lowPriorityQueue.remove(thisRequest);
  highPriorityQueue.remove(thisRequest);
  delete thisRequest;

  // Even though a get() CONSUMES memory, it could be the case that we had a
  // race condition where multiple puts() succeeded before this thread woke up
  // and we need to wake up another thread.
  tryWakeBlockedThreads();

  return buffer;
}

void MemoryManager::put(uint8_t* buffer, uint64_t workerID) {
  ScopedLock scopedLock(&lock);
  ++numPuts;

  // Get the size of the returned buffer
  ABORT_IF(bufferSizes.count(buffer) != 1,
           "Tried to put unknown buffer %p", buffer);
  uint64_t bufferSize = bufferSizes[buffer];

  // Free the buffer
  bufferSizes.erase(buffer);
  remainingBytes += bufferSize;
  delete[] buffer;

  ABORT_IF(remainingBytes > capacity,
           "Somehow we have %llu bytes remaining but only %llu in total",
           remainingBytes, capacity);
  if (verbose) {
    StatusPrinter::add("[%llu] put() returned buffer %p with size %llu",
                       workerID, buffer, bufferSize);
    StatusPrinter::add("MEMORY REMAINING [%llu / %llu]",
                       remainingBytes, capacity);
  }

  tryWakeBlockedThreads();
}

StatLogger* MemoryManager::initIntervalStatLogger() {
  StatLogger* intervalLogger = new StatLogger("MemoryManager");

  lowQueueLengthStatID = intervalLogger->registerStat(
    "low_queue_length_times_100");
  highQueueLengthStatID = intervalLogger->registerStat(
    "high_queue_length_times_100");
  remainingBytesStatID = intervalLogger->registerStat("remaining_bytes");

  return intervalLogger;
}

void MemoryManager::logIntervalStats(StatLogger& intervalLogger) const {
  intervalLogger.add(lowQueueLengthStatID, lowPriorityQueue.size() * 100);
  intervalLogger.add(highQueueLengthStatID, highPriorityQueue.size() * 100);
  intervalLogger.add(remainingBytesStatID, remainingBytes);
}

void MemoryManager::tryWakeBlockedThreads() {
  // IMPLEMENTED POLICIES:
  // ASAP = service a request that can proceed
  // FIFO = service the first request in the list if it can proceed, otherwise
  //        keep waiting
  // MLFQ_ASAP = low priority and high priority queues. Low priority is serviced
  //        with ASAP. After a request blocks for 3x the running average block
  //        request time, it moves to the high priority queue which is serviced
  //        FIFO

  if (wakePolicy == ASAP) {
    // If there is a blocked request that can be serviced, do it.
    for (std::list<RequestInfo*>::iterator iter = lowPriorityQueue.begin();
         iter != lowPriorityQueue.end(); ++iter) {
      if (remainingBytes >= (*iter)->size) {
        // Wake the worker waiting on this request
        pthread_cond_signal(conditionVariables[(*iter)->workerID]);
        // Don't wake any others
        break;
      }
    }
  } else if (wakePolicy == FIFO) {
    // Wake up the first request in the list if possible
    if (lowPriorityQueue.size() > 0) {
      RequestInfo*& firstRequest = lowPriorityQueue.front();
      if (remainingBytes >= firstRequest->size) {
        pthread_cond_signal(conditionVariables[firstRequest->workerID]);
      }
    }
  } else if (wakePolicy == MLFQ_ASAP) {
    // Escalate long-waiting requests to high priority
    uint64_t now = Timer::posixTimeInMicros();
    for (std::list<RequestInfo*>::iterator iter = lowPriorityQueue.begin();
         iter != lowPriorityQueue.end(); ++iter) {
      // Use 1x average block time as the threshold for aging
      // TODO(MC): This 1x average is kind of arbitrary. Better solution?
      if (now - (*iter)->timestamp > averageBlockTime) {
        // Move to high priority queue
        highPriorityQueue.push_back(*iter);
        // Delete from low priority queue and set the iterator to the previous
        // element.
        iter = lowPriorityQueue.erase(iter);
        --iter;
      } else {
        // This request is too new, and all subsequent requests in this queue
        // will also be too new.
        break;
      }
    }

    // Wake the first high priority thread if there is one
    if (!highPriorityQueue.empty()) {
      RequestInfo*& firstRequest = highPriorityQueue.front();
      if (remainingBytes >= firstRequest->size) {
        pthread_cond_signal(conditionVariables[firstRequest->workerID]);
      }
    } else {
      // Wake a low priority thread using ASAP
      for (std::list<RequestInfo*>::iterator iter = lowPriorityQueue.begin();
           iter != lowPriorityQueue.end(); ++iter) {
        if (remainingBytes >= (*iter)->size) {
          // Wake the worker waiting on this request
          pthread_cond_signal(conditionVariables[(*iter)->workerID]);
          // Don't wake any others
          break;
        }
      }
    }
  } else {
    ABORT("Got unknown wake policy %llu", wakePolicy);
  }
}

bool MemoryManager::canService(RequestInfo* request) {
  if (wakePolicy == ASAP) {
    return true;
  } else if (wakePolicy == FIFO) {
    ABORT_IF(lowPriorityQueue.empty(),
             "Somehow the queue is empty, but thread %llu wants to wake up",
             request->workerID);
    return lowPriorityQueue.front() == request;
  } else if (wakePolicy == MLFQ_ASAP) {
    if (!highPriorityQueue.empty()) {
      return highPriorityQueue.front() == request;
    }
    return true;
  } else {
    ABORT("Got unknown wake policy %llu", wakePolicy);
  }
  // Unreachable, but otherwise we get warnings
  return false;
}

void MemoryManager::updateAverageBlockTime(uint64_t requestTime) {
  ++numCompletedBlockedRequests;
  // All values are unsigned, so make sure to prevent negative results from
  // subtraction
  if (requestTime >= averageBlockTime) {
    averageBlockTime += (requestTime - averageBlockTime) /
      (numCompletedBlockedRequests);
  } else {
    averageBlockTime -= (averageBlockTime - requestTime) /
      (numCompletedBlockedRequests);
  }
}
