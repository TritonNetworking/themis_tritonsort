#ifndef MEMORY_MANAGER_H
#define MEMORY_MANAGER_H

#include <list>
#include <map>
#include <pthread.h>
#include <stdint.h>

#include "core/IntervalStatLoggerClient.h"
#include "core/StatLogger.h"

/**
   A MemoryManager is a model of what our ideal dynamic allocation manager might
   look like. Memory is fetched/returned with get()/put() calls much like buffer
   pools. However, unlike buffer pools get() takes a size argument that
   instructs the MemoryManager to hand out a buffer of the specified size.

   If there is no more memory available, the memory manager will force the
   requesting thread to block. A number of policies are supported for waking
   blocked threads:

   ASAP = Scan the queue from the front for any request that can be serviced
          immediately and wake it up.

   FIFO = Service the head of the queue before the rest

   MLFQ_ASAP = There are two queues: low and high. Requests come in to the low
               queue where they are serviced with ASAP. If a request waits for
               enough time (currently defined as > average block time), it is
               escalated to the high priority queue which is serviced FIFO.

   NOTE 1: MemoryManager, despite the name, is not a user-level memory
   management system. Memory is allocated on get() with new[] and freed on put()
   with delete[]. We can plug in other kinds of allocators in place of the glibc
   malloc/free, such as jemalloc or tcmalloc.

   NOTE 2: MemoryManager must inherit from BaseWorker in order to work with the
   IntervalStatLogger. This is a brittle coupling built into the Themis core and
   should be fixed at some point.
 */
class MemoryManager : public IntervalStatLoggerClient {
public:
  enum WakePolicy {
    ASAP, // Wake up threads as soon as possible, starving earlier requests
    FIFO,  // Wake up threads in first in, first out order
    MLFQ_ASAP // 2 queues. High = FIFO, Low = ASAP. Promotion time = 1x average
  };

  /// Constructor
  /**
     \param capacity the total number of bytes that can be allocate

     \param verbose set to true if the memory manager should print extra info

     \param wakePolicy specifies how the memory manager should wake up blocked
            threads
   */
  MemoryManager(uint64_t capacity, bool verbose, WakePolicy wakePolicy);

  /// Destructor
  ~MemoryManager();

  /**
     Gets a buffer of a particular size from the memory manager. If the request
     cannot be satisfied, the caller waits on a condition variable until enough
     bytes are freed up.

     \param size the number of bytes requested

     \param workerID the ID of the calling worker

     \return the allocated buffer
   */
  uint8_t* get(uint64_t size, uint64_t workerID);

  /**
     Returns a buffer to the memory manager. If there were threads blocked on
     get() requests that can be satisfied after the buffer is freed up, then
     put() will signal those threads.

     \param buffer the buffer to be returned

     \param workerID the ID of the calling worker
   */
  void put(uint8_t* buffer, uint64_t workerID);

  StatLogger* initIntervalStatLogger();

  /**
     Logs interval stats.  These include the blocking queue size and maybe some
     more.
   */
  void logIntervalStats(StatLogger& intervalLogger) const;
private:
  struct RequestInfo {
    uint64_t workerID;
    uint64_t size;
    uint64_t timestamp;
    RequestInfo(uint64_t _workerID, uint64_t _size) :
      workerID(_workerID),
      size(_size),
      timestamp(Timer::posixTimeInMicros()) {}
  };

  /**
     Picks a blocked thread that can make progress and signals it. This function
     should be abstracted into a strategy when we implement this into Themis
     proper.

     NOTE: This function is called at the end of both get() and put() because
     of possible race conditions. Suppose a put() frees up enough memory for
     thread A to proceed, and another put() comes along that could also free up
     thread B. If the second put() beats thread A to the lock, then the only way
     thread B will be able to wake up is if thread A calls this function at the
     end of its get() routine.
   */
  void tryWakeBlockedThreads();

  /**
     Predicate for allowing a thread to be serviced. This function should be
     abstracted into a strategy when porting to Themis proper.

     \param request the request to be serviced
   */
  bool canService(RequestInfo* request);

  /**
     Update the running average of block times. The running averaage is used by
     the MLFQ_ASAP policy when picking a promotion timeout.

     /param requestTime the time that a blocked request had to wait before being
            serviced.
   */
  void updateAverageBlockTime(uint64_t requestTime);

  bool verbose;
  WakePolicy wakePolicy;
  uint64_t capacity;
  uint64_t remainingBytes;
  std::map<uint8_t*, uint64_t> bufferSizes;

  std::list<RequestInfo*> lowPriorityQueue;
  std::list<RequestInfo*> highPriorityQueue;

  uint64_t averageBlockTime;
  uint64_t numCompletedBlockedRequests;

  // Stats
  Timer runTimer;
  StatLogger logger;

  uint64_t numGets;
  uint64_t numPuts;
  uint64_t numCondWaitCalls;
  uint64_t blockTimeStatID;
  uint64_t lowQueueLengthStatID;
  uint64_t highQueueLengthStatID;
  uint64_t remainingBytesStatID;
  uint64_t requestSizeStatID;

  // Locks
  pthread_mutex_t lock;
  std::map<uint64_t, pthread_cond_t*> conditionVariables;
};

#endif // MEMORY_MANAGER_H
