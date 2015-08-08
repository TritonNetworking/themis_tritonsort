#ifndef THEMIS_THREAD_SAFE_WORK_QUEUE_H
#define THEMIS_THREAD_SAFE_WORK_QUEUE_H

#include <pthread.h>

#include "core/WorkQueue.h"

class ThreadSafeWorkQueue {
public:
  ThreadSafeWorkQueue();
  virtual ~ThreadSafeWorkQueue();
  void push(Resource* resource);
  /// Pop a resource off the queue if one exists, and atomically check for
  /// the existence of more work.
  bool pop(Resource*& destResource, bool &noMoreWork);
  Resource* blockingPop();
  uint64_t size();
  uint64_t totalWorkSizeInBytes();
  bool empty();
  bool willNotReceiveMoreWork();
  uint64_t moveWorkToQueue(WorkQueue& destQueue);

private:
  pthread_mutex_t lock;
  pthread_cond_t waitingForPush;

  WorkQueue queue;
};

#endif // THEMIS_THREAD_SAFE_WORK_QUEUE_H
