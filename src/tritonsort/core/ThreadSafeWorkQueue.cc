#include "core/ScopedLock.h"
#include "core/ThreadSafeWorkQueue.h"

ThreadSafeWorkQueue::ThreadSafeWorkQueue() {
  pthread_mutex_init(&lock, NULL);
  pthread_cond_init(&waitingForPush, NULL);
}

ThreadSafeWorkQueue::~ThreadSafeWorkQueue() {
  pthread_mutex_destroy(&lock);
  pthread_cond_destroy(&waitingForPush);
}

void ThreadSafeWorkQueue::push(Resource* resource) {
  pthread_mutex_lock(&lock);
  queue.push(resource);
  if (resource == NULL) {
    // If multiple threads were waiting on this queue when the NULL gets pushed,
    // we should wake them all because no more work units are coming.
    pthread_cond_broadcast(&waitingForPush);
  } else {
    // However if this is a regular work unit then just wake a single thread so
    // we don't have threads waking and sleeping excessively.
    pthread_cond_signal(&waitingForPush);
  }
  pthread_mutex_unlock(&lock);
}

bool ThreadSafeWorkQueue::pop(Resource*& destResource, bool& noMoreWork) {
  pthread_mutex_lock(&lock);
  bool queueEmpty = queue.empty();

  if (!queueEmpty) {
    destResource = queue.front();
    queue.pop();
  }

  noMoreWork = queue.willNotReceiveMoreWork();
  pthread_mutex_unlock(&lock);

  return !queueEmpty;
}

Resource* ThreadSafeWorkQueue::blockingPop() {
  pthread_mutex_lock(&lock);
  while (!queue.willNotReceiveMoreWork() && queue.empty()) {
    pthread_cond_wait(&waitingForPush, &lock);
  }

  Resource* resource = NULL;

  if (!queue.empty()) {
    resource = queue.front();
    queue.pop();
  }

  pthread_mutex_unlock(&lock);

  return resource;
}

uint64_t ThreadSafeWorkQueue::size() {
  pthread_mutex_lock(&lock);
  uint64_t queueSize = queue.size();
  pthread_mutex_unlock(&lock);

  return queueSize;
}

uint64_t ThreadSafeWorkQueue::totalWorkSizeInBytes() {
  pthread_mutex_lock(&lock);
  uint64_t totalBytes = queue.totalWorkSizeInBytes();
  pthread_mutex_unlock(&lock);

  return totalBytes;
}

bool ThreadSafeWorkQueue::empty() {
  pthread_mutex_lock(&lock);
  bool queueEmpty = queue.empty();
  pthread_mutex_unlock(&lock);

  return queueEmpty;
}

bool ThreadSafeWorkQueue::willNotReceiveMoreWork() {
  pthread_mutex_lock(&lock);
  bool moreWork = queue.willNotReceiveMoreWork();
  pthread_mutex_unlock(&lock);

  return moreWork;
}

uint64_t ThreadSafeWorkQueue::moveWorkToQueue(WorkQueue& destQueue) {
  pthread_mutex_lock(&lock);
  uint64_t moveSize = queue.size();
  queue.moveWorkToQueue(destQueue);
  pthread_mutex_unlock(&lock);

  return moveSize;
}

