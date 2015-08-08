#include <stdlib.h>

#include "core/Resource.h"
#include "core/WorkQueue.h"

WorkQueue::WorkQueue()
  : noMoreWork(false),
    _size(0),
    totalBytes(0) {
}

void WorkQueue::push(Resource* workUnit) {
  if (workUnit == NULL) {
    noMoreWork = true;
  } else {
    work.push_back(workUnit);
    _size++;
    totalBytes += workUnit->getCurrentSize();
  }
}

void WorkQueue::pop() {
  totalBytes -= work.front()->getCurrentSize();
  work.pop_front();
  _size--;
}

Resource* WorkQueue::front() const {
  return work.front();
}

Resource* WorkQueue::back() const {
  return work.back();
}

uint64_t WorkQueue::size() const {
  return _size;
}

uint64_t WorkQueue::totalWorkSizeInBytes() const {
  return totalBytes;
}

bool WorkQueue::empty() const {
  return _size == 0;
}

void WorkQueue::moveWorkToQueue(WorkQueue& destQueue) {
  destQueue.work.splice(destQueue.work.end(), work);
  // This is equivalent to enqueueing the NULL that would have been on the end
  // of the queue
  destQueue.noMoreWork = noMoreWork;
  destQueue._size += _size;
  destQueue.totalBytes += totalBytes;
  _size = 0;
  totalBytes = 0;
}

bool WorkQueue::willNotReceiveMoreWork() const {
  return noMoreWork;
}
