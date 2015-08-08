#ifndef _TRITONSORT_THREADSAFE_QUEUE_H
#define _TRITONSORT_THREADSAFE_QUEUE_H

#include <pthread.h>
#include <queue>
#include <stdint.h>

/// Thread-safe wrapper around an STL queue
template <typename T> class ThreadSafeQueue {
public:
  /// Constructor
  ThreadSafeQueue() {
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cv, NULL);
  }

  /// Destructor
  virtual ~ThreadSafeQueue() {
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cv);
  }

  /// Push an item onto the queue
  /**
     A copy of the provided object will be pushed onto the back of the queue

     \param obj the object to push
   */
  void push(const T& obj);

  /// Pop the frontmost item from the queue in a non-blocking fashion
  /**
     \param[out] a reference to the popped element

     \return true if the queue was not empty and an element was returned, false
     otherwise. destination's value is unspecified if this method returns false
  */
  bool pop(T& destination);

  /// Pop the frontmost item from the queue in a blocking fashion
  /**
     Wait until the queue is non-empty, then pop the frontmost item from it and
     return it

     \return the frontmost item from the queue
   */
  T blockingPop();

  /// Is the queue empty?
  /**
     \warning Doing anything subsequent to this empty() call assuming that the
     queue is empty is dangerous

     \return true if the queue was empty at the time of the call, false
     otherwise
   */
  bool empty();

  /// Get the number of elements in the queue
  /**
     \warning size() is not thread-safe with respect to other operations on the
     queue, and should be considered at best a snapshot of the queue's size.

     \return the number of elements in the queue
  */
  uint64_t size();
private:
  std::queue<T> queue;
  pthread_mutex_t mutex;
  pthread_cond_t cv;
};

template <typename T> void ThreadSafeQueue<T>::push(const T& obj) {
  pthread_mutex_lock(&mutex);
  queue.push(obj);
  pthread_cond_signal(&cv);
  pthread_mutex_unlock(&mutex);
}

template <typename T> bool ThreadSafeQueue<T>::pop(T& obj) {
  bool popped = false;

  pthread_mutex_lock(&mutex);
  if (!queue.empty()) {
    obj = queue.front();
    queue.pop();
    popped = true;
  }
  pthread_mutex_unlock(&mutex);

  return popped;
}

template <typename T> T ThreadSafeQueue<T>::blockingPop() {
  pthread_mutex_lock(&mutex);
  while (queue.empty()) {
    pthread_cond_wait(&cv, &mutex);
  }
  // obj isn't a reference because the contained object will be destroyed
  // after pop() anyway.
  T obj = queue.front();
  queue.pop();
  pthread_mutex_unlock(&mutex);
  return obj;
}

template <typename T> bool ThreadSafeQueue<T>::empty() {
  bool empty = false;
  pthread_mutex_lock(&mutex);
  empty = queue.empty();
  pthread_mutex_unlock(&mutex);
  return empty;
}

template <typename T> uint64_t ThreadSafeQueue<T>::size() {
  pthread_mutex_lock(&mutex);
  uint64_t queueSize = queue.size();
  pthread_mutex_unlock(&mutex);

  return queueSize;
}
#endif // _TRITONSORT_THREADSAFE_QUEUE_H
