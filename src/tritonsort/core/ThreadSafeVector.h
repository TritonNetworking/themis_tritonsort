#ifndef THEMIS_THREAD_SAFE_VECTOR_H
#define THEMIS_THREAD_SAFE_VECTOR_H

#include <algorithm>
#include <pthread.h>
#include <stdexcept>
#include <vector>

#include "core/TritonSortAssert.h"

template <typename T> class ThreadSafeVector {
public:
  typedef typename std::vector<T>::iterator iterator;

  ThreadSafeVector()
    : iterating(false) {
    pthread_mutex_init(&lock, NULL);
  }

  virtual ~ThreadSafeVector() {
    pthread_mutex_destroy(&lock);
  }

  uint64_t size() {
    pthread_mutex_lock(&lock);
    uint64_t vectorSize = vect.size();
    pthread_mutex_unlock(&lock);

    return vectorSize;
  }

  void resize(uint64_t size) {
    pthread_mutex_lock(&lock);
    vect.resize(size);
    pthread_mutex_unlock(&lock);
  }

  T& operator[](const uint64_t& index) {
    T* element = NULL;

    pthread_mutex_lock(&lock);
    try {
      element = &(vect.at(index));
    } catch (std::out_of_range& e) {
      ABORT("Index %llu out-of-range (vector size: %llu)", index, vect.size());
    }
    pthread_mutex_unlock(&lock);

    return *element;
  }

  void push(const T& element) {
    pthread_mutex_lock(&lock);
    vect.push_back(element);
    pthread_mutex_unlock(&lock);
  }

  bool remove(const T& element) {
    pthread_mutex_lock(&lock);
    iterator iter = std::find(vect.begin(), vect.end(), element);
    bool removed = false;
    if (iter != vect.end()) {
      vect.erase(iter);
      removed = true;
    }

    pthread_mutex_unlock(&lock);
    return removed;
  }

  void clear() {
    pthread_mutex_lock(&lock);
    vect.clear();
    pthread_mutex_unlock(&lock);
  }

  void beginThreadSafeIterate() {
    pthread_mutex_lock(&lock);
    iterating = true;
  }

  iterator begin() {
    ABORT_IF(!iterating, "Must call beginThreadSafeIterate() before iterating");
    return vect.begin();
  }

  iterator end() {
    ABORT_IF(!iterating, "Must call beginThreadSafeIterate() before iterating");
    return vect.end();
  }

  void endThreadSafeIterate() {
    iterating = false;
    pthread_mutex_unlock(&lock);
  }

private:
  bool iterating;
  pthread_mutex_t lock;
  std::vector<T> vect;
};

#endif // THEMIS_THREAD_SAFE_VECTOR_H
