#include "tritonsort/config.h"
#include "core/StatusPrinter.h"
#include "core/WorkerBarrier.h"

WorkerBarrier::WorkerBarrier() {
  pthread_mutex_init(&lock, NULL);
  pthread_cond_init(&cv, NULL);
  signalled = false;
}

void WorkerBarrier::wait() {
  pthread_mutex_lock(&lock);
  while (!signalled) {
    pthread_cond_wait(&cv, &lock);
  }
  pthread_mutex_unlock(&lock);
}

void WorkerBarrier::signal() {
  pthread_mutex_lock(&lock);
  signalled = true;

  pthread_cond_signal(&cv);
  pthread_mutex_unlock(&lock);
}

void WorkerBarrier::reset() {
  signalled = false;
}

WorkerBarrier::~WorkerBarrier() {
  pthread_mutex_destroy(&lock);
  pthread_cond_destroy(&cv);
}
