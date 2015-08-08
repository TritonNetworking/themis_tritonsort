#ifndef _TRITONSORT_WORKERBARRIER_H
#define _TRITONSORT_WORKERBARRIER_H

#include <pthread.h>

/**
   All workers managed by a tracker wait on this barrier; the barrier will
   signal any waiting caller once all workers have cleared the barrier.
 */
class WorkerBarrier {
public:
  /// Constructor
  WorkerBarrier();

  /// Destructor
  virtual ~WorkerBarrier();

  /// Wait for all workers to clear the barrier
  void wait();

  /// Signal that all workers have cleared the barrier
  void signal();

  /// Reset the barrier so that it can be used again
  void reset();

private:
  pthread_mutex_t lock;
  pthread_cond_t cv;
  bool signalled;
};

#endif // _TRITONSORT_WORKERBARRIER_H
