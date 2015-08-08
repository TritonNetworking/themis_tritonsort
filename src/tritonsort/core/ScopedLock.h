#ifndef TRITONSORT_SCOPED_LOCK_H
#define TRITONSORT_SCOPED_LOCK_H

#include <pthread.h>

/**
   The whole point of a scoped lock is that you acquire a lock when you create
   the ScopedLock object and the lock is automatically released when you go out
   of scope. This makes some common things easier to deal with and more robust.

   For example:

   void f() {
     pthread_mutex_lock(&lock);

     if (condition) {
       // Have to remember to unlock the lock here or we'll deadlock
       pthread_mutex_unlock(&lock);
       return;
     }

     x += 5;

     // Have to remember to unlock here too
     pthread_mutex_unlock(&lock);
   }

   This is kind of brittle; what if we throw an exception or something?

   Replace code that looks like that with this:

   void f() {
     ScopedLock scopedLock(&lock);

     if (condition) {
       return;
     }

     x += 5;
   }

   When ScopedLock goes out of scope and gets destructed, it will release the
   lock automatically. Magic!
 */
class ScopedLock {
public:
  /// Constructor
  /**
     Locks the provided mutex at construction-time.

     \param mutex a pointer to the mutex to lock
   */
  ScopedLock(pthread_mutex_t* mutex) {
    this->mutex = mutex;
    pthread_mutex_lock(mutex);
  }

  /// Destructor
  /**
     Unlocks the mutex provided at construction as a side-effect of destruction.
   */
  virtual ~ScopedLock() {
    pthread_mutex_unlock(mutex);
  }

private:
  pthread_mutex_t* mutex;
};

#endif // TRITONSORT_SCOPED_LOCK_H
