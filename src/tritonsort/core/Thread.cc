#include <string.h>

#include "core/Thread.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"

Thread::Thread(const std::string& _threadName, void* (*_threadFunction) (void*))
  : threadID(0),
    stop(true),
    threadName(_threadName),
    threadFunction(_threadFunction),
    terminated(true) {
}

Thread::Thread(const std::string& _threadName)
  : threadID(0),
    stop(true),
    threadName(_threadName),
    threadFunction(NULL),
    terminated(true) {
}

Thread::~Thread() {
  // Make sure we've stopped and terminated.
  ASSERT(stop, "We should have called stopThread() before destruction time.");
  ASSERT(terminated, "Thread should have terminated before destruction time.");
}

void Thread::startThread(void* args) {
  int status = pthread_create(&threadID, NULL,
                              &Thread::pthreadFunction,
                              new PThreadArgs(this, args));
  ABORT_IF(status != 0, "pthread_create() returned status %d", status);
  stop = false;
  terminated = false;
}

void* Thread::stopThread() {
  // Set a boolean flag that the member function thread() can access. This lets
  // us build loops with external termination conditions.
  stop = true;

  void* result;
  int status = pthread_join(threadID, &result);

  ABORT_IF(status != 0, "pthread_join() failed with error %d: %s",
           status, strerror(status));

  terminated = true;

  return result;
}

void* Thread::pthreadFunction(void* args) {
  // We're going to pass in a struct containing both the current Thread and the
  // actual function arguments since pthreads can only take 1 void* argument.
  PThreadArgs* pthreadArgs = static_cast<PThreadArgs*>(args);
  Thread* This = pthreadArgs->This;

  setThreadName(This->threadName);

  // Run the actual thread function.
  void* result = This->thread(pthreadArgs->args);

  delete pthreadArgs;

  return result;
}

void* Thread::thread(void* args) {
  // By default, a Thread runs the function pointer passed into its constructor.
  // However, if you derive from this class and override this method, you can do
  // whatever you want as a class member function.
  if (threadFunction != NULL) {
    // We passed a function into the constructor, so run that.
    return threadFunction(args);
  }

  // If we didn't set a function, don't do anything and return NULL.
  return NULL;
}
