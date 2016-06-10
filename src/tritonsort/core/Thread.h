#ifndef THEMIS_THREAD_H
#define THEMIS_THREAD_H

#include <pthread.h>
#include <string>

namespace themis {

/**
   Thread encapsulates pthreads into a more C++-friendly interface. Threads can
   be used to run standalone functions, or can serve as the base-class for
   threaded objects. To run a standalone function, use the following interface:

   void* func(void* args); // Some function you want to run in a thread

   void* args;
   Thread threadedFunction("thread name", &func);

   threadedFunction.startThread(args);
   ...
   void* result = threadedFunction.stopThread();

   To derive from this class, override the thread() function to do whatever you
   want. Use the single-argument constructor in your derived class's
   initialization list, and then use the following interface:

   DerivedClass derived;
   void* args;
   derived.startThread(args);
   ...
   void* result = derived.stopThread();

   If you want to hide all the nasty void* stuff, or don't care about arguments
   and return values, you can make the inheritance private and then provide your
   own start and stop methods that shadow startThread and stopThread:

   DerivedClass derived;
   derived.start();
   ...
   derived.stop();

   Derived classes can check the value of the Thread::stop boolean to know when
   stopThread() has been called. This can be used to exit loops.
 */
class Thread {
public:
  /// Constructor
  /**
     Create a thread object that runs a standalone function.

     \param threadName the name that will appear to the OS, and is accessible
     through monitoring software like top

     \param threadFunction the function to run
   */
  Thread(const std::string& threadName, void* (*threadFunction) (void*));

  /// Constructor
  /**
     Create a thread object without a function. The thread will do nothing
     unless you override the thread() member function.

     \param threadName the name that will appear to the OS, and is accessible
     through monitoring software like top
   */
  Thread(const std::string& threadName);

  /// Destructor
  virtual ~Thread();

  /**
     Start the thread.

     \param args the arguments to pass to the thread's function
   */
  void startThread(void* args = NULL);

  /**
     Wait for the thread to stop.

     \return the return value of the function
   */
  void* stopThread();

protected:
  pthread_t threadID;

  /// Flag to signal the thread to stop. Derived classes can check this value in
  /// their thread() functions to know when to stop.
  bool stop;

private:
  /// Structure for passing arguments to pthreads, since they can only take a
  /// single void* argument.
  struct PThreadArgs {
    Thread* This;
    void* args;
    PThreadArgs(Thread* thread, void* threadArgs)
      : This(thread),
        args(threadArgs) {}
  };

  /**
     The function actually run by pthread_create. The pthread interface does not
     support member functions, so this static function will wrap a Thread object
     and call the thread() member function.

     \param args a PThreadArgs structure

     \return the return value of the threaded function
   */
  static void* pthreadFunction(void* args);

  /**
     This is the threaded member function run by a Thread object. By default it
     calls the function passed into the Thread constructor, but you can override
     it in derived classes to do whatever you need.

     \param args whatever arguments your function takes

     \return whatever your function returns
   */
  virtual void* thread(void* args);

  const std::string threadName;
  void* (*threadFunction) (void*);

  bool terminated;
};

} // namespace themis

#endif // THEMIS_THREAD_H
