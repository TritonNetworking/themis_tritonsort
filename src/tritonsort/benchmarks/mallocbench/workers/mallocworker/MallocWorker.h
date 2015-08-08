#ifndef MALLOC_WORKER_H
#define MALLOC_WORKER_H

#include "benchmarks/mallocbench/common/MemoryManager.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"

#include <boost/math/distributions/lognormal.hpp>

class MallocWorker {
public:
  /// Constructor
  /**
     \param _id the id of the worker

     \param _name the name of the worker

     \param _memoryManager the memory manager to get from and put to

     \param _numCheckouts the number of buffers to check out

     \param _averageBufferSize the average size of a buffer

     \param _averageWorkTime the average time to wait after get(), before put()

     \param _workTimeVariability max % deviation for work time

     \param _averageIdleTime the average time to wait after put(), before get()

     \param _idleTimeVariability max % eeviation for idle time

     \param _maxRequestSize the maximum size of a request
   */
  MallocWorker(uint64_t _id, const std::string& _name,
               MemoryManager& _memoryManager, const uint64_t _numCheckouts,
               const uint64_t _averageBufferSize,
               const uint64_t _averageWorkTime,
               const uint64_t _workTimeVariability,
               const uint64_t _averageIdleTime,
               const uint64_t _idleTimeVariability,
               const uint64_t _maxRequestSize)
    : id(_id),
      name(_name),
      memoryManager(_memoryManager),
      numCheckouts(_numCheckouts),
      averageBufferSize(_averageBufferSize),
      averageWorkTime(_averageWorkTime),
      workTimeVariability(_workTimeVariability),
      averageIdleTime(_averageIdleTime),
      idleTimeVariability(_idleTimeVariability),
      maxRequestSize(_maxRequestSize),
      logger(_name, _id),
      threadID(0),
      lognormalDistribution(0, 0.5)
    {
      blockTimeStatID = logger.registerStat("block_time");
    }

  /**
     Runs the main logic of the MallocWorker. Checks out numCheckouts buffers,
     each with _roughly_ an average size of averageBufferSize distributed
     according to boost's lognormal(location = 0, scale = 0.5)
   */
  void run() {
    logger.logDatum("start_time", Timer::posixTimeInMicros());

    for (uint64_t i = 0; i < numCheckouts; ++i) {
      double random0to1 = ((double) rand()) / RAND_MAX;
      uint64_t bufferSize = (double)averageBufferSize *
        quantile(lognormalDistribution, random0to1);\
      if (bufferSize > maxRequestSize) {
        bufferSize = maxRequestSize;
      }
      uint64_t workTime = getRandomValue(averageWorkTime, workTimeVariability);
      uint64_t idleTime = getRandomValue(averageIdleTime, idleTimeVariability);

      Timer getTimer;
      getTimer.start();
      uint8_t* buffer = memoryManager.get(bufferSize, id);
      getTimer.stop();
      if (getTimer.getElapsed() > 100) {
        // Use an explicit 100us threshold to filter unblocked gets from
        // the summary statistics
        logger.add(blockTimeStatID, getTimer.getElapsed());
      }
      usleep(workTime);
      memoryManager.put(buffer, id);
      usleep(idleTime);
    }

    logger.logDatum("end_time", Timer::posixTimeInMicros());
  }

  /**
     Spawn hack. Modified from \sa{BaseWorker::spawn()}. Creates a new thread
     for this instance of the MallocWorker.
   */
  void spawn() {
    int status = pthread_create(&threadID, NULL,
                                &MallocWorker::threadSpawner,
                                new threadArgs(this));
    if (status != 0) {
      ABORT("pthread_create() returned status %d", status);
    }
  }

  /**
     Waits for the thread to exit. Should be called from main.
   */
  void join() {
    int status = pthread_join(threadID, NULL);
    ABORT_IF(status != 0, "pthread_join() returned status %d", status);
  }

  /**
     Gets the name of the worker.

     \return the name of the worker
   */
  const std::string& getName() const {
    return name;
  }

  uint64_t id;
private:
  /**
     Compute a uniform random deviation from an average, given a maximum
     percent deviation

     \param average the average to deviate from

     \param percentDeviation the maximum percentage deviation from the average

     \return a random deviation
   */
  uint64_t getRandomDeviation(uint64_t average, uint64_t percentDeviation) {
    double maxDeviation = percentDeviation;
    // Convert to multiplicative factor
    maxDeviation /= 100;
    // Multiply average
    maxDeviation *= average;
    return (rand() % ((2 * (uint64_t)maxDeviation) + 1)) - maxDeviation;
  }

  /**
     Compute a uniform random value based on an average and a maximum percent
     deviation.

     \param average the average to deviate from

     \param percentDeviation the maximum percentage deviation from the average

     \return a random value
   */
  uint64_t getRandomValue(uint64_t average, uint64_t percentDeviation) {
    return getRandomDeviation(average, percentDeviation) + average;
  }

  const std::string name;
  MemoryManager& memoryManager;
  const uint64_t numCheckouts;
  const uint64_t averageBufferSize;
  const uint64_t averageWorkTime;
  const uint64_t workTimeVariability;
  const uint64_t averageIdleTime;
  const uint64_t idleTimeVariability;
  const uint64_t maxRequestSize;

  // Stats
  StatLogger logger;
  uint64_t blockTimeStatID;

  // Threading
  pthread_t threadID;

  struct threadArgs
  {
    MallocWorker* This;
    threadArgs(MallocWorker* r) : This(r) {}
  };

  /**
     Spawn hack. Modified from \sa{BaseWorker::threadSpawner()}. This is the
     starting function for the spawned thread. It unwraps the object from the
     arguments and calls run().
   */
  static void* threadSpawner(void* arg) {
    threadArgs* threadArg = static_cast<threadArgs*>(arg);
    MallocWorker* This = threadArg->This;

    std::ostringstream threadNameStream;
    threadNameStream << This->getName() << " " << This->id;

    setThreadName(threadNameStream.str());

    This->run();
    return NULL;
  }

  // Boost
  boost::math::lognormal lognormalDistribution;
};

#endif // MALLOC_WORKER_H
