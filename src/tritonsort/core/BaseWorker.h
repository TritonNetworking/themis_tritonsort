#ifndef _TRITONSORT_BASE_WORKER_H
#define _TRITONSORT_BASE_WORKER_H

#include <algorithm>
#include <iostream>
#include <iterator>
#include <queue>
#include <sched.h>
#include <set>
#include <sstream>
#include <stdint.h>
#include <stdlib.h>
#include <string>


#include "tritonsort/config.h"
#include "core/CPUAffinitySetter.h"
#include "core/CumulativeTimer.h"
#include "core/IntervalStatLoggerClient.h"
#include "core/MemoryAllocatorInterface.h"
#include "core/NamedObjectCollection.h"
#include "core/Params.h"
#include "core/ResourceMonitorClient.h"
#include "core/StatLogger.h"
#include "core/StatusPrinter.h"
#include "core/Thread.h"
#include "core/Timer.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"
#include "core/WorkQueue.h"
#include "core/WorkerTrackerInterface.h"
#include "core/constants.h"

class ResourceQueue;

typedef unsigned int uint;

/// Transparently defines methods and state needed by the instantiation
/// framework
#define WORKER_IMPL                                                     \
  public:                                                               \
  static BaseWorker* newInstance(                                       \
    const std::string& phaseName, const std::string& stageName,         \
    uint64_t id, Params& params,                                        \
    MemoryAllocatorInterface& memoryAllocator,                          \
    NamedObjectCollection& dependencies);


/**
   The base class from which all workers inherit
*/

class BaseWorker
  : public IntervalStatLoggerClient, public ResourceMonitorClient,
    private themis::Thread {
public:
  /// Constructor
  /**
     \param id the worker's ID, unique among the workers in its stage
     \param name the worker's stage name
  */
  BaseWorker(uint64_t id, const std::string& name);

  /// Destructor
  virtual ~BaseWorker();

  /**
     This method constructs a StatLogger and registers any stats the worker
     wishes to log with the IntervalStatLogger to it

     \return the StatLogger the worker will use to log interval stats
   */
  virtual StatLogger* initIntervalStatLogger();

  /**
      This method takes care of logging any information that the worker wishes
      to log on an interval (driven externally by the IntervalStatLogger) By
      default, this method does nothing, but individual workers can override it
      as needed.

      \param logger the StatLogger to which the worker will log interval stats
   */
  virtual void logIntervalStats(StatLogger& logger) const;

  /**
     \sa ResourceMonitorClient::resourceMonitorOutput
   */
  virtual void resourceMonitorOutput(Json::Value& obj);

  /// Spawn a new thread that runs the run() method
  void spawn();

  /// Register this worker with the resource monitor
  /**
     This method is called by the worker's parent WorkerTracker after the
     worker has been constructed and configured. The default implementation
     simply registers the worker by calling ResourceMonitor::registerClient.
   */
  virtual void registerWithResourceMonitor();

  // Unregister this worker with the resource monitor
  /**
     This method is called by the worker's parent WorkerTracker after the
     worker has been constructed and configured. The default implementation
     simply unregisters the worker by calling ResourceMonitor::unregisterClient.
   */
  virtual void unregisterWithResourceMonitor();

  /// Set the parent tracker for this worker
  /**
     \param workerTracker this worker's parent tracker
  */
  virtual void setTracker(WorkerTrackerInterface* workerTracker);

  /// Add a downstream tracker to this worker's downstream tracker list
  /**
     \param downstreamTracker the tracker to add
   */
  virtual void addDownstreamTracker(WorkerTrackerInterface* downstreamTracker);

  /// Add a downstream tracker to this worker's downstream tracker list and
  /// associate it with the given description string
  /**
     \param downstreamTracker the tracker to add
     \param trackerDescription a string describing the tracker's role
   */
  virtual void addDownstreamTracker(
    WorkerTrackerInterface* downstreamTracker,
    const std::string& trackerDescription);

  /// Copy information about this worker's downstream trackers to another worker
  /**
     When this call completes, inheritingWorker will have the same set of
     trackers (parent and downstream) that this worker does.

     \param inheritingWorker the worker to which we're passing tracker
     information
   */
  void copyTrackerInformation(BaseWorker& inheritingWorker);

  virtual void handleNamedDownstreamTracker(
    const std::string& trackerDescription, uint64_t trackerID);

  /// Injects the CPUAffinitySetter that determines the CPUs on which this
  /// worker's thread can run
  /**
     \param policy the CPUAffinitySetter for this worker
  */
  void setCPUAffinitySetter(CPUAffinitySetter& cpuAffinitySetter);

  /// Perform any state initialization necessary
  virtual void init();

  /// Perform any state cleanup necessary.
  virtual void teardown();

  /**
     Determine whether the worker is idle; used in deadlock detection.

     There are two conditions in which a worker can become idle in a way that
     the deadlock detector finds interesting:

     1) it runs out of work to process (its work queue is empty)
     2) it blocks on a saturated downstream queue

     Determine whether the worker is blocked waiting for work from upstream
     stages. Used in deadlock detection.

     It is conceivable that a race condition causes this to return true
     incorrectly, but this should be unlikely.

     \return true if the worker is blocked by upstream stages, false otherwise
   */
  virtual bool isIdle() const;

  /// \return the number of work units this worker has consumed from upstream
  /// stages
  uint64_t numWorkUnitsConsumed() const;

  // Wait for the worker's main thread to exit.
  void waitForWorkerToFinish();

  /// Get stage name for this worker
  /**
     \return the worker's stage name
   */
  const std::string& getName() const;

  /// Get ID for this worker
  /**
     \return the worker's ID
   */
  uint64_t getID() const;

  /// Log the amount of time this worker has waited for memory allocation to
  /// complete
  void startMemoryAllocationTimer();
  void stopMemoryAllocationTimer();

  /**
     Log that a work unit has been consumed by this worker

     \param workUnit the work unit that has been consumed
   */
  void logConsumed(Resource* workUnit);

  /**
     Log that a certain number of bytes has been consumed by this worker

     \param numWorkUnits the number of bytes consumed
     \param numBytes the number of bytes consumed
  */
  void logConsumed(uint64_t numWorkUnits, uint64_t numBytes);

protected:
  /// Start the timer recording that you're waiting for work
  virtual void startWaitForWorkTimer();

  /// Stop the timer recording that you're waiting for work
  virtual void stopWaitForWorkTimer();

  /// Start the enqueue timer
  void startEnqueueTimer();

  /// Stop the enqueue timer
  void stopEnqueueTimer();

  /// Start the runtime timer
  void startRuntimeTimer();

  /// Stop the runtime time
  void stopRuntimeTimer();

  /**
     Subclasses can call this method to log their worker type so that log
     processing programs can take advantage of that information

     It's probably only meaningful to use this method on direct subclasses of
     BaseWorker

     \param workerType a string differentiating this worker's type from other
     worker types
  */
  void logWorkerType(const std::string& workerType);

  /// Log input size in situations where you aren't explicitly receiving work
  /// units as input, for example when receiving data from a network socket.
  /**
     \param inputSize the size of the input to log
   */
  void logInputSize(uint64_t inputSize);

  /// Logs the runtime from the most recent run as well as information about
  /// "blockless" runtime (runtime - buffer waits)
  void logRuntimeInformation();

  /// Resets information about the current run time as well as the amount of
  /// time spent waiting for buffers during the run
  void resetRuntimeInformation();

  /**
     Wait for some work to come in and process it.

     \return true if this is the last time you'll have to process work, false
     otherwise
   */
  virtual bool processIncomingWorkUnits() = 0;

  /**
     Emit a work unit to the first registered downstream tracker

     \param workUnit a pointer to the work unit to emit
   */
  void emitWorkUnit(Resource* workUnit);

  /**
     Emit a work unit to a particular downstream tracker

     \param downstreamTrackerID the downstream tracker ID to which the work
     unit should be emitted.

     \param workUnit a pointer to the work unit to emit
  */
  void emitWorkUnit(uint64_t downstreamTrackerID, Resource* workUnit);

  /**
     In some instances, stages will want to manually mark themselves as idle or
     not idle for deadlock detection purposes. This method allows those workers
     to set their idle statuses manually.

     \param idle if true, mark the worker as idle. If false, mark it as not
     idle.
   */
  void setIdle(bool idle);

  /**
     The worker's worker ID

     \todo (AR) Make this private and have all access go through getID()
  */
  const uint64_t id;

  /// The parent tracker for this worker
  WorkerTrackerInterface* tracker;
private:
  /// Threaded function that runs mainLoop()
  void* thread(void* args);

  typedef std::vector<WorkerTrackerInterface*> WorkerTrackerVector;

  void mainLoop(bool testing = false);

  uint64_t addDownstreamTrackerReturningID(
    WorkerTrackerInterface* downstreamTracker);

  /// The number of work units that this worker has consumed from upstream
  /// stages
  uint64_t workUnitsConsumed;

  /// The number of bytes consumed by this worker
  uint64_t bytesConsumed;

  const std::string name;

  /// The worker's downstream trackers, to which it passes work units
  WorkerTrackerVector downstreamTrackers;

  /// Is the worker currently idle because it's waiting for work, or blocked
  /// waiting for downstream queues to desaturate? Used in deadlock detection.
  bool idle;

  // statistics data

  /// Time taken for a single call to run(), in micros
  uint64_t runTimeStatID;

  /// Time spent waiting for work units, in micros
  uint64_t waitTimeStatID;

  /// Time spent waiting for the first work unit, in micros
  uint64_t pipelineSaturationWaitTimeStatID;

  /// The size of an input work unit in bytes
  uint64_t inputSizeStatID;

  /// The amount of time that this worker waits for queues to become
  /// unsaturated
  uint64_t queueSaturationBlockTimeStatID;

  /// The amount of time spent waiting for memory allocations to complete
  uint64_t memoryAllocatorWaitTimeStatID;

  /// The private logger for the worker's internal statistics
  StatLogger logger;

  /// The number of work units produced by this worker
  uint64_t workUnitsProduced;

  /// The number of bytes produced by this worker
  uint64_t bytesProduced;

  CPUAffinitySetter* cpuAffinitySetter;

  Timer waitForWorkTimer;
  Timer initTimer;
  Timer teardownTimer;
  CumulativeTimer runTimer;
  Timer totalRuntimeTimer;
  Timer queueSaturationBlockTimer;
  Timer memoryAllocationWaitTimer;

  // Has the pipeline been saturated yet? Determines how wait times get logged.
  bool pipelineSaturated;
}; // BaseWorker

#endif //_TRITONSORT_BASE_WORKER_H
