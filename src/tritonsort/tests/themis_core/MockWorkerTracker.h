#ifndef _TRITONSORT_MOCKWORKERTRACKER_H
#define _TRITONSORT_MOCKWORKERTRACKER_H

#include <queue>

#include "core/WorkerTrackerInterface.h"

/**
   MockWorkerTracker is used by some tests to receive work units without passing
   them to workers. It exposes its work queue via a public method, so it can be
   used as a sink tracker in a test pipeline. Work units can be checked by
   calling getWorkQueue() when the pipeline shuts down.
 */
class MockWorkerTracker : public WorkerTrackerInterface {
public:
  /// Constructor
  /**
     \param stageName the name of the stage corresponding to this mock tracker
  */
  MockWorkerTracker(const std::string& stageName);

  /// \warning Not implemented
  virtual void spawn();

  /// \warning Not implemented
  virtual void addSource(WorkerTrackerInterface* prevTracker);

  /// Add a work unit to the queue
  virtual void addWorkUnit(Resource* workUnit);

  /// Consider this tracker as always spawned
  /**
     \return true
   */
  virtual bool hasAlreadySpawned() const;

  /// \warning Not implemented
  virtual void noMoreWork();

  /// \warning Not implemented
  virtual void notifyWorkerCompleted(uint64_t id);

  /// \warning Not implemented
  virtual void createWorkers();

  /// \warning Not implemented
  virtual void destroyWorkers();

  /// No-op
  virtual void waitForWorkersToFinish();

  /// Add a downstream tracker to the list of downstream trackers
  virtual void addDownstreamTracker(WorkerTrackerInterface* downstreamTracker);

  /// \warning Not implemented
  virtual void addDownstreamTracker(
    WorkerTrackerInterface* downstreamTracker,
    const std::string& trackerDescription);

  /// \warning Not implemented
  virtual const WorkerTrackerVector& downstreamTrackers() const;

  /// \return the list of downstream trackers added to this stage
  virtual const std::string& getStageName() const;

  /// Aborts, since we shouldn't be adding work to a mock tracker
  void getNewWork(uint64_t queueID, WorkQueue& destWorkQueue);

  /// Aborts, since we shouldn't be adding work to a mock tracker
  Resource* getNewWork(uint64_t queueID);

  /// Aborts, since we shouldn't be adding work to a mock tracker
  bool attemptGetNewWork(uint64_t queueID, Resource*& workUnit);

  /**
     Provide direct access to the work queue for testing purposes

     \return a const reference to the work queue
   */
  const std::queue<Resource*>& getWorkQueue() const;

  /// Delete any work units in the queue to prevent memory leaks
  void deleteAllWorkUnits();

private:
  const std::string stageName;
  WorkerTrackerVector downstreamTrackerVector;
  std::queue<Resource*> workUnits;
};

#endif // _TRITONSORT_MOCKWORKERTRACKER_H
