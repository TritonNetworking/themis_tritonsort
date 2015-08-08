#ifndef TRITONSORT_TRACKER_SET_H
#define TRITONSORT_TRACKER_SET_H

#include <list>

class WorkerTrackerInterface;
class IntervalStatLogger;

/// Allows the user to deal with a collection of worker trackers as a group
class TrackerSet {
public:
  typedef std::list<WorkerTrackerInterface*> TrackerList;

  /// Destructor
  virtual ~TrackerSet();

  /// Spawn all trackers in the set
  /**
     \sa WorkerTrackerInterface::spawn
   */
  void spawn();

  /// Add a tracker to the set
  /**
     \param tracker a pointer to the tracker to add
     \param source is the tracker a source tracker?
   */
  void addTracker(WorkerTrackerInterface* tracker, bool source = false);

  /// Create workers in all trackers in the set
  /**
     \sa WorkerTrackerInterface::createWorkers
   */
  void createWorkers();

  /// Wait for all trackers' workers to finish
  /**
     \sa WorkerTrackerInterface::waitForWorkersToFinish
   */
  void waitForWorkersToFinish();

  /// Destroy the workers of all trackers in the set
  void destroyWorkers();

  /**
     Expose the tracker list so memory allocator policies can get access to the
     stage graph.

     \return the tracker list
   */
  const TrackerList& getTrackerList() const;

private:
  TrackerList trackers;
  TrackerList sourceTrackers;
};

#endif // TRITONSORT_TRACKER_SET_H
