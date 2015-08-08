#ifndef THEMIS_DEFAULT_ALLOCATOR_POLICY_H
#define THEMIS_DEFAULT_ALLOCATOR_POLICY_H

#include <list>
#include <map>
#include <queue>
#include <set>

#include "core/MemoryAllocatorPolicy.h"
#include "core/TritonSortAssert.h"

class TrackerSet;

/**
   The DefaultAllocatorPolicy schedules memory requests using priorities derived
   from the pipeline's stage graph. Downstream stages are given priority over
   upstream stages. At any given point in time, the policy maintains a list of
   stage queues that are high prioritiy. Requests from the union of these queues
   are serviced FCFS. As queues are filled or drained, this set of high priority
   queues shifts to include or exclude stages.

   It is important to note that requests must be serviced in the order specified
   in the policy. In particular, this means that assertions will fail and data
   structures will become inconsistent if you try to remove a request that is
   not the nextSchedulableRequest().

   Also note that this policy is not in itself threadsafe. The allocator is
   responsible for synchronizing threads before calling into the policy.

   \TODO(MC): Should we change the name from DefaultAllocatorPolicy to something
   more specific?
*/
class DefaultAllocatorPolicy : public MemoryAllocatorPolicy {
public:
  /// Constructor
  /**
     Construct the policy's priority graph using the stages given in the
     pipeline's TrackerSet.

     \param trackerSet the set of trackers for this pipeline configuration
  */
  DefaultAllocatorPolicy(const TrackerSet& trackerSet);

  /// Destructor
  virtual ~DefaultAllocatorPolicy();

  /**
     Add a request to its group's priority queue, and adjust the set of high
     priority queues accordingly.

     \sa MemoryAllocatorPolicy::addRequest
   */
  virtual void addRequest(Request& request, const std::string& groupName);

  /**
     Remove a request from its group's priority queue, and adjust the set of
     high priority queues accordingly.

     \sa MemoryAllocatorPolicy::removeRequest
   */
  virtual void removeRequest(Request& request, const std::string& groupName);

  /**
      A request can be scheduled if there is enough available memory and it is
      the next schedulable request.

      \sa MemoryAllocatorPolicy::canScheduleRequest
   */
  virtual bool canScheduleRequest(uint64_t availability, Request& request);

  /**
      A request is next to be scheduled if has the smallest timestamp of all of
      the heads of all of the queues in the high priority set.

      \sa MemoryAllocatorPolicy::nextSchedulableRequest
   */
  virtual Request* nextSchedulableRequest(uint64_t availability);

  /**
     No-op since this policy doesn't care about use time.

     \sa MemoryAllocatorPolicy::recordUseTime
   */
  virtual void recordUseTime(uint64_t useTime);

private:
  /// A PriorityQueue represents queue in the priority graph.
  class PriorityQueue {
  public:
    /// Constructor
    PriorityQueue();

    /**
       Recursively find eligible upstream queues to promote to high priority.
       Used when a removeRequest() causes a high priority queue to become empty.
     */
    void findHighPriorityUpstreamQueues();

    /**
       Use the immediate downstream queues (incident edges) to recursively
       compute the set of all downstream (reachable) queues. Used in the
       policy's constructor.
     */
    void computeDownstreamQueues();

    std::queue<Request*> queue;
    std::list<PriorityQueue*> immediateDownstreamQueues;
    std::list<PriorityQueue*> immediateUpstreamQueues;
    // The downstream and upstream queue sets are all of the upstream
    // and downstream queues *reachable* from this queue.
    std::set<PriorityQueue*> downstreamQueues;
    std::set<PriorityQueue*> upstreamQueues;
    bool highPriority;
    bool visited;
  };

  typedef std::map<std::string, PriorityQueue*> PriorityQueueMap;
  typedef std::list<PriorityQueue*> PriorityQueueList;
  typedef std::set<PriorityQueue*> PriorityQueueSet;

  /**
     Scan through the high priority queues and find the request with the
     smallest timestamp.
   */
  void updateHighestPriorityRequest();

  PriorityQueueMap priorityQueues;
  PriorityQueueList rootPriorityQueues;

  PriorityQueueList highPriorityQueues;
  Request* highestPriorityRequest;
};

#endif // THEMIS_DEFAULT_ALLOCATOR_POLICY_H
