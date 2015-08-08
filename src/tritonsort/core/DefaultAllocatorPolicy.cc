#include <algorithm>

#include "DefaultAllocatorPolicy.h"
#include "core/MemoryAllocationContext.h"
#include "core/MemoryUtils.h"
#include "core/TrackerSet.h"
#include "core/WorkerTrackerInterface.h"

DefaultAllocatorPolicy::DefaultAllocatorPolicy(const TrackerSet& trackerSet)
  : highestPriorityRequest(NULL) {
  // Use the stage graph implicitly stored in the TrackerSet to construct the
  // graph of priority queues.
  const std::list<WorkerTrackerInterface*> trackers =
    trackerSet.getTrackerList();
  for (std::list<WorkerTrackerInterface*>::const_iterator iter =
         trackers.begin();
       iter != trackers.end(); ++iter) {
    // If no queue exists for this tracker, create one and label it as a root
    // queue.
    PriorityQueue* queue;
    const WorkerTrackerInterface* tracker = *iter;
    const std::string& groupName = tracker->getStageName();
    PriorityQueueMap::iterator queueIter = priorityQueues.find(groupName);
    if (queueIter == priorityQueues.end()) {
      // This group hasn't been seen yet, so create a root priority queue.
      queue = new (themis::memcheck) PriorityQueue();
      priorityQueues[groupName] = queue;
      rootPriorityQueues.push_back(queue);
    } else {
      // Queue already exists.
      queue = queueIter->second;
    }

    // Check for downstream trackers, and create them as non-root queues.

    typedef WorkerTrackerInterface::WorkerTrackerVector WTV;
    const WTV& downstreamTrackers = tracker->downstreamTrackers();

    for (WTV::const_iterator downstreamIter = downstreamTrackers.begin();
         downstreamIter != downstreamTrackers.end(); downstreamIter++) {
      WorkerTrackerInterface* downstreamTracker = *downstreamIter;

      if (downstreamTracker != NULL) {
        PriorityQueue* downstreamQueue;
        const std::string& downstreamGroupName =
          downstreamTracker->getStageName();

        // If no queue exists for the downstream tracker, create one. In either
        // case, its queue should not be a root queue.
        queueIter = priorityQueues.find(downstreamGroupName);

        if (queueIter == priorityQueues.end()) {
          // Create as non-root queue.
          downstreamQueue = new (themis::memcheck) PriorityQueue();
          priorityQueues[downstreamGroupName] = downstreamQueue;
        } else {
          // Downstream queue already exists, so remove it from root queue list
          // if we previously believed it to be a root queue.
          downstreamQueue = queueIter->second;

          PriorityQueueList::iterator rootListIter = find(
            rootPriorityQueues.begin(), rootPriorityQueues.end(),
            downstreamQueue);

          if (rootListIter != rootPriorityQueues.end()) {
            // Downstream queue was previously marked as root, so unmark it.
            rootPriorityQueues.erase(rootListIter);
          }
        }

        // Now that we know the downstream tracker has a queue, record the
        // graph ordering in the original queue.
        queue->immediateDownstreamQueues.push_back(downstreamQueue);
        // Also record the reverse edge.
        downstreamQueue->immediateUpstreamQueues.push_back(queue);
      }
    }
  }

  // Use the graph ordering to construct downstream and upstream queue lists.
  // First construct downstream lists recursively starting from root queues.
  for (PriorityQueueList::iterator iter = rootPriorityQueues.begin();
       iter != rootPriorityQueues.end(); ++iter) {
    (*iter)->computeDownstreamQueues();
  }

  // Next construct the upstream lists by propagating ancestor information to
  // the downstream lists.
  for (PriorityQueueMap::iterator iter = priorityQueues.begin();
       iter != priorityQueues.end(); ++iter) {
    // Add this queue as an upstream queue for all of its downstream queues.
    PriorityQueue* queue = iter->second;
    for (PriorityQueueSet::iterator downstreamIter =
           queue->downstreamQueues.begin();
         downstreamIter != queue->downstreamQueues.end(); ++downstreamIter) {
      (*downstreamIter)->upstreamQueues.insert(queue);
    }
  }
}

DefaultAllocatorPolicy::~DefaultAllocatorPolicy() {
  // Make sure all queues are empty.
  for (PriorityQueueMap::iterator iter = priorityQueues.begin();
       iter != priorityQueues.end(); ++iter) {
    PriorityQueue* queue = iter->second;
    ABORT_IF(!queue->queue.empty(),
             "All request queues should be empty when the "
             "DefaultAllocatorPolicy destructs, but the queue for %s still has "
             "%llu requests.", iter->first.c_str(), queue->queue.size());

    // Free the queue.
    delete queue;
  }
}

void DefaultAllocatorPolicy::addRequest(
  Request& request, const std::string& groupName) {
  // Check to make sure the group name is valid.
  PriorityQueueMap::iterator iter = priorityQueues.find(groupName);
  ASSERT(iter != priorityQueues.end(),
         "Could not add a request for unknown group name %s",
         groupName.c_str());

  // Add to the appropriate priority queue.
  PriorityQueue* queue = iter->second;
  queue->queue.push(&request);

  // If this addition made the queue non-empty, then we have to check its
  // priority.
  if (queue->queue.size() == 1) {
    // If there are any high priority queues downstream, then this queue is not
    // high priority.
    for (PriorityQueueSet::iterator iter = queue->downstreamQueues.begin();
         iter != queue->downstreamQueues.end(); ++iter) {
      if ((*iter)->highPriority) {
        // Found a downstream high priority queue, so this queue is low priority
        // and we can stop looking.
        return;
      }
    }

    // Since there are no high priority queues downstream, this queue is now
    // high priority.
    queue->highPriority = true;
    highPriorityQueues.push_back(queue);

    // If there any high priority queues upstream, they should be made low
    // priority.
    for (PriorityQueueSet::iterator iter = queue->upstreamQueues.begin();
         iter != queue->upstreamQueues.end(); ++iter) {
      (*iter)->highPriority = false;
      highPriorityQueues.remove(*iter);
    }

    // Update the highest priority request.
    updateHighestPriorityRequest();
  }
}

void DefaultAllocatorPolicy::removeRequest(
  Request& request, const std::string& groupName) {
  // Check to make sure the group name is valid.
  PriorityQueueMap::iterator iter = priorityQueues.find(groupName);
  ASSERT(iter != priorityQueues.end(),
         "Could not remove a request for unknown group name %s.",
         groupName.c_str());

  // Check to make sure this is a high priority queue.
  PriorityQueue* queue = iter->second;
  ASSERT(queue->highPriority,
         "removeRequest() (group %s) should only be called on a high priority "
         "queue.", groupName.c_str());

  // Check to make sure the request is the head of the queue.
  ASSERT(queue->queue.front() == &request,
         "removeRequest() (group %s) should only be called on the front of a "
         "queue.", groupName.c_str());

  // Remove from the appropriate queue.
  queue->queue.pop();

  if (queue->queue.size() == 0) {
    // This removeRequest() operation made the queue empty. Search for a new
    // high priority queue.
    queue->highPriority = false;
    highPriorityQueues.remove(queue);

    // Mark all upstream nodes as unvisited.
    for (PriorityQueueSet::iterator iter = queue->upstreamQueues.begin();
         iter != queue->upstreamQueues.end(); ++iter) {
      (*iter)->visited = false;
    }

    // Recursively search for new high priority downstream queues.
    queue->findHighPriorityUpstreamQueues();

    // Update highPriorityQueues list with the new high priority upstream
    // queues.
    for (PriorityQueueSet::iterator iter = queue->upstreamQueues.begin();
         iter != queue->upstreamQueues.end(); ++iter) {
      if ((*iter)->highPriority) {
        highPriorityQueues.push_back(*iter);
      }
    }
  }

  // Update the highest priority request in all cases.
  updateHighestPriorityRequest();
}

bool DefaultAllocatorPolicy::canScheduleRequest(
  uint64_t availability, Request& request) {
  // The request can be scheduled if it's highest priority and there's enough
  // memory available.
  return (&request == highestPriorityRequest) && (request.size <= availability);
}

MemoryAllocatorPolicy::Request* DefaultAllocatorPolicy::nextSchedulableRequest(
  uint64_t availability) {
  if (highestPriorityRequest == NULL) {
    // There are no requests at all, so return NULL.
    return NULL;
  }

  // If there's enough memory available to schedule the highest priority
  // request, then return it.
  if (highestPriorityRequest->size <= availability) {
    return highestPriorityRequest;
  }

  // Otherwise no memory allocations can be scheduled.
  return NULL;
}

void DefaultAllocatorPolicy::recordUseTime(uint64_t useTime) {
  // No-op since this policy doesn't use timing information.
  /// \TODO(MC): Change the name of this method to something easier to
  // understand.
}

void DefaultAllocatorPolicy::updateHighestPriorityRequest() {
  highestPriorityRequest = NULL;
  for (PriorityQueueList::iterator iter = highPriorityQueues.begin();
       iter != highPriorityQueues.end(); ++iter) {
    if (highestPriorityRequest == NULL) {
      highestPriorityRequest = (*iter)->queue.front();
    } else {
      // Compare the head of this queue to the highest priority request.
      Request* head = (*iter)->queue.front();
      if (head->timestamp < highestPriorityRequest->timestamp) {
        highestPriorityRequest = head;
      }
    }
  }
}

DefaultAllocatorPolicy::PriorityQueue::PriorityQueue()
  : highPriority(false),
    visited(false) {
}

void DefaultAllocatorPolicy::PriorityQueue::findHighPriorityUpstreamQueues() {
  for (PriorityQueueList::iterator iter = immediateUpstreamQueues.begin();
       iter != immediateUpstreamQueues.end(); ++iter) {
    PriorityQueue* upstreamQueue = *iter;
    // If this upstream queue has been visited by another traversal, do
    // nothing.
    if (!upstreamQueue->visited) {
      upstreamQueue->visited = true;
      if (upstreamQueue->queue.size() > 0) {
        // This upstream queue is non-empty, so make it a candidate high
        // priority queue ONLY if none of its downstream queues are high
        // priority.
        bool highPriority = true;
        for (PriorityQueueSet::iterator downstreamIter =
               upstreamQueue->downstreamQueues.begin();
             downstreamIter != upstreamQueue->downstreamQueues.end();
             ++downstreamIter) {
          // This node has a high priority downstream node, so it can't be high
          // priority.
          if ((*downstreamIter)->highPriority) {
            highPriority = false;
            break;
          }
        }
        upstreamQueue->highPriority = highPriority;

        // Everything upstream from this queue cannot be high priority, so
        // mark all upstream as visited and low priority.
        for (PriorityQueueSet::iterator upstreamIter =
               upstreamQueue->upstreamQueues.begin();
             upstreamIter != upstreamQueue->upstreamQueues.end();
             ++upstreamIter) {
          (*upstreamIter)->visited = true;
          (*upstreamIter)->highPriority = false;
        }
      } else {
        // This upstream queue is empty, so recurse deeper.
        upstreamQueue->findHighPriorityUpstreamQueues();
      }
    }
  }
}

void DefaultAllocatorPolicy::PriorityQueue::computeDownstreamQueues() {
  for (PriorityQueueList::iterator iter = immediateDownstreamQueues.begin();
       iter != immediateDownstreamQueues.end(); ++iter) {
    // Compute downstream queues for this downstream queue.
    PriorityQueue* downstreamQueue = *iter;
    downstreamQueue->computeDownstreamQueues();

    // Merge its downstream queues into ours.
    for (PriorityQueueSet::iterator downstreamIter =
           downstreamQueue->downstreamQueues.begin();
         downstreamIter != downstreamQueue->downstreamQueues.end();
         ++downstreamIter) {
      // Set insertion only adds unique queues.
      downstreamQueues.insert(*downstreamIter);
    }

    // Include the downstream queue in our downstream queue set.
    downstreamQueues.insert(downstreamQueue);
  }
}
