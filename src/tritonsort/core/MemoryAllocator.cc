#include <unistd.h>

#include "MemoryAllocator.h"
#include "core/IntervalStatLogger.h"
#include "core/MemoryAllocationContext.h"
#include "core/MemoryUtils.h"
#include "core/ResourceMonitor.h"
#include "core/ScopedLock.h"

MemoryAllocator::MemoryAllocator(
  uint64_t _capacity, uint64_t _fragmentationSleepTimeMicros,
  MemoryAllocatorPolicy& _policy, DeadlockResolverInterface& _deadlockResolver)
  : capacity(_capacity),
    availability(_capacity),
    fragmentationSleepTimeMicros(_fragmentationSleepTimeMicros),
    policy(_policy),
    deadlockResolver(_deadlockResolver),
    deadlockCheckerThreadRunning(false),
    nextCallerID(0),
    logger("MemoryAllocator"),
    numFragmentationSleeps(0) {
  pthread_mutex_init(&lock, NULL);
  pthread_mutex_init(&deadlockCheckerThreadLock, NULL);
  pthread_mutex_init(&workerMemoryUsageLock, NULL);

  // We expect allocations to be frequent, so only log summary stats.
  allocationSizeStatID = logger.registerSummaryStat("allocation_size");
  allocationTimeStatID = logger.registerSummaryStat("allocation_time");
  // Deadlock resolutions are less frequent, so log everything.
  deadlockResolutionSizeStatID = logger.registerStat(
    "deadlock_resolution_size");

  ResourceMonitor::registerClient(this, "memory_allocator");
  IntervalStatLogger::registerClient(this);
}

MemoryAllocator::~MemoryAllocator() {
  ResourceMonitor::unregisterClient(this);
  IntervalStatLogger::unregisterClient(this);

  pthread_mutex_lock(&lock);

  // Free all dynamically allocated callers.
  for (CallerMap::iterator iter = callers.begin(); iter != callers.end();
       ++iter) {
    delete iter->second;
  }

  // All memory pointers should be returned.
  TRITONSORT_ASSERT(availability == capacity,
         "All memory should be available when the memory allocator is "
         "destroyed, but only %llu of %llu bytes are available.",
         availability, capacity);
  TRITONSORT_ASSERT(allocationMetadataMap.empty(),
         "All outstanding allocations should be deallocated before the "
         "allocator is destroyed, but %llu remain.",
         allocationMetadataMap.size());

  logger.logDatum("num_fragmentation_sleeps", numFragmentationSleeps);
  logger.logDatum("num_callers", callers.size());
  callers.clear();

  pthread_mutex_unlock(&lock);

  pthread_mutex_lock(&workerMemoryUsageLock);
  workerMemoryUsage.clear();
  pthread_mutex_unlock(&workerMemoryUsageLock);

  pthread_mutex_destroy(&lock);
  pthread_mutex_destroy(&deadlockCheckerThreadLock);
  pthread_mutex_destroy(&workerMemoryUsageLock);
}

StatLogger* MemoryAllocator::initIntervalStatLogger() {
  StatLogger* intervalLogger = new StatLogger("MemoryAllocator");

  memoryUsageStatID = intervalLogger->registerStat("memory_usage");
  numOutstandingRequestsStatID = intervalLogger->registerStat(
    "num_outstanding_requests");
  numAllocationsStatID = intervalLogger->registerStat("num_allocations");

  return intervalLogger;
}

void MemoryAllocator::logIntervalStats(StatLogger& intervalLogger) const {
  // This information may be stale, but that should be fine for logging.
  intervalLogger.add(memoryUsageStatID, capacity - availability);
  intervalLogger.add(numOutstandingRequestsStatID, outstandingRequests.size());
  intervalLogger.add(numAllocationsStatID, allocationMetadataMap.size());
}

void MemoryAllocator::resourceMonitorOutput(Json::Value& obj) {
  pthread_mutex_lock(&workerMemoryUsageLock);

  obj["worker_memory_usage"] = Json::Value(Json::arrayValue);
  Json::Value& workerMemoryUsages = obj["worker_memory_usage"];

  for (WorkerMemoryUsageMap::iterator iter = workerMemoryUsage.begin();
       iter != workerMemoryUsage.end(); iter++) {
    Json::Value workerInfoObject;
    BaseWorker* worker = iter->first;
    uint64_t memoryUsed = iter->second;

    workerInfoObject["stage"] = worker->getName();
    workerInfoObject["id"] = Json::UInt64(worker->getID());
    workerInfoObject["memory_used"] = Json::UInt64(memoryUsed);

    workerMemoryUsages.append(workerInfoObject);
  }

  pthread_mutex_unlock(&workerMemoryUsageLock);
}

uint64_t MemoryAllocator::registerCaller(BaseWorker& workerCaller) {
  ScopedLock scopedLock(&lock);
  // Create a new caller and assign an ID.
  const std::string& stageName = workerCaller.getName();

  // Make sure that sub-workers of a given worker are all part of the same group
  std::string groupName(stageName, 0, stageName.find_first_of(':'));

  Caller* caller = new (themis::memcheck) Caller(
    groupName, workerCaller.getID(), workerCaller);
  uint64_t callerID = nextCallerID;
  callers[callerID] = caller;
  ++nextCallerID;

  // Update the worker caller map. The [] operator will create a new list if
  // this is the first time we've seen the worker.
  CallerIDList& callerIDs = workerCallerIDs[&workerCaller];
  callerIDs.push_back(callerID);

  return callerID;
}

void* MemoryAllocator::allocate(const MemoryAllocationContext& context,
                                uint64_t& size) {
  Timer timer;
  timer.start();

  ScopedLock scopedLock(&lock);
  uint64_t callerID = context.getCallerID();

  // Make sure there is at least one feasible request size.
  bool feasibleRequestExists = false;
  uint64_t minRequestSize = 0;
  for (std::list<uint64_t>::const_iterator iter = context.getSizes().begin();
       iter != context.getSizes().end(); ++iter) {
    if (*iter <= capacity) {
      feasibleRequestExists = true;
      break;
    }
    minRequestSize = std::min(minRequestSize, *iter);
  }
  /// \TODO(MC): Replace this abort with a blocking request that gets serviced
  // from disk.
  ABORT_IF(!feasibleRequestExists, "No feasible memory request exists. "
           "Smallest request size is %llu. Consider increasing memory "
           "capacity", minRequestSize);

  const MemoryAllocationContext::MemorySizes& sizes = context.getSizes();
  /// \TODO(MC) Do something sophisticated with the memory sizes given. For
  // example, pick a reasonable size as opposed to the first size.
  size = sizes.front();
  logger.add(allocationSizeStatID, size);

  // Create a request and add to the policy.
  // It is safe to allocate the request locally because it is not needed after
  // this function returns.

  MemoryAllocatorPolicy::Request request(context, size);
  Caller* callerInfo = callers[callerID];

  const std::string& groupName = callerInfo->groupName;
  policy.addRequest(request, groupName);

  outstandingRequests[callerID] = &request;

  callerInfo->worker.startMemoryAllocationTimer();

  uint8_t* memory = NULL;
  bool allocated = false;
  while (!allocated) {
    // Block until the request can be scheduled.
    while (!request.resolvedOnDeadlock &&
           !policy.canScheduleRequest(availability, request)) {
      request.satisfiable = false;
      pthread_cond_wait(&(callerInfo->conditionVariable), &lock);
    }
    request.satisfiable = true;

    // If the request deadlocks and is resolved by the deadlock detector,
    // request.memory will be non-NULL and will point to something like an
    // mmap()ed file, and request.resolvedOnDeadlock will be true.

    if (!request.resolvedOnDeadlock) {
      // Try to allocate memory
      memory = new (std::nothrow) uint8_t[size];
      if (memory == NULL) {
        // Fragmentation is preventing memory from being allocated. Sleep for
        // some period and then try again.
        pthread_mutex_unlock(&lock);
        ++numFragmentationSleeps;
        usleep(fragmentationSleepTimeMicros);
        pthread_mutex_lock(&lock);
        allocated = false;
      } else {
        // Allocation succeeded. Update internal state.
        availability -= size;
        allocated = true;
      }
    } else {
      // Memory was allocated through the deadlock resolver.
      memory = static_cast<uint8_t*>(request.memory);
      allocated = true;
    }
  }

  callerInfo->worker.stopMemoryAllocationTimer();

  // Update internal state
  policy.removeRequest(request, groupName);

  outstandingRequests.erase(callerID);

  // Record metadata about this memory
  AllocationMetadata* metadata =
    new (themis::memcheck) AllocationMetadata(
      size, callerID, request.resolvedOnDeadlock);
  allocationMetadataMap[memory] = metadata;

  pthread_mutex_lock(&workerMemoryUsageLock);
  workerMemoryUsage[&(callers[callerID]->worker)] += size;
  pthread_mutex_unlock(&workerMemoryUsageLock);

  // Allow another thread to wake up.
  wakeNextThread();

  timer.stop();
  logger.add(allocationTimeStatID, timer);

  return memory;
}

void* MemoryAllocator::allocate(const MemoryAllocationContext& context) {
  uint64_t dummySize = 0;
  return allocate(context, dummySize);
}

void MemoryAllocator::deallocate(void* memory) {
  ScopedLock scopedLock(&lock);

  // Make sure the memory region was actually allocated.
  AllocationMetadataMap::iterator iter = allocationMetadataMap.find(memory);
  ABORT_IF(iter == allocationMetadataMap.end(),
           "Cannot deallocate unknown memory pointer %p", memory);
  AllocationMetadata* metadata = iter->second;

  // Let the policy know about the usage time.
  uint64_t now = Timer::posixTimeInMicros();
  policy.recordUseTime(now - metadata->timestamp);

  // Free memory associated with this allocation.
  uint8_t* allocatedMemory = static_cast<uint8_t*>(memory);

  // If we allocated this memory in some special way to resolve deadlock, we
  // will probably have to free the memory in some special way (munmap,
  // etc). Otherwise, we can just delete it.
  if (metadata->resolvedOnDeadlock) {
    deadlockResolver.deallocate(allocatedMemory);
  } else {
    delete[] allocatedMemory;

    // Note that we now have more memory available (which is only true if we
    // allocated this memory normally rather than as a result of deadlock
    availability += metadata->size;
    TRITONSORT_ASSERT(availability <= capacity,
           "Somehow deallocating memory pointer %p, size %llu, resulted in an "
           "availability of %llu, but capacity is only %llu.",
           memory, metadata->size, availability, capacity);
  }

  pthread_mutex_lock(&workerMemoryUsageLock);
  workerMemoryUsage[&(callers[metadata->callerID]->worker)] -= metadata->size;
  pthread_mutex_unlock(&workerMemoryUsageLock);

  allocationMetadataMap.erase(iter);
  delete metadata;

  // Allow another thread to wake up.
  wakeNextThread();
}

void MemoryAllocator::wakeNextThread() {
  // Get the next request that can be scheduled if one exists.
  MemoryAllocatorPolicy::Request* request =
    policy.nextSchedulableRequest(availability);
  if (request != NULL) {
    request->satisfiable = true;
    // Signal the thread.
    uint64_t callerID = request->context.getCallerID();
    pthread_cond_signal(&callers[callerID]->conditionVariable);
  }
}

bool MemoryAllocator::detectAndResolveDeadlocks() {
  ScopedLock scopedLock(&lock);

  bool deadlocked = deadlockDetected();

  if (deadlocked) {
    resolveDeadlocks();
  }

  return deadlocked;
}

bool MemoryAllocator::deadlockDetected() {
  // For all worker callers, check if the worker is either waiting for work or
  // has an outstanding request that is not satisfiable
  for (WorkerCallerIDMap::iterator workerIter = workerCallerIDs.begin();
       workerIter != workerCallerIDs.end(); ++workerIter) {
    BaseWorker* worker = workerIter->first;
    CallerIDList& callerIDs = workerIter->second;
    if (!worker->isIdle()) {
      // The worker is not waiting for work, so check if its caller IDs have
      // outstanding requests.
      bool waitingForMemory = false;
      for (CallerIDList::iterator callerIter = callerIDs.begin();
           callerIter != callerIDs.end(); ++callerIter) {
        // If this caller has an outstanding request that is satisfiable, then
        // we're not deadlocked yet.
        RequestMap::iterator iter = outstandingRequests.find(*callerIter);
        if (iter != outstandingRequests.end()) {
          waitingForMemory = true;
          if ((iter->second)->satisfiable) {
            // This worker has a satisfiable request, so we're definitely not
            // deadlocked.
            return false;
          }
        }
      }

      if (!waitingForMemory) {
        // This worker isn't waiting for work or memory, so we're definitely not
        // deadlocked.
        return false;
      }
    }
  }

  // All workers are either waiting for work, or waiting for memory on an
  // unsatisfiable request, so we are deadlocked.
  return true;
}

void MemoryAllocator::resolveDeadlocks() {
  // Reaching this point means deadlock has occured.

  // Get all requests that the policy could schedule if it had (practically)
  // infinite memory
  MemoryAllocatorPolicy::Request* request = policy.nextSchedulableRequest(
    std::numeric_limits<uint64_t>::max());

  if (request != NULL) {
    uint64_t callerID = request->context.getCallerID();

    void* memory = deadlockResolver.resolveRequest(request->size);
    request->memory = memory;
    request->resolvedOnDeadlock = true;
    logger.add(deadlockResolutionSizeStatID, request->size);
    pthread_cond_signal(&callers[callerID]->conditionVariable);
  }
}

void MemoryAllocator::deadlockCheckerThread() {
  while (deadlockCheckerThreadRunning) {
    pthread_mutex_lock(&deadlockCheckerThreadLock);

    detectAndResolveDeadlocks();

    pthread_mutex_unlock(&deadlockCheckerThreadLock);

    // Sleep one second
    /// \TODO(MC): Make this parameterized
    usleep(1000000);
  }
}

void MemoryAllocator::stopDeadlockChecker() {
  ScopedLock scopedLock(&deadlockCheckerThreadLock);
  TRITONSORT_ASSERT(deadlockCheckerThreadRunning,
         "Should not be calling stopDeadlockChecker() if the deadlock checker "
         "thread is not running.");
  deadlockCheckerThreadRunning = false;

  int status = pthread_join(deadlockCheckerThreadID, NULL);

  if (status != 0) {
    ABORT("pthread_join() failed with error %d: %s", status, strerror(status));
  }
}

void* MemoryAllocator::spawnDeadlockCheckerThread(void* arg) {
  ThreadArgs* args = static_cast<ThreadArgs*>(arg);

  /// \TODO(MC): Make this name unique across allocators.
  setThreadName("MemoryAllocator");

  args->This->deadlockCheckerThread();

  return NULL;
}

void MemoryAllocator::spawnDeadlockChecker() {
  ScopedLock scopedLock(&deadlockCheckerThreadLock);
  ABORT_IF(deadlockCheckerThreadRunning,
           "Cannot call spawnDeadlockChecker() while deadlock checker thread "
           "is already running");
  deadlockCheckerThreadRunning = true;

  int status = pthread_create(
    &deadlockCheckerThreadID, NULL,
    &MemoryAllocator::spawnDeadlockCheckerThread, new ThreadArgs(this));

  ABORT_IF(status != 0, "pthread_create() returned status %d: %s",
           status, strerror(status));
}

MemoryAllocator::Caller::Caller(
  const std::string& _groupName, uint64_t _groupMemberID, BaseWorker& _worker)
  : groupName(_groupName),
    groupMemberID(_groupMemberID),
    worker(_worker) {
  pthread_cond_init(&conditionVariable, NULL);
}

MemoryAllocator::Caller::~Caller() {
  pthread_cond_destroy(&conditionVariable);
}
