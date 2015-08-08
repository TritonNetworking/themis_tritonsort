#ifndef THEMIS_MEMORY_ALLOCATOR_H
#define THEMIS_MEMORY_ALLOCATOR_H

#include <list>
#include <map>
#include <string>
#include <pthread.h>

#include "core/BaseWorker.h"
#include "core/DeadlockResolverInterface.h"
#include "core/IntervalStatLoggerClient.h"
#include "core/MemoryAllocatorInterface.h"
#include "core/MemoryAllocatorPolicy.h"
#include "core/ResourceMonitorClient.h"
#include "core/StatLogger.h"
#include "core/Timer.h"


/**
   A MemoryAllocator is a centralized scheduling mechanism for allocating memory
   dynamically. Entities, called Callers, register themselves with the
   allocator, and then can make allocation and deallocation requests. Contexts
   are used to specify allocation options. The allocator's semantics allow for
   allocate() calls to block, but deallocate() calls can never block.

   It is possible that memory fragmentation will allocation attempts to fail
   even when there is sufficient total available memory. In this case, the
   allocator pauses the request for a period of time specified by
   ALLOCATOR_FRAGMENTATION_SLEEP before trying again. This pause honors the
   priorities dictated by the policy, so if a higher priority request arrives,
   the paused request will go back to sleep when it wakes up after the specified
   sleep time.

   \\\TODO(MC): We haven't used this in a while. If we start using it again, we
   should convert it from raw pthreads to the new Thread interface.
 */
class MemoryAllocator
  : public MemoryAllocatorInterface, public IntervalStatLoggerClient,
    public ResourceMonitorClient {
public:
  /// Constructor
  /**
     \param capacity the maximum number of bytes that can be simultaneously
     allocated

     \param fragmentationSleepTimeMicros the number of microseconds to pause an
     allocation that is under capacity, but still fails due to fragmentation or
     other causes

     \param policy the MemoryAllocatorPolicy for prioritizing allocations

     \param deadlockResolver the resolution strategy for handling deadlocks
   */
  MemoryAllocator(
    uint64_t capacity, uint64_t fragmentationSleepTimeMicros,
    MemoryAllocatorPolicy& policy, DeadlockResolverInterface& deadlockResolver);

  /// Destructor
  virtual ~MemoryAllocator();

  StatLogger* initIntervalStatLogger();
  void logIntervalStats(StatLogger& intervalLogger) const;

  /// \sa ResourceMonitorClient::resourceMonitorOutput
  void resourceMonitorOutput(Json::Value& obj);

  /**
     Register a caller with the allocator so that the allocator can pause and
     resume the calling thread.

     \param workerCaller the BaseWorker that is making the allocation request

     \return a unique caller ID that this worker can use to make allocations
   */
  uint64_t registerCaller(BaseWorker& workerCaller);

  /**
     Allocate a piece of memory dictated by the allocation context. This call
     will block until the allocation succeeds.

     \param context a context declaring the caller ID, allocation size, and
     other information

     \param[out] size the size of the returned allocation, which can be used in
     the event that the user specified multiple allowable sizes in the context

     \return a pointer to the allocated memory region
   */
  void* allocate(const MemoryAllocationContext& context, uint64_t& size);

  /**
     Identical to the 2 argument allocate() method, except that the size is not
     returned as an output parameter.

     \param context a context declaring the caller ID, allocation size, and
     other information

     \return a pointer to the allocated memory region
   */
  void* allocate(const MemoryAllocationContext& context);

  /**
     De-allocate a piece of memory corresponding to some prior allocate() call.
     This method will automatically unblock threads waiting on allocate() after
     it frees up memory.

     \param a pointer to the memory region obtained from a prior allocate() call
   */
  void deallocate(void* memory);


  /// Spawn the thread responsible for determining and resolving deadlocks
  /// caused by eager allocate() calls from greedy threads.
  void spawnDeadlockChecker();

  /// Stop the deadlock checker thread.
  void stopDeadlockChecker();

  /**
      Used internally to resolve detects.

      \warning Public only for the test suite. Do not call.

      \return true if deadlock was detected and resolved, false otherwise
   */
  bool detectAndResolveDeadlocks();

private:
  /// A data structure that corresponds to a successful allocate() that is yet
  /// to be freed by deallocate().
  struct AllocationMetadata {
    uint64_t size;
    uint64_t timestamp;
    uint64_t callerID;
    bool resolvedOnDeadlock;
    /// Constructor
    /**
       \param _size the size of the allocated memory

       \param _callerID the caller that allocated this memory, used for memory
       accounting purposes

       \param _resolvedOnDeadlock whether this memory should be deallocated
       using the deadlock resolver
     */
    AllocationMetadata(
      uint64_t _size, uint64_t _callerID, bool _resolvedOnDeadlock)
      : size(_size),
        timestamp(Timer::posixTimeInMicros()),
        callerID(_callerID),
        resolvedOnDeadlock(_resolvedOnDeadlock) {}
  };

  /**
     A Caller represents any entity that will ask the allocator for memory.
     Caller information will be exposed to the allocator policy to make
     informed allocation decisions.
  */
  class Caller {
  public:
    /// Constructor
    /**
       \param groupName the name of group that this caller belongs to

       \param groupMemberID the ID of the caller within its group

       \param worker the BaseWorker caller if any
    */
    Caller(
      const std::string& groupName, uint64_t groupMemberID, BaseWorker& worker);

    /// Destructor
    virtual ~Caller();

    const std::string groupName;
    const uint64_t groupMemberID;
    BaseWorker& worker; // Non-const so we can take locks
    pthread_cond_t conditionVariable;
  };

  /// Helper struct for passing the allocator into a pthread.
  struct ThreadArgs {
    MemoryAllocator* This;
    ThreadArgs(MemoryAllocator* t) : This(t) {}
  };

  typedef std::map<void*, AllocationMetadata*> AllocationMetadataMap;
  typedef std::map<uint64_t, Caller*> CallerMap;
  typedef std::map<uint64_t, MemoryAllocatorPolicy::Request*> RequestMap;
  typedef std::map<BaseWorker*, uint64_t> WorkerMemoryUsageMap;
  typedef std::list<MemoryAllocatorPolicy::Request*> RequestList;
  typedef std::list<uint64_t> CallerIDList;
  typedef std::map<BaseWorker*, CallerIDList> WorkerCallerIDMap;

  /// Wake up the next thread that can make an allocation based on available
  /// memory and policy priorities.
  void wakeNextThread();

  /// Helper function for spawning deadlock checker pthread.
  static void* spawnDeadlockCheckerThread(void* arg);

  /// Deadlock checker thread main loop.
  void deadlockCheckerThread();

  /**
     Determine if a deadlock exists by examining outstanding allocation requests
     and worker idleness properties.

     \return true if deadlock detected
   */
  bool deadlockDetected();

  /// Resolve deadlocks using a deadlock resolver object.
  void resolveDeadlocks();

  const uint64_t capacity;
  uint64_t availability;

  const uint64_t fragmentationSleepTimeMicros;

  MemoryAllocatorPolicy& policy;
  DeadlockResolverInterface& deadlockResolver;

  pthread_mutex_t lock;
  pthread_mutex_t deadlockCheckerThreadLock;
  pthread_mutex_t workerMemoryUsageLock;

  pthread_t deadlockCheckerThreadID;
  bool deadlockCheckerThreadRunning;

  uint64_t nextCallerID;
  CallerMap callers;

  RequestMap outstandingRequests;

  AllocationMetadataMap allocationMetadataMap;
  WorkerMemoryUsageMap workerMemoryUsage;
  WorkerCallerIDMap workerCallerIDs;

  StatLogger logger;
  uint64_t numFragmentationSleeps;
  uint64_t allocationSizeStatID;
  uint64_t allocationTimeStatID;
  uint64_t deadlockResolutionSizeStatID;
  uint64_t memoryUsageStatID;
  uint64_t numOutstandingRequestsStatID;
  uint64_t numAllocationsStatID;
};

#endif // THEMIS_MEMORY_ALLOCATOR_H
