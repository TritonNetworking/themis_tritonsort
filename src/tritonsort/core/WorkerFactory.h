#ifndef TRITONSORT_WORKER_FACTORY_H
#define TRITONSORT_WORKER_FACTORY_H

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "core/ImplementationList.h"
#include "core/NamedObjectCollection.h"

class BaseWorker;
class CPUAffinitySetter;
class MemoryAllocatorInterface;
class Params;

/**
   WorkerFactory provides a factory that can be universally and easily used by
   any worker implementation. This should eliminate the need for per-worker
   factories.

   WorkerFactory relies on workers registering themselves. See
   WorkerImplementationList for more details about how registration works, and
   look at MockNewStyleWorker in the tests to see an example of a
   self-registering worker.

   Additionally, WorkerFactory provides the ability to give a worker a custom
   CachingMemoryAllocator if the worker will only be allocating fixed size
   buffers. Make sure CACHING_ALLOCATOR.phase.stage is set to true and make sure
   CACHED_MEMORY.phase.stage is set to the total amount of memory that all
   workers in this stage will cache. In particular, this feature allows the
   subdivision of stage memory between NUMA domains since first-touch should
   ensure that a worker W only allocates and accesses memory on its own NUMA
   node.
 */
class WorkerFactory {
public:
  /// Constructor
  /**
     \param params a Params object that will be passed to created instances

     \param cpuAffinitySetter the CPUAffinitySetter that will control how
     workers are assigned to cores

     \param memoryAllocator a memory allocator that the workers created by this
     factory will use to allocate buffer memory
   */
  WorkerFactory(
    Params& params, CPUAffinitySetter& cpuAffinitySetter,
    MemoryAllocatorInterface& memoryAllocator);

  /// Destructor
  ~WorkerFactory();

  /// Register a set of worker implementations with this factory
  /**
     \param implListName the namespace in which to scope the implementations in
     the list

     \param implementationList the implementation list itself
   */
  void registerImpls(const std::string& implListName,
                     const ImplementationList& implementationList);

  /// Pass a dependency to the workers instantiated by this factory
  /**
     A ``dependency'' is an object that a worker needs to run. Workers can
     refer to the dependency by name in their newInstance method. See
     WorkerImplementationList for more details.

     \param dependencyName the name of the dependency. Workers' factory methods
     refer to the dependency by this name to retrieve it

     \param dependency a pointer to the dependency
   */
  void addDependency(const std::string& dependencyName, void* dependency);

  /// Pass a dependency to the workers instantiated by this factory
  /**
     A ``dependency'' is an object that a worker needs to run. Workers can
     refer to the dependency by name in their newInstance method. See
     WorkerImplementationList for more details.

     \param scope the scope in which to name the dependency

     \param dependencyName the name of the dependency. Workers' factory methods
     refer to the dependency by this name to retrieve it

     \param dependency a pointer to the dependency
   */
  void addDependency(const std::string& scope,
                     const std::string& dependencyName, void* dependency);

  /// Create a new instance of a worker
  /**
     This method uses the factory method defined in the global worker
     implementation list (see WorkerImplementationList) to construct a new
     worker. It assumes that the worker created has the input and output types
     given as template parameters and performs a runtime check to that effect,
     failing if the returned worker's input and output type do not match those
     given in the template.

     \param application the name of the application under which the worker is
     registered.

     \param workerType the worker type name under which the worker is registered

     \param implParamName the name of the parameter whose value is the
     implementation name to use. The parameter will be retrieved from the
     Params object given at construction time

     \param phaseName the name of the phase in which the worker is operating

     \param stageName the stage name to assign to the worker

     \param id the ID to assign to the worker

     \return a new instance of the specified worker type
   */
  BaseWorker* newInstance(
    const std::string& application, const std::string& workerType,
    const std::string& implParamName, const std::string& phaseName,
    const std::string& stageName, uint64_t id);

  /// Create a new instance of a worker
  /**
     This method uses the factory method defined in the global worker
     implementation list (see WorkerImplementationList) to construct a new
     worker. It assumes that the worker created has the input and output types
     given as template parameters and performs a runtime check to that effect,
     failing if the returned worker's input and output type do not match those
     given in the template.

     \param workerType the worker type name under which the worker is registered

     \param implParamName the name of the parameter whose value is the
     implementation name to use. The parameter will be retrieved from the
     Params object given at construction time

     \param phaseName the name of the phase in which the worker is operating

     \param stageName the stage name to assign to the worker

     \param id the ID to assign to the worker

     \return a new instance of the specified worker type
   */
  BaseWorker* newInstance(
    const std::string& workerType, const std::string& implParamName,
    const std::string& phaseName, const std::string& stageName, uint64_t id);

private:
  typedef std::map<std::string, const ImplementationList&> ImplListMap;
  typedef ImplListMap::iterator ImplListMapIter;
  typedef std::list<MemoryAllocatorInterface*> AllocatorList;

  NamedObjectCollection dependencies;
  Params& params;
  CPUAffinitySetter& cpuAffinitySetter;
  MemoryAllocatorInterface& memoryAllocator;

  ImplListMap registeredImpls;

  AllocatorList customAllocators;

  BaseWorker* constructNewWorker(
    WorkerFactoryMethod factoryMethod, uint64_t id,
    const std::string& phaseName, const std::string& stageName);
};


#endif // TRITONSORT_WORKER_FACTORY_H
