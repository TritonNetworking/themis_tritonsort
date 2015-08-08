#include "core/BaseWorker.h"
#include "core/CPUAffinitySetter.h"
#include "core/CachingMemoryAllocator.h"
#include "core/ImplementationList.h"
#include "core/MemoryAllocatorInterface.h"
#include "core/NamedObjectCollection.h"
#include "core/Params.h"
#include "core/TritonSortAssert.h"
#include "core/WorkerFactory.h"

WorkerFactory::WorkerFactory(
  Params& _params, CPUAffinitySetter& _cpuAffinitySetter,
  MemoryAllocatorInterface& _memoryAllocator)
  : params(_params),
    cpuAffinitySetter(_cpuAffinitySetter),
    memoryAllocator(_memoryAllocator) {
}

WorkerFactory::~WorkerFactory() {
  // Garbage collect all custom allocators.
  for (AllocatorList::iterator iter = customAllocators.begin();
       iter != customAllocators.end(); iter++) {
    delete *iter;
  }
}

void WorkerFactory::addDependency(const std::string& dependencyName,
                                  void* dependency) {
  dependencies.add(dependencyName, dependency);
}

void WorkerFactory::addDependency(const std::string& scope,
                                  const std::string& dependencyName,
                                  void* dependency) {
  dependencies.add(scope, dependencyName, dependency);
}

void WorkerFactory::registerImpls(
  const std::string& implListName,
  const ImplementationList& implementationList) {

  registeredImpls.insert(
    std::pair<std::string, const ImplementationList&>(
      implListName, implementationList));
}

BaseWorker* WorkerFactory::newInstance(
  const std::string& application, const std::string& workerType,
  const std::string& implParamName, const std::string& phaseName,
  const std::string& stageName, uint64_t id) {

  const std::string& myImplName = params.get<std::string>(implParamName);

  ImplListMapIter iter = registeredImpls.find(application);

  ABORT_IF(iter == registeredImpls.end(), "Can't find worker "
           "implementations for application '%s'", application.c_str());

  const ImplementationList& appWorkerImpls = iter->second;

  const ImplementationList& stageImpls = appWorkerImpls.getStageImplList(
    workerType);

  WorkerFactoryMethod factoryMethod = stageImpls.getImplFactoryMethod(
    myImplName);

  return constructNewWorker(factoryMethod, id, phaseName, stageName);
}

BaseWorker* WorkerFactory::newInstance(
  const std::string& workerType, const std::string& implParamName,
  const std::string& phaseName, const std::string& stageName, uint64_t id) {

  const std::string& myImplName = params.get<std::string>(implParamName);

  ImplListMapIter iter = registeredImpls.find(workerType);

  ABORT_IF(iter == registeredImpls.end(), "Can't find worker "
           "implementation '%s'", workerType.c_str());

  const ImplementationList& stageImpls = iter->second;

  WorkerFactoryMethod factoryMethod = stageImpls.getImplFactoryMethod(
    myImplName);

  return constructNewWorker(factoryMethod, id, phaseName, stageName);
}

BaseWorker* WorkerFactory::constructNewWorker(
  WorkerFactoryMethod factoryMethod, uint64_t id, const std::string& phaseName,
  const std::string& stageName) {

  MemoryAllocatorInterface* allocator = &memoryAllocator;
  // Check for the existence of a custom caching allocator.
  if (params.contains("CACHING_ALLOCATOR." + phaseName + "." +  stageName) &&
      params.get<bool>("CACHING_ALLOCATOR." + phaseName + "." + stageName)) {
    // Create a caching allocator for this worker.
    uint64_t cachedMemory = params.get<uint64_t>(
      "CACHED_MEMORY." + phaseName + "." + stageName);
    uint64_t numWorkers = params.get<uint64_t>(
      "NUM_WORKERS." + phaseName + "." + stageName);
    uint64_t bufferSize = params.get<uint64_t>(
      "DEFAULT_BUFFER_SIZE." + phaseName + "." + stageName);
    uint64_t alignment = params.get<uint64_t>(
      "ALIGNMENT." + phaseName + "." + stageName);

    // Since the buffer factory is going to add alignment, make sure we account
    // for it in the cached memory size.
    bufferSize += alignment;
    uint64_t numBuffers = cachedMemory / (numWorkers * bufferSize);

    allocator = new CachingMemoryAllocator(bufferSize, numBuffers);
    // Make sure we save this allocator so we can destroy it later.
    customAllocators.push_back(allocator);
  }

  BaseWorker* worker = factoryMethod(
    phaseName, stageName, id, params, *allocator, dependencies);

  worker->setCPUAffinitySetter(cpuAffinitySetter);

  return worker;

}
