#ifndef THEMIS_MAPREDUCE_WORKERS_MULTI_JOB_DEMUX_H_
#define THEMIS_MAPREDUCE_WORKERS_MULTI_JOB_DEMUX_H_

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "core/WorkerTrackerInterface.h"

/**
   The MultiJobDemux is a wrapper around multiple workers, one per job ID. When
   a buffer is received by this stage, it is forwarded to the appropriate
   worker. Workers are created lazily.
*/
template <typename T> class MultiJobDemux
  : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param _phaseName the name of the phase in which this worker is executing

     \param stageName the name of the worker's parent stage

     \param id the worker's ID within its parent stage

     \param _params a Params object this worker will use to initialize its
     coordinator client and children

     \param _memoryAllocator memory allocator used by children for allocation

     \param _dependencies set of external dependencies to be propagated to
     children
   */
  MultiJobDemux(
    const std::string& _phaseName, const std::string& stageName, uint64_t id,
    Params& _params, MemoryAllocatorInterface& _memoryAllocator,
    NamedObjectCollection& _dependencies)
    : SingleUnitRunnable<KVPairBuffer>(id, stageName),
      phaseName(_phaseName),
      params(_params),
      memoryAllocator(_memoryAllocator),
      dependencies(_dependencies) {
  }

  /// Destructor
  virtual ~MultiJobDemux() {
    for (typename JobIDToWorker::iterator iter = workerForJob.begin();
         iter != workerForJob.end(); iter++) {
      delete (iter->second);
    }
    workerForJob.clear();
  }

  /// If a child worker doesn't already exist for this buffer's job, create
  /// it. Then, run the child worker on the buffer.
  void run(KVPairBuffer* buffer) {
    const std::set<uint64_t>& jobIDs = buffer->getJobIDs();

    TRITONSORT_ASSERT(jobIDs.size() == 1, "Expected each buffer to have exactly one "
           "job ID at this point in the pipeline");

    uint64_t jobID = *(jobIDs.begin());

    T* worker = NULL;

    typename JobIDToWorker::iterator iter = workerForJob.find(jobID);

    if (iter != workerForJob.end()) {
      worker = iter->second;
    } else {
      std::ostringstream oss;
      oss << getName() << ":" << jobID;

      worker = static_cast<T*>(T::newInstance(
        phaseName, oss.str(), getID(), params, memoryAllocator, dependencies));

      // Child worker will inherit parent worker's tracker information
      copyTrackerInformation(*worker);
      worker->init();

      workerForJob.insert(std::make_pair(jobID, worker));
    }

    worker->run(buffer);
  }

  /// Tear down all child workers
  void teardown() {
    for (typename JobIDToWorker::iterator iter = workerForJob.begin();
         iter != workerForJob.end(); iter++) {
      (iter->second)->teardown();
    }
  }

private:
  typedef std::map<uint64_t, T*> JobIDToWorker;

  const std::string phaseName;
  Params& params;
  MemoryAllocatorInterface& memoryAllocator;
  NamedObjectCollection& dependencies;

  JobIDToWorker workerForJob;
};


template <typename T> BaseWorker* MultiJobDemux<T>::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  return new MultiJobDemux<T>(
    phaseName, stageName, id, params, memoryAllocator, dependencies);
}


#endif // THEMIS_MAPREDUCE_WORKERS_MULTI_JOB_DEMUX_H_
