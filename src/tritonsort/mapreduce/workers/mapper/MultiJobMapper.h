#ifndef THEMIS_MAPRED_MULTI_JOB_MAPPER_H
#define THEMIS_MAPRED_MULTI_JOB_MAPPER_H

#include <map>

#include "core/SingleUnitRunnable.h"
#include "core/WorkerTrackerInterface.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

class Mapper;

/**
   Passes a buffer through a map function for each job in its job set in
   sequence
 */
class MultiJobMapper : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param phaseName the name of the phase in which this worker is running

     \param stageName the name of this worker's parent stage

     \param id the worker's ID within its parent stage

     \param params a Params object this worker will use to configure child
     Mappers and its coordinator client

     \param memoryAllocator memory allocator used by children to allocate
     buffers

     \param dependencies set of external dependencies to propagate to children
   */
  MultiJobMapper(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    Params& params, MemoryAllocatorInterface& memoryAllocator,
    NamedObjectCollection& dependencies);

  /// Destructor
  virtual ~MultiJobMapper();

  void run(KVPairBuffer* buffer);
  void teardown();

private:
  typedef std::map<uint64_t, Mapper*> JobIDToMapper;

  const std::string phaseName;
  Params& params;
  MemoryAllocatorInterface& memoryAllocator;
  NamedObjectCollection& dependencies;

  JobIDToMapper mapperForJob;

  WorkerTrackerInterface* parentTracker;
};


#endif // THEMIS_MAPRED_MULTI_JOB_MAPPER_H
