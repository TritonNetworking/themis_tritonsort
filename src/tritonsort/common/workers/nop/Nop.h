#ifndef THEMIS_NOP_WORKER_H
#define THEMIS_NOP_WORKER_H

#include "core/Resource.h"
#include "core/SingleUnitRunnable.h"

/**
   This worker simply passes any work unit given to it to the next
   stage. Useful for seeing the effects of not running a stage on a work unit.
 */
class Nop : public SingleUnitRunnable<Resource> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id this worker's unique ID within its parent stage

     \param stageName the name of the stage for which this worker is working
   */
  Nop(uint64_t id, const std::string& stageName);

  /// Pass the given resource to the next stage
  /**
     \param resource the resource to pass to the next stage
   */
  void run(Resource* resource);
};

#endif // THEMIS_NOP_WORKER_H
