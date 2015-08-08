#ifndef THEMIS_SINK_WORKER_H
#define THEMIS_SINK_WORKER_H

#include "core/Resource.h"
#include "core/SingleUnitRunnable.h"

/**
   A worker whose sole purpose is to delete any work units it receives; useful
   for checking a portion of a dataflow graph for performance
 */
class Sink : public SingleUnitRunnable<Resource> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the unique ID of this worker within its parent stage
     \param stageName the name of the worker's parent stage
   */
  Sink(uint64_t id, const std::string& stageName);

  /// Destructor
  virtual ~Sink() {}

  /// Delete the provided work unit
  /**
     \param resource the work unit to be deleted
   */
  void run(Resource* resource);
};

#endif // THEMIS_SINK_WORKER_H
