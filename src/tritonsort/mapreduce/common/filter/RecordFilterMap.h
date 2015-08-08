#ifndef THEMIS_RECORD_FILTER_MAP_H
#define THEMIS_RECORD_FILTER_MAP_H

#include <pthread.h>
#include <stdint.h>

#include "core/constants.h"

/**
   RecordFilterMap provides a way to lazily construct record filters for
   particular jobs, as well as providing the ability to share record filters
   across workers easily.
 */

class CoordinatorClientInterface;
class Params;
class RecordFilter;

class RecordFilterMap {
public:
  /// Constructor
  /**
     The client that this RecordFilterMap uses to communicate with the
     coordinator will be constructed based on params by
     CoordinatorClientFactory

     \param params a Params object that will be used to construct record filters
   */
  RecordFilterMap(const Params& params);

  /// Constructor
  /**
     \param params a Params object that will be used to construct record filters

     \param coordinatorClient the client that the RecordFilterMap will use to
     communicate with the coordinator
   */
  RecordFilterMap(
    const Params& params, CoordinatorClientInterface* coordinatorClient);

  /// Destructor
  virtual ~RecordFilterMap();

  /// Get the record filter for a particular job, if any
  /**
     \param jobID the job whose record filter is to be retrieved

     \return the job's record filter, or NULL if no record filter is required
     for this job
   */
  const RecordFilter* get(uint64_t jobID);

private:
  DISALLOW_COPY_AND_ASSIGN(RecordFilterMap);

  typedef std::map<uint64_t, RecordFilter*> JobToRecordFilterMap;

  const Params& params;

  CoordinatorClientInterface* coordinatorClient;

  pthread_mutex_t lock;
  JobToRecordFilterMap filterMap;
};


#endif // THEMIS_RECORD_FILTER_MAP_H
