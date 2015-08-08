#ifndef THEMIS_MAPRED_PARTITION_FUNCTION_MAP_H
#define THEMIS_MAPRED_PARTITION_FUNCTION_MAP_H

#include <list>
#include <map>
#include <pthread.h>
#include <stdint.h>

#include "core/constants.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/PartitionMap.h"

class KeyPartitionerInterface;
class Params;
class PartitionFunctionInterface;

/**
   PartitionFunctionMap provides a way to lazily construct partition functions
   for particular jobs, as well as providing the ability to share partition
   functions among workers easily.
 */
class PartitionFunctionMap {
public:
  /// Constructor
  /**
     \param params a Params object that will be used to construct partition
     functions

     \param phaseName the name of the phase
   */
  PartitionFunctionMap(const Params& params, const std::string& phaseName);

  /// Destructor
  virtual ~PartitionFunctionMap();

  /// Get the partition function for a particular job
  /**
     Thread-safe.

     \param jobID the job whose partition function is to be retrieved

     \return the partition function for the given job
   */
  PartitionFunctionInterface* const get(uint64_t jobID);

private:
  DISALLOW_COPY_AND_ASSIGN(PartitionFunctionMap);

  typedef std::map<uint64_t, PartitionFunctionInterface*> InternalFunctionMap;
  const Params& params;

  CoordinatorClientInterface* coordinatorClient;

  PartitionMap partitionMap;

  // As a side-effect of creating partition functions, we often create
  // KeyPartitionerInterface objects that need to be garbage collected. This
  // list stores those objects, and the destructor garbage collects them
  std::list<KeyPartitionerInterface*> keyPartitioners;
  pthread_mutex_t lock;

  InternalFunctionMap functions;
};


#endif // THEMIS_MAPRED_PARTITION_FUNCTION_MAP_H
