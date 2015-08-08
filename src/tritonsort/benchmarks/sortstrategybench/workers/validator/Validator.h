#ifndef SORT_STRATEGY_BENCH_VALIDATOR_H
#define SORT_STRATEGY_BENCH_VALIDATOR_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/sorting/SortStrategyFactory.h"
#include "mapreduce/common/sorting/SortStrategyInterface.h"

/**
   Validator is a worker that determines how much scratch space a Sorter will
   need to sort an input buffer using the selected sort strategy. This scratch
   size is stored in an external variable. If the scratch size is greater than
   the allocator's capacity, the Validator will drop the buffer on the floor,
   which prevents the allocator from aborting.

   The external scratch size variable is specified as a dependency named:
      scratch_size

   \warning The Validator expects exactly 1 sort strategy and exactly 1 work
   unit

   \warning The Validator does not guarantee the allocator will be able to
   allocate the scratch buffer without blocking. It simply guarantees the
   allocator will not crash immediately.
 */
class Validator : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker's id

     \param stageName the worker's name

     \param stratFactory a factory that will be used to populate the list of
     sort strategies

     \param allocatorCapacity the maximum capacity of the allocator in bytes

     \param[out] scratchSize the validator will store the required scratch size
     in this variable
   */
  Validator(
    uint64_t id, const std::string& stageName,
    const SortStrategyFactory& stratFactory,
    uint64_t allocatorCapacity, uint64_t& scratchSize);

  /**
     \sa SingleUnitRunnable::run

     Examine the scratch space required to sort the buffer and only emit the
     buffer if the scratch space is within the allocator's capacity
   */
  void run(KVPairBuffer* buffer);

private:
  typedef std::vector<SortStrategyInterface*> SortStrategyInterfaceList;

  SortStrategyInterfaceList sortStrategies;
  uint64_t allocatorCapacity;
  uint64_t& scratchSize;

  bool ranOnce;
};

#endif // SORT_STRATEGY_BENCH_VALIDATOR_H
