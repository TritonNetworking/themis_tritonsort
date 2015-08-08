#include "core/Params.h"
#include "mapreduce/common/sorting/QuickSortStrategy.h"
#include "mapreduce/common/sorting/RadixSortStrategy.h"
#include "mapreduce/common/sorting/SortStrategyFactory.h"

SortStrategyFactory::SortStrategyFactory(
  const std::string& sortStrategy, bool _useSecondaryKeys)
  : strategy(sortStrategy),
    useSecondaryKeys(_useSecondaryKeys) {
}

SortStrategyInterface* SortStrategyFactory::newSortStrategy(
  SortAlgorithm algorithm) {
  SortStrategyInterface* strat = NULL;

  switch (algorithm) {
  case QUICK_SORT:
    strat = new QuickSortStrategy(useSecondaryKeys);
    break;
  case RADIX_SORT_MAPREDUCE:
    strat = new RadixSortStrategy(useSecondaryKeys);
    break;
  default:
    ABORT("Don't know how to handle specified sort algorithm");
    break;
  }

  return strat;
}


void SortStrategyFactory::populateOrderedSortStrategyList(
  std::vector<SortStrategyInterface*>& strategyList) const {

  bool anyStrategy = (strategy == "ANY");

  if (anyStrategy || strategy == "RADIX_SORT") {
    SortStrategyInterface* radixSort = new RadixSortStrategy(useSecondaryKeys);
    strategyList.push_back(radixSort);
  }

  if (anyStrategy || strategy == "QUICK_SORT") {
    SortStrategyInterface* quickSort = new QuickSortStrategy(useSecondaryKeys);
    strategyList.push_back(quickSort);
  }

  ABORT_IF(strategyList.size() == 0,
           "Unknown sort strategy %s. Specify RADIX_SORT, QUICK_SORT, or ANY",
           strategy.c_str());
}
