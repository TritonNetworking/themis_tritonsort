#ifndef TRITONSORT_MAPREDUCE_SORT_STRATEGY_FACTORY_H
#define TRITONSORT_MAPREDUCE_SORT_STRATEGY_FACTORY_H

#include <string>
#include <vector>

#include "common/sort_constants.h"

class SortStrategyInterface;

/**
   A factory for constructing sort strategies.
 */
class SortStrategyFactory {
public:
  SortStrategyFactory(const std::string& sortStrategy, bool useSecondaryKeys);

  /**
     Constructs a sort strategy object using a particular SortAlgorithm.

     \param algorithm an enum corresponding to the sort algorithm to use
   */
  SortStrategyInterface* newSortStrategy(SortAlgorithm algorithm);

  /**
     Populate the provided strategy list with either the selected strategy, or,
     if ANY is specified, an instance of each sort strategy, ordered in
     ascending order by big O running time

     \param strategyList[out] the list to populate

     \param strategy a string naming the strategy to use, or ANY if any strategy
     is acceptable
   */
  void populateOrderedSortStrategyList(
    std::vector<SortStrategyInterface*>& strategyList) const;

private:
  const std::string strategy;
  const bool useSecondaryKeys;
};

#endif // TRITONSORT_MAPREDUCE_SORT_STRATEGY_FACTORY_H
