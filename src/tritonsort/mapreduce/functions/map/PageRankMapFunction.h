#ifndef MAPRED_PAGE_RANK_MAP_FUNCTION_H
#define MAPRED_PAGE_RANK_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

class PageRankMapFunction : public MapFunction {
public:
  PageRankMapFunction();

  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
};

#endif // MAPRED_PAGE_RANK_MAP_FUNCTION_H
