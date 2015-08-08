#ifndef MAPRED_WEX_LINK_EXTRACTOR_MAP_FUNCTION_H
#define MAPRED_WEX_LINK_EXTRACTOR_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

/**
   This map function extracts internal links from rows in the Freebase
   wikipedia Extraction (WEX) database.

   The map function expects key/value pairs of the form <WEX filename,
   tab-separated WEX database row>, and emits <source internal page name,
   destination internal page name> pairs.
 */
class WEXLinkExtractorMapFunction : public MapFunction {
public:
  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);
};

#endif // MAPRED_WEX_LINK_EXTRACTOR_MAP_FUNCTION_H
