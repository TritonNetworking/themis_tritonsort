#ifndef MAPRED_WEX_TEXT_EXTRACTOR_MAP_FUNCTION_H
#define MAPRED_WEX_TEXT_EXTRACTOR_MAP_FUNCTION_H

#include "mapreduce/functions/map/MapFunction.h"

/**
   This map function extracts article text from rows in the Freebase
   wikipedia Extraction (WEX) database.

   The map function expects key/value pairs of the form <WEX filename,
   tab-separated WEX database row>, and emits <source internal page name,
   article text> pairs.
 */
class WEXTextExtractorMapFunction : public MapFunction {
public:
  /**
     Given a key/value pair of the form: <WEX filename, tab-separated WEX
     database row>, emit <source internal page name, article text>

     \sa MapFunction::map
   */
  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);
};

#endif // MAPRED_WEX_TEXT_EXTRACTOR_MAP_FUNCTION_H
