#ifndef MAPRED_INVERTED_INDEX_MAP_FUNCTION_H
#define MAPRED_INVERTED_INDEX_MAP_FUNCTION_H

#include <map>

#include "mapreduce/functions/map/MapFunction.h"

/**
   Map function for synthetic inverted index computation
 */
class InvertedIndexMapFunction : public MapFunction {
public:
  /// Constructor
  /**
     \param numValueBytesPerWord the input tuple's value will be split into a
     number of "words" of this length.
   */
  InvertedIndexMapFunction(uint64_t numValueBytesPerWord);

  /// Produce a mapping from "words" to the "document" in which they appear
  /**
     Treats each input tuple as a (document name, document contents)
     pair. Splits the value into a number of words and counts the number of
     times each word occurs. For each (word, word count) pair attained in this
     way, emits a (word, document name + word count) pair.

     \sa MapFunction::map
   */
  void map(KeyValuePair& kvPair, KVPairWriterInterface& writer);

private:
  typedef uint32_t WordCount;
  typedef std::map<std::string, WordCount> WordCountMap;
  typedef WordCountMap::iterator WordCountMapIter;

  const uint64_t numValueBytesPerWord;
};

#endif // MAPRED_INVERTED_INDEX_MAP_FUNCTION_H
