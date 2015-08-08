#ifndef THEMIS_MAPRED_CLICK_LOG_SESSION_SUMMARIZER_REDUCE_FUNCTION_H
#define THEMIS_MAPRED_CLICK_LOG_SESSION_SUMMARIZER_REDUCE_FUNCTION_H

#include "mapreduce/functions/reduce/ReduceFunction.h"

class ClickLogSessionSummarizerReduceFunction : public ReduceFunction {
public:
  /// Constructor
  /**
     \param sessionTimeThreshold click logs are grouped into "sessions" that
     are at most this length, in microseconds
   */
  ClickLogSessionSummarizerReduceFunction(uint64_t sessionTimeThreshold);

  /**
     This reduce function expects to receive key/value pairs with the following
     structure:

     <user ID, timestamp . URL>

     It assumes that these records are sorted lexicographically by user ID,
     then by timestamp.

     If it finds two timestamps that are more than sessionTimeThreshold apart,
     it joins all click logs between those two timestamps into a "session",
     which it emits as a single record with the following structure:

     <user ID, first timestamp . last timestamp . first URL size . last URL
     size . first URL . last URL>

     All integers are stored big-endian.
   */
  void reduce(
    const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
    KVPairWriterInterface& writer);

private:
  struct SessionHeader {
    uint64_t firstTimestamp;
    uint64_t lastTimestamp;
    uint32_t firstURLSize;
    uint32_t lastURLSize;
  };

  const uint64_t sessionTimeThreshold;
};

#endif // THEMIS_MAPRED_CLICK_LOG_SESSION_SUMMARIZER_REDUCE_FUNCTION_H
