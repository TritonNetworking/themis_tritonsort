#ifndef MAPREDUCE_REDUCE_FUNCTION_H
#define MAPREDUCE_REDUCE_FUNCTION_H

#include <stdint.h>

#include "mapreduce/common/KVPairWriterInterface.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/KVPairIterator.h"

/**
   All reduce functions run by the Reducer worker must extend this base class.

   A reduce function takes a key and all values with that key emitted by map
   functions, and emits zero or more key/value pairs.
 */
class ReduceFunction {
public:
  /// Destructor
  virtual ~ReduceFunction() {}

  /**
     Execute the reduce function on a collection of key/value pairs.

     \param key an opaque pointer to a copy of the key whose tuples are to be
     processed

     \param keyLength the size of the memory region pointed to by key

     \param iterator an iterator that will provide the KeyValuePairs for a
     given key

     \param writer the reduce function emits tuples by calling
     KVPairWriterInterface::write or KVPairWriterInterface::setupWrite /
     KVPairWriterInterface::commitWrite on this object
   */
  virtual void reduce(
    const uint8_t* key, uint64_t keyLength, KVPairIterator& iterator,
    KVPairWriterInterface& writer) = 0;

  /**
     Perform any per-buffer configuration that needs to be done in the reducer.
     A stateless reduce function can leave this method empty.
   */
  virtual void configure() {}
};

#endif // MAPREDUCE_REDUCE_FUNCTION_H
