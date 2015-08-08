#ifndef MAPRED_MAP_FUNCTION_H
#define MAPRED_MAP_FUNCTION_H

#include "core/Params.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/KVPairWriterInterface.h"

/**
   All map functions run by the Mapper worker must extend this base class.

   A map function takes a single key/value pair as input and emits zero or more
   key/value pairs.
 */
class MapFunction {
public:
  /// Destructor
  virtual ~MapFunction() {}

  /**
     Perform any initialization that needs to occur after this map function is
     constructed but before it processes any tuples.

     By default, this function is a no-op.

     \param params a Params object containing parameters that the function may
     use to configure itself
   */
  virtual void init(const Params& params) {
    // Noop
  }

  /**
     Execute the map function on a single key/value pair

     \param kvPair the key/value pair on which to perform the map function

     \param writer the map function emits tuples by calling
     KVPairWriterInterface::write or KVPairWriterInterface::setupWrite /
     KVPairWriterInterface::commitWrite on this object
   */
  virtual void map(KeyValuePair& kvPair, KVPairWriterInterface& writer) = 0;

  /**
     Perform any cleanup on the map function that needs to occur after the map
     function has finished processing all tuples but before it is destructed.
   */
  virtual void teardown(KVPairWriterInterface& writer) {
    // Noop
  }

  /**
     Used to configure the map function for mapping tuples from a particular
     buffer. This gets called once for every new buffer the mapper encounters.
     Examples of uses include taking particular actions based on the source name
     of a buffer to be mapped.

     \param buffer the next buffer to be mapped
   */
  virtual void configure(KVPairBuffer* buffer) {
    // Noop
  }
};

#endif // MAPRED_MAP_FUNCTION_H
