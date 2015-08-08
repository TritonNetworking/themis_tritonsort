#ifndef MAPRED_KV_PAIR_WRITER_INTERFACE_H
#define MAPRED_KV_PAIR_WRITER_INTERFACE_H

#include <stdint.h>

#include "mapreduce/common/KeyValuePair.h"

/**
   Interface to which all classes that facilitate writing of key/value pairs by
   map and reduce tasks must conform.
 */
class KVPairWriterInterface {
public:
  /// Destructor
  virtual ~KVPairWriterInterface() {}

  /// Copy the given key/value pair to the appropriate buffer
  /**
     \param kvPair the key/value pair to be copied
   */
  virtual void write(KeyValuePair& kvPair) = 0;

  /**
     \param key a pointer to the key for the tuple that is to be written

     \param keyLength the key's length in bytes

     \param maxValueLength the maximum length for the value

     \return a pointer to memory to which the value can be written
  */
  virtual uint8_t* setupWrite(const uint8_t* key, uint32_t keyLength,
                              uint32_t maxValueLength) = 0;

  /**
     "Commit" the write for the tuple, writing it to the appropriate buffer.

     \param valueLength the actual length of the value
   */
  virtual void commitWrite(uint32_t valueLength) = 0;

  /// Flush any internally-stored key/value pairs to the next stage
  virtual void flushBuffers() = 0;

  /// \return the number of bytes passed to write() calls
  virtual uint64_t getNumBytesCallerTriedToWrite() const = 0;

  /// \return the number of bytes actually written by this KVPairWriter
  virtual uint64_t getNumBytesWritten() const = 0;

  /// \return the number of tuples written to this KVPairWriter
  virtual uint64_t getNumTuplesWritten() const = 0;
};

#endif // MAPRED_KV_PAIR_WRITER_INTERFACE_H
