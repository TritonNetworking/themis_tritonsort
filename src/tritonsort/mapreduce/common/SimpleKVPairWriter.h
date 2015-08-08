#ifndef MAPRED_SIMPLE_KV_PAIR_WRITER_H
#define MAPRED_SIMPLE_KV_PAIR_WRITER_H

#include <boost/function.hpp>

#include "mapreduce/common/KVPairWriterInterface.h"

class KVPairBuffer;

/**
   SimpleKVPairWriter is a key/value pair writer that writes whole tuples to a
   single buffer. If the buffer isn't large enough to hold the tuple, it emits
   the buffer and fetches another that is large enough.
 */
class SimpleKVPairWriter : public KVPairWriterInterface {
public:
  /// Inputs: buffer to emit and buffer number.
  typedef boost::function<void (KVPairBuffer*)> EmitBufferCallback;

  /// Inputs: minimum size, Output: a buffer to fill
  typedef boost::function<KVPairBuffer* (uint64_t)> GetBufferCallback;

  /// Constructor
  /**
     \param emitBufferCallback function that will be called when the writer
     needs to emit a buffer

     \param getBufferCallback function that will be called when the writer
     needs to get a buffer
   */
  SimpleKVPairWriter(
    EmitBufferCallback emitBufferCallback, GetBufferCallback getBufferCallback);

  /// Destructor
  virtual ~SimpleKVPairWriter();

  /// Write a key/value pair to the buffer.
  /// \sa KVPairWriteInterface::write
  void write(KeyValuePair& kvPair);

  /// \sa KVPairWriterInterface::setupWrite
  /// \warning Not implemented. Aborts
  uint8_t* setupWrite(
    const uint8_t* key, uint32_t keyLength, uint32_t maxValueLength);

  /// \sa KVPairWriterInterface::commitWrite
  /// \warning Not implemented. Aborts
  void commitWrite(uint32_t valueLength);

  // Emit all buffers to downstream tracker
  void flushBuffers();

  // \sa KVPairWriterInterface::getNumBytesCallerTriedToWrite
  uint64_t getNumBytesCallerTriedToWrite() const;

  // \sa KVPairWriterInterface::getNumBytesWritten
  uint64_t getNumBytesWritten() const;

  // \sa KVPairWriterInterface::getNumTuplesWritten
  uint64_t getNumTuplesWritten() const;

private:
  /// Internally used to commit appends and emit buffers.
  void emitBuffer();

  EmitBufferCallback emitBufferCallback;
  GetBufferCallback getBufferCallback;

  KVPairBuffer* buffer;
  uint8_t* appendPointer;
  uint64_t appendLength;

  // Write statistics
  uint64_t tuplesWritten;
  uint64_t bytesWritten;
};

#endif // MAPRED_SIMPLE_KV_PAIR_WRITER_H
