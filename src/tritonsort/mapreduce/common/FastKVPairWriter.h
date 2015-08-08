#ifndef MAPRED_FAST_KV_PAIR_WRITER_H
#define MAPRED_FAST_KV_PAIR_WRITER_H

#include <boost/function.hpp>
#include <vector>

#include "mapreduce/common/KVPairWriterInterface.h"

class KVPairBuffer;
class PartitionFunctionInterface;

/**
   FastKVPairWriter is a key/value pair writer that is optimized for
   performance in the common case. FastKVPairWriter does not support
   write strategies or record filters. It assumes that the mapper is writing
   whole tuples and the job is a non-recovery job.
 */
class FastKVPairWriter : public KVPairWriterInterface {
public:
  /// Inputs: buffer to emit and buffer number.
  typedef boost::function<void (KVPairBuffer*, uint64_t)> EmitBufferCallback;

  /// Input: buffer number for which we're getting a buffer. Output: a buffer
  /// that we can use for that buffer number.
  typedef boost::function<KVPairBuffer* (uint64_t)> GetBufferCallback;

  /// Input: the buffer to return to the caller
  typedef boost::function<void (KVPairBuffer*)> PutBufferCallback;

  /// Input: the key/value pair to log
  typedef boost::function<void (KeyValuePair&)> LogSampleCallback;

  /// Two inputs: # bytes written and # tuples written
  typedef boost::function<void (uint64_t, uint64_t)> LogWriteStatsCallback;

  /// Constructor
  /**
     \param partitionFunction the partition function that will determine the
     buffer to which a tuple gets written based on its key

     \param emitBufferCallback function that will be called when the writer
     needs to emit a buffer

     \param getBufferCallback function that will be called when the writer
     needs to get a buffer

     \param putBufferCallback function that will be called when the writer
     needs to return a buffer that it isn't going to use

     \param logSampleCallback function that will be called when the writer
     wants to log a sample

     \param logWriteStatsCallback function that will be called when the writer
     wants to log the number of tuples and bytes it has written

     \param garbageCollectPartitionFunction partition function should be
     garbage-collected by this writer

     \param writeWithoutHeaders
   */
  FastKVPairWriter(
    const PartitionFunctionInterface* partitionFunction,
    uint64_t outputTupleSampleRate, EmitBufferCallback emitBufferCallback,
    GetBufferCallback getBufferCallback, PutBufferCallback putBufferCallback,
    LogSampleCallback logSampleCallback,
    LogWriteStatsCallback logWriteStatsCallback,
    bool garbageCollectPartitionFunction, bool writeWithoutHeaders);

  /// Destructor
  virtual ~FastKVPairWriter();

  /// Write a key/value pair to the appropriate buffer
  /// \sa KVPairWriteInterface::write
  void write(KeyValuePair& kvPair);

  /// \sa KVPairWriterInterface::setupWrite
  uint8_t* setupWrite(
    const uint8_t* key, uint32_t keyLength, uint32_t maxValueLength);

  /// \sa KVPairWriterInterface::commitWrite
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
  EmitBufferCallback emitBufferCallback;
  GetBufferCallback getBufferCallback;
  PutBufferCallback putBufferCallback;
  LogSampleCallback logSampleCallback;
  LogWriteStatsCallback logWriteStatsCallback;

  /// The number of tuples to skip between sampling tuples for size information
  const uint64_t outputTupleSampleRate;

  const bool garbageCollectPartitionFunction;
  const bool writeWithoutHeaders;

  /// The partition function that will map keys to a value in [0, numBuffers)
  const PartitionFunctionInterface* partitionFunction;

  /// Array of numBuffers buffers
  std::vector<KVPairBuffer*> buffers;

  // State for setup/commit append.
  uint8_t* appendKeyPointer;
  uint8_t* appendValuePointer;
  uint32_t appendKeyLength;
  uint64_t appendPartition;

  // Write statistics
  uint64_t tuplesWritten;
  uint64_t bytesWritten;
};

#endif // MAPRED_FAST_KV_PAIR_WRITER_H
