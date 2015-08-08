#ifndef MAPRED_KV_PAIR_WRITER_H
#define MAPRED_KV_PAIR_WRITER_H

#include <boost/function.hpp>
#include <queue>

#include "mapreduce/common/KVPairWriterInterface.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/TupleSizeLoggingStrategyInterface.h"

class KVPairBuffer;
class KVPairWriteStrategyInterface;
class PartitionFunctionInterface;
class RecordFilter;

/**
   KVPairWriter manages the writing of key/value pairs to KVPairBuffers on
   behalf of a given worker.
 */
class KVPairWriter : public KVPairWriterInterface {
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
     \param numBuffers the number of buffers the KVPairWriter will manage

     \param partitionFunction the partition function that will determine the
     buffer to which a tuple gets written based on its key

     \param recordFilter a filter that, if non-NULL, controls whether records
     get emitted or not

     \param writeStrategy the write strategy that the writer should use. If
     NULL, DefaultKVPairWriteStrategy is used

     \param outputTupleSampleRate the rate at which logSampleCallback should be
     called to sample records output by the writer

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
   */
  KVPairWriter(
    uint64_t numBuffers, const PartitionFunctionInterface& partitionFunction,
    KVPairWriteStrategyInterface* writeStrategy, uint64_t outputTupleSampleRate,
    const RecordFilter* recordFilter, EmitBufferCallback emitBufferCallback,
    GetBufferCallback getBufferCallback, PutBufferCallback putBufferCallback,
    LogSampleCallback logSampleCallback,
    LogWriteStatsCallback logWriteStatsCallback,
    bool garbageCollectPartitionFunction = false);

  /// Destructor
  virtual ~KVPairWriter();

  /// Write a key/value pair to the appropriate buffer
  /// \sa KVPairWriteInterface::write
  void write(KeyValuePair& kvPair);

  /// \sa KVPairWriterInterface::setupWrite
  uint8_t* setupWrite(const uint8_t* key, uint32_t keyLength,
                      uint32_t maxValueLength);

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
  /// Retrieve the KVPairBuffer appropriate for this particular key as a
  /// preparation step before appending to it
  /**
     Hash the key to determine the appropriate buffer. If the buffers array
     doesn't have a buffer in the appropriate slot, fetch one from the parent
     worker. If the size of the tuple you're about to append would overflow the
     buffer, emit the buffer and fetch a new replacement buffer, again using
     the parent worker.

     \todo Consult the write strategy when hashing the key to compute the buffer
     number

     \param key a pointer to the key

     \param keyLength the length of the key in bytes

     \param tupleSize the size of the tuple you're about to append

     \param[out] bufferNumber the index in the buffers array of the returned
     buffer NOTE: uses the original key to compute the buffer number
   */
  KVPairBuffer* getBufferForKey(
    const uint8_t* key, uint32_t keyLength, uint64_t tupleSize,
    uint64_t& bufferNumber);

  /// Retrieve a new buffer from the parent worker
  /**
     \param[out] buffer the buffer that was retrieved

     \param bufNo the index in the buffers array where the buffer is stored at
     the end of this method

     \param minimumCapacity the buffer retrieved must have at least this many
     bytes of available space
   */
  void getNewBuffer(KVPairBuffer*& buffer, uint64_t bufNo,
                    uint64_t minimumCapacity);

  inline void resetPendingWriteState() {
    tupleAppendBuffer = NULL;
    tupleAppendBufferNumber = 0;
    tupleAppendKeyPtr = NULL;
    tupleAppendKeyLength = 0;
    tupleAppendValuePtr = NULL;
    tupleAppendTmpValue = NULL;
  }

  // Various callback functions
  EmitBufferCallback emitBufferCallback;
  GetBufferCallback getBufferCallback;
  PutBufferCallback putBufferCallback;
  LogSampleCallback logSampleCallback;
  LogWriteStatsCallback logWriteStatsCallback;

  /// The number of buffers for which the KVPairWriter is responsible
  const uint64_t numBuffers;

  /// The number of tuples to skip between sampling tuples for size information
  const uint64_t outputTupleSampleRate;

  const bool garbageCollectPartitionFunction;

  /// The partition function that will map keys to a value in [0, numBuffers)
  const PartitionFunctionInterface& partitionFunction;

  const RecordFilter* recordFilter;

  /// Array of numBuffers buffers
  std::vector<KVPairBuffer*> buffers;

  // Pointer to the buffer currently being written to by a setup/commitWrite
  KVPairBuffer* tupleAppendBuffer;

  // tupleAppendBuffer's buffer number
  uint64_t tupleAppendBufferNumber;

  // Pointer to the key and value being written as part of a setup/commitWrite
  uint8_t* tupleAppendKeyPtr;
  uint32_t tupleAppendKeyLength;
  uint8_t* tupleAppendValuePtr;

  // Temporary memory for a setup/commitWrite when using strategies that alter
  // the writer's value before appending
  uint8_t* tupleAppendTmpValue;

  /// A pointer to the write strategy that the writer will use
  KVPairWriteStrategyInterface* writeStrategy;

  /// Number of bytes written to the KVPairWriter
  uint64_t tuplesWritten;

  /// Number of bytes written to the KVPairWriter
  uint64_t bytesWritten;
};

#endif // MAPRED_KV_PAIR_WRITER_H
