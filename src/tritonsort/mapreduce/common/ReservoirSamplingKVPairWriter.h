#ifndef MAPRED_RESERVOIR_SAMPLING_KV_PAIR_WRITER_H
#define MAPRED_RESERVOIR_SAMPLING_KV_PAIR_WRITER_H

#include <boost/function.hpp>
#include <vector>

#include "mapreduce/common/KVPairWriteStrategyInterface.h"
#include "mapreduce/common/KVPairWriterInterface.h"

class KVPairBuffer;
class RecordFilter;

/**
   ReservoirSamplingKVPairWriter is a special KVPairWriter that can be used in
   phase 0 to sample a data set more evenly than prefix-sampling. After the
   sample buffer fills up to half-capacity, tuples are randomly "replaced" by
   using an append-and-invalidate scheme. Once the buffer fills completely, the
   invalid tuples are removed by to compact the buffer and regain more append
   space.

   Because of the append-and-invalidate nature of this implementation, reservoir
   sampling can be used over prefix-sampling in all cases. Additionally, it has
   the benefit of creating a more accurate sample distribution when the data set
   is heteroscedastic, i.e., the distribution changes over the length of the
   file. Regular prefix sampling will fail to observe the complete distribution
   in this case.

   Two potential downsides of reservoir sampling are loss of precision and
   increased runtime.  Precision can be lost because fewer samples are taken on
   the whole, due to reservation of the append-space at the end of the sample
   buffer.  Running time can potentially be increased because the sample buffer
   is not emitted until the entire file is sampled.
 */
class ReservoirSamplingKVPairWriter : public KVPairWriterInterface {
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

  /// Two inputs: # bytes written, # bytes seen, # tuples written and # tuples
  /// seen
  typedef boost::function<void (uint64_t, uint64_t, uint64_t, uint64_t)> LogWriteStatsCallback;

  /// Constructor
  /**
     \param outputTupleSampleRate the number of output tuples to skip between
     logging samples

     \param recordFilter a filter that, if non-NULL, will determine whether
     records get written or not

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

     \param orderPreserving if true we should use a write strategy that writes
     the actual keys to preserve ordering.

     \param writeStrategy a strategy that dictates how keys and values should be
     written.
   */
  ReservoirSamplingKVPairWriter(
    uint64_t outputTupleSampleRate, const RecordFilter* recordFilter,
    EmitBufferCallback emitBufferCallback, GetBufferCallback getBufferCallback,
    PutBufferCallback putBufferCallback, LogSampleCallback logSampleCallback,
    LogWriteStatsCallback logWriteStatsCallback,
    KVPairWriteStrategyInterface* writeStrategy);

  /// Destructor
  ~ReservoirSamplingKVPairWriter();

  /// Probabilistically write tuples using the reservoir sampling algorithm
  /// \sa KVPairWriterInterface::write
  void write(KeyValuePair& kvPair);

  /// \warning ABORTS, not implemented
  /// \sa KVPairWriterInterface::setupWrite
  uint8_t* setupWrite(
    const uint8_t* key, uint32_t keyLength, uint32_t maxValueLength);

  /// \warning ABORTS, not implemented
  /// \sa KVPairWriterInterface::commitWrite
  virtual void commitWrite(uint32_t valueLength);

  /**
     Emit the buffer of samples to the downstream tracker. This is the only way
     for this writer to emit its buffer.
   */
  virtual void flushBuffers();

  /// \return the number of bytes that would have been written if all tuples had
  /// been sampled perfectly
  virtual uint64_t getNumBytesCallerTriedToWrite() const;

  /**
     \return the actual number of bytes emitted by the reservoir sampling
     replacement algorithm

     \warning this value is set in flushBuffers() and will be 0 if
     flushBuffers() is not called first
  */
  virtual uint64_t getNumBytesWritten() const;

  /**
     \return the actual number of tuples emitted by the reservoir sampling
     replacement algorithm

     \warning this value is set in flushBuffers() and will be 0 if
     flushBuffers() is not called first
  */
  virtual uint64_t getNumTuplesWritten() const;

private:
  void compactSampleBuffer();

  void writeSampleRecordToBuffer(
    const uint8_t* key, uint32_t keyLength, const uint8_t* value,
    uint32_t valueLength);

  inline void resetPendingWriteState() {
    setupWriteKey = NULL;
    setupWriteKeyLength = 0;
    setupWriteMaxValueLength = 0;

    if (setupWriteValue != NULL) {
      delete[] setupWriteValue;
      setupWriteValue = NULL;
    }
  }

  const RecordFilter* recordFilter;
  const uint64_t outputTupleSampleRate;

  // Various callback functions
  EmitBufferCallback emitBufferCallback;
  GetBufferCallback getBufferCallback;
  PutBufferCallback putBufferCallback;
  LogSampleCallback logSampleCallback;
  LogWriteStatsCallback logWriteStatsCallback;

  KVPairWriteStrategyInterface* writeStrategy;

  KVPairBuffer* buffer;
  uint64_t maxSamples;
  uint64_t sampleSize;

  std::vector<uint64_t> validTuples;

  uint64_t tuplesSeen;
  uint64_t tuplesWritten;
  uint64_t bytesSeen;
  uint64_t bytesWritten;

  const uint8_t* setupWriteKey;
  uint32_t setupWriteKeyLength;

  uint8_t* setupWriteValue;
  uint32_t setupWriteMaxValueLength;

};

#endif // MAPRED_RESERVOIR_SAMPLING_KV_PAIR_WRITER_H
