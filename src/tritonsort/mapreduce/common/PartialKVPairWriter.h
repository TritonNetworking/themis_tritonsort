#ifndef MAPRED_PARTIAL_KV_PAIR_WRITER_H
#define MAPRED_PARTIAL_KV_PAIR_WRITER_H

#include <boost/function.hpp>
#include <vector>

#include "mapreduce/common/KVPairWriterInterface.h"

class KVPairBuffer;
class PartitionFunctionInterface;

/**
   PartialKVPairWriter is a key/value pair writer that can write partial tuples
   to buffers and fills every buffer to the brim. It is intended to be used in
   the case when buffers are being written to disk, for example in the Demux or
   the Reducer.

   Since fixed-size output buffers are used, certain cases, for example
   setup/commit may be inefficient. In this case, if the tuple is too large
   to fit in the buffer, an extra memcpy will take place because the buffer
   provided to the user is contiguous in memory. To avoid a lot of overhead,
   make sure your output buffers are large enough so that this extra copy
   is infrequent.
 */
class PartialKVPairWriter : public KVPairWriterInterface {
public:
  /// Inputs: buffer to emit and buffer number.
  typedef boost::function<void (KVPairBuffer*, uint64_t)> EmitBufferCallback;

  /// Output: a buffer that we can use for that buffer number.
  typedef boost::function<KVPairBuffer* ()> GetBufferCallback;

  /// Constructor
  /**
     \param emitBufferCallback function that will be called when the writer
     needs to emit a buffer

     \param getBufferCallback function that will be called when the writer
     needs to get a buffer

     \param writeWithoutHeaders if true, write tuples without MapReduce headers

     \param partialSerialize if false, don't serialize partial tuples
     (for daytona minutesort)
   */
  PartialKVPairWriter(
    EmitBufferCallback emitBufferCallback, GetBufferCallback getBufferCallback,
    bool writeWithoutHeaders, bool partialSerialize=true);

  /// Destructor
  virtual ~PartialKVPairWriter();

  void setPartitionFunction(
    PartitionFunctionInterface& partitionFunction, uint64_t numPartitions,
    bool localPartitioning, uint64_t partitionGroup, uint64_t partitionOffset);

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
  /// Data structure for recording append pointers.
  struct AppendInfo {
    KVPairBuffer* buffer;
    uint8_t* pointer;
    uint64_t length;

    AppendInfo()
      : buffer(NULL),
        pointer(NULL),
        length(0) {}
  };

  /// Get a buffer and setup an append
  /**
     \param partition the partition the buffer corresponds to
   */
  void getBuffer(uint64_t partition);

  /// Commit the append and emit the buffer
  /**
     \param partition the partition the buffer corresponds to
   */
  void emitBuffer(uint64_t partition);

  EmitBufferCallback emitBufferCallback;
  GetBufferCallback getBufferCallback;

  const bool writeWithoutHeaders;

  const bool partialSerialize;

  std::vector<AppendInfo*> appendInfos;

  // Partition function related fields
  PartitionFunctionInterface* partitionFunction;
  bool localPartitioning;
  uint64_t partitionGroup;
  uint64_t partitionOffset;

  // setupWrite() needs to save information about the tuple for the subsequent
  // call to commitWrite().
  uint32_t setupKeyLength;
  uint32_t setupMaxValueLength;
  uint64_t setupPartition;
  uint8_t* setupTuple;
  uint8_t* temporaryBuffer;

  // Write statistics
  uint64_t tuplesWritten;
  uint64_t bytesWritten;
};

#endif // MAPRED_PARTIAL_KV_PAIR_WRITER_H
