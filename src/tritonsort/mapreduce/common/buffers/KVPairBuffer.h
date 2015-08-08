#ifndef MAPRED_KV_PAIR_BUFFER_H
#define MAPRED_KV_PAIR_BUFFER_H

#include <set>

#include "common/buffers/BaseBuffer.h"
#include "common/buffers/BufferList.h"
#include "mapreduce/common/KeyValuePair.h"

/**
   KVPairBuffer is the canonical buffer for MapReduce. It consists of complete
   key value pairs that are formatted using an 8 byte header followed by key and
   value:

   [ uint32_t keyLength, uint32_t valueLength, uint8_t* key, uint8_t* value ]

   KVPairBuffer support many ways of reading and writing tuples that vary in
   terms of performance and ease of use.
 */
class KVPairBuffer : public BaseBuffer {
public:
  // Metadata that the sender will send before the buffer to inform receiving
  // nodes about buffer length, job ID, and partition group.
  struct NetworkMetadata {
    uint64_t bufferLength;
    uint64_t jobID;
    uint64_t partitionGroup;
    uint64_t partitionID;
  };

  /// Constructor
  /**
     \sa BaseBuffer::BaseBuffer(
     MemoryAllocatorInterface& memoryAllocator, uint64_t callerID,
     uint64_t capacity, uint64_t alignmentMultiple)
   */
  KVPairBuffer(
    MemoryAllocatorInterface& memoryAllocator, uint64_t callerID,
    uint64_t capacity, uint64_t alignmentMultiple = 0);

  /// Constructor
  /**
     \param memoryAllocator the memory allocator that was used to allocate
     memoryRegion

     \param memoryRegion a pointer to the memory that will hold the buffer's
     data

     \param capacity the capacity of the buffer in bytes

     \param alignmentMultiple the buffer will access a portion of the memory
     region aligned to a multiple of this number for direct I/O reasons
   */
  KVPairBuffer(MemoryAllocatorInterface& memoryAllocator,
               uint8_t* memoryRegion, uint64_t capacity,
               uint64_t alignmentMultiple = 0);

  /// Constructor
  /**
     \param memoryRegion a pointer to the memory that will hold the buffer's
     data

     \param capacity the capacity of the buffer in bytes

     \param alignmentMultiple the buffer will access a portion of the memory
     region aligned to a multiple of this number for direct I/O reasons
   */
  KVPairBuffer(uint8_t* memoryRegion, uint64_t capacity,
               uint64_t alignmentMultiple = 0);

  /// Constructor
  /**
     Create a KVPairBuffer that auto destructs its memory region without an
     allocator.

     \param capacity the capacity of the buffer in bytes

     \param alignmentMultiple the buffer will access a portion of the memory
     region aligned to a multiple of this number for direct I/O reasons
   */
  KVPairBuffer(uint64_t capacity, uint64_t alignmentMultiple = 0);

  /**
     Append a KeyValuePair object to the buffer.

     \param kvPair the pair to append
   */
  virtual void addKVPair(KeyValuePair& kvPair);

  /**
     Iterate over the buffer while fetching tuples as KeyValuePair objects.

     \param kvPair[out] where the tuple will be stored

     \return true if a buffer was fetched and false if the iteration is complete
   */
  virtual bool getNextKVPair(KeyValuePair& kvPair);

  /**
     Reset the iterator position to the beginning of the buffer.
   */
  void resetIterator();

  /**
     Set the iterator to a given position. Used by the Reducer.

     \param position where the iterator should be set
   */
  void setIteratorPosition(uint64_t position);

  /**
     Get the current iterator position. Used by the Reducer.

     \return the current iterator position
   */
  uint64_t getIteratorPosition();

  /// Analogous to BaseBuffer::setupAppend, but for key/value pairs
  /**
     \param keyLength the length of a key

     \param maxValueLength the maximum length that a value is allowed to have

     \param[out] key a pointer to the memory region to which the caller can
     write the record's key

     \param[out] value a pointer to the memory region to which the caller can
     write the record's value

     \param writeWithoutHeader if true, don't write headers to the buffer
   */
  void setupAppendKVPair(
    uint32_t keyLength, uint32_t maxValueLength, uint8_t*& key,
    uint8_t*& value, bool writeWithoutHeader=false);

  /// Analogous to BaseBuffer::commitAppend, but for key/value pairs
  /**
     \param key a pointer to the memory region to which the caller has
     written the record's key

     \param value a pointer to the memory region to which the caller has
     written the record's value

     \param valueLength the length of the value to be appended

     \param writeWithoutHeader if true, don't write headers to the buffer
   */
  void commitAppendKVPair(
    const uint8_t* key, const uint8_t* value, uint32_t valueLength,
    bool writeWithoutHeader=false);

  /// Analogous to BaseBuffer::abortAppend, but for key/value pairs
  /**
     \param kvPair a KeyValuePair object whose contents were populated using
     setupAppendKVPair

     \param writeWithoutHeader if true, don't write headers to the buffer
   */
  void abortAppendKVPair(
    const uint8_t* key, const uint8_t* value, bool writeWithoutHeader=false);

  /**
     Clear the buffer's internal state so it can be reused by another worker.
   */
  void clear();

  /**
     Get the number of tuples in the buffer. This information is cached and
     recalculated on demand if the cache becomes stale.

     \return the number of tuples in the buffer
   */
  uint64_t getNumTuples();

  /**
     Get the minimum length of a key in the buffer. This information is cached
     and recalculated on demand if the cache becomes stale.

     \return the minimum length of a key in this buffer
   */
  uint32_t getMinKeyLength();

  /**
     Get the maximum length of a key in the buffer. This information is cached
     and recalculated on demand if the cache becomes stale.

     \return the maximum length of a key in this buffer
   */
  uint32_t getMaxKeyLength();

  /**
     Exists only for Receiver duck typing. Remove ASAP.

     Alias for BaseBuffer::getCurrentSize()
   */
  uint64_t numBytesWithCompletePairs() {
    return getCurrentSize();
  }

  /**
     Sets the ID of the node corresponding to this buffer. Used when buffers
     are sent over the network.

     \param node the ID of the node corresponding to this buffer
   */
  inline void setNode(uint64_t node) {
    this->node = node;
  }

  /**
     Gets the ID of the node corresponding to this buffer.

     \return the node for this buffer
   */
  inline uint64_t getNode() const {
    return node;
  }

  /**
     Append arbitrary data to the buffer.

     \sa BaseBuffer::append

     \param data the byte array to append

     \param length the number of bytes to append
   */
  inline virtual void append(const uint8_t* data, uint64_t length) {
    BaseBuffer::append(data, length);
    cached = false;
  }

  /**
     Commit a 'safe' append to the buffer.

     \sa BaseBuffer::commitAppend

     \param ptr the append pointer given by setupAppend()

     \param actualAppendLength the number of bytes to commit
   */
  virtual void commitAppend(const uint8_t* ptr, uint64_t actualAppendLength) {
    BaseBuffer::commitAppend(ptr, actualAppendLength);
    cached = false;
  }

  /**
     Set the partition group associated with this buffer. Used to route buffers
     to the appropriate demux.

     \param partitionGroup the partition group for all records in this buffer
   */
  inline void setPartitionGroup(uint64_t partitionGroup) {
    this->partitionGroup = partitionGroup;
  }

  /**
     \return the partition group associated with this buffer
   */
  inline uint64_t getPartitionGroup() const {
    return partitionGroup;
  }

  /**
     Add a new job ID to this buffer.

     \param jobID the job ID to add
   */
  inline void addJobID(uint64_t jobID) {
    jobIDSet.insert(jobID);
  }

  /**
     Add a set of job IDs to this buffer.

     \param jobIDs the set of job IDs to add
   */
  inline void addJobIDSet(const std::set<uint64_t>& jobIDs) {
    for (std::set<uint64_t>::iterator iter = jobIDs.begin();
         iter != jobIDs.end(); iter++) {
      addJobID(*iter);
    }
  }

  inline const std::set<uint64_t>& getJobIDs() const {
    return jobIDSet;
  }

  /**
     Set the logical disk ID associated with this buffer.

     \param id the logical disk ID for this buffer
   */
  inline void setLogicalDiskID(uint64_t id) {
    logicalDiskID = id;
  }

  /**
     Get the logical disk ID associated with this buffer.

     \return the logical disk ID for this buffer
   */
  inline uint64_t getLogicalDiskID() const {
    return logicalDiskID;
  }

  /**
     Set the chunk ID for this buffer

     \param id the chunk ID for this buffer
   */
  inline void setChunkID(uint64_t id) {
    chunkID = id;
  }

  /**
     Get the chunk ID for this buffer

     \return the chunk ID for this buffer
   */
  inline uint64_t getChunkID() const {
    return chunkID;
  }

  /**
     Record where the data in the buffer was sourced from. For example, this
     could be the name of a file in the case of reader buffers.

     \param sourceName the name of the source
   */
  void setSourceName(const std::string& sourceName) {
    this->sourceName = sourceName;
  }

  /**
     \return the source of the data in this buffer
   */
  const std::string& getSourceName() {
    return sourceName;
  }

  NetworkMetadata* getNetworkMetadata();

protected:
  /**
     Calculates metadata about the tuples in the buffer. Specifically, this
     counts the number of tuples and the minimum and maximum length of any key.
     This information is cached and updated incrementally if possible to avoid
     extra scans through the buffer.
   */
  virtual void calculateTupleMetadata();

  uint64_t offset;

  // Caching number of tuples and bytes to speed up queries on number of
  // complete bytes and pairs
  bool cached;
  uint64_t numTuples;
  uint32_t minKeyLength;
  uint32_t maxKeyLength;
private:
  /**
     Initializes buffer with default values. Called in both destructors.
   */
  void init();

  uint64_t node;
  uint64_t partitionGroup;
  uint64_t poolID; // Used for multi pools
  uint64_t logicalDiskID;
  uint64_t chunkID; // Used for large partitions
  std::set<uint64_t> jobIDSet;

  // Identifies the source of the data in this buffer. eg. a filename
  std::string sourceName;

  // Stores the pending append pointer for a setupAppendKVPair
  uint32_t pendingKeyLength;
  uint32_t pendingMaxValueLength;
  uint8_t* pendingAppendPtr;
};

#endif // MAPRED_KV_PAIR_BUFFER_H
