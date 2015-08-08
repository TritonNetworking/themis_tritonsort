#ifndef THEMIS_REDUCE_KV_PAIR_ITERATOR_H
#define THEMIS_REDUCE_KV_PAIR_ITERATOR_H

#include "mapreduce/common/KVPairIterator.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

/**
   An iterator designed for the needs of reduce functions. Iterates over all
   records with the same key, expecting some external driver (usually Reducer)
   to advance the iterator from one key to the next.
 */
class ReduceKVPairIterator : public KVPairIterator {
public:
  /// Constructor
  /**
     \param buffer the buffer from which the iterator will draw records
   */
  ReduceKVPairIterator(KVPairBuffer& buffer);

  /// Return the next record with the key whose values are currently being
  /// iterated over
  /**
     \param[out] kvPair will be set to point to the contents of the next record

     \return true if there are more records with the current key, false
     otherwise
   */
  bool next(KeyValuePair& kvPair);

  /// Resets the iterator, moving it back to the first record with the current
  /// key
  void reset();

  /// Instruct the iterator to advance to the next key
  /**
     \warning Don't delete `key`; it points to the buffer's internal memory

     \param[out] key will be set to point to the current key
     \param[out] keyLength will be set to the current key's length

     \return true if there are more keys to iterate through in this buffer;
     false otherwise
   */
  bool startNextKey(const uint8_t*& key, uint32_t& keyLength);
private:
  const uint8_t* currentKey;
  uint32_t currentKeyLength;
  uint64_t currentKeyStartPosition;

  const uint8_t* nextKey;
  uint32_t nextKeyLength;
  uint64_t nextKeyStartPosition;

  bool nextKeyStartPositionKnown;
  bool doneWithKeyGroup;

  bool noMoreTuples;

  KVPairBuffer& buffer;
};

#endif // THEMIS_REDUCE_KV_PAIR_ITERATOR_H
