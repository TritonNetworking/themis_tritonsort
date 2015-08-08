#ifndef THEMIS_AGGREGATING_HASH_COUNTER_H
#define THEMIS_AGGREGATING_HASH_COUNTER_H

#include <boost/unordered_map.hpp>
#include <map>
#include <queue>
#include <stdint.h>

#include "core/BaseWorker.h"
#include "core/Hash.h"
#include "core/ResourceMonitor.h"
#include "mapreduce/common/KVPairWriterInterface.h"
#include "mapreduce/common/KeyValuePair.h"

template <typename T> class AggregatingHashCounter
  : public ResourceMonitorClient {
public:
  /// Constructor
  /**
     \param _maxCapacity the maximum number of simultaneous counts the counter
     can maintain at a time. If this number is not even, it will be rounded
     up to the nearest even number to simplify the implementation. When the
     counter hits this capacity, it will flush the keys correspond to the
     (_maxCapacity / 2) lowest counts.

     \param _writer the key/value pair writer that the counter will use to
     write flushed key/count pairs.
   */
  AggregatingHashCounter(
    uint64_t _maxCapacity,
    KVPairWriterInterface& _writer)
    : maxCapacity(_maxCapacity + (_maxCapacity % 2)),
      nextEmptySlot(0),
      writer(_writer),
      updateID(0),
      misses(0),
      hits(0),
      logger("aggregating_hash_counter") {

    lastEmptySlot = maxCapacity;

    // Reserve maximum capacity for the count vector and cache all the Count
    // objects you will ever need
    countVector.reserve(maxCapacity);

    for (uint64_t i = 0; i < maxCapacity; i++) {
      countVector.push_back(NULL);
      freeCounts.push(new Count());
    }

    ResourceMonitor::registerClient(this, "aggregating_hash_counter");
  }

  virtual ~AggregatingHashCounter() {
    logger.logDatum("hits", hits);
    logger.logDatum("misses", misses);

    ABORT_IF(freeCounts.size() != maxCapacity,
             "Some Count objects that were used haven't been returned to the "
             "free list");

    while (!freeCounts.empty()) {
      delete freeCounts.front();
      freeCounts.pop();
    }
  }

  void resourceMonitorOutput(Json::Value& obj) {
    obj["hits"] = Json::UInt64(hits);
    obj["misses"] = Json::UInt64(misses);
  }

  void add(const uint8_t* key, uint32_t keyLength, T amount) {
    uint64_t hash = Hash::hash(key, keyLength);

    CountMapIter iter = counts.find(hash);

    if (iter != counts.end()) {
      hits++;

      Count* countHandle = iter->second;
      countHandle->count += amount;
      countHandle->lastUpdate = updateID++;
    } else {
      misses++;

      // If the heap is too big to store another count, emit the half of the
      // count vector with the smallest counts
      if (nextEmptySlot == lastEmptySlot) {
        // If this was your first time flushing elements from the vector, make
        // sure that you flush when you hit _half_ capacity for all subsequent
        // flushes

        if (lastEmptySlot == maxCapacity) {
          lastEmptySlot = maxCapacity / 2;
        }

        // Sort the vector in order by count, preferring items updated further
        // in the past to break ties
        std::sort(countVector.begin(), countVector.end(), comparator);

        uint64_t index = 0;
        CountVectorIter countVectorIter = countVector.begin();

        while (index < lastEmptySlot) {
          emitCount(*countVectorIter);
          freeCounts.push(*countVectorIter);

          *countVectorIter = NULL;
          countVectorIter++;
          index++;
        }

        // The elements in [0, lastEmptySlot) are now free to be replaced with
        // new counts.
        nextEmptySlot = 0;
      }

      ABORT_IF(freeCounts.empty(), "Ran out of free Count objects");
      Count* count = freeCounts.front();
      freeCounts.pop();
      count->hash = hash;
      count->count = amount;
      count->lastUpdate = updateID++;

      // Insert the new count into the next free slot
      countVector[nextEmptySlot++] = count;

      // Also store it into the lookup table
      counts[hash] = count;

      // Remember the key associated with this hash
      uint8_t* newKey = new uint8_t[keyLength];
      memcpy(newKey, key, keyLength);

      hashToKey.insert(
        std::pair<uint64_t, KeyStruct>(hash, KeyStruct(newKey, keyLength)));
    }
  }

  void flush() {
    for (CountVectorIter iter = countVector.begin(); iter != countVector.end();
         iter++) {
      Count* count = *iter;
      if (count != NULL) {
        emitCount(count);
        *iter = NULL;
        freeCounts.push(count);
      }
    }

    nextEmptySlot = 0;
    lastEmptySlot = maxCapacity;

    counts.clear();
  }

private:
  class Count {
  public:
    Count()
      : hash(0),
        count(0),
        lastUpdate(0) {
    }

    uint64_t hash;
    T count;
    uint64_t lastUpdate;
  };

  class CountComparator {
  public:
    bool operator() (const Count* x, const Count* y) const {
      if (x->count == y->count) {
        return x->lastUpdate < y->lastUpdate;
      } else {
        return x->count < y->count;
      }
    }
  };

  struct KeyStruct {
    uint8_t* key;
    uint32_t keyLength;

    KeyStruct(uint8_t* _key, uint32_t _keyLength)
      : key(_key), keyLength(_keyLength) {
    }
  };

  inline void emitCount(Count* count) {
    KeyValuePair kvPair;

    HashToKeyMapIter iter = hashToKey.find(count->hash);
    ABORT_IF(iter == hashToKey.end(),
             "Can't find key corresponding to hash %llu", count->hash);

    kvPair.setKey(iter->second.key, iter->second.keyLength);
    kvPair.setValue(reinterpret_cast<uint8_t*>(&(count->count)), sizeof(T));

    writer.write(kvPair);

    delete[] iter->second.key;
    hashToKey.erase(iter);

    counts.erase(count->hash);
  }

  typedef boost::unordered_map<uint64_t, Count*> CountMap;
  typedef typename CountMap::iterator CountMapIter;
  typedef boost::unordered_map<uint64_t, KeyStruct> HashToKeyMap;
  typedef typename HashToKeyMap::iterator HashToKeyMapIter;
  typedef std::vector<Count*> CountVector;
  typedef typename CountVector::iterator CountVectorIter;

  std::queue<Count*> freeCounts;

  const uint64_t maxCapacity;

  CountMap counts;

  uint64_t nextEmptySlot;
  uint64_t lastEmptySlot;
  CountComparator comparator;
  CountVector countVector;

  HashToKeyMap hashToKey;

  KVPairWriterInterface& writer;

  uint64_t updateID;

  uint64_t misses;
  uint64_t hits;

  StatLogger logger;
};

#endif // THEMIS_AGGREGATING_HASH_COUNTER_H
