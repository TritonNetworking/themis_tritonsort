#ifndef MAPRED_MERGER_H
#define MAPRED_MERGER_H

#include <list>
#include <map>

#include "common/WriteTokenPool.h"
#include "core/MultiQueueRunnable.h"
#include "core/constants.h"
#include "mapreduce/common/ChunkMap.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/boundary/HeapEntryPtrComparator.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

/**
   Merger is a worker used in phase three to merge sorted chunk files into a
   single sorted stream per partition. It operates on all partitions
   simultaneously by round-robining between partitions every time a merged
   buffer is emitted. Users should set the upstream converter's quota as large
   as possible to avoid potential deadlock situations.

   \TODO: Support multiple merger threads by spreading partitions across
   Mergers.
 */
class Merger : public MultiQueueRunnable<KVPairBuffer> {
WORKER_IMPL
public:
  /// Constructor
  /**
     \param stageName the name of the stage

     \param id the id of the worker

     \param memoryAllocator the memory allocator for creating new buffers

     \param chunkMap the mapping of partition chunks to disks created in the
     splitsort half of phase three

     \param defaultBufferSize the default size of merger output buffers

     \param alignmentSize the alignment size of merger output bufers

     \param tokenPool the read token pool to return to
  */
  Merger(
    const std::string& stageName, uint64_t id,
    MemoryAllocatorInterface& memoryAllocator, ChunkMap& chunkMap,
    uint64_t defaultBufferSize, uint64_t alignmentSize,
    WriteTokenPool& tokenPool);

  /// Merge sorted chunks into a single stream of partition buffers
  void run();

  /// Check data structures are clean when the worker tears down.
  void teardown();

private:
  typedef std::map<uint64_t, KeyValuePair> TupleTable;
  typedef std::map<uint64_t, KVPairBuffer*> BufferTable;

  std::map<uint64_t, TupleTable> tupleTables;
  std::map<uint64_t, BufferTable> inputBufferTables;
  std::map<uint64_t, TupleHeap> tupleHeaps;
  std::map<uint64_t, KVPairBuffer*> outputBuffers;
  std::map<uint64_t, uint64_t> completedChunks;
  std::map<uint64_t, uint64_t> offsetMap;

  std::list<uint64_t> partitions;

  uint64_t jobID;

  KVPairBufferFactory bufferFactory;

  WriteTokenPool& tokenPool;

  const ChunkMap::SizeMap& chunkSizeMap;
  ChunkMap::SizeMap bytesMergedMap;

  DISALLOW_COPY_AND_ASSIGN(Merger);
};

#endif // MAPRED_MERGER_H
