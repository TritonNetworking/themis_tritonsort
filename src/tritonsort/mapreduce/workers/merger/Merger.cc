#include "core/MemoryUtils.h"
#include "mapreduce/workers/merger/Merger.h"

Merger::Merger(
  const std::string& stageName, uint64_t id,
  MemoryAllocatorInterface& memoryAllocator, ChunkMap& chunkMap,
  uint64_t defaultBufferSize, uint64_t alignmentSize,
  WriteTokenPool& _tokenPool)
  : MultiQueueRunnable(id, stageName),
    jobID(0),
    bufferFactory(*this, memoryAllocator, defaultBufferSize, alignmentSize),
    tokenPool(_tokenPool),
    chunkSizeMap(chunkMap.getSizeMap()) {

  const ChunkMap::DiskMap& diskMap = chunkMap.getDiskMap();

  uint64_t currentOffset = 0;
  for (ChunkMap::DiskMap::const_iterator iter = diskMap.begin();
       iter != diskMap.end(); iter++) {
    uint64_t partitionID = iter->first;
    uint64_t numChunks = (iter->second).size();

    // Build offset map.
    offsetMap[partitionID] = currentOffset;
    currentOffset += numChunks;

    // Construct data structures.
    partitions.push_back(partitionID);

    for (uint64_t i = 0; i < numChunks; i++) {
      tupleTables[partitionID][i] = KeyValuePair();
      inputBufferTables[partitionID][i] = NULL;
    }

    tupleHeaps[partitionID] = TupleHeap();
    outputBuffers[partitionID] = NULL;

    completedChunks[partitionID] = 0;

    // Initialize bytes merged map.
    for (uint64_t i = 0; i < numChunks; i++) {
      bytesMergedMap[partitionID][i] = 0;
    }
  }
}

void Merger::run() {
  // Initialize all partition data structures by filling tuple tables and heaps.
  for (std::list<uint64_t>::iterator iter = partitions.begin();
       iter != partitions.end(); iter++) {
    TupleTable& tupleTable = tupleTables.at(*iter);
    BufferTable& inputBufferTable = inputBufferTables.at(*iter);
    TupleHeap& tupleHeap = tupleHeaps.at(*iter);
    uint64_t& queueOffset = offsetMap.at(*iter);

    for (TupleTable::iterator tupleTableIter = tupleTable.begin();
         tupleTableIter != tupleTable.end(); tupleTableIter++) {
      uint64_t chunkID = tupleTableIter->first;
      uint64_t queueID = queueOffset + chunkID;

      KeyValuePair& keyValuePair = tupleTableIter->second;
      KVPairBuffer*& inputBuffer = inputBufferTable.at(chunkID);

      // Block until we get a buffer from this chunk.
      inputBuffer = getNewWork(queueID);
      WriteToken* token = inputBuffer->getToken();
      if (token != NULL) {
        tokenPool.putToken(token);
      }
      ASSERT(inputBuffer->getChunkID() == chunkID,
             "Merger fetched buffer with chunk ID %llu from queue %llu, "
             "offset %llu. Expected chunk ID %llu.",
             inputBuffer->getChunkID(), queueID, queueOffset, chunkID);

      // Set job ID.
      if (jobID == 0) {
        const std::set<uint64_t>& jobIDs = inputBuffer->getJobIDs();
        ASSERT(jobIDs.size() == 1, "Expected buffers entering merger to have "
               "exactly one job ID; this one has %llu", jobIDs.size());
        jobID = *(jobIDs.begin());
      }

      // Fetch the first tuple.
      bool gotTuple = inputBuffer->getNextKVPair(keyValuePair);
      ABORT_IF(!gotTuple, "First buffer for chunk %llu did not contain a tuple",
               chunkID);

      // Add a heap entry.
      HeapEntry *h = new (themis::memcheck) HeapEntry(keyValuePair);
      h->id = chunkID;
      tupleHeap.push(h);
    }
  }

  while (!partitions.empty()) {
    // Merge all partitions in round-robin fashion.
    for (std::list<uint64_t>::iterator iter = partitions.begin();
         iter != partitions.end(); iter++) {
      uint64_t partitionID = *iter;
      TupleTable& tupleTable = tupleTables.at(partitionID);
      BufferTable& inputBufferTable = inputBufferTables.at(partitionID);
      TupleHeap& tupleHeap = tupleHeaps.at(partitionID);
      KVPairBuffer*& outputBuffer = outputBuffers.at(partitionID);
      uint64_t& numCompletedChunks = completedChunks.at(partitionID);
      uint64_t& queueOffset = offsetMap.at(partitionID);
      std::map<uint64_t, uint64_t>& chunkBytesMerged =
        bytesMergedMap.at(partitionID);
      const std::map<uint64_t, uint64_t>& totalChunkSizeMap =
        chunkSizeMap.at(partitionID);

      // Service this partition until either we emit a buffer, or we finish
      // the partition completely.
      bool serviceNextPartition = false;
      while (!serviceNextPartition) {
        // Append the top of the heap to the output buffer.
        HeapEntry* top = tupleHeap.top();
        uint64_t chunkID = top->id;
        tupleHeap.pop();
        delete top;

        KeyValuePair& kvPair = tupleTable.at(chunkID);
        KVPairBuffer*& inputBuffer = inputBufferTable.at(chunkID);
        uint64_t& bytesMerged = chunkBytesMerged.at(chunkID);
        const uint64_t& chunkSize = totalChunkSizeMap.at(chunkID);

        if (outputBuffer != NULL &&
            kvPair.getWriteSize() + outputBuffer->getCurrentSize() >
            outputBuffer->getCapacity()) {
          // We can't fit this tuple in the current buffer, so emit.
          if (outputBuffer != NULL) {
            emitWorkUnit(outputBuffer);
            serviceNextPartition = true;
            outputBuffer = NULL;
          }
        }

        if (outputBuffer == NULL) {
          // Get a new output buffer for this partition that is large enough to
          // hold this tuple.
          uint64_t bufferSize = std::max(
            bufferFactory.getDefaultSize(), kvPair.getWriteSize());
          outputBuffer = bufferFactory.newInstance(bufferSize);
          outputBuffer->setLogicalDiskID(partitionID);
          outputBuffer->addJobID(jobID);
        }

        outputBuffer->addKVPair(kvPair);
        bytesMerged += kvPair.getWriteSize();

        // Add a new heap entry from this chunk.
        bool gotTuple = inputBuffer->getNextKVPair(kvPair);
        if (!gotTuple) {
          // We've already merged all the tuples from this buffer.
          delete inputBuffer;

          if (bytesMerged == chunkSize) {
            // We're done merging this chunk.
            numCompletedChunks++;
          } else {
            // Get a new buffer for this chunk.
            inputBuffer = getNewWork(queueOffset + chunkID);

            WriteToken* token = inputBuffer->getToken();
            if (token != NULL) {
              tokenPool.putToken(token);
            }

            // Get the first tuple from the new buffer.
            gotTuple = inputBuffer->getNextKVPair(kvPair);
            ABORT_IF(!gotTuple, "Buffer for chunk %llu did not contain a tuple",
                     chunkID);
          }
        }

        if (gotTuple) {
          // Add a heap entry.
          HeapEntry *h = new (themis::memcheck) HeapEntry(kvPair);
          h->id = chunkID;
          tupleHeap.push(h);
        }

        if (numCompletedChunks == tupleTable.size()) {
          // We're done merging this partition.
          serviceNextPartition = true;
          if (outputBuffer != NULL) {
            emitWorkUnit(outputBuffer);
            outputBuffer = NULL;
          }

          // Remove this partition from the round-robin service order.
          iter = partitions.erase(iter);
          // Move iterator back one so the for loop advancement will be correct.
          iter--;
        }
      }
    }
  }
}

void Merger::teardown() {
  // Verify data structures are empty.
  ASSERT(partitions.size() == 0, "Still merging %llu partitions at teardown.",
         partitions.size());

  for (std::list<uint64_t>::iterator iter = partitions.begin();
       iter != partitions.end(); iter++) {
    uint64_t partitionID = *iter;
    BufferTable& inputBufferTable = inputBufferTables.at(partitionID);
    TupleHeap& tupleHeap = tupleHeaps.at(partitionID);
    KVPairBuffer*& outputBuffer = outputBuffers.at(partitionID);
    uint64_t& numCompletedChunks = completedChunks.at(partitionID);

    for (BufferTable::iterator bufferIter = inputBufferTable.begin();
         bufferIter != inputBufferTable.end(); bufferIter++) {
      uint64_t chunkID = bufferIter->first;
      KVPairBuffer*& buffer = bufferIter->second;
      ABORT_IF(buffer != NULL, "At teardown input buffer for partition %llu "
             "chunk %llu is non-NULL", partitionID, chunkID);
    }

    ABORT_IF(!tupleHeap.empty(), "At teardown, tuple heap for partition %llu "
             "has %llu elements.", partitionID, tupleHeap.size());

    ABORT_IF(outputBuffer != NULL, "At teardown, output buffer for partition "
           "%llu is non-NULL", partitionID);

    ABORT_IF(numCompletedChunks != inputBufferTable.size(), "At teardown, only "
           "%llu of %llu chunks completed", numCompletedChunks,
           inputBufferTable.size());
  }
}

BaseWorker* Merger::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  ChunkMap* chunkMap = dependencies.get<ChunkMap>("chunk_map");

  uint64_t defaultBufferSize = params.get<uint64_t>(
    "DEFAULT_BUFFER_SIZE." + phaseName + "." + stageName);

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), stageName.c_str());

  WriteTokenPool* tokenPool = dependencies.get<WriteTokenPool>(
    "read_token_pool");

  Merger* merger = new Merger(
    stageName, id, memoryAllocator, *chunkMap, defaultBufferSize,
    alignmentSize, *tokenPool);

  return merger;
}
