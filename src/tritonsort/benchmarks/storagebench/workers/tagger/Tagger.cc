#include "benchmarks/storagebench/workers/tagger/Tagger.h"
#include "common/PartitionFile.h"
#include "common/buffers/ByteStreamBuffer.h"
#include "core/MemoryUtils.h"
#include "mapreduce/common/StreamInfo.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"

Tagger::Tagger(
  uint64_t id, const std::string& stageName, uint64_t _numPartitions,
  FilenameToStreamIDMap& _filenameToStreamIDMap)
  : SingleUnitRunnable<ByteStreamBuffer>(id, stageName),
    numPartitions(_numPartitions),
    filenameToStreamIDMap(_filenameToStreamIDMap) {
}

void Tagger::run(ByteStreamBuffer* buffer) {
  // Byte stream readers use empty buffers to signal the end of a byte stream
  // Ignore such end-of-stream buffers
  if (!buffer->empty()) {
    // First construct a KVPairBuffer with the same underlying memory region
    // since the writer can only take KVPairBuffers.
    KVPairBuffer* kvPairBuffer =
      new (themis::memcheck) KVPairBuffer(NULL, 0, 0);
    kvPairBuffer->stealMemory(*buffer);

    // Extract the job ID from the filename.
    const StreamInfo& streamInfo =
      filenameToStreamIDMap.getStreamInfo(buffer->getStreamID());
    const std::string& filename = streamInfo.getFilename();
    PartitionFile file(filename);
    uint64_t jobID = file.getJobID();

    // Randomly re-assign the partition ID to spread partitions out across the
    // cluster.
    uint64_t partitionID = lrand48() % numPartitions;

    // Set partitionID and jobID.
    kvPairBuffer->setLogicalDiskID(partitionID);
    kvPairBuffer->addJobID(jobID);

    // Also set the partition group to the partition ID for use in mixediobench
    kvPairBuffer->setPartitionGroup(partitionID);

    emitWorkUnit(kvPairBuffer);
  }

  // Deleting the original buffer will not free its memory since it has been
  // stolen by kvPairBuffer, but this is fine because the writer will free the
  // memory when it deletes the kvPairBuffer.
  delete buffer;
}

BaseWorker* Tagger::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  FilenameToStreamIDMap* filenameToStreamIDMap =
    dependencies.get<FilenameToStreamIDMap>(
      stageName, "filename_to_stream_id_map");

  uint64_t numPartitions = params.get<uint64_t>("NUM_PARTITIONS");

  Tagger* tagger = new Tagger(
    id, stageName, numPartitions, *filenameToStreamIDMap);

  return tagger;
}
