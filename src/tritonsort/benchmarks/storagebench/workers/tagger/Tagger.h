#ifndef STORAGEBENCH_TAGGER_H
#define STORAGEBENCH_TAGGER_H

#include "common/buffers/ByteStreamBuffer.h"
#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/FilenameToStreamIDMap.h"

/**
   Tagger is a worker that uses the filename information associated with a byte
   stream to tag output KVPairBuffers with the partition ID and job ID.

   Partition IDs are randomly re-assigned in order to spread output partitions
   across all disks in the cluster.  This is necessary because input files can
   only be generated using specific partition ranges per disk, due to the
   internals of the writer.
 */
class Tagger : public SingleUnitRunnable<ByteStreamBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param id the worker id

     \param stageName the name of the stage

     \param numPartitions the number of partitions in the cluster

     \param filenameToStreamIDMap an object that associates stream IDs with
     filename information
   */
  Tagger(
    uint64_t id, const std::string& stageName, uint64_t numPartitions,
    FilenameToStreamIDMap& filenameToStreamIDMap);

  /// Destructor
  virtual ~Tagger() {}

  /**
     Given a byte stream buffer, extract its filename, and emit a key/value
     pair buffer with the same underlying memory but tagged with the
     appropriate partition ID and job ID.

     \param buffer a byte stream buffer to tag
   */
  void run(ByteStreamBuffer* buffer);

private:
  const uint64_t numPartitions;

  FilenameToStreamIDMap& filenameToStreamIDMap;
};

#endif // STORAGEBENCH_TAGGER_H
