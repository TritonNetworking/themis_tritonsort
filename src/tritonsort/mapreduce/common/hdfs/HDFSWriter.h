#ifndef THEMIS_MAPRED_HDFS_WRITER_H
#define THEMIS_MAPRED_HDFS_WRITER_H

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/CoordinatorClientInterface.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/hdfs/HDFSClient.h"

/**
   Writes buffers to HDFS
 */
class HDFSWriter : public SingleUnitRunnable<KVPairBuffer> {
WORKER_IMPL

public:
  /// Constructor
  /**
     \param phaseName the phase in which this worker is executing

     \param stageName the name of this worker's parent stage

     \param id the worker's ID within its parent stage

     \param params a Params object that the writer will use to configure its
     coordinator client
   */
  HDFSWriter(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    Params& params);

  /// Destructor
  virtual ~HDFSWriter();

  /// Append the buffer to an appropriate file based on its job ID and
  /// intermediate partition number
  void run(KVPairBuffer* buffer);

private:
  class OutputPartitionWriter {
  public:
    OutputPartitionWriter(
      uint64_t jobID, uint64_t outputPartitionID, const std::string& host,
      uint32_t port, const std::string& outputDir);

    virtual ~OutputPartitionWriter() {}

    void write(KVPairBuffer& buffer);
  private:
    HDFSClient hdfsClient;
    bool firstWrite;
    std::string filePath;
  };

  class OutputDirectoryInfo {
  public:
    OutputDirectoryInfo(
      const std::string& host, uint32_t port, const std::string& path);

    const std::string host;
    const uint32_t port;
    const std::string path;
  };

  typedef std::map<std::pair<uint64_t, uint64_t>, OutputPartitionWriter*>
  OutputPartitionWriterMap;
  typedef std::map<uint64_t, OutputDirectoryInfo*> OutputDirectoryInfoMap;

  OutputPartitionWriterMap outputPartitionWriters;

  OutputDirectoryInfoMap outputDirectories;

  CoordinatorClientInterface* coordinatorClient;
};

#endif // THEMIS_MAPRED_HDFS_WRITER_H
