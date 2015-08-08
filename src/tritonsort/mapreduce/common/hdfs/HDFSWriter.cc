#include "mapreduce/common/hdfs/HDFSWriter.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/URL.h"

HDFSWriter::HDFSWriter(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  Params& params)
  : SingleUnitRunnable<KVPairBuffer>(id, stageName),
    coordinatorClient(NULL) {

  coordinatorClient = CoordinatorClientFactory::newCoordinatorClient(
    params, phaseName, stageName, id);
}

HDFSWriter::~HDFSWriter() {
  if (coordinatorClient != NULL) {
    delete coordinatorClient;
    coordinatorClient = NULL;
  }

  for (OutputPartitionWriterMap::iterator iter = outputPartitionWriters.begin();
       iter != outputPartitionWriters.end(); iter++) {
    delete iter->second;
  }

  outputPartitionWriters.clear();

  for (OutputDirectoryInfoMap::iterator iter = outputDirectories.begin();
       iter != outputDirectories.end(); iter++) {
    delete iter->second;
  }

  outputDirectories.clear();
}

void HDFSWriter::run(KVPairBuffer* buffer) {
  const std::set<uint64_t>& jobIDs = buffer->getJobIDs();
  ABORT_IF(jobIDs.size() != 1, "Expect a buffer written to HDFS to be "
           "associated with a single job ID; this buffer is associated "
           "with %llu", jobIDs.size());

  uint64_t jobID = *(jobIDs.begin());
  uint64_t logicalDiskUID = buffer->getLogicalDiskID();

  std::pair<uint64_t, uint64_t> key(jobID, logicalDiskUID);

  OutputPartitionWriterMap::iterator writerIter = outputPartitionWriters.find(
    key);

  OutputPartitionWriter* writer = NULL;

  if (writerIter == outputPartitionWriters.end()) {
    OutputDirectoryInfoMap::iterator outputDirsIter =
      outputDirectories.find(jobID);

    OutputDirectoryInfo* dirInfo = NULL;

    if (outputDirsIter != outputDirectories.end()) {
      dirInfo = outputDirsIter->second;
    } else {
      const URL& outputURL = coordinatorClient->getOutputDirectory(jobID);

      dirInfo = new OutputDirectoryInfo(
        outputURL.host(), outputURL.port(), outputURL.path());

      outputDirsIter = outputDirectories.insert(std::make_pair(jobID, dirInfo))
        .first;
    }

    writer = new OutputPartitionWriter(
      jobID, logicalDiskUID, dirInfo->host, dirInfo->port, dirInfo->path);

    outputPartitionWriters.insert(
      std::pair< std::pair<uint64_t, uint64_t>, OutputPartitionWriter* >(
        key, writer));
  } else {
    writer = writerIter->second;
  }

  writer->write(*buffer);
  delete buffer;
}

HDFSWriter::OutputPartitionWriter::OutputPartitionWriter(
  uint64_t jobID, uint64_t outputPartitionID, const std::string& host,
  uint32_t port, const std::string& outputDir)
  : hdfsClient(host, port),
    firstWrite(true) {

  std::ostringstream oss;
  oss << outputDir << "/job_" << jobID << "_output_" << outputPartitionID;

  filePath.assign(oss.str());
}

void HDFSWriter::OutputPartitionWriter::write(KVPairBuffer& buffer) {
  if (firstWrite) {
    hdfsClient.create(filePath, buffer);
    firstWrite = false;
  } else {
    hdfsClient.append(filePath, buffer);
  }
}

HDFSWriter::OutputDirectoryInfo::OutputDirectoryInfo(
  const std::string& _host, uint32_t _port, const std::string& _path)
  : host(_host), port(_port), path(_path) {
}

BaseWorker* HDFSWriter::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  return new HDFSWriter(phaseName, stageName, id, params);
}
