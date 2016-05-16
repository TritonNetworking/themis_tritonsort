#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/CoordinatorClientFactory.h"
#include "mapreduce/common/JobInfo.h"
#include "mapreduce/common/URL.h"
#include "mapreduce/workers/writer/MultiProtocolWriter.h"
#include "mapreduce/workers/writer/Writer.h"

MultiProtocolWriter::MultiProtocolWriter(
  const std::string& _phaseName, const std::string& stageName,
  uint64_t id, Params& _params, MemoryAllocatorInterface& _memoryAllocator,
  NamedObjectCollection& _dependencies)
  : SingleUnitRunnable<KVPairBuffer>(id, stageName),
    phaseName(_phaseName),
    dependencies(_dependencies),
    params(_params),
    memoryAllocator(_memoryAllocator) {

  coordinatorClient = CoordinatorClientFactory::newCoordinatorClient(
    params, phaseName, stageName, id);
}

MultiProtocolWriter::~MultiProtocolWriter() {
  for (WriterMap::iterator iter = writerMap.begin(); iter != writerMap.end();
       iter++) {
    delete iter->second;
  }
  writerMap.clear();
}

void MultiProtocolWriter::run(KVPairBuffer* buffer) {
  const std::set<uint64_t>& jobIDs = buffer->getJobIDs();

  ASSERT(jobIDs.size() == 1, "Expected a KVPairBuffer arriving at a writer to "
         "only have one job ID; this one had %llu", jobIDs.size());

  uint64_t jobID = *(jobIDs.begin());

  WriterMap::iterator iter = writerMap.find(jobID);

  WriteWorker* writer = NULL;

  if (iter != writerMap.end()) {
    writer = iter->second;
  } else {
    const URL& outputURL = coordinatorClient->getOutputDirectory(jobID);

    const std::string& scheme = outputURL.scheme();

    if (scheme == "local") {
      std::ostringstream oss;
      oss << getName() << ":local_" << jobID;

      writer = static_cast<WriteWorker*>(
        Writer::newInstance(
          phaseName, oss.str(), getID(), params, memoryAllocator,
          dependencies));
    } else {
      ABORT("Unknown scheme '%s' for output URL '%s'", scheme.c_str(),
            outputURL.fullURL().c_str());
    }

    writer->registerWithResourceMonitor();

    writer->init();

    writerMap.insert(std::pair<uint64_t, WriteWorker*>(jobID, writer));
  }

  writer->run(buffer);
}

void MultiProtocolWriter::teardown() {
  for (WriterMap::iterator iter = writerMap.begin(); iter != writerMap.end();
       iter++) {
    WriteWorker* writer = iter->second;
    writer->unregisterWithResourceMonitor();
    writer->teardown();

  }
}

BaseWorker* MultiProtocolWriter::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  return new MultiProtocolWriter(
    phaseName, stageName, id, params, memoryAllocator, dependencies);
}
