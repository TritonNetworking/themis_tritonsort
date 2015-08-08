#include "mapreduce/workers/mapper/Mapper.h"
#include "mapreduce/workers/mapper/MultiJobMapper.h"

MultiJobMapper::MultiJobMapper(
  const std::string& _phaseName, const std::string& stageName, uint64_t id,
  Params& _params, MemoryAllocatorInterface& _memoryAllocator,
  NamedObjectCollection& _dependencies)
  : SingleUnitRunnable(id, stageName),
    phaseName(_phaseName),
    params(_params),
    memoryAllocator(_memoryAllocator),
    dependencies(_dependencies) {

}

MultiJobMapper::~MultiJobMapper() {
  for (JobIDToMapper::iterator iter = mapperForJob.begin();
       iter != mapperForJob.end(); iter++) {
    delete (iter->second);
  }
}

void MultiJobMapper::run(KVPairBuffer* buffer) {
  const std::set<uint64_t>& jobIDs = buffer->getJobIDs();

  for (std::set<uint64_t>::iterator jobIDIter = jobIDs.begin();
       jobIDIter != jobIDs.end(); jobIDIter++) {
    buffer->resetIterator();

    uint64_t jobID = *jobIDIter;

    JobIDToMapper::iterator mapperIter = mapperForJob.find(jobID);
    Mapper* mapper = NULL;

    if (mapperIter != mapperForJob.end()) {
      mapper = mapperIter->second;
    } else {
      std::ostringstream oss;
      oss << getName() << ':' << jobID;

      mapper = static_cast<Mapper*>(
        Mapper::newInstance(
          phaseName, oss.str(), getID(), params, memoryAllocator,
          dependencies));

      // The new mapper inherits information about my downstream trackers
      copyTrackerInformation(*mapper);

      mapper->init();

      mapperForJob.insert(std::pair<uint64_t, Mapper*>(jobID, mapper));
    }

    mapper->map(buffer, jobID);
  }

  delete buffer;
}

void MultiJobMapper::teardown() {
  // Run teardown on each subordinate Mapper
  for (JobIDToMapper::iterator iter = mapperForJob.begin();
       iter != mapperForJob.end(); iter++) {
    (iter->second)->teardown();
  }
}

BaseWorker* MultiJobMapper::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  return new MultiJobMapper(
    phaseName, stageName, id, params, memoryAllocator, dependencies);
}
