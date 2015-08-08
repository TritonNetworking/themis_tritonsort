#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/workers/writer/BaseWriter.h"
#include "mapreduce/workers/writer/Writer.h"

Writer::Writer(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  Params& params, NamedObjectCollection& dependencies, uint64_t _maxWriteSize,
  uint64_t _alignmentSize)
  : SingleUnitRunnable<KVPairBuffer>(id, stageName),
    maxWriteSize(_maxWriteSize),
    alignmentSize(_alignmentSize),
    logger(stageName, id),
    writer(*(BaseWriter::newBaseWriter(
               phaseName, stageName, id, params, dependencies, File::WRITE,
               logger))) {
  writeTimeStatID = logger.registerHistogramStat("write_time", 100);
}

Writer::~Writer() {
  delete &writer;
}

void Writer::run(KVPairBuffer* buffer) {
  // Issue a blocking write.
  File* file = writer.getFile(buffer);

  writeTimer.start();
  file->write(
    buffer->getRawBuffer(), buffer->getCurrentSize(), maxWriteSize,
    alignmentSize);
  writeTimer.stop();

  // Log information about the write.
  writer.logBufferWritten(buffer);
  logger.add(writeTimeStatID, writeTimer);

  delete buffer;
}

void Writer::teardown() {
  writer.teardown();
}

BaseWorker* Writer::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  // If this worker was spawned by a MultiProtocolWriter, want to make sure it
  // gets configured according to the parameters in use by its parent stage
  std::string parentStageName(stageName, 0, stageName.find_first_of(':'));

  // Write size is unlimited, unless specified.
  uint64_t maxWriteSize = std::numeric_limits<uint64_t>::max();
  if (params.contains("MAX_WRITE_SIZE." + phaseName)) {
    maxWriteSize = params.get<uint64_t>("MAX_WRITE_SIZE." + phaseName);
  }

  uint64_t alignmentSize = params.getv<uint64_t>(
    "ALIGNMENT.%s.%s", phaseName.c_str(), parentStageName.c_str());

  Writer* writer = new Writer(
    phaseName, stageName, id, params, dependencies, maxWriteSize,
    alignmentSize);

  return writer;
}
