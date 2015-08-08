#include "KVPairWriterParentWorker.h"

KVPairWriterParentWorker::KVPairWriterParentWorker(
  MemoryAllocatorInterface& memoryAllocator,
  uint64_t _defaultBufferSize)
  : SingleUnitRunnable<KVPairBuffer>(0, "dummy-stage"),
    defaultBufferSize(_defaultBufferSize),
    bufferFactory(*this, memoryAllocator, 0) {
}

KVPairBuffer* KVPairWriterParentWorker::getBufferForWriter(
  uint64_t minCapacity) {
  return bufferFactory.newInstance(
    ((minCapacity / defaultBufferSize) + 1) * defaultBufferSize);
}

void KVPairWriterParentWorker::putBufferFromWriter(KVPairBuffer* buffer) {
  delete buffer;
}

void KVPairWriterParentWorker::emitBufferFromWriter(
  KVPairBuffer* buffer, uint64_t bufferNumber) {
  buffer->resetIterator();
  emittedBuffers.push_back(buffer);
}

void KVPairWriterParentWorker::run(KVPairBuffer* buffer) {
  ABORT("This class is a stub and is not meant to be run");
}

const std::list<KVPairBuffer*>&
KVPairWriterParentWorker::getEmittedBuffers() const {
  return emittedBuffers;
}

void KVPairWriterParentWorker::returnEmittedBuffersToPool() {
  for (std::list<KVPairBuffer*>::iterator iter = emittedBuffers.begin();
       iter != emittedBuffers.end(); iter++) {
    delete *iter;
  }

  emittedBuffers.clear();
}

BaseWorker* KVPairWriterParentWorker::newInstance(
  const std::string& phaseName, const std::string& stageName,
  uint64_t id, Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies) {

  uint64_t defaultBufferSize = params.get<uint64_t>("DEFAULT_BUFFER_SIZE");

  KVPairWriterParentWorker* parent = new KVPairWriterParentWorker(
    memoryAllocator, defaultBufferSize);

  return parent;
}
