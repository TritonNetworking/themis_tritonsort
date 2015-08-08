#include <arpa/inet.h>
#include <string.h>

#include "common/AlignmentUtils.h"
#include "core/File.h"
#include "core/MemoryUtils.h"
#include "core/Utils.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/functions/map/DiskBenchmarkMapFunction.h"

DiskBenchmarkMapFunction::DiskBenchmarkMapFunction(
  const std::string& _hostname, uint64_t _writeSize, uint64_t _alignmentSize,
  uint64_t writeBufferSize, uint64_t _benchmarkDataSize)
  : alignmentSize(_alignmentSize), hostname(_hostname),
    writeSize(_writeSize - (_writeSize % _alignmentSize)),
    benchmarkDataSize(_benchmarkDataSize),
    zeroKey(NULL), zeroKeyLength(1),
    randomDataLength(writeBufferSize + _alignmentSize) {

  // We'll use whatever data happens to be in memory at the time for writing.
  unalignedRandomData = new (themis::memcheck) uint8_t[
    writeBufferSize + alignmentSize];

  // Align the buffer to an alignmentMultiple-divisible offset if alignment is
  // required

  if (alignmentSize > 0) {
    randomData = align(unalignedRandomData, alignmentSize);
  } else {
    randomData = unalignedRandomData;
  }

  zeroKey = new (themis::memcheck) uint8_t[zeroKeyLength];
  memset(zeroKey, 0, zeroKeyLength);
}

DiskBenchmarkMapFunction::~DiskBenchmarkMapFunction() {
  if (unalignedRandomData != NULL) {
    delete[] unalignedRandomData;
    unalignedRandomData = NULL;
  }

  if (zeroKey != NULL) {
    delete[] zeroKey;
    zeroKey = NULL;
  }
}

void DiskBenchmarkMapFunction::map(
  KeyValuePair& kvPair, KVPairWriterInterface& writer) {

  ABORT_IF(kvPair.getKeyLength() == 0, "Encountered a zero-length key when "
           "we didn't expect to");

  std::string dirname((const char*) kvPair.getKey(),
                      kvPair.getKeyLength());

  File benchmarkFile(dirname + "/benchmark_file");
  benchmarkFile.open(File::WRITE, true);
  benchmarkFile.enableDirectIO();
  benchmarkFile.preallocate(benchmarkDataSize);

  Timer writeBenchmarkTimer;

  int fd = benchmarkFile.getFileDescriptor();
  const char* benchmarkFilename = benchmarkFile.getFilename().c_str();

  writeBenchmarkTimer.start();
  for (uint64_t i = 0; i < benchmarkDataSize / randomDataLength; i++) {
    blockingWrite(fd, randomData, randomDataLength, writeSize,
                  alignmentSize, benchmarkFilename);
  }
  benchmarkFile.sync();
  benchmarkFile.close();
  writeBenchmarkTimer.stop();

  uint32_t writeRate = benchmarkDataSize / writeBenchmarkTimer.getElapsed();
  writeRate = htonl(writeRate);

  std::string completeDiskPath(hostname);
  completeDiskPath += ":";
  completeDiskPath += dirname;

  uint32_t valueLength = sizeof(writeRate) + completeDiskPath.size();
  uint8_t* valueBuffer = writer.setupWrite(
    zeroKey, zeroKeyLength, valueLength);
  memcpy(valueBuffer, &writeRate, sizeof(writeRate));
  memcpy(valueBuffer + sizeof(writeRate), completeDiskPath.c_str(),
         completeDiskPath.size());
  writer.commitWrite(valueLength);
}
