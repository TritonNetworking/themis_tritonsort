#include <algorithm>
#include <iostream>
#include <new>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "common/DummyWorker.h"
#include "common/MainUtils.h"
#include "common/SimpleMemoryAllocator.h"
#include "core/File.h"
#include "core/Params.h"
#include "core/StatusPrinter.h"
#include "core/TritonSortAssert.h"
#include "mapreduce/common/KVPairBufferFactory.h"
#include "mapreduce/common/sorting/RadixSortStrategy.h"

int main(int argc, char** argv) {
  uint64_t randomSeed = Timer::posixTimeInMicros() * getpid();
  srand(randomSeed);

  signal(SIGSEGV, sigsegvHandler);

  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <log dir> <intermediate file>" << std::endl;
    exit(1);
  }

  Params params;
  params.add<std::string>("LOG_DIR", argv[1]);
  params.add<std::string>("CHANNEL_STATUS_HEADER", "STATUS");
  params.add<std::string>("CHANNEL_STATISTIC_HEADER", "STATISTIC");
  params.add<std::string>("CHANNEL_PARAM_HEADER", "PARAM");

  StatusPrinter::init(&params);
  StatusPrinter::spawn();

  // Create a radix sort strategy object that gets scratch buffers from the
  // mock user.
  RadixSortStrategy radixSort(false);

  DummyWorker dummyWorker(0, "radixsortbench");

  SimpleMemoryAllocator memoryAllocator;

  // Use a factory here for fidelity
  // 700MB buffer
  const uint64_t BUFFER_SIZE = 700000000;
  const uint64_t ALIGNMENT_MULTIPLE = 512;
  KVPairBufferFactory factory(
    dummyWorker, memoryAllocator, BUFFER_SIZE, ALIGNMENT_MULTIPLE);

  KVPairBuffer* inputBuffer = factory.newInstance();
  KVPairBuffer* outputBuffer = factory.newInstance();

  // Fill the input buffer
  File file(argv[2]);
  file.open(File::WRITE);

  uint64_t readSize = std::min<uint64_t>(BUFFER_SIZE, file.getCurrentSize());
  const uint8_t* appendBuffer = inputBuffer->setupAppend(readSize);

  file.read(const_cast<uint8_t*>(appendBuffer), readSize, 0, 0);

  inputBuffer->commitAppend(appendBuffer, readSize);

  file.close();

  // Sort it 10 times
  for (int i = 0; i < 10; ++i) {
    inputBuffer->resetIterator();
    outputBuffer->setCurrentSize(0);
    outputBuffer->resetIterator();

    uint8_t* scratchBuffer = new (std::nothrow) uint8_t[
      radixSort.getRequiredScratchBufferSize(inputBuffer)];
    radixSort.setScratchBuffer(scratchBuffer);
    radixSort.sort(inputBuffer, outputBuffer);
    delete[] scratchBuffer;
  }

  // Free up memory
  delete inputBuffer;
  delete outputBuffer;

  StatusPrinter::flush();
  StatusPrinter::teardown();

  return 0;
}
