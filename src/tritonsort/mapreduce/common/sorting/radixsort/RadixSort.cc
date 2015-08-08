#include <limits>

#include "mapreduce/common/KeyValuePair.h"
#include "mapreduce/common/buffers/KVPairBuffer.h"
#include "mapreduce/common/sorting/radixsort/RadixSort.h"

RadixSort::RadixSort(bool _useSecondaryKeys)
  : useSecondaryKeys(_useSecondaryKeys),
    scratchBuffer(NULL),
    inputBuffer(NULL),
    outputBuffer(NULL),
    maxKeySize(0),
    offsetSize(UINT64_T),
    inputBuckets(0),
    bucketTables((BucketTable[2])
                 { BucketTable(histogram), BucketTable(histogram) }),
    logger("RadixSort") {
  sortTimeStatID = logger.registerStat("sort_time");
  distributeFromBufferStatID = logger.registerStat("distribute_buffer_time");
  distributeFromBucketStatID = logger.registerStat("distribute_bucket_time");
  copyOutputTimeStatID = logger.registerStat("copy_output_time");
}

void RadixSort::setScratchBuffer(uint8_t* scratchBuffer) {
  this->scratchBuffer = scratchBuffer;
}

uint64_t RadixSort::getRequiredScratchBufferSize(KVPairBuffer* buffer) const {
  return getScratchBufferEntrySize(buffer) * buffer->getNumTuples() * 2;
}

void RadixSort::sort(KVPairBuffer* inputBuffer, KVPairBuffer* outputBuffer) {
  this->inputBuffer = inputBuffer;
  this->outputBuffer = outputBuffer;

  setup();

  sortTimer.start();

  // Step 1. Read keys from the input buffer into buckets.
  distributeKeysFromInputBuffer();

  // Steps 2, 3. Repeatedly re-bucket keys between input and output buckets
  // until all bytes of the key have been used for bucketing.
  while (distributeKeysFromBuckets());

  // Step 4. Copy sorted tuples to the output buffer using offset tags
  // associated with sorted keys.
  copyOutputToBuffer();

  sortTimer.stop();
  logger.add(sortTimeStatID, sortTimer.getElapsed());

  teardown();
}

void RadixSort::setup() {
  ABORT_IF(inputBuffer == NULL, "Must set non-NULL input buffer");
  ABORT_IF(outputBuffer == NULL, "Must set non-NULL output buffer");

  ASSERT(inputBuffer->getCurrentSize() <= outputBuffer->getCapacity(), "Output "
         "buffer (capacity %llu) must be at least as large as input buffer "
         "(size %llu) to sort.", outputBuffer->getCapacity(),
         inputBuffer->getCurrentSize());

  // Set the number of tuples to be bucketed
  uint64_t numTuples = inputBuffer->getNumTuples();
  bucketTables[0].setNumEntries(numTuples);
  bucketTables[1].setNumEntries(numTuples);

  // Set the maximum key size
  maxKeySize = inputBuffer->getMaxKeyLength();

  if (useSecondaryKeys) {
    maxKeySize += sizeof(uint64_t);
  }

  bucketTables[0].setMaxKeySize(maxKeySize);
  bucketTables[1].setMaxKeySize(maxKeySize);

  // Set the offset size
  offsetSize = getOffsetSize(inputBuffer);
  bucketTables[0].setOffsetSize(offsetSize);
  bucketTables[1].setOffsetSize(offsetSize);

  // Make sure that the caller provided a scratch buffer
  ABORT_IF(scratchBuffer == NULL, "Scratch buffer wasn't set before sorting");

  // Give the first half of the buffer to table 0, and the second half to
  // table 1.
  uint64_t entrySize = getScratchBufferEntrySize(inputBuffer);
  bucketTables[0].setBuffer(scratchBuffer);
  bucketTables[1].setBuffer(scratchBuffer + (numTuples * entrySize));
}

void RadixSort::teardown() {
  // Make sure we don't re-use a stale scratch buffer
  scratchBuffer = NULL;
}

void RadixSort::distributeKeysFromInputBuffer() {
  timer.start();

  // Use the initial histogram to initialize bucket sizes
  deriveInitialHistogram();
  BucketTable& outputBuckets = getOutputBuckets();
  outputBuckets.resetBuckets();

  // If maxKeySize is positive, start at the last byte of the key. Otherwise,
  // set keyOffset to 0 to skip subsequent bucketing steps.
  keyOffset = maxKeySize > 0 ? maxKeySize - 1 : 0;
  outputBuckets.setKeyOffset(keyOffset);

  // Read input buffer into buckets
  // Iterate manually for extra speed since this is the compute bound part of
  // the system.
  uint64_t offset = 0;
  const uint8_t* inputBufferStart = inputBuffer->getRawBuffer();
  uint8_t* buffer = const_cast<uint8_t*>(inputBufferStart);
  uint8_t* end = buffer + inputBuffer->getCurrentSize();

  while (buffer < end) {
    // Compute the offset into the input buffer
    offset = buffer - inputBufferStart;

    // Write the key entry
    uint32_t keyLength = KeyValuePair::keyLength(buffer);

    if (useSecondaryKeys) {
      keyLength += sizeof(uint64_t);
    }

    outputBuckets.addEntryFromBuffer(
      KeyValuePair::key(buffer), keyLength, offset);

    // Advance to the next tuple
    buffer = KeyValuePair::nextTuple(buffer);
  }

  timer.stop();
  logger.add(distributeFromBufferStatID, timer.getElapsed());
}

bool RadixSort::distributeKeysFromBuckets() {
  timer.start();

  // Move 1 byte up the key
  if (keyOffset == 0) {
    return false;
  }
  --keyOffset;

  // Swap inputs and outputs
  swapBucketTables();
  BucketTable& inputBuckets = getInputBuckets();
  BucketTable& outputBuckets = getOutputBuckets();

  // Setup output buckets
  outputBuckets.resetBuckets();
  outputBuckets.setKeyOffset(keyOffset);

  // Re-bucket entries from input to output
  uint8_t* entry = NULL;
  if (keyOffset > 0) {
    while (inputBuckets.readNextEntry(entry)) {
      outputBuckets.addEntry(entry);
    }
  } else {
    while (inputBuckets.readNextEntry(entry)) {
      outputBuckets.addEntryForLastIteration(entry);
    }
  }

  timer.stop();
  logger.add(distributeFromBucketStatID, timer.getElapsed());

  return true;
}

void RadixSort::copyOutputToBuffer() {
  timer.start();

  // Get a raw pointer to the output buffer
  const uint8_t* appendPointer =
    outputBuffer->setupAppend(inputBuffer->getCurrentSize());
  uint8_t* rawOutputBuffer = const_cast<uint8_t*>(appendPointer);

  BucketTable& outputBuckets = getOutputBuckets();

  // Copy whole tuples from the output buckets to the output buffer
  uint8_t* entry = NULL;
  uint64_t bytesCopied = 0;
  uint64_t offset = 0;
  uint64_t tupleSize = 0;
  uint8_t* inputBufferStart = const_cast<uint8_t*>(inputBuffer->getRawBuffer());

  while (outputBuckets.readNextEntry(entry)) {
    // entry points to a region of RadixSort memory whose first maxKeySize
    // bytes is the key, and the following (2|4,8) bytes
    // is an offset from the beginning of the input buffer.
    //
    // Skip the key and read the offset.
    if (offsetSize == UINT32_T) {
      // Offset is 4 bytes
      offset = *(reinterpret_cast<uint32_t*>(entry + maxKeySize));
    } else if(offsetSize == UINT16_T) {
      // Offset is 2 bytes
      offset = *(reinterpret_cast<uint16_t*>(entry + maxKeySize));
    } else {
      // Offset is the full 8 bytes
      offset = *(reinterpret_cast<uint64_t*>(entry + maxKeySize));
    }

    // Locate the original tuple and get its size
    tupleSize = KeyValuePair::tupleSize(inputBufferStart + offset);

    // Copy the original tuple to the output buffer
    rawOutputBuffer = static_cast<uint8_t*>(
      mempcpy(rawOutputBuffer, inputBufferStart + offset, tupleSize));
    bytesCopied += tupleSize;
  }

  // Commit the append to the output KVPairBuffer
  outputBuffer->commitAppend(appendPointer, bytesCopied);

  timer.stop();
  logger.add(copyOutputTimeStatID, timer.getElapsed());
}

void RadixSort::deriveInitialHistogram() {
  histogram.reset();

  // Make one pass over the input data to create the initial histogram for the
  // first pass of bucketing.
  // Iterate manually for extra speed since this is the compute bound part of
  // the system
  uint8_t lastByte = 0;
  uint8_t* buffer = const_cast<uint8_t*>(inputBuffer->getRawBuffer());
  uint8_t* end = buffer + inputBuffer->getCurrentSize();

  while (buffer < end) {
    // Increment the histogram
    uint32_t keyLength = KeyValuePair::keyLength(buffer);

    if (useSecondaryKeys) {
      keyLength += sizeof(uint64_t);
    }

    if (maxKeySize > 0 && keyLength == maxKeySize) {
      lastByte = KeyValuePair::key(buffer)[maxKeySize - 1];
    } else {
      lastByte = 0;
    }
    histogram.increment(lastByte);

    // Advance to the next tuple
    buffer = KeyValuePair::nextTuple(buffer);
  }
}
