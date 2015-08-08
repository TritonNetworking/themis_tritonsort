#include <fcntl.h>
#include <gsl/gsl_randist.h>
#include <gsl/gsl_rng.h>
#include <limits>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "core/ByteOrder.h"
#include "core/Timer.h"

const uint32_t MAX_URL_SIZE = 255;
const uint32_t WRITE_BUFFER_SIZE = 16 * 1024 * 1024;

inline uint64_t randomUInt64(gsl_rng* rnd) {
  uint64_t bottom = gsl_rng_uniform_int(
    rnd, std::numeric_limits<uint32_t>::max());

  uint64_t top = gsl_rng_uniform_int(
    rnd, std::numeric_limits<uint32_t>::max());

  uint64_t rand = (top << 32) | bottom;
  return rand;
}

void writeBufferToFile(
  int fp, uint8_t* writeBuffer, uint32_t writeBufferOffset) {
  uint64_t bytesToWrite = writeBufferOffset;

  while (bytesToWrite > 0) {
    ssize_t bytesWritten = write(
      fp, writeBuffer + (writeBufferOffset - bytesToWrite), bytesToWrite);

    if (bytesWritten == -1) {
      perror("write()");
      exit(1);
    }

    bytesToWrite -= bytesWritten;
  }
}

int main (int argc, char** argv) {
  if (argc != 4) {
    fprintf(
      stderr, "Usage: %s <output file> <max clicks> <# bytes>\n",
      argv[0]);
    exit(1);
  }

  const char* outputFileName = argv[1];
  uint64_t maxClicks = strtoll(argv[2], NULL, 0);
  uint64_t totalBytes = strtoll(argv[3], NULL, 0);

  uint8_t* writeBuffer = new uint8_t[WRITE_BUFFER_SIZE];
  uint32_t writeBufferOffset = 0;

  int fp = open(outputFileName, O_CREAT | O_TRUNC | O_RDWR, 00644);

  if (fp == -1) {
    perror("open()");
    exit(1);
  }

  // Set up the random number generator
  gsl_rng_env_setup();
  const gsl_rng_type* rngType = gsl_rng_mt19937;
  gsl_rng* rnd = gsl_rng_alloc(rngType);
  gsl_rng_set(rnd, getpid() ^ Timer::posixTimeInMicros());

  uint64_t numBytes = 0;

  // Stop when we hit the total number of desired bytes
  while (numBytes < totalBytes) {
    // Generate a user ID at random
    uint64_t uid = randomUInt64(rnd);

    // Generate a start timestamp at random
    uint64_t startTimestamp = randomUInt64(rnd);

    // Generate a number of clicks at random from 1 to maxClicks inclusive
    uint32_t numClicks = (
      gsl_rng_uniform_int(
        rnd, std::numeric_limits<uint32_t>::max()) % maxClicks) + 1;

    // Tuple's key is uid, value is <timestamp, URL>
    uint64_t timestamp = startTimestamp;

    for (uint64_t i = 0; i < numClicks; i++) {
      // The URL portion of the value can be garbage bytes because we never
      // touch it, but we still have to write it out
      uint32_t urlSize = gsl_rng_uniform_int(rnd, MAX_URL_SIZE - 1) + 1;

      uint32_t keySize = sizeof(uid);
      uint32_t valueSize = sizeof(startTimestamp) + urlSize;

      uint32_t tupleSize = (2 * sizeof(uint32_t)) + keySize + valueSize;

      // If we need to drain the buffer, drain it
      if (writeBufferOffset + tupleSize > WRITE_BUFFER_SIZE) {
        writeBufferToFile(fp, writeBuffer, writeBufferOffset);
        writeBufferOffset = 0;
      }

      // Write key and value headers to the buffer
      memcpy(writeBuffer + writeBufferOffset, &keySize, sizeof(keySize));
      writeBufferOffset += sizeof(keySize);
      memcpy(writeBuffer + writeBufferOffset, &valueSize, sizeof(valueSize));
      writeBufferOffset += sizeof(valueSize);

      // Write the key to the buffer
      memcpy(writeBuffer + writeBufferOffset, &uid, sizeof(uid));
      writeBufferOffset += sizeof(uid);

      uint64_t bigEndianTimestamp = hostToBigEndian64(timestamp);

      // Write the timestamp portion of the value to the buffer
      memcpy(writeBuffer + writeBufferOffset, &bigEndianTimestamp,
             sizeof(bigEndianTimestamp));
      writeBufferOffset += sizeof(bigEndianTimestamp);

      // Add garbage to the value for the URL
      writeBufferOffset += urlSize;

      // Next click will occur one second from now
      timestamp++;

      // We've now written an additional tuple's worth of bytes
      numBytes += tupleSize;
    }
  }

  // If the buffer's still got data in it, drain it
  if (writeBufferOffset > 0) {
    writeBufferToFile(fp, writeBuffer, writeBufferOffset);
  }

  if (fsync(fp) == -1) {
    perror("fsync()");
    exit(1);
  }

  close(fp);



  delete[] writeBuffer;
}
