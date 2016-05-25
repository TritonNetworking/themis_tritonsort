#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "BlockingReadTest.h"
#include "common/AlignmentUtils.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"

extern const char* TEST_WRITE_ROOT;

BlockingReadTest::BlockingReadTest() {
  memset(nonAlignedFilename, 0, MAX_FILENAME_SIZE);
  sprintf(nonAlignedFilename, "%s/nonalignedreadtestfile", TEST_WRITE_ROOT);
}

void BlockingReadTest::TearDown() {
  if (fileExists(nonAlignedFilename)) {
    unlink(nonAlignedFilename);
  }
}

TEST_F(BlockingReadTest, testReadWithNonAlignedSize) {
  // Create an unaligned file.
  uint64_t alignment = 512;
  uint64_t fileSize = (alignment * 1000) - 1;

  uint8_t* writeBuffer = new uint8_t[fileSize];
  for (uint64_t i = 0; i < fileSize; i++) {
    writeBuffer[i] = i;
  }

  int fp = open(nonAlignedFilename, O_WRONLY | O_CREAT | O_TRUNC, 00644);

  if (fp == -1) {
    FAIL() << "Initial open() failed";
  }

  if (write(fp, writeBuffer, fileSize) == -1) {
    FAIL() << "Initial write() failed";
  }

  close(fp);

  // Open file without O_DIRECT and read the whole thing.
  fp = open(nonAlignedFilename, O_RDONLY);
  if (fp == -1) {
    FAIL() << "open() for reading failed";
  }

  uint64_t maxReadSize = 100 * alignment;
  uint8_t* readBuffer = new uint8_t[fileSize];
  memset(readBuffer, 0, fileSize);
  ASSERT_NO_THROW(
    blockingRead(
      fp, readBuffer, fileSize, maxReadSize, 0, nonAlignedFilename));

  // Verify the read buffer's contents.
  for (uint64_t i = 0; i < fileSize; i++) {
    EXPECT_EQ(writeBuffer[i], readBuffer[i]);
  }

  close(fp);

  // Now open with O_DIRECT and read the whole thing.
  fp = open(nonAlignedFilename, O_RDONLY | O_DIRECT);
  if (fp == -1) {
    FAIL() << "open() for reading failed";
  }

  // Now blockingRead should fail because the file is not aligned.
  ASSERT_THROW(
    blockingRead(
      fp, readBuffer, fileSize, maxReadSize, alignment, nonAlignedFilename),
    AssertionFailedException);

  close(fp);

  // Finally open with O_DIRECT and read only an aligned number of bytes.
  fp = open(nonAlignedFilename, O_RDONLY | O_DIRECT);
  if (fp == -1) {
    FAIL() << "open() for reading failed";
  }

  // Aligned blockingRead call should succeed.
  memset(readBuffer, 0, fileSize);
  uint64_t readSize = fileSize - (fileSize % alignment);
  ASSERT_NO_THROW(
    blockingRead(
      fp, readBuffer, readSize, maxReadSize, alignment, nonAlignedFilename));

  // Verify the read buffer's contents.
  for (uint64_t i = 0; i < fileSize; i++) {
    if (i < readSize) {
      EXPECT_EQ(writeBuffer[i], readBuffer[i]);
    } else {
      EXPECT_EQ(static_cast<uint8_t>(0), readBuffer[i]);
    }
  }

  // Even if you try to read the rest of the file without alignment checks, the
  // fact that O_DIRECT is on should trigger an abort.
  ASSERT_THROW(
    blockingRead(
      fp, readBuffer + readSize, fileSize - readSize, maxReadSize, 0,
      nonAlignedFilename),
    AssertionFailedException);

  // But if we turn O_DIRECT off then things should work fine.
  if (fcntl(fp, F_SETFL, O_WRONLY) == -1) {
    FAIL() << "fcntl() failed";
  }
  ASSERT_NO_THROW(
    blockingRead(
      fp, readBuffer + readSize, fileSize - readSize, maxReadSize, 0,
      nonAlignedFilename));

  // Verify the read buffer's contents.
  for (uint64_t i = 0; i < fileSize; i++) {
      EXPECT_EQ(writeBuffer[i], readBuffer[i]);
  }

  close(fp);
  delete[] readBuffer;
  delete[] writeBuffer;
}
