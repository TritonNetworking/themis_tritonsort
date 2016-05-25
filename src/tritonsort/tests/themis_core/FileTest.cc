#include <fcntl.h>
#include <unistd.h>

#include "core/TritonSortAssert.h"
#include "tests/themis_core/AutoUnlinkFileWrapper.h"
#include "tests/themis_core/FileTest.h"

extern const char* TEST_WRITE_ROOT;

TEST_F(FileTest, testPreallocate) {
  AutoUnlinkFileWrapper wrapper(
    std::string(TEST_WRITE_ROOT) + "/file_test_prealloc");
  File& file = wrapper.file;

  file.open(File::WRITE, true);

  uint64_t preallocSize = 12800;
  file.preallocate(preallocSize);

  EXPECT_TRUE(file.getCurrentSize() == preallocSize);

  file.close();
}

TEST_F(FileTest, testTruncateOnClose) {
  AutoUnlinkFileWrapper wrapper(
    std::string(TEST_WRITE_ROOT) + "/file_test_trunc");
  File& file = wrapper.file;

  file.open(File::WRITE, true);
  file.preallocate(54370);

  // File should truncate back to zero on close, since we didn't write anything
  // to it
  file.close();

  // The file's size should be zero
  EXPECT_TRUE(file.getCurrentSize() == 0);
}

TEST_F(FileTest, testOpenNonexistentFileForReading) {
  File file(std::string(TEST_WRITE_ROOT) + "/nonexistent_file");

  ASSERT_THROW(file.open(File::READ), AssertionFailedException);
}

TEST_F(FileTest, testReadFlags) {
  AutoUnlinkFileWrapper wrapper(
    std::string(TEST_WRITE_ROOT) + "/file_test_read_flags");
  File& file = wrapper.file;

  file.open(File::READ, true);
  int flags = fcntl(file.getFileDescriptor(), F_GETFL);
  EXPECT_TRUE((flags | O_RDONLY) == flags);
  file.close();
}

TEST_F(FileTest, testWriteFlags) {
  AutoUnlinkFileWrapper wrapper(
    std::string(TEST_WRITE_ROOT) + "/file_test_write_flags");
  File& file = wrapper.file;

  file.open(File::WRITE, true);
  int flags = fcntl(file.getFileDescriptor(), F_GETFL);
  EXPECT_TRUE((flags | O_WRONLY) == flags);
  file.close();
  file.open(File::WRITE);
  file.enableDirectIO();
  flags = fcntl(file.getFileDescriptor(), F_GETFL);
  EXPECT_TRUE((flags | O_WRONLY | O_DIRECT) == flags);
  file.disableDirectIO();
  flags = fcntl(file.getFileDescriptor(), F_GETFL);
  EXPECT_TRUE((flags | O_WRONLY) == flags);
  file.close();
}

TEST_F(FileTest, testClose) {
  AutoUnlinkFileWrapper wrapper(
    std::string(TEST_WRITE_ROOT) + "/file_test_write_flags");
  File& file = wrapper.file;

  file.open(File::WRITE, true);
  EXPECT_TRUE(file.getFileDescriptor() >= 0);
  file.close();
  ASSERT_THROW(file.getFileDescriptor(), AssertionFailedException);
}
