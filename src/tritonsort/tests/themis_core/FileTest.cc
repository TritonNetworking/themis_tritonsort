#include <fcntl.h>
#include <unistd.h>

#include "core/TritonSortAssert.h"
#include "tests/themis_core/AutoUnlinkFileWrapper.h"
#include "tests/themis_core/FileTest.h"

extern const char* TEST_WRITE_ROOT;

void FileTest::testPreallocate() {
  AutoUnlinkFileWrapper wrapper(
    std::string(TEST_WRITE_ROOT) + "/file_test_prealloc");
  File& file = wrapper.file;

  file.open(File::WRITE, true);

  uint64_t preallocSize = 12800;
  file.preallocate(preallocSize);

  CPPUNIT_ASSERT_MESSAGE(
    "File does not match expected size",
    file.getCurrentSize() == preallocSize);

  file.close();
}

void FileTest::testTruncateOnClose() {
  AutoUnlinkFileWrapper wrapper(
    std::string(TEST_WRITE_ROOT) + "/file_test_trunc");
  File& file = wrapper.file;

  file.open(File::WRITE, true);
  file.preallocate(54370);

  // File should truncate back to zero on close, since we didn't write anything
  // to it
  file.close();

  // The file's size should be zero
  CPPUNIT_ASSERT_MESSAGE(
    "File size was not truncated as expected", file.getCurrentSize() == 0);
}

void FileTest::testOpenNonexistentFileForReading() {
  File file(std::string(TEST_WRITE_ROOT) + "/nonexistent_file");

  CPPUNIT_ASSERT_THROW(file.open(File::READ), AssertionFailedException);
}

void FileTest::testReadFlags() {
  AutoUnlinkFileWrapper wrapper(
    std::string(TEST_WRITE_ROOT) + "/file_test_read_flags");
  File& file = wrapper.file;

  file.open(File::READ, true);
  int flags = fcntl(file.getFileDescriptor(), F_GETFL);
  CPPUNIT_ASSERT_MESSAGE("File not open for read as expected",
                         (flags | O_RDONLY) == flags);
  file.close();
}

void FileTest::testWriteFlags() {
  AutoUnlinkFileWrapper wrapper(
    std::string(TEST_WRITE_ROOT) + "/file_test_write_flags");
  File& file = wrapper.file;

  file.open(File::WRITE, true);
  int flags = fcntl(file.getFileDescriptor(), F_GETFL);
  CPPUNIT_ASSERT_MESSAGE("File not open for write as expected",
                         (flags | O_WRONLY) == flags);
  file.close();
  file.open(File::WRITE);
  file.enableDirectIO();
  flags = fcntl(file.getFileDescriptor(), F_GETFL);
  CPPUNIT_ASSERT_MESSAGE("File not open for write and direct I/O as expected",
                         (flags | O_WRONLY | O_DIRECT) == flags);
  file.disableDirectIO();
  flags = fcntl(file.getFileDescriptor(), F_GETFL);
  CPPUNIT_ASSERT_MESSAGE("File not open for write and as expected",
                         (flags | O_WRONLY) == flags);
  file.close();
}

void FileTest::testClose() {
  AutoUnlinkFileWrapper wrapper(
    std::string(TEST_WRITE_ROOT) + "/file_test_write_flags");
  File& file = wrapper.file;

  file.open(File::WRITE, true);
  CPPUNIT_ASSERT_MESSAGE("File descriptor should have been a non-negative "
                         "number here", file.getFileDescriptor() >= 0);
  file.close();
  CPPUNIT_ASSERT_THROW_MESSAGE(
    "File should complain if you get its descriptor while closed",
    file.getFileDescriptor(), AssertionFailedException);
}
