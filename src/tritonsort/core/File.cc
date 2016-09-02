#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "core/File.h"
#include "core/MemoryUtils.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"

File::File(const std::string& _filename)
  : filename(_filename),
    currentMode(CLOSED),
    fileDescriptor(-1),
    directIO(false),
    preallocated(false),
    filePosition(0),
    alignedBytesRead(0),
    alignedBytesWritten(0) {
}

void File::open(AccessMode mode, bool create) {
  int flags = 0;

  switch (mode) {
  case READ:
  case READ_LIBAIO:
  case READ_POSIXAIO:
    flags = O_RDONLY;
    break;
  case WRITE:
  case WRITE_LIBAIO:
  case WRITE_POSIXAIO:
    flags = O_WRONLY;
    break;
  case READ_WRITE:
    flags = O_RDWR;
    break;
  default:
    ABORT("Opening in mode %d unsupported", mode);
    break;
  }

  if (create) {
    flags = flags | O_CREAT | O_TRUNC;

    fileDescriptor = ::open(filename.c_str(), flags, 00644);
  } else {
    fileDescriptor = ::open(filename.c_str(), flags);
  }

  ABORT_IF(fileDescriptor == -1, "open('%s') failed with error %d: %s",
           filename.c_str(), errno, strerror(errno));

  currentMode = mode;
}

void File::enableDirectIO() {
  ABORT_IF(fileDescriptor == -1, "File must be open to enable direct IO.");

  // Get current flags
  int flags = fcntl(fileDescriptor, F_GETFL);
  ABORT_IF(flags == -1, "fcntl(F_GETFL) failed with error %d: %s",
           errno, strerror(errno));

  // Turn on direct IO
  flags |= O_DIRECT;
  ABORT_IF(fcntl(fileDescriptor, F_SETFL, flags) == -1,
           "fcntl(F_SETFL) failed with error %d: %s", errno, strerror(errno));
  directIO = true;
}

void File::disableDirectIO() {
  ABORT_IF(fileDescriptor == -1, "File must be open to disable direct IO.");

  // Get current flags
  int flags = fcntl(fileDescriptor, F_GETFL);
  ABORT_IF(flags == -1, "fcntl(F_GETFL) failed with error %d: %s",
           errno, strerror(errno));

  // Turn off direct IO
  flags &= ~O_DIRECT;
  ABORT_IF(fcntl(fileDescriptor, F_SETFL, flags) == -1,
           "fcntl(F_SETFL) failed with error %d: %s", errno, strerror(errno));
  directIO = false;
}

bool File::directIOEnabled() const {
  return directIO;
}

bool File::isOpened() const {
  return fileDescriptor != -1;
}

int File::getFileDescriptor() const {
  ABORT_IF(fileDescriptor == -1, "Can't retrieve file descriptor if the file "
           "(%s) hasn't been opened yet", filename.c_str());
  return fileDescriptor;
}

void File::preallocate(uint64_t size) {
  ABORT_IF(fileDescriptor == -1, "File must be open to preallocate space");
  TRITONSORT_ASSERT(currentMode == WRITE || currentMode == READ_WRITE ||
         currentMode == WRITE_POSIXAIO || currentMode == WRITE_LIBAIO,
         "File must be open for writing to be preallocated");

  ABORT_IF(posix_fallocate(fileDescriptor, 0, size) == -1, "posix_fallocate() "
           "failed with error %d: %s", errno, strerror(errno));
  preallocated = true;
}

void File::sync() const {
  if (currentMode == WRITE_POSIXAIO) {
    // Use asynchronous fsync
    struct aiocb controlBlock;
    memset(&controlBlock, 0, sizeof(controlBlock));
    controlBlock.aio_fildes = fileDescriptor;
    // We'll poll for this request in this function.
    controlBlock.aio_sigevent.sigev_notify = SIGEV_NONE;

    // Issue the aio fsync call.
    aiocb* controlBlockPointer = &controlBlock;
    int status = aio_fsync(O_SYNC, &controlBlock);
    ABORT_IF(status != 0, "aio_fsync() failed %d : %s", errno, strerror(errno));

    // Wait for it to complete.
    status = aio_suspend(&controlBlockPointer, 1, NULL);
    ABORT_IF(status != 0,
    "aio_suspend() failed %d : %s", errno, strerror(errno));
  } else if (currentMode == WRITE || currentMode == READ_WRITE ||
             currentMode == WRITE_LIBAIO) {
    // Use synchronous fsync even if we're writing with libaio since io_fsync()
    // isn't implemented.
    int status = fsync(fileDescriptor);

    ABORT_IF(status == -1, "fsync(%s) failed with error %d: %s",
             filename.c_str(), errno, strerror(errno));
  }
}

void File::read(
  uint8_t* buffer, uint64_t size, uint64_t maxReadSize,
  uint64_t alignmentSize) {
  ABORT_IF(fileDescriptor == -1, "Can't read from the file "
           "(%s) if it hasn't been opened yet", filename.c_str());
  TRITONSORT_ASSERT(currentMode == READ || currentMode == READ_WRITE,
         "File not open for reading.");

  if (directIO && alignmentSize > 0 && size % alignmentSize != 0) {
    // This read is unaligned, read as much as we can with direct IO and then
    // disable it.
    uint64_t alignedSize = size - (size % alignmentSize);
    uint64_t bytesRead = blockingRead(
      fileDescriptor, buffer, alignedSize, maxReadSize, alignmentSize,
      filename.c_str());
    alignedBytesRead += bytesRead;
    disableDirectIO();

    ABORT_IF(bytesRead != alignedSize,
             "Encountered early EOF: tried to read %llu bytes aligned but read "
             "%llu", alignedSize, bytesRead);

    // Adjust the buffer and read size for the unaligned portion
    buffer += alignedSize;
    size -= alignedSize;
  }

  if (!directIO) {
    // Don't do alignment checks if direct IO is turned off.
    alignmentSize = 0;
  }

  uint64_t bytesRead = blockingRead(
    fileDescriptor, buffer, size, maxReadSize, alignmentSize,
    filename.c_str());

    ABORT_IF(bytesRead != size,
             "Encountered early EOF: tried to read %llu bytes but read %llu",
             size, bytesRead);

  if (directIO) {
    alignedBytesRead += bytesRead;
  }
}

void File::preparePosixAIORead(
  uint8_t* buffer, uint64_t size, uint64_t maxIOSize) {
  TRITONSORT_ASSERT(currentMode == READ_POSIXAIO,
         "File not open for reading with Posix AIO.");
  prepareAIO(buffer, size, maxIOSize);
}

void File::prepareLibAIORead(
  uint8_t* buffer, uint64_t size, uint64_t maxIOSize) {
  TRITONSORT_ASSERT(currentMode == READ_LIBAIO,
         "File not open for reading with libaio.");
  prepareAIO(buffer, size, maxIOSize);
}

bool File::submitNextPosixAIORead(
  uint8_t* buffer, uint64_t alignmentSize, struct aiocb*& controlBlock,
  bool& disableDirectIORequired) {
  TRITONSORT_ASSERT(currentMode == READ_POSIXAIO,
         "File not open for reading with Posix AIO.");
  return submitNextPosixAIO(
    buffer, alignmentSize, controlBlock, disableDirectIORequired);
}

bool File::submitNextLibAIORead(
  uint8_t* buffer, uint64_t alignmentSize, io_context_t* context,
  struct iocb*& controlBlock, bool& disableDirectIORequired) {
  TRITONSORT_ASSERT(currentMode == READ_LIBAIO,
         "File not open for reading with libaio.");
  return submitNextLibAIO(
    buffer, alignmentSize, context, controlBlock, disableDirectIORequired);
}

void File::write(
  const uint8_t* buffer, uint64_t size, uint64_t maxWriteSize,
  uint64_t alignmentSize) {
  ABORT_IF(fileDescriptor == -1, "Can't write to the file (%s) if it hasn't "
           "been opened yet", filename.c_str());
  TRITONSORT_ASSERT(currentMode == WRITE || currentMode == READ_WRITE,
         "File not open for writing.");

  if (directIO && alignmentSize > 0 && size % alignmentSize != 0) {
    // This write is unaligned, write as much as we can with direct IO and then
    // disable it.
    uint64_t alignedSize = size - (size % alignmentSize);
    blockingWrite(
      fileDescriptor, buffer, alignedSize, maxWriteSize, alignmentSize,
      filename.c_str());
    alignedBytesWritten += alignedSize;
    disableDirectIO();

    // Adjust the buffer and write size for the unaligned portion
    buffer += alignedSize;
    size -= alignedSize;
  }

  if (!directIO) {
    // Don't do alignment checks if direct IO is turned off.
    alignmentSize = 0;
  }

  blockingWrite(
    fileDescriptor, buffer, size, maxWriteSize, alignmentSize,
    filename.c_str());
  if (directIO) {
    alignedBytesWritten += size;
  }
}

void File::write(const std::string& str) {
  write(reinterpret_cast<const uint8_t*>(str.c_str()), str.size(), 0, 0);
}

void File::preparePosixAIOWrite(
  uint8_t* buffer, uint64_t size, uint64_t maxIOSize) {
  TRITONSORT_ASSERT(currentMode == WRITE_POSIXAIO,
         "File not open for writing with Posix AIO.");
  prepareAIO(buffer, size, maxIOSize);
}

void File::prepareLibAIOWrite(
  uint8_t* buffer, uint64_t size, uint64_t maxIOSize) {
  TRITONSORT_ASSERT(currentMode == WRITE_LIBAIO,
         "File not open for writing with libaio.");
  prepareAIO(buffer, size, maxIOSize);
}

bool File::submitNextPosixAIOWrite(
  uint8_t* buffer, uint64_t alignmentSize, struct aiocb*& controlBlock,
  bool& disableDirectIORequired) {
  TRITONSORT_ASSERT(currentMode == WRITE_POSIXAIO,
         "File not open for writing with Posix AIO.");
  return submitNextPosixAIO(
    buffer, alignmentSize, controlBlock, disableDirectIORequired);
}

bool File::submitNextLibAIOWrite(
  uint8_t* buffer, uint64_t alignmentSize, io_context_t* context,
  struct iocb*& controlBlock, bool& disableDirectIORequired) {
  TRITONSORT_ASSERT(currentMode == WRITE_LIBAIO,
         "File not open for writing with libaio.");
  return submitNextLibAIO(
    buffer, alignmentSize, context, controlBlock, disableDirectIORequired);
}

uint64_t File::seek(int64_t offset, SeekMode seekMode) {
  ABORT_IF(fileDescriptor == -1, "Can't seek within the file (%s) if it hasn't "
           "been opened yet", filename.c_str());

  if (currentMode == READ_POSIXAIO || currentMode == READ_LIBAIO ||
      currentMode == WRITE_POSIXAIO || currentMode == WRITE_LIBAIO) {
    // Asynchronous I/O doesn't support lseek(), so instead just set the file
    // position.
    if (seekMode == FROM_BEGINNING) {
      filePosition = offset;
    } else if (seekMode == FROM_END) {
      // offset is assumed to be negative.
      filePosition = getCurrentSize() + offset;
    } else {
      filePosition += offset;
    }

    return filePosition;
  } else {
    // Use lseek since we're using synchronous I/O.
    int whence;
    if (seekMode == FROM_BEGINNING) {
      whence = SEEK_SET;
    } else if (seekMode == FROM_END) {
      whence = SEEK_END;
    } else {
      whence = SEEK_CUR;
    }

    off64_t newOffset = lseek64(fileDescriptor, offset, whence);
    ABORT_IF(newOffset == -1, "lseek64() failed "
             "with error %d: %s", errno, strerror(errno));
    return static_cast<uint64_t>(newOffset);
  }
}

void File::close() {
  // Make sure any dirty pages get flushed to disk.
  sync();

  // If the file was preallocated, truncate file size to free unused space.
  if (preallocated) {
    int64_t cursorPosition = 0;
    if (currentMode == WRITE || currentMode == READ_WRITE) {
      // Truncate to current file cursor.
      cursorPosition = lseek(fileDescriptor, 0, SEEK_CUR);
    } else if (currentMode == WRITE_POSIXAIO || currentMode == WRITE_LIBAIO) {
      // Current file cursor is undefined for asynchronous writes, so truncate
      // to the number of bytes written.
      cursorPosition = filePosition;
    } else {
      ABORT("Invalid mode. Must be writable.");
    }

    ABORT_IF(cursorPosition == -1, "lseek() failed with error %d: %s", errno,
             strerror(errno));

    ABORT_IF(ftruncate(fileDescriptor, cursorPosition) == -1, "ftruncate() "
             "failed with error %d: %s", errno, strerror(errno));
  }

  // Make sure we sync the metadata from the ftruncate() command
  sync();

  // Inform the kernel to free cached pages associated with this file
  int status = posix_fadvise(fileDescriptor, 0, 0, POSIX_FADV_DONTNEED);
  ABORT_IF(status != 0, "posix_fadvise(%s) failed with error %d",
           filename.c_str(), status);

  status = ::close(fileDescriptor);

  ABORT_IF(status == -1, "close(%s) failed with error %d: %s",
           filename.c_str(), errno, strerror(errno));
  fileDescriptor = -1;
  currentMode = CLOSED;
}

void File::unlink() {
  ABORT_IF(fileDescriptor != -1,
           "Cannot unlink() an open file. Call close() first.");
  int status = ::unlink(filename.c_str());
  ABORT_IF(status == -1, "unlink(%s) failed with error %d: %s",
           filename.c_str(), errno, strerror(errno));
}

uint64_t File::getCurrentSize() const {
  if (fileDescriptor != -1) {
    return getFileSize(fileDescriptor);
  } else {
    return getFileSize(filename.c_str());
  }
}

const std::string& File::getFilename() const {
  return filename;
}

uint64_t File::getAlignedBytesRead() const {
  return alignedBytesRead;
}

uint64_t File::getAlignedBytesWritten() const {
  return alignedBytesWritten;
}

void File::rename(const std::string& newName) {
  ::rename(filename.c_str(), newName.c_str());
}

void File::prepareAIO(uint8_t* buffer, uint64_t size, uint64_t maxIOSize) {
  ABORT_IF(fileDescriptor == -1, "Can't perform AIO on file (%s) if it hasn't "
           "been opened yet", filename.c_str());
  TRITONSORT_ASSERT(currentMode == READ_POSIXAIO || currentMode == READ_LIBAIO ||
         currentMode == WRITE_POSIXAIO || currentMode == WRITE_LIBAIO,
         "File not open for asynchronous I/O.");

  uint64_t numIOs = size / maxIOSize;
  if (size % maxIOSize) {
    // The last I/O is partial.
    numIOs++;
  }

  uint64_t offset = 0;
  uint64_t remainingBytes = size;
  for (uint64_t i = 0; i < numIOs; i++) {
    uint64_t ioSize = std::min<uint64_t>(remainingBytes, maxIOSize);

    if (currentMode == READ_POSIXAIO || currentMode == WRITE_POSIXAIO) {
      // We're using Posix AIO.
      struct aiocb* controlBlock = new (themis::memcheck) struct aiocb;
      memset(controlBlock, 0, sizeof(struct aiocb));

      // Prepare the I/O to be issued later.
      controlBlock->aio_fildes = fileDescriptor;
      controlBlock->aio_offset = filePosition + offset;
      controlBlock->aio_buf = buffer + offset;
      controlBlock->aio_nbytes = ioSize;
      // Issue this request with the highest priority.
      controlBlock->aio_reqprio = sysconf(_SC_AIO_PRIO_DELTA_MAX);
      // Use polling rather than a signal event to detect completion.
      controlBlock->aio_sigevent.sigev_notify = SIGEV_NONE;

      // Store the control block for later.
      posixAIOControlBlockMap[buffer].push_back(controlBlock);
    } else {
      // We're using libaio.
      // Create a new AIO control block for this I/O.
      struct iocb* controlBlock = new (themis::memcheck) struct iocb;
      memset(controlBlock, 0, sizeof(struct iocb));

      // Prepare the I/O to be issued later.
      if (currentMode == READ_LIBAIO) {
        io_prep_pread(
          controlBlock, fileDescriptor, buffer + offset, ioSize,
          filePosition + offset);
      } else {
        io_prep_pwrite(
          controlBlock, fileDescriptor, buffer + offset, ioSize,
          filePosition + offset);
      }

      // Store the control block for later.
      libAIOControlBlockMap[buffer].push_back(controlBlock);
    }

    // Advance the offset.
    offset += ioSize;
    remainingBytes -= ioSize;
  }

  // Sanity check:
  TRITONSORT_ASSERT(remainingBytes == 0, "Somehow we still have %llu bytes left after "
         "preparing all AIO control blocks", remainingBytes);

  // Update file position marker.
  filePosition += size;
}

bool File::submitNextPosixAIO(
  uint8_t* buffer, uint64_t alignmentSize, struct aiocb*& controlBlock,
  bool& disableDirectIORequired) {
  TRITONSORT_ASSERT(currentMode == READ_POSIXAIO || currentMode == WRITE_POSIXAIO,
         "File not open with posix AIO.");

  PosixAIOControlBlockMap::iterator iter = posixAIOControlBlockMap.find(buffer);
  if (iter == posixAIOControlBlockMap.end()) {
    // We already issued all I/Os for this buffer.
    return false;
  }
  controlBlock = iter->second.front();
  uint64_t ioSize = controlBlock->aio_nbytes;

  if (directIO && alignmentSize > 0 && ioSize % alignmentSize != 0) {
    // I/O is unaligned, so we need to disable O_DIRECT.
    disableDirectIORequired = true;
    return false;
  }

  // Issue the I/O asynchronously.
  if (currentMode == READ_POSIXAIO) {
    int status = aio_read(controlBlock);
    ABORT_IF(status != 0,
             "aio_read() failed with error %d: %s", errno, strerror(errno));
    if (directIO) {
      alignedBytesRead += ioSize;
    }
  } else {
    int status = aio_write(controlBlock);
    ABORT_IF(status != 0,
             "aio_write() failed with error %d: %s", errno, strerror(errno));
    if (directIO) {
      alignedBytesWritten += ioSize;
    }
  }

  // Remove the control block from the list, and possibly remove the buffer from
  // the map.
  iter->second.pop_front();
  if (iter->second.empty()) {
    // No more control blocks left to submit, so remove this buffer from the map
    // so that subsequent calls return false.
    posixAIOControlBlockMap.erase(iter);
  }

  return true;
}

bool File::submitNextLibAIO(
  uint8_t* buffer, uint64_t alignmentSize, io_context_t* context,
  struct iocb*& controlBlock, bool& disableDirectIORequired) {
  TRITONSORT_ASSERT(currentMode == READ_LIBAIO || currentMode == WRITE_LIBAIO,
         "File not open with libaio.");

  LibAIOControlBlockMap::iterator iter = libAIOControlBlockMap.find(buffer);
  if (iter == libAIOControlBlockMap.end()) {
    // We already issued all I/Os for this buffer.
    return false;
  }
  controlBlock = iter->second.front();
  uint64_t ioSize = controlBlock->u.c.nbytes;

  if (directIO && alignmentSize > 0 && ioSize % alignmentSize != 0) {
    // I/O is unaligned, so we need to disable O_DIRECT.
    disableDirectIORequired = true;
    return false;
  }

  // Issue the I/O asynchronously within the context provided by the user.
  int status = io_submit(*context, 1, &controlBlock);
  ABORT_IF(status != 1, "io_submit() failed with error code %d", status);
  if (directIO) {
    if (currentMode == READ_LIBAIO) {
      alignedBytesRead += ioSize;
    } else {
      alignedBytesWritten += ioSize;
    }
  }

  // Remove the control block from the list, and possibly remove the buffer from
  // the map.
  iter->second.pop_front();
  if (iter->second.empty()) {
    // No more control blocks left to submit, so remove this buffer from the map
    // so that subsequent calls return false.
    libAIOControlBlockMap.erase(iter);
  }

  return true;
}
