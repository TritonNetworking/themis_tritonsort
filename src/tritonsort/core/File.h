#ifndef TRITONSORT_FILE_H
#define TRITONSORT_FILE_H

#include <aio.h>
#include <libaio.h>
#include <list>
#include <map>
#include <string>

#include "Resource.h"

/**
   File is a resource container for standard file descriptors that supports
   opening and closing and the retrieval of file name and file size.
 */
class File : public Resource {
public:
  /// Access modes that the file can be opened in
  enum AccessMode {
    READ, /** open for reading */
    READ_LIBAIO, /** open for reading asynchronously with native AIO */
    READ_POSIXAIO, /** open for reading asynchronously with native AIO */
    WRITE, /** open for writing */
    WRITE_LIBAIO, /** open for writing asynchronously with native AIO */
    WRITE_POSIXAIO, /** open for writing asynchronously with posix AIO */
    READ_WRITE, /** open for reading and writing */
    CLOSED /** file isn't currently open in any mode */
  };

  enum SeekMode {
    FROM_BEGINNING,
    FROM_END,
    FROM_CURRENT
  };

  /// Constructor
  /**
     \param filename the path to the file
   */
  File(const std::string& filename);

  /// Open the file
  /**
     \param mode the access mode in which to open the file

     \param create if true, create the file if it doesn't exist
   */
  void open(AccessMode mode, bool create = false);

  /// Turn on O_DIRECT
  void enableDirectIO();

  /// Turn off O_DIRECT
  void disableDirectIO();

  /// Is O_DIRECT enabled?
  /**
     \return true if O_DIRECT is enabled, false otherwise
   */
  bool directIOEnabled() const;

  /// Is the file opened?
  /**
     \return true if the file is opened, false if not
   */
  bool isOpened() const;

  /**
     Read from the file into a buffer. Aborts if EOF is found before all bytes
     have been read.

     \param buffer the buffer into which to read

     \param size the number of bytes to read

     \param maxReadSize the maximum size of a single read() syscall

     \param alignmentSize how reads should be aligned
   */
  void read(
    uint8_t* buffer, uint64_t size, uint64_t maxReadSize = 0,
    uint64_t alignmentSize = 0);

  /// Prepare a read with posix AIO.
  /// \sa prepareAIO
  void preparePosixAIORead(
    uint8_t* buffer, uint64_t size, uint64_t maxIOSize);

  /// Prepare a read with lib.
  /// \sa prepareAIO
  void prepareLibAIORead(
    uint8_t* buffer, uint64_t size, uint64_t maxIOSize);

  /// Submit the next read with posix AIO.
  /// \sa submitNextPosixAIO
  bool submitNextPosixAIORead(
    uint8_t* buffer, uint64_t alignmentSize, struct aiocb*& controlBlock,
    bool& disableDirectIORequired);

  /// Submit the next read with libaio.
  /// \sa submitNextLibAIO
  bool submitNextLibAIORead(
    uint8_t* buffer, uint64_t alignmentSize, io_context_t* context,
    struct iocb*& controlBlock, bool& disableDirectIORequired);

  /// Write a buffer to the file
  /**
     \param buffer the buffer from which to write

     \param size the number of bytes to write

     \param maxWriteSize the maximum size of a single write() syscall

     \param alignmentSize how writes should be aligned
   */
  void write(
    const uint8_t* buffer, uint64_t size, uint64_t maxWriteSize = 0,
    uint64_t alignmentSize = 0);

  /// Write a string to the file
  /**
     \param str the string to write
   */
  void write(const std::string& str);

  /// Prepare a write with posix AIO.
  /// \sa prepareAIO
  void preparePosixAIOWrite(
    uint8_t* buffer, uint64_t size, uint64_t maxIOSize);

  /// Prepare a write with lib.
  /// \sa prepareAIO
  void prepareLibAIOWrite(
    uint8_t* buffer, uint64_t size, uint64_t maxIOSize);

  /// Submit the next write with posix AIO.
  /// \sa submitNextPosixAIO
  bool submitNextPosixAIOWrite(
    uint8_t* buffer, uint64_t alignmentSize, struct aiocb*& controlBlock,
    bool& disableDirectIORequired);

  /// Submit the next write with libaio.
  /// \sa submitNextLibAIO
  bool submitNextLibAIOWrite(
    uint8_t* buffer, uint64_t alignmentSize, io_context_t* context,
    struct iocb*& controlBlock, bool& disableDirectIORequired);

  uint64_t seek(int64_t offset, SeekMode seekMode);

  /// Get the file's descriptor
  /**
     \return a standard file descriptor for the file
   */
  int getFileDescriptor() const;

  /// Preallocate some space for the file
  /**
     Requires that the file be open for writing.

     \param size the amount of space to pre-allocate
   */
  void preallocate(uint64_t size);

  /// Flush the contents of the file from internal buffers
  void sync() const;

  /// Close the file
  /**
     \warning The file descriptor for the file will become invalid after this
     call completes
   */
  void close();

  /// Unlink the file from the underlying file system.
  /**
     \warning Aborts if the file is open when unlink() is called.
   */
  void unlink();

  /// Get the size of the file in bytes
  /**
     \return the file's size in bytes
   */
  uint64_t getCurrentSize() const;

  /// Get the filename for this file
  /**
     \return the path to the file
   */
  const std::string& getFilename() const;

  /// Get the number of aligned bytes read with direct IO
  /**
     \return the number of aligned bytes read
   */
  uint64_t getAlignedBytesRead() const;

  /// Get the number of aligned bytes written with direct IO
  /**
     \return the number of aligned bytes written
   */
  uint64_t getAlignedBytesWritten() const;

  /**
     Rename the file.

     \param newName the new file name
   */
  void rename(const std::string& newName);

private:
  typedef std::list<struct aiocb*> PosixAIOControlBlockList;
  typedef std::list<struct iocb*> LibAIOControlBlockList;
  typedef std::map<uint8_t*, PosixAIOControlBlockList> PosixAIOControlBlockMap;
  typedef std::map<uint8_t*, LibAIOControlBlockList> LibAIOControlBlockMap;

  /// Prepare an I/O to be submitted asynchronously. No I/Os are actually
  /// submitted from this function.
  /**
     \param buffer the buffer to read into or write from

     \param size the number of bytes to be read or written

     \param maxIOSize the maximum size of an individual read() or write() call
   */
  void prepareAIO(
    uint8_t* buffer, uint64_t size, uint64_t maxIOSize);

  /// Submit the next posix AIO in a sequence of I/Os queued up by a prepare
  /// call.
  /**
     \param buffer the buffer to read into or write from, which is used to
     retrieve enqueued I/Os

     \param alignmentSize how writes should be aligned

     \param[out] controlBlock an output parameter that will hold the control
     block corresponding to the submitted I/O

     \param[out] disableDirectIORequired if true, the user needs to disable
     direct IO before continuing with the next IO

     \return true if an I/O was submitted, and false if all I/Os enqueued by the
     corresponding prepare call have previously been submitted
   */
  bool submitNextPosixAIO(
    uint8_t* buffer, uint64_t alignment, struct aiocb*& controlBlock,
    bool& disableDirectIORequired);

  /// Submit the next libaio AIO in a sequence of I/Os queued up by a prepare
  /// call.
  /**
     \param buffer the buffer to read into or write from, which is used to
     retrieve enqueued I/Os

     \param alignmentSize how writes should be aligned

     \param context the libaio context in which to read or write

     \param[out] controlBlock an output parameter that will hold the control
     block corresponding to the submitted I/O

     \param[out] disableDirectIORequired if true, the user needs to disable
     direct IO before continuing with the next IO

     \return true if an I/O was submitted, and false if all I/Os enqueued by the
     corresponding prepare call have previously been submitted
   */
  bool submitNextLibAIO(
    uint8_t* buffer, uint64_t alignment, io_context_t* context,
    struct iocb*& controlBlock, bool& disableDirectIORequired);

  const std::string filename;
  AccessMode currentMode;

  int fileDescriptor;
  // True if O_DIRECT is set.
  bool directIO;
  bool preallocated;

  // Used only for asynchronous I/O, for which a file's current position isn't
  // well defined. Instead, use this offset from the front of the file for
  // serializing selection of read/write positions within the file.
  uint64_t filePosition;

  uint64_t alignedBytesRead;
  uint64_t alignedBytesWritten;

  PosixAIOControlBlockMap posixAIOControlBlockMap;
  LibAIOControlBlockMap libAIOControlBlockMap;
};

typedef std::list<File*> FileList;

#endif // TRITONSORT_FILE_H
