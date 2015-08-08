#ifndef THEMIS_MEMORY_MAPPED_FILE_DEADLOCK_RESOLVER_H
#define THEMIS_MEMORY_MAPPED_FILE_DEADLOCK_RESOLVER_H

#include <map>
#include <set>
#include <string>

#include "core/DeadlockResolverInterface.h"
#include "core/IntervalStatLoggerClient.h"
#include "core/ResourceMonitorClient.h"
#include "core/StatLogger.h"
#include "core/constants.h"

class File;

/**
   A MemoryMappedFileDeadlockResolver resolves memory deadlocks by allocating
   buffers backed by on-disk files with mmap(). The resolver tries to spread
   backing files as evenly as possible across a set of disks passed in at
   construction time. Backing files are placed in '/mmap' subdirectories and are
   named by creation timestamps.
 */
class MemoryMappedFileDeadlockResolver :
  public DeadlockResolverInterface, public IntervalStatLoggerClient,
  public ResourceMonitorClient {
public:
  /// Constructor
  /**
     \param diskList a list of directories on different disks to use as
     storage locations for memory mapped files.
   */
  MemoryMappedFileDeadlockResolver(const StringList& diskList);

  /// Destructor
  virtual ~MemoryMappedFileDeadlockResolver();

  StatLogger* initIntervalStatLogger();
  void logIntervalStats(StatLogger& intervalLogger) const;

  /// \sa ResourceMonitorClient::resourceMonitorOutput
  void resourceMonitorOutput(Json::Value& obj);

  /**
     Resolves a request for the given size by allocating a memory-mapped file of
     that size.

     \param size the size of the memory to map to disk.

     \return a pointer to memory backed by disk
   */
  void* resolveRequest(uint64_t size);

  /**
     Unmaps the memory-mapped file corresponding to this piece of memory and
     deletes the backing file from the file system.

     \param memory the original pointer to the memory mapped region.
   */
  void deallocate(void* memory);

private:
  /// A data structure that tracks the number of memory mapped bytes on a disk.
  class Disk {
  public:
    /// Constructor
    /**
       Creates the underlying directory in the file system.

       \param directory the location where mmap files will be created
     */
    Disk(const std::string& directory);

    /// Destructor
    /**
       Removes the underlying directory from the file system.
     */
    virtual ~Disk();

    const std::string directory;
    uint64_t memoryMappedBytes;
  };

  /// A comparator so disks can be sorted by fewest memory mapped bytes.
  class DiskLessThanComparator {
  public:
    bool operator() (const Disk* const& disk1, const Disk* const& disk2) const {
      if (disk1->memoryMappedBytes == disk2->memoryMappedBytes) {
        return disk1 < disk2;
      }

      return disk1->memoryMappedBytes < disk2->memoryMappedBytes;
    }
  };

  /// A data structure that tracks memory mapped files on disks.
  struct MemoryMappedFile {
    MemoryMappedFile(File& _file, uint64_t _fileSize, Disk& _disk)
      : file(_file),
        fileSize(_fileSize),
        disk(_disk) {
    }

    File& file;
    uint64_t fileSize;
    Disk& disk;
  };

  typedef std::set<Disk*, DiskLessThanComparator> SortedDiskList;
  typedef std::map<void*, MemoryMappedFile*> FileMap;

  SortedDiskList disks;
  FileMap memoryMappedFiles;

  pthread_mutex_t diskListLock;

  StatLogger logger;
  uint64_t numOpenFilesStatID;
  uint64_t numMmapedBytesStatID;

  uint64_t numOpenFiles;
  uint64_t numMmapedBytes;
};

#endif // THEMIS_MEMORY_MAPPED_FILE_DEADLOCK_RESOLVER_H
