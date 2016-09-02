#include <errno.h>
#include <sstream>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "core/File.h"
#include "core/IntervalStatLogger.h"
#include "core/MemoryMappedFileDeadlockResolver.h"
#include "core/MemoryUtils.h"
#include "core/ResourceMonitor.h"
#include "core/Timer.h"
#include "core/TritonSortAssert.h"

MemoryMappedFileDeadlockResolver::MemoryMappedFileDeadlockResolver(
  const StringList& diskList)
  : logger("MemoryMappedFileDeadlockResolver"),
    numOpenFiles(0),
    numMmapedBytes(0) {
  for (StringList::const_iterator iter = diskList.begin();
       iter != diskList.end(); ++iter) {
    const std::string& directory = *iter;
    Disk* disk = new (themis::memcheck) Disk(directory + "/mmap");
    disks.insert(disk);
  }

  pthread_mutex_init(&diskListLock, NULL);

  // Log number of disks because we know it right now.
  logger.logDatum("num_disks", disks.size());

  ResourceMonitor::registerClient(this, "memory_mapped_file_deadlock_resolver");
  IntervalStatLogger::registerClient(this);
}

MemoryMappedFileDeadlockResolver::~MemoryMappedFileDeadlockResolver() {
  ResourceMonitor::unregisterClient(this);
  IntervalStatLogger::unregisterClient(this);

  TRITONSORT_ASSERT(memoryMappedFiles.empty(),
         "There should be no more memory mapped files at destruction time.");
  // Free disk list.
  for (SortedDiskList::iterator iter = disks.begin(); iter != disks.end();
       ++iter) {
    delete *iter;
  }

  pthread_mutex_destroy(&diskListLock);
}

StatLogger* MemoryMappedFileDeadlockResolver::initIntervalStatLogger() {
  StatLogger* intervalLogger = new StatLogger(
    "MemoryMappedFileDeadlockResolver");

  numOpenFilesStatID = intervalLogger->registerStat("num_open_files");
  numMmapedBytesStatID = intervalLogger->registerStat("num_mmaped_bytes");

  return intervalLogger;
}

void MemoryMappedFileDeadlockResolver::logIntervalStats(
  StatLogger& intervalLogger) const {

  // This information may be stale, but that should be fine for logging.
  intervalLogger.add(numOpenFilesStatID, numOpenFiles);
  intervalLogger.add(numMmapedBytesStatID, numMmapedBytes);
}

void* MemoryMappedFileDeadlockResolver::resolveRequest(uint64_t size) {
  // mmap() this request to the disk with the fewest mmap'd bytes, which is the
  // first in the disk list.
  Disk* disk = *(disks.begin());
  // Move this disk to its new position in the list.
  pthread_mutex_lock(&diskListLock);
  disks.erase(disks.begin());
  disk->memoryMappedBytes += size;
  disks.insert(disk);
  pthread_mutex_unlock(&diskListLock);

  // Filename is '<mmap_directory>/<timestamp>'
  std::ostringstream fileName;
  fileName << disk->directory << "/" << Timer::posixTimeInMicros();
  File* file = new (themis::memcheck) File(fileName.str());

  // Fill the file with bytes.
  file->open(File::READ_WRITE, true);
  file->preallocate(size);
  uint64_t fileSize = file->getCurrentSize();
  TRITONSORT_ASSERT(fileSize == size,
         "Tried to preallocate %llu bytes but file is only %llu bytes.",
         size, fileSize);

  // mmap() the file
  int fd = file->getFileDescriptor();
  void* memory = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  ABORT_IF(memory == MAP_FAILED,
           "mmap() failed with errno %d: %s", errno, strerror(errno));

  // Record this mmap() so it can be munmap()'d
  MemoryMappedFile* memoryMappedFile =
    new (themis::memcheck) MemoryMappedFile(*file, fileSize, *disk);
  memoryMappedFiles[memory] = memoryMappedFile;

  // Update statistics
  ++numOpenFiles;
  numMmapedBytes += size;

  return memory;
}

void MemoryMappedFileDeadlockResolver::deallocate(void* memory) {
  // Find the file that this memory was mapped to.
  FileMap::iterator iter = memoryMappedFiles.find(memory);
  ABORT_IF(iter == memoryMappedFiles.end(),
           "Cannot deallocate() unknown memory region %p", memory);
  MemoryMappedFile* memoryMappedFile = iter->second;
  File& file = memoryMappedFile->file;
  uint64_t size = memoryMappedFile->fileSize;
  Disk& disk = memoryMappedFile->disk;

  // Un-map the file.
  int status = munmap(memory, size);
  ABORT_IF(status != 0,
           "munmap() of %xp failed with status %d", memory, status);

  // Close and unlink the file from the file system.
  file.close();
  file.unlink();

  // Clean up data structures.
  memoryMappedFiles.erase(iter);
  delete &file;
  delete memoryMappedFile;

  // Move the disk to its new position in the list.
  pthread_mutex_lock(&diskListLock);
  disks.erase(&disk);
  disk.memoryMappedBytes -= size;
  disks.insert(&disk);
  pthread_mutex_unlock(&diskListLock);

  // Update statistics
  --numOpenFiles;
  numMmapedBytes -= size;
}

void MemoryMappedFileDeadlockResolver::resourceMonitorOutput(
  Json::Value& obj) {
  obj["mmap_byte_totals"] = Json::Value(Json::arrayValue);
  Json::Value& memoryMappedByteTotals = obj["mmap_byte_totals"];

  pthread_mutex_lock(&diskListLock);
  for (SortedDiskList::iterator iter = disks.begin(); iter != disks.end();
       ++iter) {
    Disk* disk = *iter;
    Json::Value mmapDisk;
    mmapDisk["disk"] = disk->directory;
    mmapDisk["bytes"] = Json::UInt64(disk->memoryMappedBytes);

    memoryMappedByteTotals.append(mmapDisk);
  }
  pthread_mutex_unlock(&diskListLock);
}

MemoryMappedFileDeadlockResolver::Disk::Disk(const std::string& _directory)
  : directory(_directory),
    memoryMappedBytes(0) {
  // Make the mmap directory for the disk.
  int status = mkdir(directory.c_str(), 00755);
  ABORT_IF(status == -1,  "mkdir(%s) failed with error %d: %s",
           directory.c_str(), errno, strerror(errno));
}

MemoryMappedFileDeadlockResolver::Disk::~Disk() {
  // Remove the mmap directory on disk.
  int status = rmdir(directory.c_str());
  ABORT_IF(status == -1,  "rmdir(%s) failed with error %d: %s",
           directory.c_str(), errno, strerror(errno));
}
