#include <iostream>
#include <set>

#include "core/File.h"
#include "core/LogDataContainer.h"
#include "core/MemoryUtils.h"
#include "core/Params.h"
#include "core/ScopedLock.h"
#include "core/StatWriter.h"
#include "core/StatusPrinter.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"

pthread_mutex_t StatWriter::singletonWriterLock = PTHREAD_MUTEX_INITIALIZER;
StatWriter* StatWriter::singletonWriter = NULL;

StatWriter::StatWriter(Params& _params)
  : Thread("StatWriter"),
    nextStatLoggerID(1),
    params(_params),
    currentPhaseName("PHASE_NAME_UNSET"),
    nextPhaseName(NULL),
    currentEpoch(0),
    nextEpoch(NULL),
    stopWriter(false),
    writerRunning(false) {

  safeMutexInit(nextStatLoggerIDLock);
  safeMutexInit(logLineDescriptorMapLock);
  safeMutexInit(phaseAndEpochLock);
  safeMutexInit(runningWriterStateLock);

  int status = pthread_cond_init(&phaseAndEpochChanged, NULL);

  ABORT_IF(status != 0, "pthread_cond_init() failed with error %d: %s",
           errno, strerror(errno));

  const std::string& logDirName = params.get<std::string>("LOG_DIR");

  std::string hostname;
  getHostname(hostname);

  std::ostringstream oss;
  oss << logDirName << '/' << hostname << "_stats.log";

  logFile = new File(oss.str());
  logFile->open(File::WRITE, true);

  oss.str("");
  oss << logDirName << '/' << hostname << "_stat_descriptors.log";

  descriptorsFile = new File(oss.str());
  descriptorsFile->open(File::WRITE, true);
    }

StatWriter::~StatWriter() {
  if (logFile != NULL) {
    logFile->sync();
    logFile->close();
    delete logFile;
    logFile = NULL;
  }

  if (descriptorsFile != NULL) {
    descriptorsFile->sync();
    descriptorsFile->close();
    delete descriptorsFile;
    descriptorsFile = NULL;
  }

  int status = pthread_cond_destroy(&phaseAndEpochChanged);

  ABORT_IF(status != 0, "pthread_cond_destroy() failed with error %d: %s",
           errno, strerror(errno));

  safeMutexDestroy(phaseAndEpochLock);
  safeMutexDestroy(logLineDescriptorMapLock);
  safeMutexDestroy(nextStatLoggerIDLock);
  safeMutexDestroy(runningWriterStateLock);
}

void StatWriter::init(Params& params) {
  ScopedLock scopedLock(&singletonWriterLock);

  if (params.get<bool>("ENABLE_STAT_WRITER")) {
    singletonWriter = new StatWriter(params);
  }
}

void StatWriter::teardownThread() {
  pthread_mutex_lock(&runningWriterStateLock);
  stopWriter = true;
  pthread_mutex_unlock(&runningWriterStateLock);

  Thread::stopThread();
}

void StatWriter::teardown() {
  ScopedLock scopedLock(&singletonWriterLock);

  // No need to teardown if we never initialized the writer
  if (singletonWriter != NULL) {
    singletonWriter->teardownThread();
    delete singletonWriter;
    singletonWriter = NULL;
  }
}

void StatWriter::addStatContainer(StatContainerInterface* statContainer) {
  ScopedLock scopedLock(&singletonWriterLock);

  ABORT_IF(statContainer == NULL, "Can't add a NULL stat container to "
           "StatWriter");

  if (singletonWriter != NULL) {
    singletonWriter->addStatContainerInternal(statContainer);
  } else {
    delete statContainer;
  }
}

void StatWriter::addStatContainerInternal(
  StatContainerInterface* statContainer) {

  statContainerQueue.push(statContainer);
}

void StatWriter::addLogDataContainer(LogDataContainer* logDataContainer) {
  ScopedLock scopedLock(&singletonWriterLock);

  if (singletonWriter != NULL) {
    singletonWriter->addLogDataContainerInternal(logDataContainer);
  } else {
    delete logDataContainer;
  }
}

void StatWriter::addLogDataContainerInternal(
  LogDataContainer* logDataContainer) {

  logDataContainerQueue.push(logDataContainer);
}

void StatWriter::addLogLineDescriptor(
  uint64_t loggerID, uint64_t statID, LogLineDescriptor* logLineDescriptor) {
  ScopedLock scopedLock(&singletonWriterLock);

  if (singletonWriter != NULL) {
    singletonWriter->addLogLineDescriptorInternal(
      loggerID, statID, logLineDescriptor);
  } else {
    delete logLineDescriptor;
  }
}

void StatWriter::addLogLineDescriptorInternal(
  uint64_t loggerID, uint64_t statID, LogLineDescriptor* logLineDescriptor) {
  ScopedLock scopedLock(&logLineDescriptorMapLock);

  std::pair<uint64_t, uint64_t> descriptorKey(loggerID, statID);

  LogLineDescriptorMap::iterator iter = logLineDescriptorMap.find(
    descriptorKey);

  if (iter == logLineDescriptorMap.end()) {
    logLineDescriptorMap.insert(
      iter, std::pair< std::pair<uint64_t, uint64_t>, LogLineDescriptor*>(
        descriptorKey, logLineDescriptor));
  } else {
    ABORT("Shouldn't insert log line descriptor for a given container more "
          "than once");
  }
}

void StatWriter::setCurrentPhaseName(const std::string& phaseName) {
  ScopedLock scopedLock(&singletonWriterLock);

  if (singletonWriter != NULL) {
    singletonWriter->setPhaseName(phaseName);
  }
}

void StatWriter::setCurrentEpoch(uint64_t epoch) {
  ScopedLock scopedLock(&singletonWriterLock);

  if (singletonWriter != NULL) {
    singletonWriter->setEpoch(epoch);
  }
}

uint64_t StatWriter::registerLogger() {
  ScopedLock scopedLock(&singletonWriterLock);

  if (singletonWriter != NULL) {
    return singletonWriter->registerStatLogger();
  } else {
    return 0;
  }
}

uint64_t StatWriter::registerStatLogger() {
  ScopedLock lock(&nextStatLoggerIDLock);

  return nextStatLoggerID++;
}

void StatWriter::setPhaseName(const std::string& phaseName) {
  // Check whether the writer is running or not
  pthread_mutex_lock(&runningWriterStateLock);
  bool writerRunningSnapshot = writerRunning;
  pthread_mutex_unlock(&runningWriterStateLock);

  if (writerRunningSnapshot) {
    // If the writer is running, we need to make sure the change is done at the
    // appropriate time
    ScopedLock lock(&phaseAndEpochLock);
    ABORT_IF(nextPhaseName != NULL, "Can't change phase name multiple times "
             "concurrently");
    nextPhaseName = new (themis::memcheck) std::string(phaseName);
    statContainerQueue.push(NULL);

    while (nextPhaseName != NULL) {
      pthread_cond_wait(&phaseAndEpochChanged, &phaseAndEpochLock);
    }

    ABORT_IF(currentPhaseName.compare(phaseName) != 0,
             "Phase name change didn't complete successfully");
  } else {
    pthread_mutex_lock(&phaseAndEpochLock);
    // If the writer isn't running, we don't care; just change the name
    currentPhaseName.assign(phaseName);
    pthread_mutex_unlock(&phaseAndEpochLock);
  }
}

void StatWriter::setEpoch(uint64_t epoch) {
  // Check whether the writer is running or not
  pthread_mutex_lock(&runningWriterStateLock);
  bool writerRunningSnapshot = writerRunning;
  pthread_mutex_unlock(&runningWriterStateLock);

  if (writerRunningSnapshot) {
    // If the writer is running, we need to make sure the change is done at the
    // appropriate time
    ScopedLock lock(&phaseAndEpochLock);

    ABORT_IF(nextEpoch != NULL, "Can't change epoch multiple times "
             "concurrently");

    nextEpoch = new (themis::memcheck) uint64_t(epoch);
    statContainerQueue.push(NULL);

    while (nextEpoch != NULL) {
      pthread_cond_wait(&phaseAndEpochChanged, &phaseAndEpochLock);
    }

    ABORT_IF(currentEpoch != epoch, "Epoch change didn't complete "
             "successfully");
  } else {
    pthread_mutex_lock(&phaseAndEpochLock);
    // If the writer isn't running, we don't care; just change the name
    currentEpoch = epoch;
    pthread_mutex_unlock(&phaseAndEpochLock);
  }
}

void StatWriter::spawn() {
  ScopedLock scopedLock(&singletonWriterLock);

  if (singletonWriter != NULL) {
    singletonWriter->startThread();
  }
}

void* StatWriter::thread(void* args) {
  // Keep draining and writing stat and data containers until you're told to
  // stop

  pthread_mutex_lock(&runningWriterStateLock);
  writerRunning = true;
  pthread_mutex_unlock(&runningWriterStateLock);

  bool shouldStop = false;

  while (!shouldStop || !statContainerQueue.empty() ||
         !logDataContainerQueue.empty()) {
    // Check to see if we should be stopping
    pthread_mutex_lock(&runningWriterStateLock);
    if (stopWriter) {
      shouldStop = true;
    }
    pthread_mutex_unlock(&runningWriterStateLock);

    // Dequeue any stat containers that are queued up. To prevent dequeueing
    // forever, snapshot the queue's current size and pop that many elements
    // from it. Since we're the only thread dequeueing, this should be a lower
    // bound on the number of elements in the queue
    uint64_t statContainersToDequeue = statContainerQueue.size();

    // We can't block on an empty queue because we still need to check
    // stopWriter; if the containers queue is empty, just wait 0.5 seconds and
    // try again
    /// \todo(AR) this should also be configurable
    if (statContainersToDequeue == 0 && !shouldStop) {
      usleep(500000);
    } else {
      while (statContainersToDequeue > 0) {
        StatContainerInterface* container = NULL;
        ABORT_IF(!statContainerQueue.pop(container), "Expected to pop an "
                 "element from stat containers queue at this point, but "
                 "didn't");

        if (container == NULL) {
          // If the current container is NULL, we should change the phase and
          // epoch

          // First, drain the log data container queue so that old log data
          // containers are associated with the right phase
          drainLogDataContainerQueue();

          // Now, change the phase name and/or epoch and signal the changer
          // that the change has occurred
          pthread_mutex_lock(&phaseAndEpochLock);
          if (nextPhaseName != NULL) {
            currentPhaseName.assign(*nextPhaseName);
            delete nextPhaseName;
            nextPhaseName = NULL;
          }

          if (nextEpoch != NULL) {
            currentEpoch = *nextEpoch;
            delete nextEpoch;
            nextEpoch = NULL;
          }
          pthread_cond_signal(&phaseAndEpochChanged);
          pthread_mutex_unlock(&phaseAndEpochLock);
        } else {
          // Otherwise, we should write it

          std::pair<uint64_t, uint64_t> containerID(
            container->getParentLoggerID(), container->getContainerID());

          LogLineDescriptorMap::iterator iter = logLineDescriptorMap.find(
            containerID);

          ABORT_IF(iter == logLineDescriptorMap.end(), "Can't find log line "
                   "descriptor for stat logger %llu container %llu",
                   containerID.first, containerID.second);

          LogLineDescriptor* descriptor = iter->second;

          container->writeStatsToFile(
            *logFile, *descriptor, currentPhaseName, currentEpoch);
        }

        if (container != NULL) {
          delete container;
        }

        statContainersToDequeue--;
      }
    }

    // Drain all the LogDataContainers we've been given, if any
    drainLogDataContainerQueue();
  }

  // At this point, both queues should be empty
  ABORT_IF(!statContainerQueue.empty() || !logDataContainerQueue.empty(),
           "Expected both stat queues to be empty after stopping, but one of "
           "them was found to be non-empty");

  // We're done writing to the log file
  logFile->sync();
  logFile->close();
  delete logFile;
  logFile = NULL;

  // Add all log line descriptors from stat containers to the set of log line
  // descriptors
  pthread_mutex_lock(&logLineDescriptorMapLock);
  for (LogLineDescriptorMap::iterator iter = logLineDescriptorMap.begin();
       iter != logLineDescriptorMap.end(); iter++) {
    logLineDescriptionSet.insert((iter->second)->getDescriptionJson());
    delete iter->second;
  }
  logLineDescriptorMap.clear();
  pthread_mutex_unlock(&logLineDescriptorMapLock);

  // Now that we've de-duplicated log line descriptions, write each of them to
  // the descriptor file as part of one large JSON array
  Json::Value logLineDescriptions(Json::arrayValue);

  for (std::set<Json::Value>::iterator iter = logLineDescriptionSet.begin();
       iter != logLineDescriptionSet.end(); iter++) {
    logLineDescriptions.append(*iter);
  }

  Json::FastWriter writer;
  std::string outputString;
  outputString.assign(writer.write(logLineDescriptions));

  descriptorsFile->write(outputString);
  descriptorsFile->sync();
  descriptorsFile->close();
  delete descriptorsFile;
  descriptorsFile = NULL;

  pthread_mutex_lock(&runningWriterStateLock);
  writerRunning = false;
  pthread_mutex_unlock(&runningWriterStateLock);

  return NULL;
}

void StatWriter::safeMutexInit(pthread_mutex_t& mutex) {
  int status = pthread_mutex_init(&mutex, NULL);
  ABORT_IF(status != 0, "pthread_mutex_init() failed with error %d: %s",
           errno, strerror(errno));
}

void StatWriter::safeMutexDestroy(pthread_mutex_t& mutex) {
  int status = pthread_mutex_destroy(&mutex);

  ABORT_IF(status != 0, "pthread_mutex_destroy() failed with error %d: %s",
           errno, strerror(errno));
}

void StatWriter::drainLogDataContainerQueue() {
  LogDataContainer* dataContainer = NULL;
  int logFileDescriptor = logFile->getFileDescriptor();

  while (logDataContainerQueue.pop(dataContainer)) {
    dataContainer->write(
      logFileDescriptor, currentPhaseName, currentEpoch);
    dataContainer->addLogLineDescriptions(logLineDescriptionSet);
    delete dataContainer;
  }

}
