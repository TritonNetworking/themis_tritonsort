#include "core/IntervalStatLogger.h"
#include "core/MemoryQuota.h"
#include "core/TritonSortAssert.h"

MemoryQuota::MemoryQuota(const std::string& _quotaName, uint64_t _quota)
  : quotaName(_quotaName),
    quota(_quota),
    usage(0) {
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&quotaNotExceeded, NULL);

  IntervalStatLogger::registerClient(this);
}

MemoryQuota::~MemoryQuota() {
  IntervalStatLogger::unregisterClient(this);

  ABORT_IF(usage != 0, "Expected memory usage (%llu) for quota '%s' to be zero "
           "at destruction time", usage, quotaName.c_str());
  pthread_mutex_destroy(&mutex);
  pthread_cond_destroy(&quotaNotExceeded);
}

void MemoryQuota::addUsage(uint64_t addedUsage) {
  pthread_mutex_lock(&mutex);
  while (usage + addedUsage > quota) {
    pthread_cond_wait(&quotaNotExceeded, &mutex);
  }
  usage += addedUsage;
  pthread_mutex_unlock(&mutex);
}

void MemoryQuota::removeUsage(uint64_t removedUsage) {
  pthread_mutex_lock(&mutex);
  ABORT_IF(usage < removedUsage, "Removing more usage (%llu) from quota "
           "'%s' than we currently have (%llu).", removedUsage,
           quotaName.c_str(), usage);
  usage -= removedUsage;
  pthread_cond_broadcast(&quotaNotExceeded);
  pthread_mutex_unlock(&mutex);
}

StatLogger* MemoryQuota::initIntervalStatLogger() {
  StatLogger* intervalLogger = new StatLogger(quotaName);
  usageStatID = intervalLogger->registerStat("memory_usage");

  return intervalLogger;
}

void MemoryQuota::logIntervalStats(StatLogger& intervalLogger) const {
  intervalLogger.add(usageStatID, usage);
}
