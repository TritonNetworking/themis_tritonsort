#ifndef THEMIS_MEMORY_QUOTA_H
#define THEMIS_MEMORY_QUOTA_H

#include <pthread.h>
#include <stdint.h>
#include <string>

#include "core/IntervalStatLoggerClient.h"
#include "core/StatLogger.h"

class MemoryQuota : public IntervalStatLoggerClient {
public:
  MemoryQuota(const std::string& quotaName, uint64_t quota);
  virtual ~MemoryQuota();

  void addUsage(uint64_t addedUsage);
  void removeUsage(uint64_t removedUsage);

  StatLogger* initIntervalStatLogger();
  void logIntervalStats(StatLogger& intervalLogger) const;
private:
  const std::string quotaName;
  const uint64_t quota;
  uint64_t usage;

  pthread_mutex_t mutex;
  pthread_cond_t quotaNotExceeded;

  uint64_t usageStatID;
};

#endif // THEMIS_MEMORY_QUOTA_H
