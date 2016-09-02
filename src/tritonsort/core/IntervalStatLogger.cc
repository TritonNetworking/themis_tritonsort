#include <limits>
#include <unistd.h>

#include "core/IntervalStatLogger.h"
#include "core/IntervalStatLoggerClient.h"
#include "core/Params.h"
#include "core/ScopedLock.h"
#include "core/TritonSortAssert.h"

themis::Thread IntervalStatLogger::thread(
    "IntervalLogger", &IntervalStatLogger::run);
bool IntervalStatLogger::initialized = false;
bool IntervalStatLogger::spawned = false;
bool IntervalStatLogger::stop = false;
uint64_t IntervalStatLogger::pollInterval =
  std::numeric_limits<uint64_t>::max();
RegisteredClientMap IntervalStatLogger::registeredClients;
pthread_mutex_t IntervalStatLogger::clientMapLock;
pthread_mutex_t IntervalStatLogger::stateChangeLock = PTHREAD_MUTEX_INITIALIZER;

void IntervalStatLogger::init(Params* params) {
  pthread_mutex_lock(&stateChangeLock);

  spawned = false;
  stop = false;

  pollInterval = params->get<uint64_t>("STAT_POLL_INTERVAL");

  pthread_mutex_init(&clientMapLock, NULL);

  initialized = true;

  pthread_mutex_unlock(&stateChangeLock);
}

void IntervalStatLogger::registerClient(IntervalStatLoggerClient* client) {
  // Scoped check for initialized status
  {
    ScopedLock lock(&stateChangeLock);

    if (!initialized) {
      return;
    }
  }

  pthread_mutex_lock(&clientMapLock);

  RegisteredClientMap::iterator iter = registeredClients.find(client);

  if (iter == registeredClients.end()) {
    // If the client is already registered, don't register it again
    StatLogger* logger = client->initIntervalStatLogger();

    if (logger != NULL) {
      registeredClients.insert(std::make_pair(client, logger));
    }
  }

  pthread_mutex_unlock(&clientMapLock);
}

void IntervalStatLogger::unregisterClient(IntervalStatLoggerClient* client) {
  // Scoped check for initialized status
  {
    ScopedLock lock(&stateChangeLock);

    if (!initialized) {
      return;
    }
  }

  pthread_mutex_lock(&clientMapLock);

  RegisteredClientMap::iterator iter = registeredClients.find(client);

  if (iter != registeredClients.end()) {
    delete iter->second;
    registeredClients.erase(iter);
  }

  pthread_mutex_unlock(&clientMapLock);
}

void IntervalStatLogger::spawn() {
  ScopedLock lock(&stateChangeLock);

  if (!initialized) {
    return;
  }

  thread.startThread();

  spawned = true;
}


void IntervalStatLogger::teardown() {
  // Scoped change of state to tell the child thread to stop
  {
    ScopedLock lock(&stateChangeLock);

    if (!initialized) {
      return;
    }

    TRITONSORT_ASSERT(spawned, "Can't teardown an interval stat logger that hasn't been "
           "spawned");
    stop = true;
    initialized = false;
  }

  thread.stopThread();

  pthread_mutex_lock(&clientMapLock);

  for (RegisteredClientMap::iterator iter = registeredClients.begin();
       iter != registeredClients.end(); iter++) {
    delete iter->second;
  }

  registeredClients.clear();

  pthread_mutex_unlock(&clientMapLock);

  pthread_mutex_destroy(&clientMapLock);
}

void* IntervalStatLogger::run(void* arg) {
  bool loggerShouldShutDown = false;

  while (!loggerShouldShutDown) {
    pthread_mutex_lock(&stateChangeLock);
    loggerShouldShutDown = stop;
    pthread_mutex_unlock(&stateChangeLock);

    pthread_mutex_lock(&clientMapLock);

    for (RegisteredClientMap::iterator iter = registeredClients.begin();
         iter != registeredClients.end(); iter++) {
      (iter->first)->logIntervalStats(*(iter->second));
    }

    pthread_mutex_unlock(&clientMapLock);

    usleep(pollInterval);
  }

  return NULL;
}
