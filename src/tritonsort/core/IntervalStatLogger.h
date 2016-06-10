#ifndef THEMIS_INTERVAL_STAT_LOGGER_H
#define THEMIS_INTERVAL_STAT_LOGGER_H

#include "core/Thread.h"

#include <pthread.h>
#include <stdint.h>
#include <map>
#include <vector>

class IntervalStatLoggerClient;
class Params;
class StatLogger;

typedef std::map<IntervalStatLoggerClient*, StatLogger*> RegisteredClientMap;

/**
   Clients can register with the IntervalStatLogger to have a statistic logged
   at some fixed interval.
 */

class IntervalStatLogger {
public:
  /**
     Initializes the IntervalStatLogger using the given parameters. The polling
     period, in microseconds, is given by the STAT_POLL_INTERVAL parameter.

     \param params the parameter set used to configure this interval logger
   */
  static void init(Params* params);

  /**
     Registers a IntervalStatLoggerClient with this interval stat logger. This
     client will have IntervalStatLoggerClient::logIntervalStats called on it
     periodically based on the configuration specified at construction time.

     \param client a pointer to the IntervalStatLoggerClient to register
   */
  static void registerClient(IntervalStatLoggerClient* client);

  /**
     Unregister a client from the IntervalStatLogger. The client must have
     previously been registered using IntervalStatLogger::registerClient.

     \param client a pointer to the IntervalStatLoggerClient to unregister
   */
  static void unregisterClient(IntervalStatLoggerClient* client);

  /**
     Starts the interval logger. It will poll registered clients at a fixed
     interval until IntervalStatLogger::teardown is called.
   */
  static void spawn();

  /**
     Stops the interval stat logger.
   */
  static void teardown();

private:
  /// Implements the main logic loop for the IntervalStatLogger in a pthread.
  static void* run(void* arg);

  static pthread_mutex_t stateChangeLock;
  static themis::Thread thread;
  static bool initialized;
  static bool spawned;
  static bool stop;

  static uint64_t pollInterval;

  static pthread_mutex_t clientMapLock;
  static RegisteredClientMap registeredClients;
};

#endif // THEMIS_INTERVAL_STAT_LOGGER_H
