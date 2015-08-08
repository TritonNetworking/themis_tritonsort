#ifndef _TRITONSORT_RESOURCEMONITOR_H
#define _TRITONSORT_RESOURCEMONITOR_H

#include <map>
#include <pthread.h>
#include <set>
#include <stdint.h>
#include <string>

#include "core/ResourceMonitorClient.h"
#include "core/Socket.h"
#include "core/Thread.h"

class Params;

typedef std::set<std::string> KeySet;
typedef std::set<ResourceMonitorClient*> ClientSet;
typedef std::map<std::string, ClientSet> KeyToClientSetMap;
typedef std::map<ResourceMonitorClient*, KeySet> ClientToKeySetMap;

/**
   The resource monitor provides an external interface to some of the system's
   internals. Clients can register with the resource monitor by implementing
   the interface given in ResourceMonitorClient.

   Users of the resource monitor communicate with it by opening a connection to
   the monitor's port (given by the parameter MONITOR_PORT). The monitor will
   dump a JSON object.

   When a client registers with the monitor, it does so with a particular
   key. The JSON output by the resource monitor is a collection of key/value
   pairs, where each value is an array of JSON objects, one per client that
   registered with the particular key.

   \sa ResourceMonitorClient
 */
class ResourceMonitor {
public:
  /**
     Initializes the resource monitor's internal state

     \param params a Params object that contains the parameters that the
     monitor will use to initialize itself
   */
  static void init(Params* params);

  /**
     Tears down the monitor's internal state and shuts it down.

     \warning This method should only be called if ResourceMonitor::spawn() was
     called at some point in the past.
   */
  static void teardown();

  /// Register a client with the resource monitor.
  /**
     All clients of the resource monitor must extend ResourceMonitorClient.

     \param client a pointer to the client that is registering

     \param keyFormat a printf-style format string giving the key under which
     the client is registering

     \param ... the format arguments for the key format string
   */
  static void registerClient(ResourceMonitorClient* client,
                             const char* keyFormat, ...);

  /// Unregister a client with the resource monitor.
  /**
     All clients of the resource monitor must extend ResourceMonitorClient. It
     is assumed that the client being passed has previously registered by
     calling ResourceMonitor::registerClient.

     \param client a pointer to the client that is deregistering
   */
  static void unregisterClient(ResourceMonitorClient* client);

  /**
     Starts the resource monitor
   */
  static void spawn();


private:
  static pthread_mutex_t lock;
  static Thread thread;
  static bool stop;

  static bool initialized;

  static KeyToClientSetMap keyToClients;
  static ClientToKeySetMap clientToKeys;

  static Socket* serverSocket;
  static uint64_t socketBufferSize;

  static void* run(void* arg);

  /**
     \param[out] outputStr the string to which client output will be written
   */
  static void queryAllClients(std::string& outputStr);
  static void handleNewConnection(int socket);
}; // ResourceMonitor

#endif // _TRITONSORT_RESOURCEMONITOR_H
