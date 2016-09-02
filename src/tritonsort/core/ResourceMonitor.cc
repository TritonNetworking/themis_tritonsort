#include <arpa/inet.h>
#include <errno.h>
#include <sstream>
#include <stdarg.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include "Utils.h"
#include "core/Params.h"
#include "core/ResourceMonitor.h"
#include "core/ScopedLock.h"
#include "core/TritonSortAssert.h"
#include "third-party/jsoncpp.h"


pthread_mutex_t ResourceMonitor::lock;
themis::Thread ResourceMonitor::thread(
    "ResourceMonitor", &ResourceMonitor::run);
bool ResourceMonitor::stop = false;
bool ResourceMonitor::initialized = false;
Socket* ResourceMonitor::serverSocket = NULL;
uint64_t ResourceMonitor::socketBufferSize = 0;

KeyToClientSetMap ResourceMonitor::keyToClients;
ClientToKeySetMap ResourceMonitor::clientToKeys;

void ResourceMonitor::init(Params* params) {
  // Do not initialize if port is not named in config
  // This prevents unnecessary select() overhead if monitoring is not needed
  if (!params->contains("MONITOR_PORT")) {
    return;
  }

  pthread_mutex_init(&lock, NULL);
  stop = false;

  serverSocket = new Socket();
  serverSocket->listen(params->get<std::string>("MONITOR_PORT"), 1);
  socketBufferSize = params->get<int32_t>("TCP_RECEIVE_BUFFER_SIZE");

  // Allow registerPool(), spawn(), teardown() calls to succeed
  initialized = true;
}

void ResourceMonitor::teardown() {
  if (!initialized) {
    return;
  }

  stop = true;

  thread.stopThread();

  if (serverSocket != NULL) {
    serverSocket->close();
    delete serverSocket;
  }

  pthread_mutex_destroy(&lock);
}

void ResourceMonitor::registerClient(ResourceMonitorClient* client,
                                     const char* keyFormat, ...) {
  if (!initialized) {
    return;
  }

  ScopedLock scopedLock(&lock);

  va_list ap;

  va_start(ap, keyFormat);
  char buffer[1024];
  int stringSize = vsnprintf(buffer, 1024, keyFormat, ap);
  va_end(ap);

  std::string resourceMonitorKey(buffer, stringSize);

  keyToClients[resourceMonitorKey].insert(client);
  clientToKeys[client].insert(resourceMonitorKey);
}

void ResourceMonitor::unregisterClient(ResourceMonitorClient* client) {
  if (!initialized) {
    return;
  }

  ScopedLock scopedLock(&lock);

  ClientToKeySetMap::iterator clientToKeysIter = clientToKeys.find(client);

  if (clientToKeysIter == clientToKeys.end()) {
    // This client wasn't registered in the first place, so short-circuit
    return;
  }

  KeySet& keySet = clientToKeysIter->second;

  for (KeySet::iterator setIter = keySet.begin(); setIter != keySet.end();
       setIter++) {
    const std::string& key = *setIter;

    KeyToClientSetMap::iterator keyToClientsIter = keyToClients.find(key);

    ABORT_IF(keyToClientsIter == keyToClients.end(), "The mapping from keys "
             "to clients in the resource monitor was corrupted at some point "
             "in the past; key-to-clients map isn't the direct reverse of "
             "client-to-keys map");

    ClientSet& clients = keyToClientsIter->second;

    ABORT_IF(clients.erase(client) != 1, "Mapping from keys to clients "
             "was corrupted at some point in the past; client claims to be "
             "registered under key %s, but is not in the appropriate map",
             key.c_str());

    if (clients.size() == 0) {
      keyToClients.erase(keyToClientsIter);
    }
  }

  clientToKeys.erase(clientToKeysIter);
}

void ResourceMonitor::queryAllClients(std::string& outputStr) {
  ScopedLock scopedLock(&lock);

  std::ostringstream response;

  Json::FastWriter writer;

  Json::Value root;

  for (KeyToClientSetMap::iterator mapIter = keyToClients.begin();
       mapIter != keyToClients.end(); mapIter++) {
    const std::string& key = mapIter->first;
    ClientSet& clientSet = mapIter->second;

    Json::Value& objectListForKey = root[key];

    for (ClientSet::iterator clientSetIter = clientSet.begin();
         clientSetIter != clientSet.end(); clientSetIter++) {

      Json::Value clientObject;

      ResourceMonitorClient* currentClient = *clientSetIter;
      currentClient->resourceMonitorOutput(clientObject);

      objectListForKey.append(clientObject);
    }

    root[key] = objectListForKey;
  }

  outputStr.assign(writer.write(root));
}

void ResourceMonitor::handleNewConnection(Socket* clientSock) {
  // Return resource availability information.
  // Note: queryAllClients() is threadsafe
  std::string response;
  queryAllClients(response);
  unsigned int msgLen = strlen(response.c_str());

  blockingWrite(clientSock->getFD(),
                reinterpret_cast<const uint8_t*>(response.c_str()), msgLen, 0,
                0, "resource monitor");

  clientSock->close();
}

void* ResourceMonitor::run(void* arg) {
  while(!stop) {
    // Try accepting connectionwith 0.5s timeout
    Socket* clientSocket = serverSocket->accept(500000, socketBufferSize);
    if (clientSocket != NULL) {
      // handler function should ensure thread safety!
      handleNewConnection(clientSocket);
      // Clean up client socket.
      delete clientSocket;
    }
  }
  return NULL;
}

void ResourceMonitor::spawn() {
  if (!initialized) {
    return;
  }

  thread.startThread();
}
