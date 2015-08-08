#include "FCFSMemoryAllocatorPolicy.h"
#include "core/ScopedLock.h"

FCFSMemoryAllocatorPolicy::FCFSMemoryAllocatorPolicy() {
  pthread_mutex_init(&mutex, NULL);
}

FCFSMemoryAllocatorPolicy::~FCFSMemoryAllocatorPolicy() {
  pthread_mutex_destroy(&mutex);
}

void FCFSMemoryAllocatorPolicy::addRequest(Request& request,
                                           const std::string& groupName) {
  ScopedLock lock(&mutex);
  requests.push_back(&request);
}

void FCFSMemoryAllocatorPolicy::removeRequest(Request& request,
                                              const std::string& groupName) {
  ScopedLock lock(&mutex);

  for (RequestList::iterator iter = requests.begin();
       iter != requests.end(); iter++) {
    if (*iter == &request) {
      requests.erase(iter);
      return;
    }
  }
}

bool FCFSMemoryAllocatorPolicy::canScheduleRequest(
  uint64_t availability, Request& request) {

  ScopedLock lock(&mutex);
  return request.size < availability;
}

MemoryAllocatorPolicy::Request*
FCFSMemoryAllocatorPolicy::nextSchedulableRequest(uint64_t availability) {
  MemoryAllocatorPolicy::Request* request = requests.front();

  if (request != NULL && request->size < availability) {
    return request;
  } else {
    return NULL;
  }
}

void FCFSMemoryAllocatorPolicy::recordUseTime(uint64_t useTime) {
  // No-op
}

