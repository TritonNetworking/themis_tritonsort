#ifndef THEMIS_WRITE_TOKEN_POOL_H
#define THEMIS_WRITE_TOKEN_POOL_H

#include <stdint.h>
#include <set>

#include "core/ResourceMonitorClient.h"

class WriteToken;
template <typename T> class ThreadSafeQueue;

/**
   A shared pool of write tokens. Chainers must acquire a write token for the
   appropriate disk before sending data to its downstream writer.
 */
class WriteTokenPool : public ResourceMonitorClient {
public:
  /// Constructor
  /**
     \param tokensPerDisk the quantity of WriteTokens that should be
     generated for each disk

     \param numDisks the number of writers for which tokens should be
     generated
   */
  WriteTokenPool(uint64_t tokensPerDisk, uint64_t numDisks);

  /// Destructor
  virtual ~WriteTokenPool();

  /// Give the resource monitor the number of tokens in each pool
  /**
     \sa ResourceMonitorClient::resourceMonitorOutput
   */
  void resourceMonitorOutput(Json::Value& obj);

  /// Get a token for one of a set of disks, blocking until one is available
  /**
     \param diskIDSet a set of disk IDs for which the caller is willing
     receive a token

     \return a token for one of the disk IDs in diskIDSet
   */
  WriteToken* getToken(const std::set<uint64_t>& diskIDSet);

  /// Try to get a token for one of a set of disks
  /**
     \param diskIDSet a set of disks IDs for which the caller is willing
     receive a token

     \return a token for one of the disk IDs in diskIDSet, or NULL if no
     token is available for any disk ID in diskIDSet
   */
  WriteToken* attemptGetToken(const std::set<uint64_t>& diskIDSet);

  /// Return a token to the pool
  /**
     \param token the token to return to the pool
   */
  void putToken(WriteToken* token);

private:
  const uint64_t tokensPerDisk;
  const uint64_t numDisks;

  // One thread-safe queue of tokens per writer. Opted to use an array here
  // rather than a std::vector because I don't think STL vectors are guaranteed
  // thread-safe and I wanted concurrent access to different token queues to
  // work properly without unnecessary blocking.
  ThreadSafeQueue<WriteToken*>* tokenQueues;
};

#endif // THEMIS_WRITE_TOKEN_POOL_H
