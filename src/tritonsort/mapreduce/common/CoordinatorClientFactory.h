#ifndef THEMIS_MAPRED_COORDINATOR_CLIENT_FACTORY_H
#define THEMIS_MAPRED_COORDINATOR_CLIENT_FACTORY_H

#include <stdint.h>
#include <string>

class CoordinatorClientInterface;
class Params;

/**
   CoordinatorClientFactory is used by workers to construct a client-side
   interface to Themis's coordinator.
 */
class CoordinatorClientFactory {
public:
  virtual ~CoordinatorClientFactory() {}

  /// Create a new coordinator client
  /**
     The coordinator client to be instantiated is determined by the value of
     the COORDINATOR_CLIENT parameter.

     Currently supported clients: 'redis', 'debug', 'none',
       redis: use redis as the backing store for the coordinator
       debug: minimal functionality coordinator client that relies on params
       none: return a NULL client object, so themis can ignore the coordinator

     \param params a Params object that the coordinator client can use to
     configure itself

     \param phaseName the name of the phase in which the caller is executing

     \param role the caller's "role" (typically its stage name, but can be
     arbitrary; 'reader', 'writer', etc.).

     \param id the caller's unique ID within its role
   */
  static CoordinatorClientInterface* newCoordinatorClient(
    const Params& params, const std::string& phaseName, const std::string& role,
    uint64_t id);
};


#endif // THEMIS_MAPRED_COORDINATOR_CLIENT_FACTORY_H
