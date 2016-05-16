#ifndef MAPRED_MULTI_PROTOCOL_READER_H
#define MAPRED_MULTI_PROTOCOL_READER_H

#include <map>
#include <string>

#include "core/SingleUnitRunnable.h"
#include "mapreduce/common/ReadRequest.h"

/**
   MultiProtocol reader demultiplexes read requests based on URL schemes, or
   protocols. Currently it only supports synchronous reads from local disk.
   Internally it maintains reader workers
   for each protocol. These readers are never actually spawned, but their run()
   function is invoked for each applicable work unit.

   Due to the nature of this reader, the child protocol readers must be
   SingleUnitRunnables, which means MultiProtocolReader cannot handle
   asynchronous reads.
 */
class MultiProtocolReader : public SingleUnitRunnable<ReadRequest> {
WORKER_IMPL

private:
  typedef SingleUnitRunnable<ReadRequest> ProtocolReader;

public:
  /// Constructor
  /**
     \param phaseName the name of the phase in which this worker is executing

     \param stageName the name of the worker's parent stage

     \param id the worker's ID within its parent stage

     \param params a Params object that will be used to configure the worker's
     coordinator client
   */
  MultiProtocolReader(
    const std::string& phaseName, const std::string& stageName, uint64_t id,
    Params& params);

  /// Destructor
  virtual ~MultiProtocolReader();

  void init();
  void teardown();

  /// Direct the read request to the appropriate protocol reader based on URL
  /// scheme.
  /**
     \param readRequest the read to service
   */
  void run(ReadRequest* readRequest);

  /// Register protocol readers with resource monitor
  virtual void registerWithResourceMonitor();

  /// Unregister protocol readers with resource monitor
  virtual void unregisterWithResourceMonitor();

  void setTracker(WorkerTrackerInterface* workerTracker);

  void addDownstreamTracker(WorkerTrackerInterface* downstreamTracker);

  void addDownstreamTracker(
    WorkerTrackerInterface* downstreamTracker,
    const std::string& trackerDescription);

private:
  typedef std::map<ReadRequest::Protocol, ProtocolReader*> ProtocolReaderMap;

  /// Add a new protocol reader to the internal map.
  /**
     \param protocol the protocol type

     \param reader the worker associated with this protocol
   */
  void addProtocolReader(
    ReadRequest::Protocol protocol, ProtocolReader* reader);

  ProtocolReaderMap protocolReaderMap;
};

#endif // MAPRED_MULTI_PROTOCOL_READER_H
