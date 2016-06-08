#ifndef _TRITONSORT_RESOURCEMONITORCLIENT_H
#define _TRITONSORT_RESOURCEMONITORCLIENT_H

/**
   Interface for resources to be examined by the ResourceMonitor.
   ResourcePool<T>, for example, will inherit from this in order to connect to
   the monitor.
 */

#include <json/json.h>

class ResourceMonitorClient {
public:
  /// Destructor
  virtual ~ResourceMonitorClient() {}

  /// Add JSON data to the data to be output by the resource monitor
  /**
     \param obj a reference to the JSON object stream that the resource monitor
     will eventually output
   */
  virtual void resourceMonitorOutput(Json::Value& obj) = 0;
};

#endif // _TRITONSORT_RESOURCEMONITORCLIENT_H
