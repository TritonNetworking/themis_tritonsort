#ifndef THEMIS_RECOVERY_INFO_H
#define THEMIS_RECOVERY_INFO_H

#include <list>
#include <stdint.h>
#include <utility>

#include "core/constants.h"

class RecoveryInfo {
public:
  typedef std::list< std::pair<uint64_t, uint64_t> > PartitionRangeList;

  RecoveryInfo(
    uint64_t recoveringJob, const PartitionRangeList& partitionRangesToRecover);
  virtual ~RecoveryInfo() {}

  const uint64_t recoveringJob;
  const PartitionRangeList partitionRangesToRecover;

private:
  DISALLOW_COPY_AND_ASSIGN(RecoveryInfo);
};


#endif // THEMIS_RECOVERY_INFO_H
