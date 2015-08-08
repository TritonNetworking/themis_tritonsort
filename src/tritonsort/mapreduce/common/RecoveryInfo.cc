#include "mapreduce/common/RecoveryInfo.h"

RecoveryInfo::RecoveryInfo(
  uint64_t _recoveringJob,
  const std::list< std::pair<uint64_t, uint64_t> >& _partitionRangesToRecover)
  : recoveringJob(_recoveringJob),
    partitionRangesToRecover(_partitionRangesToRecover) {
}
