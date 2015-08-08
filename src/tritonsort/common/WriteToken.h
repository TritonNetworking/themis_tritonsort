#ifndef TRITONSORT_WRITE_TOKEN_H
#define TRITONSORT_WRITE_TOKEN_H

#include "core/Resource.h"

/**
   A write token is a convenient way to indicate the availability of workers
   between two stages that are separated by a number of intermediate stages.

   In our particular instance, in TritonSort and MapReduce the chainer and the
   writer are separated by the coalescer. The chainer needs to be able to know
   when to emit a list of LD buffers to the writer, and to which writer. If it
   were to operate blindly, the chainer would just emit long chains at random,
   causing the queues on some disks to be longer than others and many LD
   buffers to be stuck in queues when we could be doing useful work with them.

   To solve this problem, the chainer grabs a token that it passes through the
   coalescer to the writer. The writer returns the token when it's done with
   its write; this serves as the notification to the chainer that the writer
   has some spare capacity on one of its disks. By segmenting these tokens by
   disk in a ResourceMultiPool, the chainer can request a token for a collection
   of disks and receive a token for one of the available ones.
 */
class WriteToken : public Resource {
public:
  /// Constructor
  /**
     \param _diskID the ID of the disk to which this token belongs
   */
  WriteToken(uint64_t _diskID) : diskID(_diskID) {}

  /**
     \return the ID of the disk to which this token belongs
   */
  uint64_t getDiskID() const {
    return diskID;
  }

  /// \sa Resource::getCurrentSize
  uint64_t getCurrentSize() const {
    return 0;
  }

private:
  const uint64_t diskID;
};

#endif // TRITONSORT_WRITE_TOKEN_H
