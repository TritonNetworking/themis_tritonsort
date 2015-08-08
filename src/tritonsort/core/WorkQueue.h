#ifndef TRITONSORT_WORK_QUEUE_H
#define TRITONSORT_WORK_QUEUE_H

#include <list>
#include <stdint.h>

class Resource;

/**
   Exports a queue abstraction but allows large amounts of work to be moved
   between queues in constant time.

   \warning All work units are Resource*'s, so the user must dynamic cast to
   ensure type safety.

   \warning Not thread safe!
 */
class WorkQueue {
public:
  /// Constructor
  WorkQueue();

  /// Push a work unit onto the queue
  /**
     If the work unit being pushed is NULL, this is interpreted as a signal
     that the queue will never receive additional work

     \param workUnit a pointer to the work unit to push
   */
  void push(Resource* workUnit);

  /// Remove the first work unit from the queue
  void pop();

  /// Get the first work unit from the queue
  /**
     \return the first work unit currently in the queue
   */
  Resource* front() const;

  /// Get the last work unit in the queue
  /**
     \return the last work unit currently in the queue
   */
  Resource* back() const;

  /// Get the size of the work queue
  /**
     \return the size of the work queue
   */
  uint64_t size() const;

  /// Get the total size of the work queue's work units in bytes
  uint64_t totalWorkSizeInBytes() const;

  /// Is the work queue empty?
  /**
     \return true if the work queue is empty, false otherwise
   */
  bool empty() const;

  /// Will this work queue receive more work?
  /**
     \return true if someone tried to enqueue NULL to this work queue in the
     past, indicating that we will never see any more work, and false otherwise
   */
  bool willNotReceiveMoreWork() const;

  /// Move all the work from this queue onto another queue
  /**
     Append the contents of this queue onto the provided queue. At the end of
     the operation, the size of this queue will be zero.

     \param destQueue the queue to which to move this queue's work
   */
  void moveWorkToQueue(WorkQueue& destQueue);

private:
  std::list<Resource*> work;
  bool noMoreWork;
  uint64_t _size;
  uint64_t totalBytes;
};

#endif // TRITONSORT_WORK_QUEUE_H
