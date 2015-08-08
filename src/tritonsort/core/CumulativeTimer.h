#ifndef TRITONSORT_CUM_TIMER_H
#define TRITONSORT_CUM_TIMER_H

#include "Timer.h"

/**
   A timer that can be stopped multiple times and keeps track of the total
amount of elapsed time recorded.
*/

class CumulativeTimer : public Timer {
public:
  /// Constructor
  CumulativeTimer();

  /// Stops the timer but does not reset the total elapsed time
  void stop();

  /// Resets the timer, resetting the total elapsed time as well.
  void reset();

  // Get the total elapsed time recorded by the timer since it was last reset.
  /**
     \return the total elapsed time in microseconds
  */
  uint64_t getElapsed() const;
private:

  /// The total elapsed time in microseconds since the timer was reset.
  uint64_t totalElapsed;
};

#endif // TRITONSORT_CUM_TIMER_H
