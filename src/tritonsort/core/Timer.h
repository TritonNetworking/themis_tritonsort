#ifndef _TRITONSORT_TIMER_H
#define _TRITONSORT_TIMER_H


#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>

/// A basic timer implementation
/**
   When Timer::start is called, it records a time. When Timer::stop() is
   called, it records another time. Calling Timer::getElapsed() will produce
   the elapsed time between the calls to Timer::start and Timer::stop.

   This class is designed to encapsulate things like hrtimers and utime and to
   automatically deal with issues like clock skew between start() and stop().

   \todo (AR) Eventually all calls to utime() should be replaced with this
   class.
 */
class Timer {
public:
  /// Constructor
  Timer();

  /// Destructor
  virtual ~Timer() {}

  /// Start the timer
  void start();

  /// Stop the timer
  virtual void stop();

  /// Get the elapsed time in microseconds
  /**
     If, due to clock drift, the timer's start time is after its end time, this
     method returns 0 to avoid returning enormous elapsed times.

     \return elapsed time between Timer::start and Timer::stop.
   */
  virtual uint64_t getElapsed() const;

  /// Get start timestamp
  /**
     \return Time that Timer::start was called in microseconds since UNIX epoch.
   */
  uint64_t getStartTime() const;

  /// Get stop timestamp
  /**
     \return Time that Timer::stop was called in microseconds since UNIX epoch.
   */
  uint64_t getStopTime() const;

  /// Get time since Timer::start
  /**
     If, due to clock drift, the current time is less than the time at which
     Timer::start was called, this method returns 0.

     \return the amount of time in microseconds that has elapsed since
     Timer::start was called; allows for measuring time without repeatedly
     starting and stopping the timer.
   */
  uint64_t getRunningTime() const;

  /// Has the timer been started?
  /**
     \return true if Timer::start has been called and Timer::stop has not,
     false otherwise
   */
  bool isRunning() const;

  /// Get time since UNIX epoch in microseconds
  /**
     \return time since the UNIX epoch in microseconds
   */
  static inline uint64_t posixTimeInMicros() {
    struct timeval time;
    gettimeofday(&time, NULL);

    return (((uint64_t) time.tv_sec) * 1000000) + time.tv_usec;
  }

protected:
  /// Start timestamp (microsecond since UNIX epoch)
  uint64_t startTime;

  /// Stop timestamp (microsecond since UNIX epoch)
  uint64_t stopTime;

  /// Has the clock been started?
  bool started;
};

#endif // _TRITONSORT_TIMER_H
