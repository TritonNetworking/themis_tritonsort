#include "Timer.h"

Timer::Timer() {
  startTime = 0;
  stopTime = 0;
  started = false;
}

void Timer::start() {
  startTime = Timer::posixTimeInMicros();
  started = true;
}

void Timer::stop() {
  stopTime = Timer::posixTimeInMicros();
  started = false;
}

uint64_t Timer::getElapsed() const {
  if (stopTime < startTime) {
    // If you stopped before you start, virtually no time has passed or your
    // clock skewed wildly; avoid returning enormous times here
    return 0;
  } else {
    return stopTime - startTime;
  }
}

uint64_t Timer::getStartTime() const {
  return startTime;
}

uint64_t Timer::getStopTime() const {
  return stopTime;
}

uint64_t Timer::getRunningTime() const {
  uint64_t curTime = Timer::posixTimeInMicros();
  if (curTime < startTime) {
    return 0;
  } else {
    return curTime - startTime;
  }
}

bool Timer::isRunning() const {
  return started;
}
