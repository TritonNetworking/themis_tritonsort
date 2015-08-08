#include "CumulativeTimer.h"

CumulativeTimer::CumulativeTimer() {
  reset();
}

void CumulativeTimer::stop() {
  Timer::stop();

  if (stopTime >= startTime) {
    totalElapsed += stopTime - startTime;
  }
}

void CumulativeTimer::reset() {
  totalElapsed = 0;
  startTime = 0;
  stopTime = 0;
}

uint64_t CumulativeTimer::getElapsed() const {
  return totalElapsed;
}
