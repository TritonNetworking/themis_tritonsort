#include "TrackerSet.h"
#include "WorkerTrackerInterface.h"

TrackerSet::~TrackerSet() {
}

void TrackerSet::spawn() {
  for (TrackerList::iterator iter = sourceTrackers.begin();
       iter != sourceTrackers.end(); iter++) {
    (*iter)->spawn();
  }
}

void TrackerSet::addTracker(WorkerTrackerInterface* tracker, bool source) {
  trackers.push_back(tracker);

  if (source) {
    sourceTrackers.push_back(tracker);
  }
}

void TrackerSet::createWorkers() {
  for (TrackerList::iterator iter = trackers.begin(); iter != trackers.end();
       iter++) {
    (*iter)->createWorkers();
  }
}

void TrackerSet::waitForWorkersToFinish() {
  for (TrackerList::iterator iter = trackers.begin(); iter != trackers.end();
       iter++) {
    (*iter)->waitForWorkersToFinish();
  }
}

void TrackerSet::destroyWorkers() {
  for (TrackerList::iterator iter = trackers.begin(); iter != trackers.end();
       iter++) {
    (*iter)->destroyWorkers();
  }
}

const TrackerSet::TrackerList& TrackerSet::getTrackerList() const {
  return trackers;
}
