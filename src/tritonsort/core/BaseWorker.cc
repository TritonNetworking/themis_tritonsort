#include <pthread.h>
#include <stdexcept>

#include "core/BaseWorker.h"
#include "core/IntervalStatLogger.h"
#include "core/Resource.h"
#include "core/ResourceMonitor.h"
#include "core/ScopedLock.h"

BaseWorker::BaseWorker(uint64_t _id, const std::string& _name)
    : themis::Thread(static_cast<std::ostringstream&>(
             std::ostringstream().flush() << _name << " " << _id).str()),
    id(_id),
    tracker(NULL),
    workUnitsConsumed(0),
    bytesConsumed(0),
    name(_name),
    idle(false),
    logger(name, id),
    workUnitsProduced(0),
    bytesProduced(0),
    pipelineSaturated(false) {
  // TODO: thresholds should be user-configurable somehow
  runTimeStatID = logger.registerSummaryStat("runtime");
  waitTimeStatID = logger.registerSummaryStat("wait");
  pipelineSaturationWaitTimeStatID =
    logger.registerSummaryStat("pipeline_saturation_wait");
  inputSizeStatID = logger.registerHistogramStat("input_size", 1000);
  queueSaturationBlockTimeStatID = logger.registerSummaryStat(
    "queue_saturation_block_time");
  memoryAllocatorWaitTimeStatID = logger.registerSummaryStat(
    "allocation_wait_time");
}

BaseWorker::~BaseWorker() {
  logger.logDatum("work_units_produced", workUnitsProduced);
  logger.logDatum("work_units_consumed", workUnitsConsumed);
  logger.logDatum("bytes_produced", bytesProduced);
  logger.logDatum("bytes_consumed", bytesConsumed);
}

StatLogger* BaseWorker::initIntervalStatLogger() {
  // By default, don't log any interval stats
  return NULL;
}

void BaseWorker::logIntervalStats(StatLogger& intervalLogger) const {
  // No-op
}

void BaseWorker::startMemoryAllocationTimer() {
  memoryAllocationWaitTimer.start();
}

void BaseWorker::stopMemoryAllocationTimer() {
  memoryAllocationWaitTimer.stop();
  logger.add(memoryAllocatorWaitTimeStatID, memoryAllocationWaitTimer);
}

void BaseWorker::registerWithResourceMonitor() {
  ResourceMonitor::registerClient(this, getName().c_str());
}

void BaseWorker::unregisterWithResourceMonitor() {
  ResourceMonitor::unregisterClient(this);
}

void BaseWorker::resourceMonitorOutput(Json::Value& obj) {
  obj["type"] = "worker";
  obj["id"] = Json::UInt64(this->getID());
  obj["work_units_produced"] = Json::UInt64(workUnitsProduced);
  obj["work_units_consumed"] = Json::UInt64(workUnitsConsumed);
  obj["bytes_produced"] = Json::UInt64(bytesProduced);
  obj["bytes_consumed"] = Json::UInt64(bytesConsumed);
}

void BaseWorker::spawn() {
  ABORT_IF(cpuAffinitySetter == NULL,
           "Must set a CPUAffinitySetter before spawning");

  // Start mainLoop() in a separate thread.
  themis::Thread::startThread();

  // Set the CPU affinity of the mainLoop() thread
  cpu_set_t cpuAffinityMask;

  cpuAffinitySetter->setAffinityMask(getName(), id, cpuAffinityMask);

  logCPUAffinityMask(
    getName(), id, cpuAffinityMask, cpuAffinitySetter->getNumCores());

  int status = pthread_setaffinity_np(
      themis::Thread::threadID, sizeof(cpu_set_t), &cpuAffinityMask);

  if (status != 0) {
    ABORT("Setting CPU affinity failed: error %d: %s", status,
          strerror(status));
  }
}

void BaseWorker::setTracker(WorkerTrackerInterface* workerTracker) {
  tracker = workerTracker;
}

uint64_t BaseWorker::addDownstreamTrackerReturningID(
  WorkerTrackerInterface* downstreamTracker) {

  uint64_t trackerID = downstreamTrackers.size();
  downstreamTrackers.push_back(downstreamTracker);

  return trackerID;
}

void BaseWorker::addDownstreamTracker(
  WorkerTrackerInterface* downstreamTracker) {

  addDownstreamTrackerReturningID(downstreamTracker);
}

void BaseWorker::addDownstreamTracker(
  WorkerTrackerInterface* downstreamTracker,
  const std::string& trackerDescription) {

  uint64_t trackerID = addDownstreamTrackerReturningID(downstreamTracker);
  handleNamedDownstreamTracker(trackerDescription, trackerID);
}

void BaseWorker::copyTrackerInformation(BaseWorker& inheritingWorker) {
  inheritingWorker.tracker = tracker;
  inheritingWorker.downstreamTrackers.clear();
  inheritingWorker.downstreamTrackers.assign(
    downstreamTrackers.begin(), downstreamTrackers.end());
}

void BaseWorker::handleNamedDownstreamTracker(
  const std::string& trackerDescription, uint64_t trackerID) {

  // Workers that want to handle this have to override this method
  ABORT("You must override BaseWorker::handleNamedDownstreamTracker to "
        "handle trackers with custom names");
}

void BaseWorker::setCPUAffinitySetter(CPUAffinitySetter& cpuAffinitySetter) {
  this->cpuAffinitySetter = &cpuAffinitySetter;
}

void BaseWorker::init() {
  // No init procedure defined by default
}

/// Perform any state cleanup necessary.
void BaseWorker::teardown() {
  // No teardown procedure defined by default
}

/// \return the number of work units consumed by this worker
uint64_t BaseWorker::numWorkUnitsConsumed() const {
  return workUnitsConsumed;
}

bool BaseWorker::isIdle() const {
  return idle;
}

void BaseWorker::startWaitForWorkTimer() {
  idle = true;
  if (!waitForWorkTimer.isRunning()) {
    // Only start the time if it's not currently runing. This allows workers to
    // make consecutive startWaitForWorkTimer() calls without resetting the
    // timer.
    waitForWorkTimer.start();
  }
}

void BaseWorker::stopWaitForWorkTimer() {
  idle = false;
  if (waitForWorkTimer.isRunning()) {
    waitForWorkTimer.stop();

    if (pipelineSaturated) {
      // Log this wait as a regular wait time
      logger.add(waitTimeStatID, waitForWorkTimer);
    } else {
      // The first wait is special because it represents the time spent waiting
      // for the pipeline to saturate.
      logger.add(pipelineSaturationWaitTimeStatID, waitForWorkTimer);
      // Log the actual time this worker starts processing useful work.
      logger.logDatum("worker_start_time", Timer::posixTimeInMicros());
      pipelineSaturated = true;
    }
  }
}

void BaseWorker::startEnqueueTimer() {
  idle = true;
  if (!queueSaturationBlockTimer.isRunning()) {
    queueSaturationBlockTimer.start();
  }
}

void BaseWorker::stopEnqueueTimer() {
  idle = false;
  if (queueSaturationBlockTimer.isRunning()) {
    queueSaturationBlockTimer.stop();
    logger.add(queueSaturationBlockTimeStatID, queueSaturationBlockTimer);
  }
}

void BaseWorker::startRuntimeTimer() {
  if (!runTimer.isRunning()) {
    runTimer.start();
  }
}

void BaseWorker::stopRuntimeTimer() {
  if (runTimer.isRunning()) {
    runTimer.stop();
  }
}

void BaseWorker::logWorkerType(const std::string& workerType) {
  logger.logDatum("worker_type", workerType);
}

const std::string& BaseWorker::getName() const {
  return name;
}

uint64_t BaseWorker::getID() const {
  return id;
}

void BaseWorker::logInputSize(uint64_t inputSize) {
  logger.add(inputSizeStatID, inputSize);
}

void BaseWorker::logRuntimeInformation() {
  logger.add(runTimeStatID, runTimer);
  runTimer.reset();
}

void BaseWorker::resetRuntimeInformation() {
}

void BaseWorker::mainLoop(bool testing) {
  bool done = false;

  try {
    initTimer.start();
    init();
    initTimer.stop();
  } catch (std::bad_alloc exception) {
    ABORT("Ran out of memory while initializing %s %llu", getName().c_str(),
          getID());
  }

  // Register with resource monitor and interval logger now that the worker is
  // initialized properly.
  registerWithResourceMonitor();
  IntervalStatLogger::registerClient(this);

  logger.logDatum("init", initTimer);

  totalRuntimeTimer.start();
  while (!done) {
    done = processIncomingWorkUnits();
  }
  totalRuntimeTimer.stop();
  logger.logDatum("worker_runtime", totalRuntimeTimer);

  teardownTimer.start();

  // Un-register the worker before calling teardown for safety.
  unregisterWithResourceMonitor();
  IntervalStatLogger::unregisterClient(this);

  teardown();
  teardownTimer.stop();
  logger.logDatum("teardown", teardownTimer);
  // Log the actual time this worker stops processing useful work (including
  // teardown), but only if we actually got a work unit.
  if (pipelineSaturated) {
    logger.logDatum("worker_stop_time", Timer::posixTimeInMicros());
  }

  if (!testing) {
    tracker->notifyWorkerCompleted(id);
  }

  // For the purposes of checking for deadlock, consider this worker as waiting
  // for work, since it cannot make forward progress anymore.
  idle = true;
}

void BaseWorker::waitForWorkerToFinish() {
  themis::Thread::stopThread();
}


void BaseWorker::emitWorkUnit(
  uint64_t downstreamTrackerID, Resource* workUnit) {

  WorkerTrackerInterface* tracker = NULL;

  try {
    tracker = downstreamTrackers.at(downstreamTrackerID);
  } catch (std::out_of_range& exception) {
    ABORT("Downstream tracker ID %llu invalid (out-of-bounds)",
          downstreamTrackerID);
  }

  bytesProduced += workUnit->getCurrentSize();

  startEnqueueTimer();
  tracker->addWorkUnit(workUnit);
  stopEnqueueTimer();

  workUnitsProduced++;
}

void BaseWorker::setIdle(bool _idle) {
  idle = _idle;
}

void BaseWorker::emitWorkUnit(Resource* workUnit) {
  emitWorkUnit(0, workUnit);
}

void BaseWorker::logConsumed(Resource* workUnit) {
  bytesConsumed += workUnit->getCurrentSize();
  workUnitsConsumed++;
}

void BaseWorker::logConsumed(uint64_t numWorkUnits, uint64_t numBytes) {
  bytesConsumed += numBytes;
  workUnitsConsumed += numWorkUnits;
}

void* BaseWorker::thread(void* args) {
  mainLoop();
  return NULL;
}
