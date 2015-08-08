#include "common/DummyWorkUnit.h"
#include "common/SimpleMemoryAllocator.h"
#include "core/Params.h"
#include "core/WorkerFactory.h"
#include "tests/themis_core/CountWorkUnit.h"
#include "tests/themis_core/MockWorkerTracker.h"
#include "tests/themis_core/TestWorkerImpls.h"
#include "tests/themis_core/WorkerTrackerTest.h"

void WorkerTrackerTest::testMultiSource() {
  /**
     emitter_1 emits a certain number of work units that countdown counts. When
     all of emitter 1's work units are emitted (since it's a source tracker),
     it will send countdown the "no more work" signal. Countdown should tell
     emitter_2 to spawn, receive a number of work units from it, and then shut
     down, emitting (2 * num work units) as its final work unit count.
   */

  Params params;

  uint64_t numCores = 16;

  uint64_t workUnitsToEmit = 3;
  uint64_t numEmitters = 1;

  uint64_t countdownNumber = workUnitsToEmit * numEmitters;
  uint64_t numCountdowns = 1;

  params.add<uint64_t>("CORES_PER_NODE", numCores);
  params.add<uint64_t>("NUM_WORKERS.test.emitter_1", numEmitters);
  params.add<uint64_t>("NUM_WORKERS.test.emitter_2", numEmitters);
  params.add<uint64_t>("NUM_WORKERS.test.countdown", numCountdowns);

  params.add<uint64_t>("EMITTER_WORK_UNIT_COUNT",
                       workUnitsToEmit);
  params.add<uint64_t>("COUNTDOWN_NUMBER", countdownNumber);
  params.add<std::string>("WORKER_IMPLS.test.emitter_1", "Emitter");
  params.add<std::string>("WORKER_IMPLS.test.emitter_2", "Emitter");
  params.add<std::string>("WORKER_IMPLS.test.countdown", "Countdown");

  WorkerTracker emitter1Tracker(params, "test", "emitter_1");
  WorkerTracker emitter2Tracker(params, "test", "emitter_2");
  WorkerTracker countdownTracker(params, "test", "countdown");
  MockWorkerTracker sinkTracker("sink");

  CPUAffinitySetter cpuAffinitySetter(params, "test");

  TestWorkerImpls testWorkerImpls;

  SimpleMemoryAllocator memoryAllocator;

  WorkerFactory workerFactory(params, cpuAffinitySetter, memoryAllocator);
  workerFactory.addDependency("tracker_to_spawn", &emitter2Tracker);
  workerFactory.registerImpls("test_worker", testWorkerImpls);

  emitter1Tracker.isSourceTracker();
  emitter1Tracker.addDownstreamTracker(&countdownTracker);
  emitter1Tracker.setFactory(&workerFactory, "test_worker");

  emitter2Tracker.isSourceTracker();
  emitter2Tracker.addDownstreamTracker(&countdownTracker);
  emitter2Tracker.setFactory(&workerFactory, "test_worker");

  countdownTracker.setFactory(&workerFactory, "test_worker");
  countdownTracker.addDownstreamTracker(&sinkTracker);

  emitter1Tracker.createWorkers();
  emitter2Tracker.createWorkers();
  countdownTracker.createWorkers();

  emitter1Tracker.spawn();

  emitter1Tracker.waitForWorkersToFinish();
  emitter2Tracker.waitForWorkersToFinish();
  countdownTracker.waitForWorkersToFinish();

  const std::queue<Resource*>& outWorkUnits = sinkTracker.getWorkQueue();

  CPPUNIT_ASSERT_EQUAL((size_t) 1, outWorkUnits.size());
  CountWorkUnit* workUnit = dynamic_cast<CountWorkUnit*>(outWorkUnits.front());
  CPPUNIT_ASSERT_MESSAGE("Got NULL or incorrectly typed work unit",
                         workUnit != NULL);
  CPPUNIT_ASSERT_EQUAL(numEmitters * workUnitsToEmit * 2, workUnit->count);

  sinkTracker.deleteAllWorkUnits();

  emitter1Tracker.destroyWorkers();
  emitter2Tracker.destroyWorkers();
  countdownTracker.destroyWorkers();
}

WorkerTracker* WorkerTrackerTest::setupTrackerForQueueingTests(
  CPUAffinitySetter*& cpuAffinitySetter,
  SimpleMemoryAllocator*& memoryAllocator) {

  Params params;
  uint64_t numCores = 16;
  uint64_t numItemHolders = 4;

  params.add<uint64_t>("CORES_PER_NODE", numCores);
  params.add<uint64_t>("NUM_WORKERS.test.item_holder", numItemHolders);
  params.add<std::string>(
    "WORKER_IMPLS.test.item_holder", "ManualConsumerWorker");

  WorkerTracker* itemHolderTracker = new WorkerTracker(
    params, "test", "item_holder");

  cpuAffinitySetter = new CPUAffinitySetter(params, "test");

  memoryAllocator = new SimpleMemoryAllocator();

  TestWorkerImpls testWorkerImpls;

  WorkerFactory workerFactory(params, *cpuAffinitySetter, *memoryAllocator);
  workerFactory.registerImpls("test_worker", testWorkerImpls);

  itemHolderTracker->isSourceTracker();
  itemHolderTracker->setFactory(&workerFactory, "test_worker");

  itemHolderTracker->createWorkers();

  return itemHolderTracker;
}

void WorkerTrackerTest::testDefaultWorkQueueingPolicy() {
  WorkerTracker* itemHolderTracker = NULL;
  CPUAffinitySetter* cpuAffinitySetter = NULL;
  SimpleMemoryAllocator* memoryAllocator = NULL;

  itemHolderTracker = setupTrackerForQueueingTests(
    cpuAffinitySetter, memoryAllocator);

  WorkerTracker::WorkerVector& workers = itemHolderTracker->getWorkers();

  // Even though there are multiple item holder workers, the tracker should
  // have a single global queue.
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());

  // If the first worker takes all the work units it can, it will get them all.
  std::vector<uint64_t> expectedQueueSizes;
  expectedQueueSizes.push_back(7);
  expectedQueueSizes.push_back(0);
  expectedQueueSizes.push_back(0);
  expectedQueueSizes.push_back(0);

  checkQueueSizes(workers, expectedQueueSizes);
  clearAllWorkerQueues(workers);
  expectedQueueSizes.clear();

  // If, however, some other worker, say the last, takes all the work units,
  // the first won't get any.
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());
  itemHolderTracker->addWorkUnit(new DummyWorkUnit());

  static_cast<ManualConsumerWorker*>(workers[3])->getAllWorkFromTracker();

  expectedQueueSizes.push_back(0);
  expectedQueueSizes.push_back(0);
  expectedQueueSizes.push_back(0);
  expectedQueueSizes.push_back(7);

  checkQueueSizes(workers, expectedQueueSizes);
  clearAllWorkerQueues(workers);
  expectedQueueSizes.clear();

  delete memoryAllocator;
  delete cpuAffinitySetter;
  itemHolderTracker->destroyWorkers();
  delete itemHolderTracker;
}

void WorkerTrackerTest::testMultiDestination() {
  Params params;

  uint64_t numCores = 16;
  uint64_t numWorkers = 1;

  params.add<uint64_t>("CORES_PER_NODE", numCores);
  params.add<uint64_t>("NUM_WORKERS.test.multi_dest", numWorkers);
  params.add<std::string>(
    "WORKER_IMPLS.test.multi_dest", "MultiDestinationWorker");

  WorkerTracker multiDestinationTracker(params, "test", "multi_dest");
  MockWorkerTracker defaultSinkTracker("default_sink");
  MockWorkerTracker redSinkTracker("red_sink");
  MockWorkerTracker blueSinkTracker("blue_sink");
  MockWorkerTracker greenSinkTracker("green_sink");

  TestWorkerImpls testWorkerImpls;
  SimpleMemoryAllocator memoryAllocator;

  CPUAffinitySetter cpuAffinitySetter(params, "test");

  WorkerFactory workerFactory(params, cpuAffinitySetter, memoryAllocator);
  workerFactory.registerImpls("test_worker", testWorkerImpls);

  multiDestinationTracker.isSourceTracker();
  multiDestinationTracker.addDownstreamTracker(&defaultSinkTracker);
  multiDestinationTracker.addDownstreamTracker(&redSinkTracker, "red");
  multiDestinationTracker.addDownstreamTracker(&blueSinkTracker, "blue");
  multiDestinationTracker.addDownstreamTracker(&greenSinkTracker, "green");

  multiDestinationTracker.setFactory(&workerFactory, "test_worker");

  multiDestinationTracker.createWorkers();

  multiDestinationTracker.addWorkUnit(new StringWorkUnit("green"));
  multiDestinationTracker.addWorkUnit(new StringWorkUnit("red"));
  multiDestinationTracker.addWorkUnit(new StringWorkUnit("blam"));
  multiDestinationTracker.addWorkUnit(new StringWorkUnit("red"));
  multiDestinationTracker.addWorkUnit(new StringWorkUnit("blue"));
  multiDestinationTracker.addWorkUnit(new StringWorkUnit("ham"));
  multiDestinationTracker.addWorkUnit(new StringWorkUnit("green"));
  multiDestinationTracker.addWorkUnit(new StringWorkUnit("spam"));
  multiDestinationTracker.addWorkUnit(new StringWorkUnit("blue"));
  multiDestinationTracker.addWorkUnit(new StringWorkUnit("green"));

  multiDestinationTracker.spawn();

  multiDestinationTracker.waitForWorkersToFinish();

  checkSinkTrackerQueue(redSinkTracker, 2, "red");
  checkSinkTrackerQueue(blueSinkTracker, 2, "blue");
  checkSinkTrackerQueue(greenSinkTracker, 3, "green");

  std::queue<Resource*> defaultWorkQueue(defaultSinkTracker.getWorkQueue());

  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(3), defaultWorkQueue.size());

  CPPUNIT_ASSERT_EQUAL(
    std::string("blam"), dynamic_cast<StringWorkUnit*>(
      defaultWorkQueue.front())->getString());
  defaultWorkQueue.pop();
  CPPUNIT_ASSERT_EQUAL(
    std::string("ham"), dynamic_cast<StringWorkUnit*>(
      defaultWorkQueue.front())->getString());
  defaultWorkQueue.pop();
  CPPUNIT_ASSERT_EQUAL(
    std::string("spam"), dynamic_cast<StringWorkUnit*>(
      defaultWorkQueue.front())->getString());
  defaultWorkQueue.pop();

  defaultSinkTracker.deleteAllWorkUnits();
  multiDestinationTracker.destroyWorkers();
}

void WorkerTrackerTest::checkSinkTrackerQueue(
  MockWorkerTracker& tracker, uint64_t size,
  const std::string& expectedString) {

  // Have to copy-construct so that we can pop from it
  std::queue<Resource*> workQueue(tracker.getWorkQueue());

  CPPUNIT_ASSERT_EQUAL(size, workQueue.size());

  while (!workQueue.empty()) {
    StringWorkUnit* workUnit = dynamic_cast<StringWorkUnit*>(workQueue.front());

    CPPUNIT_ASSERT_MESSAGE("Encountered unexpected NULL work unit "
                           "in work queue", workUnit != NULL);

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Work unit ended up in an incorrect queue for its string",
      expectedString, workUnit->getString());

    workQueue.pop();
  }

  tracker.deleteAllWorkUnits();
}


void WorkerTrackerTest::checkQueueSizes(
  WorkerTracker::WorkerVector& workers,
  std::vector<uint64_t>& expectedQueueSizes) {

  for (uint64_t i = 0; i < workers.size(); i++) {
    static_cast<ManualConsumerWorker*>(workers[i])->getAllWorkFromTracker();
    CPPUNIT_ASSERT_EQUAL(
      expectedQueueSizes[i],
      static_cast<ManualConsumerWorker*>(workers[i])->workUnitQueueSize());
  }
}

void WorkerTrackerTest::clearAllWorkerQueues(
  WorkerTracker::WorkerVector& workers) {

  workers.beginThreadSafeIterate();
  for (WorkerTracker::WorkerVector::iterator iter = workers.begin();
       iter != workers.end(); iter++) {
    static_cast<ManualConsumerWorker*>(*iter)->clearWorkUnitQueue();
  }
  workers.endThreadSafeIterate();
}
