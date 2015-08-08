#include <boost/filesystem.hpp>

#include "core/Params.h"
#include "mapreduce/common/RecoveryInfo.h"
#include "mapreduce/common/boundary/DiskBackedBoundaryKeyList.h"
#include "mapreduce/common/filter/RecordFilter.h"
#include "mapreduce/common/filter/RecordFilterMap.h"
#include "tests/mapreduce/common/MockCoordinatorClient.h"
#include "tests/mapreduce/common/RecordFilterTest.h"

extern const char* TEST_WRITE_ROOT;

void RecordFilterTest::testLoadFromMap() {
  uint64_t recoveringJobID = 42;
  uint64_t jobID = 44;
  uint64_t numPartitions = 10;

  Params params;
  params.add<std::string>("COORDINATOR_CLIENT", "redis");

  boost::filesystem::path boundaryListPath(
    boost::filesystem::path(TEST_WRITE_ROOT) / "record_filter_test.boundaries");

  std::string boundaryListFilename(boundaryListPath.string());

  if (fileExists(boundaryListFilename)) {
    unlink(boundaryListFilename.c_str());
  }

  params.add<std::string>("DISK_BACKED_BOUNDARY_LIST.42", boundaryListFilename);

  // Populate boundary key list for failed job
  DiskBackedBoundaryKeyList* boundaryList =
    DiskBackedBoundaryKeyList::newBoundaryKeyList(
      params, recoveringJobID, numPartitions);

  for (uint64_t i = 0; i < numPartitions; i++) {
    uint8_t key[1];
    memset(key, i + 1, 1);

    boundaryList->addBoundaryKey(key, 1);
  }

  delete boundaryList;

  // Make sure coordinator client will give back appropriate recovery
  // information

  std::list< std::pair<uint64_t, uint64_t> > partitionsToRecover;
  partitionsToRecover.push_back(std::make_pair<uint64_t, uint64_t>(1, 5));
  partitionsToRecover.push_back(std::make_pair<uint64_t, uint64_t>(7, 9));

  RecoveryInfo* recoveryInfo = new RecoveryInfo(
    recoveringJobID, partitionsToRecover);

  MockCoordinatorClient* mockCoordinatorClient = new MockCoordinatorClient();
  mockCoordinatorClient->setRecoveryInfo(jobID, recoveryInfo);

  RecordFilterMap recordFilterMap(params, mockCoordinatorClient);

  const RecordFilter* nullRecordFilter = recordFilterMap.get(recoveringJobID);
  CPPUNIT_ASSERT(nullRecordFilter == NULL);

  const RecordFilter* recordFilter = recordFilterMap.get(jobID);
  CPPUNIT_ASSERT(recordFilter != NULL);

  uint8_t keyInFirstRange[3] = {3, 1, 2};
  uint8_t keyBetweenFirstAndSecondRange[1] = {7};
  uint8_t keyInSecondRange[4] = {8, 0xff, 0xff, 0xff};
  uint8_t firstKeyInThirdRange[1] = {9};
  uint8_t secondKeyInThirdRange[4] = {0xff, 0xff, 0xff, 0xff};

  CPPUNIT_ASSERT(recordFilter->pass(keyInFirstRange, 3));
  CPPUNIT_ASSERT(!recordFilter->pass(keyBetweenFirstAndSecondRange, 1));
  CPPUNIT_ASSERT(recordFilter->pass(keyInSecondRange, 4));
  CPPUNIT_ASSERT(recordFilter->pass(firstKeyInThirdRange, 1));
  CPPUNIT_ASSERT(recordFilter->pass(secondKeyInThirdRange, 4));
}
