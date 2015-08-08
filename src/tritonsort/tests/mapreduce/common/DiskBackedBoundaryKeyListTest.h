#ifndef THEMIS_DISK_BACKED_BOUNDARY_KEY_LIST_TEST_H
#define THEMIS_DISK_BACKED_BOUNDARY_KEY_LIST_TEST_H

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/TestSuite.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestCaller.h>

class DiskBackedBoundaryKeyList;

class DiskBackedBoundaryKeyListTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( DiskBackedBoundaryKeyListTest );
  CPPUNIT_TEST( testLoadBoundaryKeys );
  CPPUNIT_TEST( testPersistAcrossReinstantiation );
  CPPUNIT_TEST( testPartitionBoundaryRange );
  CPPUNIT_TEST_SUITE_END();
public:
  void testLoadBoundaryKeys();
  void testPersistAcrossReinstantiation();
  void testPartitionBoundaryRange();
private:
  DiskBackedBoundaryKeyList* newTestKeyList(uint64_t numPartitions);
  void validateTestKeyList(
    DiskBackedBoundaryKeyList* boundaryKeyList, uint64_t numPartitions);
};

#endif // THEMIS_DISK_BACKED_BOUNDARY_KEY_LIST_TEST_H
