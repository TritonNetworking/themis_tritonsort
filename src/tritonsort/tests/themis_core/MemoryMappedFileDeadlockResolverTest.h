#ifndef THEMIS_MEMORY_MAPPED_FILE_DEADLOCK_RESOLVER_TEST_H
#define THEMIS_MEMORY_MAPPED_FILE_DEADLOCK_RESOLVER_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class MemoryMappedFileDeadlockResolverTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( MemoryMappedFileDeadlockResolverTest );
  CPPUNIT_TEST( testResolve );
  CPPUNIT_TEST( testBadDeallocate );
  CPPUNIT_TEST_SUITE_END();

public:
  void testResolve();
  void testBadDeallocate();
};

#endif // THEMIS_MEMORY_MAPPED_FILE_DEADLOCK_RESOLVER_TEST_H
