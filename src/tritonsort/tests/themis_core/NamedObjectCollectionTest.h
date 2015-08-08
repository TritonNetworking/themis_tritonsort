#ifndef TRITONSORT_NAMED_OBJECT_COLLECTION_TEST_H
#define TRITONSORT_NAMED_OBJECT_COLLECTION_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

class NamedObjectCollectionTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( NamedObjectCollectionTest );
  CPPUNIT_TEST( testGloballyScopedObject );
  CPPUNIT_TEST( testScopedObject );
  CPPUNIT_TEST( testObjectInGlobalScopeButNotInLocalScope );
  CPPUNIT_TEST_SUITE_END();

public:
  void testGloballyScopedObject();
  void testScopedObject();
  void testObjectInGlobalScopeButNotInLocalScope();
};

#endif // TRITONSORT_NAMED_OBJECT_COLLECTION_TEST_H
