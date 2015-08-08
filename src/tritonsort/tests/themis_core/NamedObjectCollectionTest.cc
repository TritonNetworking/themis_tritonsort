#include "NamedObjectCollectionTest.h"
#include "common/DummyWorkUnit.h"
#include "core/NamedObjectCollection.h"

void NamedObjectCollectionTest::testGloballyScopedObject() {
  NamedObjectCollection collection;

  DummyWorkUnit workUnit;

  collection.add("dummy", &workUnit);
  CPPUNIT_ASSERT(collection.contains<DummyWorkUnit>("dummy"));
  CPPUNIT_ASSERT_EQUAL(collection.get<DummyWorkUnit>("dummy"), &workUnit);
}

void NamedObjectCollectionTest::testScopedObject() {
  NamedObjectCollection collection;

  DummyWorkUnit workUnit;

  collection.add("some_scope", "dummy", &workUnit);

  CPPUNIT_ASSERT(collection.contains<DummyWorkUnit>(
                   "some_scope", "dummy"));
  CPPUNIT_ASSERT_EQUAL(collection.get<DummyWorkUnit>("some_scope", "dummy"),
                       &workUnit);

  // "dummy" doesn't exist in global scope, so this should fail.
  CPPUNIT_ASSERT(!collection.contains<DummyWorkUnit>("dummy"));
  CPPUNIT_ASSERT_THROW(collection.get<DummyWorkUnit>("dummy"),
                       AssertionFailedException);
}

void NamedObjectCollectionTest::testObjectInGlobalScopeButNotInLocalScope() {
  NamedObjectCollection collection;

  DummyWorkUnit workUnit;

  collection.add("dummy", &workUnit);

  CPPUNIT_ASSERT_EQUAL(collection.get<DummyWorkUnit>(
                         "nonexistant_scope", "dummy"),
                       &workUnit);
}


