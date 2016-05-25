#include "NamedObjectCollectionTest.h"
#include "common/DummyWorkUnit.h"
#include "core/NamedObjectCollection.h"

TEST_F(NamedObjectCollectionTest, testGloballyScopedObject) {
  NamedObjectCollection collection;

  DummyWorkUnit workUnit;

  collection.add("dummy", &workUnit);
  EXPECT_TRUE(collection.contains<DummyWorkUnit>("dummy"));
  EXPECT_EQ(collection.get<DummyWorkUnit>("dummy"), &workUnit);
}

TEST_F(NamedObjectCollectionTest, testScopedObject) {
  NamedObjectCollection collection;

  DummyWorkUnit workUnit;

  collection.add("some_scope", "dummy", &workUnit);

  EXPECT_TRUE(collection.contains<DummyWorkUnit>("some_scope", "dummy"));
  EXPECT_EQ(collection.get<DummyWorkUnit>("some_scope", "dummy"), &workUnit);

  // "dummy" doesn't exist in global scope, so this should fail.
  EXPECT_TRUE(!collection.contains<DummyWorkUnit>("dummy"));
  ASSERT_THROW(collection.get<DummyWorkUnit>("dummy"),
               AssertionFailedException);
}

TEST_F(NamedObjectCollectionTest, testObjectInGlobalScopeButNotInLocalScope) {
  NamedObjectCollection collection;

  DummyWorkUnit workUnit;

  collection.add("dummy", &workUnit);

  EXPECT_EQ(collection.get<DummyWorkUnit>("nonexistant_scope", "dummy"),
            &workUnit);
}
