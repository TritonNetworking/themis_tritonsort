#include "mapreduce/common/provenance/SourceFileRangesSet.h"
#include "tests/mapreduce/common/provenance/SourceFileRangesSetTest.h"

void SourceFileRangesSetTest::testMergeRanges() {
  SourceFileRangesSet s;

  s.add(0, 0, 10);
  s.add(0, 10, 30);
  s.add(0, 50, 75);

  SourceFileRangesSet expected;
  expected.add(0, 0, 30);
  expected.add(0, 50, 75);

  CPPUNIT_ASSERT(s.equals(expected));
}

void SourceFileRangesSetTest::testMarshal() {
  SourceFileRangesSet s;

  s.add(0, 0, 30);
  s.add(0, 50, 75);
  s.add(5, 20, 21);
  s.add(5, 99, 999);
  s.add(5, 1050, 1075);

  uint8_t* memory = NULL;
  uint64_t length;

  s.marshal(memory, length);

  CPPUNIT_ASSERT(memory != NULL);

  SourceFileRangesSet* demarshaled = SourceFileRangesSet::demarshal(
    memory, length);

  CPPUNIT_ASSERT(s.equals(*demarshaled));
  CPPUNIT_ASSERT(demarshaled->equals(s));
}

void SourceFileRangesSetTest::testMergeSets() {
  SourceFileRangesSet s1;
  s1.add(0, 0, 30);
  s1.add(0, 50, 75);
  s1.add(0, 80, 100);
  s1.add(1, 30, 40);
  s1.add(2, 30, 60);

  SourceFileRangesSet s2;
  s2.add(0, 30, 50);
  s2.add(1, 20, 30);
  s2.add(1, 50, 60);
  s2.add(3, 25, 75);

  SourceFileRangesSet expected;
  expected.add(0, 0, 75);
  expected.add(0, 80, 100);
  expected.add(1, 20, 40);
  expected.add(1, 50, 60);
  expected.add(2, 30, 60);
  expected.add(3, 25, 75);

  s1.merge(s2);

  CPPUNIT_ASSERT(expected.equals(s1));
  CPPUNIT_ASSERT(s1.equals(expected));
}

void SourceFileRangesSetTest::testEquals() {
  SourceFileRangesSet s1;

  s1.add(0, 0, 10);
  s1.add(0, 10, 30);
  s1.add(0, 50, 75);

  SourceFileRangesSet s2;
  s2.add(0, 0, 10);

  CPPUNIT_ASSERT(!s1.equals(s2));
  CPPUNIT_ASSERT(!s2.equals(s1));

  s2.add(0, 10, 30);
  s2.add(0, 50, 75);

  CPPUNIT_ASSERT(s1.equals(s2));
  CPPUNIT_ASSERT(s2.equals(s1));

  s2.add(1, 20, 25);

  CPPUNIT_ASSERT(!s1.equals(s2));
  CPPUNIT_ASSERT(!s2.equals(s1));

  s1.add(1, 20, 22);
  s1.add(1, 22, 30);
  s2.add(1, 25, 30);

  CPPUNIT_ASSERT(s1.equals(s2));
  CPPUNIT_ASSERT(s2.equals(s1));
}

