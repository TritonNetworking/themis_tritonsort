#ifndef TRITONSORT_LOG_LINE_DESCRIPTOR_TEST_H
#define TRITONSORT_LOG_LINE_DESCRIPTOR_TEST_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>
#include <json/value.h>

#include "config.h"

class LogLineDescriptorTest : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE( LogLineDescriptorTest );
#ifdef TRITONSORT_ASSERTS
  CPPUNIT_TEST( testNoFinalizeThrowsException );
#endif //TRITONSORT_ASSERTS

  CPPUNIT_TEST( testConstantFields );
  CPPUNIT_TEST( testNoTypeNameThrowsException );
  CPPUNIT_TEST( testInheritingDescriptor );
  CPPUNIT_TEST_SUITE_END();
public:
  void testConstantFields();
  void testNoTypeNameThrowsException();
  void testInheritingDescriptor();
  void testNoFinalizeThrowsException();

private:
  void readJsonObjectFromFile(const std::string& filename, Json::Value& value);
};

#endif // TRITONSORT_LOG_LINE_DESCRIPTOR_TEST_H
