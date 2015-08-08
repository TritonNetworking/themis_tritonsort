#ifndef _TRITONSORT_CORE_TEST_SUITE_H
#define _TRITONSORT_CORE_TEST_SUITE_H

#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestSuite.h>
#include <cppunit/extensions/HelperMacros.h>

#include "Base64Test.h"
#include "BatchRunnableTest.h"
#include "BlockingReadTest.h"
#include "ByteOrderTest.h"
#include "CPUAffinitySetterTest.h"
#include "DefaultAllocatorPolicyTest.h"
#include "FileTest.h"
#include "GlobTest.h"
#include "LogLineDescriptorTest.h"
#include "MemoryAllocatorTests.h"
#include "MemoryMappedFileDeadlockResolverTest.h"
#include "MemoryUtilsTest.h"
#include "NamedObjectCollectionTest.h"
#include "ParamsTest.h"
#include "ResourceQueueTest.h"
#include "ResourceSchedulerTest.h"
#include "StatLoggerTest.h"
#include "StatWriterTest.h"
#include "ThreadSafeQueueTest.h"
#include "WorkerTrackerTest.h"

class CoreTestSuite : public CppUnit::TestSuite {
public:
  CoreTestSuite() : CppUnit::TestSuite("TritonSort Core") {
    addTest(BatchRunnableTest::suite());
    addTest(BlockingReadTest::suite());
    addTest(ByteOrderTest::suite());
    addTest(DefaultAllocatorPolicyTest::suite());
    addTest(FileTest::suite());
    addTest(GlobTest::suite());
    addTest(LogLineDescriptorTest::suite());
    addTest(MemoryAllocatorTests::suite());
    addTest(MemoryMappedFileDeadlockResolverTest::suite());
    addTest(MemoryUtilsTest::suite());
    addTest(NamedObjectCollectionTest::suite());
    addTest(ParamsTest::suite());
    addTest(ResourceQueueTest::suite());
    addTest(ResourceSchedulerTest::suite());
    addTest(StatLoggerTest::suite());
    addTest(StatWriterTest::suite());
    addTest(ThreadSafeQueueTest::suite());
    addTest(WorkerTrackerTest::suite());
    addTest(CPUAffinitySetterTest::suite());
    addTest(Base64Test::suite());
  }
};

#endif // _TRITONSORT_CORE_TEST_SUITE_H
