#include "mapreduce/common/ReadRequest.h"
#include "tests/mapreduce/common/ReadRequestTest.h"

void ReadRequestTest::testConstructFromURL() {
  std::set<uint64_t> jobIDs;
  jobIDs.insert(0xdeadbeef);

  ReadRequest* request = ReadRequest::fromURL(
    jobIDs, "hdfs://dcswitch103:5000/192.168.5.103/c0d6p1/alexras/"
    "4nodes_400GB/input_103_c0d6p1", 42, 650, 1);

  CPPUNIT_ASSERT(request != NULL);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(1), request->jobIDs.size());
  CPPUNIT_ASSERT_EQUAL(
    static_cast<uint64_t>(1), request->jobIDs.count(0xdeadbeef));
  CPPUNIT_ASSERT_EQUAL(ReadRequest::HDFS, request->protocol);
  CPPUNIT_ASSERT_EQUAL(std::string("dcswitch103"), request->host);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint32_t>(5000), request->port);
  CPPUNIT_ASSERT_EQUAL(
    std::string("/192.168.5.103/c0d6p1/alexras/4nodes_400GB/input_103_c0d6p1"),
    request->path);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(42), request->offset);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(650), request->length);
  CPPUNIT_ASSERT_EQUAL(static_cast<uint64_t>(1), request->diskID);

  delete request;
}
