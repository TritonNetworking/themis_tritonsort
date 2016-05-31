#include "mapreduce/common/ReadRequest.h"
#include "tests/mapreduce/common/ReadRequestTest.h"

TEST_F(ReadRequestTest, testConstructFromURL) {
  std::set<uint64_t> jobIDs;
  jobIDs.insert(0xdeadbeef);

  ReadRequest* request = ReadRequest::fromURL(
    jobIDs, "local://dcswitch103:5000/192.168.5.103/c0d6p1/alexras/"
    "4nodes_400GB/input_103_c0d6p1", 42, 650, 1);

  EXPECT_TRUE(request != NULL);
  EXPECT_EQ(static_cast<uint64_t>(1), request->jobIDs.size());
  EXPECT_EQ(static_cast<uint64_t>(1), request->jobIDs.count(0xdeadbeef));
  EXPECT_EQ(ReadRequest::FILE, request->protocol);
  EXPECT_EQ(std::string("dcswitch103"), request->host);
  EXPECT_EQ(static_cast<uint32_t>(5000), request->port);
  EXPECT_EQ(
    std::string("/192.168.5.103/c0d6p1/alexras/4nodes_400GB/input_103_c0d6p1"),
    request->path);
  EXPECT_EQ(static_cast<uint64_t>(42), request->offset);
  EXPECT_EQ(static_cast<uint64_t>(650), request->length);
  EXPECT_EQ(static_cast<uint64_t>(1), request->diskID);

  delete request;
}
