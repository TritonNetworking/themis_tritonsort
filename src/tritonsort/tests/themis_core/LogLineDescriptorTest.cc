#define __STDC_FORMAT_MACROS 1

#include <boost/filesystem.hpp>
#include <fcntl.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "LogLineDescriptorTest.h"
#include "core/File.h"
#include "core/LogLineDescriptor.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"

extern const char* TEST_BINARY_ROOT;

void LogLineDescriptorTest::readJsonObjectFromFile(
  const std::string& filename, Json::Value& value) {
  Json::Reader reader;

  boost::filesystem::path filePath(
    boost::filesystem::path(TEST_BINARY_ROOT) / "themis_core" / filename);

  File referenceFile(filePath.string());
  referenceFile.open(File::READ);
  uint64_t fileSize = referenceFile.getCurrentSize();

  char* fileBuffer = new char[fileSize];

  referenceFile.read(reinterpret_cast<uint8_t*>(fileBuffer), fileSize);
  referenceFile.close();

  EXPECT_TRUE(reader.parse(fileBuffer, fileBuffer + fileSize, value));

  delete[] fileBuffer;
}

TEST_F(LogLineDescriptorTest, testConstantFields) {
  LogLineDescriptor descriptor;

  descriptor.setLogLineTypeName("SMOO")
    .addConstantStringField("spam", "blah")
    .addConstantUIntField("eggs", 288230376151711744)
    .addConstantIntField("ham", -281474976710656)
    .addField("foo", LogLineFieldInfo::STRING)
    .finalize();

  Json::Value referenceDescription;
  readJsonObjectFromFile("test_constant_fields.json", referenceDescription);

  std::string referenceLogLineFormat(
    "SMOO\t%s\t%" PRIu64 "\tblah\t288230376151711744\t-281474976710656\t%s\n");

  const Json::Value& description = descriptor.getDescriptionJson();

  if (referenceDescription != description) {
    Json::StyledWriter writer;

    std::ostringstream oss;
    oss << "JSON object doesn't match the reference" << std::endl << std::endl;
    oss << "Reference:" << std::endl;
    oss << writer.write(referenceDescription) << std::endl << std::endl;
    oss << "Mine:" << std::endl;
    oss << writer.write(description) << std::endl << std::endl;

    FAIL() << oss.str();
  }

  EXPECT_EQ(referenceLogLineFormat, descriptor.getLogLineFormatString());
}

TEST_F(LogLineDescriptorTest, testNoTypeNameThrowsException) {
  LogLineDescriptor descriptor;

  descriptor.addConstantStringField("spam", "blah")
    .addConstantUIntField("eggs", 15)
    .addConstantIntField("ham", -46)
    .addField("foo", LogLineFieldInfo::STRING);

  ASSERT_THROW(descriptor.finalize(), AssertionFailedException);
}

TEST_F(LogLineDescriptorTest, testInheritingDescriptor) {
  LogLineDescriptor parent;

  parent.addConstantStringField("spam", "blah")
    .addConstantUIntField("eggs", 288230376151711744)
    .addField("bar", LogLineFieldInfo::UNSIGNED_INTEGER);

  LogLineDescriptor child(parent);
  child.setLogLineTypeName("FOO")
    .addField("baz", LogLineFieldInfo::SIGNED_INTEGER)
    .finalize();

  Json::Value referenceDescription;
  readJsonObjectFromFile(
    "test_inheriting_descriptor.json", referenceDescription);

  std::string referenceLogLineFormat(
    "FOO\t%s\t%" PRIu64 "\tblah\t288230376151711744\t%" PRIu64 "\t%" PRId64
    "\n");

  EXPECT_TRUE(referenceDescription == child.getDescriptionJson());
  EXPECT_EQ(referenceLogLineFormat, child.getLogLineFormatString());
}

#ifdef TRITONSORT_ASSERTS
TEST_F(LogLineDescriptorTest, testNoFinalizeThrowsException) {
  LogLineDescriptor descriptor;

  descriptor.setLogLineTypeName("SMOO")
    .addConstantStringField("spam", "blah")
    .addConstantUIntField("eggs", 15)
    .addConstantIntField("ham", -46)
    .addField("foo", LogLineFieldInfo::STRING);

  ASSERT_THROW(descriptor.getDescriptionJson(), AssertionFailedException);
}
#endif //TRITONSORT_ASSERTS
