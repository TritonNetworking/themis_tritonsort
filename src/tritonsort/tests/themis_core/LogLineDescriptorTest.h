#ifndef TRITONSORT_LOG_LINE_DESCRIPTOR_TEST_H
#define TRITONSORT_LOG_LINE_DESCRIPTOR_TEST_H

#include <json/value.h>
#include "gtest/gtest.h"

#include "config.h"

class LogLineDescriptorTest : public ::testing::Test {
protected:
  void readJsonObjectFromFile(const std::string& filename, Json::Value& value);
};

#endif // TRITONSORT_LOG_LINE_DESCRIPTOR_TEST_H
