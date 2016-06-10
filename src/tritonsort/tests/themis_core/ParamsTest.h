#ifndef _TRITONSORT_TEST_PARAMSTEST_H
#define _TRITONSORT_TEST_PARAMSTEST_H

#include <string>

#include "core/Params.h"
#include "third-party/googletest.h"

class ParamsTest : public ::testing::Test {
public:
  void SetUp();
  void TearDown();

protected:
  char LOG_FILE_NAME[2550];
};
#endif // _TRITONSORT_TEST_PARAMSTEST_H
