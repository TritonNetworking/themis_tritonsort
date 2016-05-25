#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "GlobTest.h"
#include "core/File.h"
#include "core/Glob.h"
#include "core/TritonSortAssert.h"

void GlobTest::SetUp() {
  mkdir("glob_test_dir1", 00644);
  mkdir("glob_test_dir2", 00644);
  creat("glob_test_foo.txt", 00644);
  creat("glob_test_bar.txt", 00644);
  creat("glob_test_quux.txt", 00644);
}

void GlobTest::TearDown() {
  rmdir("glob_test_dir1"); rmdir("glob_test_dir2");
  unlink("glob_test_foo.txt");
  unlink("glob_test_bar.txt");
  unlink("glob_test_quux.txt");
}

TEST_F(GlobTest, testSimpleFileGlob) {
  Glob glob("glob_test_*.txt");

  const StringList& files = glob.getFiles();

  EXPECT_EQ((size_t) 3, files.size());

  StringList::const_iterator iter = files.begin();

  const std::string& file1 = *iter;
  EXPECT_EQ(std::string("glob_test_bar.txt"),
                       file1);
  iter++;

  const std::string& file2 = *iter;
  EXPECT_EQ(std::string("glob_test_foo.txt"),
                       file2);
  iter++;

  const std::string& file3 = *iter;
  EXPECT_EQ(std::string("glob_test_quux.txt"),
                       file3);
  iter++;
}

TEST_F(GlobTest, testNoFilesFound) {
  Glob glob("no_files_here_*_oops.txt");

  EXPECT_EQ((size_t) 0, glob.getFiles().size());
}

TEST_F(GlobTest, testNoWildcards) {
  Glob glob("glob_test_foo.txt");

  const StringList& files = glob.getFiles();

  EXPECT_EQ((size_t) 1, files.size());

  const std::string& file = files.front();
  EXPECT_EQ(std::string("glob_test_foo.txt"),
                       file);
}

TEST_F(GlobTest, testSimpleDirectoryGlob) {
  Glob sampleGlob("glob_test_dir*");

  const StringList& dirs = sampleGlob.getDirectories();

  EXPECT_EQ((size_t) 2, dirs.size());

  StringList::const_iterator iter = dirs.begin();

  const std::string& firstDir = *iter;
  EXPECT_EQ(std::string("glob_test_dir1"), firstDir);
  iter++;

  const std::string& secondDir = *iter;
  EXPECT_EQ(std::string("glob_test_dir2"), secondDir);
}
