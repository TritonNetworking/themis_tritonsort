#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "GlobTest.h"
#include "core/File.h"
#include "core/Glob.h"
#include "core/TritonSortAssert.h"

void GlobTest::setUp() {
  mkdir("glob_test_dir1", 00644);
  mkdir("glob_test_dir2", 00644);
  creat("glob_test_foo.txt", 00644);
  creat("glob_test_bar.txt", 00644);
  creat("glob_test_quux.txt", 00644);
}

void GlobTest::tearDown() {
  rmdir("glob_test_dir1");
  rmdir("glob_test_dir2");
  unlink("glob_test_foo.txt");
  unlink("glob_test_bar.txt");
  unlink("glob_test_quux.txt");
}

void GlobTest::testSimpleFileGlob() {
  Glob glob("glob_test_*.txt");

  const StringList& files = glob.getFiles();

  CPPUNIT_ASSERT_EQUAL((size_t) 3, files.size());

  StringList::const_iterator iter = files.begin();

  const std::string& file1 = *iter;
  CPPUNIT_ASSERT_EQUAL(std::string("glob_test_bar.txt"),
                       file1);
  iter++;

  const std::string& file2 = *iter;
  CPPUNIT_ASSERT_EQUAL(std::string("glob_test_foo.txt"),
                       file2);
  iter++;

  const std::string& file3 = *iter;
  CPPUNIT_ASSERT_EQUAL(std::string("glob_test_quux.txt"),
                       file3);
  iter++;
}

void GlobTest::testNoFilesFound() {
  Glob glob("no_files_here_*_oops.txt");

  CPPUNIT_ASSERT_EQUAL((size_t) 0, glob.getFiles().size());
}

void GlobTest::testNoWildcards() {
  Glob glob("glob_test_foo.txt");

  const StringList& files = glob.getFiles();

  CPPUNIT_ASSERT_EQUAL((size_t) 1, files.size());

  const std::string& file = files.front();
  CPPUNIT_ASSERT_EQUAL(std::string("glob_test_foo.txt"),
                       file);
}

void GlobTest::testSimpleDirectoryGlob() {
  Glob sampleGlob("glob_test_dir*");

  const StringList& dirs = sampleGlob.getDirectories();

  CPPUNIT_ASSERT_EQUAL((size_t) 2, dirs.size());

  StringList::const_iterator iter = dirs.begin();

  const std::string& firstDir = *iter;
  CPPUNIT_ASSERT_EQUAL(std::string("glob_test_dir1"), firstDir);
  iter++;

  const std::string& secondDir = *iter;
  CPPUNIT_ASSERT_EQUAL(std::string("glob_test_dir2"), secondDir);
}
