#include <re2/re2.h>
#include <list>
#include <iostream>
#include <fstream>

#include "core/Utils.h"
#include "core/Timer.h"

using namespace re2;

typedef std::list<RE2*> RegexList;

int grepFile(std::string& filename, RegexList& regexes) {
  Timer grepTimer;
  grepTimer.start();

  std::ifstream fileStream(filename.c_str());

  uint64_t linesGrepped = 0;

  std::string currentLine;

  while (fileStream) {
    std::getline(fileStream, currentLine);

    uint64_t regexNumber = 0;

    for (RegexList::iterator regexIter = regexes.begin();
         regexIter != regexes.end(); regexIter++) {

      RE2* regex = *regexIter;

      if (RE2::FullMatch(currentLine, *regex)) {
        std::cout << regexNumber << '\t' << currentLine << std::endl;
      }

      regexNumber++;
    }
    linesGrepped ++;
  }

  fileStream.close();
  grepTimer.stop();

  double elapsedTimeInSeconds = grepTimer.getElapsed() / 1000000.0;

  std::cerr << "Grepped " << linesGrepped << " lines in "
            << elapsedTimeInSeconds << " seconds. Rate: "
            << getFileSize(filename.c_str()) / grepTimer.getElapsed() << " MBps"
            << std::endl;

  return 0;
}

void printUsage() {
  std::cerr << "Usage: loggrep <filename> <regex 1> [<regex 2> [<regex 3> ...]]"
            << std::endl;
}

int main (int argc, char** argv) {
  RegexList regexes;

  if (argc <= 2) {
    std::cerr << "Expected at least one regular expression argument"
              << std::endl;
    printUsage();
    return 1;
  }

  std::string filename(argv[1]);

  for (uint64_t arg_index = 2; arg_index < argc; arg_index++) {
    RE2* regex = new RE2(argv[arg_index]);
    std::cerr << regex->pattern() << std::endl;
    regexes.push_back(regex);
  }

  std::cerr << regexes.size() << " regular expressions" << std::endl;

  return grepFile(filename, regexes);
}
