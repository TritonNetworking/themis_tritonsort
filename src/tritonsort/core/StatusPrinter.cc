#include <iostream>
#include <stdlib.h>
#include <sstream>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "core/Params.h"
#include "core/StatusPrinter.h"
#include "core/TritonSortAssert.h"
#include "core/Utils.h"

std::queue<std::string> StatusPrinter::messages;
std::queue<StatusPrinter::Channel> StatusPrinter::channelsForMessages;
pthread_mutex_t StatusPrinter::statusPrintMutex;
pthread_cond_t StatusPrinter::messageQueueNotEmpty;
bool StatusPrinter::stop = false;
Thread StatusPrinter::thread("StatusPrinter", &StatusPrinter::run);
std::map<StatusPrinter::Channel, std::string> StatusPrinter::channelHeaders;
std::ostream* StatusPrinter::outputStream;

std::string StatusPrinter::getDateTimeLogName(Params* params) {
  const std::string& logDirName = params->get<std::string>("LOG_DIR");

  std::string hostname;
  getHostname(hostname);

  std::ostringstream oss;
  oss << logDirName << '/' << hostname << ".log";

  return oss.str();
}

void StatusPrinter::init(Params* params) {
  pthread_mutex_init(&statusPrintMutex, NULL);
  pthread_cond_init(&messageQueueNotEmpty, NULL);
  stop = false;

  if (!params->contains("LOG_FILE")) {
    params->add<std::string>("LOG_FILE", getDateTimeLogName(params));
  }

  const std::string& logFilePath = params->get<std::string>("LOG_FILE");
  const char* logFilePathCStr = logFilePath.c_str();
  std::cout << "Logging to " << logFilePath << std::endl;

  int fp = creat(logFilePathCStr, 00644);
  if (fp == -1) {
    ASSERT(fp != -1, "Creating log file '%s' failed with error %d: %s",
           logFilePathCStr, errno, strerror(errno));
  } else {
    close(fp);
  }

  outputStream = new std::ofstream(logFilePathCStr);

  // TODO: Make an iterable Enumerator class (or find one in boost or elsewhere)
  // so we can iterate through channels instead of doing this by hand

  channelHeaders[CH_STATUS] = params->get<std::string>("CHANNEL_STATUS_HEADER");
  channelHeaders[CH_STATISTIC] = params->get<std::string>(
    "CHANNEL_STATISTIC_HEADER");
  channelHeaders[CH_PARAM] = params->get<std::string>("CHANNEL_PARAM_HEADER");
}

void StatusPrinter::teardown() {
  pthread_mutex_lock(&statusPrintMutex);
  stop = true;
  pthread_cond_signal(&messageQueueNotEmpty);
  pthread_mutex_unlock(&statusPrintMutex);

  thread.stopThread();

  delete outputStream;

  pthread_mutex_destroy(&statusPrintMutex);
  pthread_cond_destroy(&messageQueueNotEmpty);
}

void StatusPrinter::add(const StatusPrinter::Channel& channel, const std::string& message) {
  pthread_mutex_lock(&statusPrintMutex);
  channelsForMessages.push(channel);
  messages.push(
    message + "(" + boost::lexical_cast<std::string>(Timer::posixTimeInMicros())
    + ")");
  pthread_cond_signal(&messageQueueNotEmpty);
  pthread_mutex_unlock(&statusPrintMutex);
}

void StatusPrinter::add(const StatusPrinter::Channel& channel, const char* messageFormat ...) {
  va_list ap;
  va_start(ap, messageFormat);
  add(channel, messageFormat, ap);
}

void StatusPrinter::add(const StatusPrinter::Channel& channel, const char* messageFormat, va_list& ap) {
  char buffer[500];
  vsnprintf(buffer, 500, messageFormat, ap);
  va_end(ap);

  std::string message(buffer);
  add(channel, message);
}

void StatusPrinter::add(const std::string& message) {
  add(CH_STATUS, message);
}

void StatusPrinter::add(const char* messageFormat ...) {
  va_list ap;
  va_start(ap, messageFormat);
  add(CH_STATUS, messageFormat, ap);
}

void StatusPrinter::printNextMessage() {
  StatusPrinter::Channel myChannel = channelsForMessages.front();

  (*outputStream) << channelHeaders[myChannel] << ' ' << messages.front() << std::endl;
  channelsForMessages.pop();
  messages.pop();
}

void StatusPrinter::flush() {
  pthread_mutex_lock(&statusPrintMutex);
  (*outputStream).flush();
  pthread_mutex_unlock(&statusPrintMutex);
}

void* StatusPrinter::run(void* arg) {
  while(!stop) {
    pthread_mutex_lock(&statusPrintMutex);
    while (messages.empty() && !stop) {
      pthread_cond_wait(&messageQueueNotEmpty, &statusPrintMutex);
    }

    if (!messages.empty()) {
      printNextMessage();
    }

    pthread_mutex_unlock(&statusPrintMutex);
  }

  pthread_mutex_lock(&statusPrintMutex);

  while (!messages.empty()) {
    printNextMessage();
  }

  pthread_mutex_unlock(&statusPrintMutex);

  return NULL;
}

void StatusPrinter::spawn() {
  thread.startThread();
}
