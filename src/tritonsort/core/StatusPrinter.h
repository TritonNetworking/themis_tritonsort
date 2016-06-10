#ifndef _TRITONSORT_STATUSPRINTER_H
#define _TRITONSORT_STATUSPRINTER_H

#include <string>
#include <queue>
#include <pthread.h>
#include <map>
#include <stdlib.h>
#include <stdarg.h>

#include "core/Thread.h"

class Params;

/**
   A thread-safe printer for printing statistics and status messages to a log
   file
 */
class StatusPrinter {
public:
  /** \enum Channel
     The status printer can print to any one of a number of channels. These
     channels are enumerated here.

     Channels distinguish themselves by writing the channel's header at the
     beginning of the log line. The name for a channel is given by the
     CHANNEL_XXX_HEADER parameter, where XXX is replaced with the anme of the
     channel; for example, StatusPrinter::CH_STATUS's header is given by the
     param CHANNEL_STATUS_HEADER.

     \todo (AR) This is heaps and heaps of inelegant. We really need a better
     solution.
  */
  enum Channel {CH_STATUS,/**< use this channel for general status messages */
                CH_STATISTIC, /**< use this channel when logging statistics */
                CH_PARAM /**< use this channel when logging parameters */
  };
  /// Initialize the status printer.
  /**
     Note that this does not start the printer;
     for that, use StatusPrinter::spawn.

     If the provided Params object has defined the parameter LOG_FILE, the
     value of that parameter will be used as the path to the log file.
     Otherwise, a file whose name is auto-generated based on the current date
     and time will be placed in a directory given by the LOG_DIR parameter.

     \param params the Params object containing configuration information for
     the printer
   */
  static void init(Params* params);

  /// Tear down the status printer
  /**
     If the status printer is running, this method stops it.
   */
  static void teardown();

  /// Log a message
  /**
     \param channel the channel on which to log
     \param message the message to log
   */
  static void add(const Channel& channel, const std::string& message);

  /// Log a message (printf-style syntax)
  /**
     \param channel the channel on which to log
     \param messageFormat the printf-style format of the message to log
     \param ... the formatting parameters
   */
  static void add(const Channel& channel, const char* messageFormat ...);

  /// Log a message to the StatusPrinter::CH_STATUS channel
  /**
     \sa StatusPrinter::add(const Channel&, const std::string&)
   */
  static void add(const std::string& message);

  /// Log a message to the StatusPrinter::CH_STATUS channel
  /**
     \sa StatusPrinter::add(const Channel&, const char*)
   */
  static void add(const char* messageFormat ...);

  /// Flush any pending log messages to the file
  /**
     This method blocks until all pending log messages have been written to the
     file.
   */
  static void flush();

  /// Start the status printer
  /**
     Start the status printer's thread. Log messages that are added prior to
     calling StatusPrinter::spawn are not guaranteed to be written to the log.

     \warning You should not call this method unless you have called
     StatusPrinter::init first.
   */
  static void spawn();

  /// \cond PRIVATE
  /**
     The status printer's main thread. Called by StatusPrinter::spawn()

     \param arg pointer to the status printer's arguments. Should always be
     NULL.
   */
  static void* run(void* arg);
  /// \endcond
private:
  static std::map<Channel, std::string> channelHeaders;

  static pthread_mutex_t statusPrintMutex;
  static pthread_cond_t messageQueueNotEmpty;

  static themis::Thread thread;
  static bool stop;

  static void add(const Channel& channel, const char* messageFormat, va_list& ap);
  static void printNextMessage();

  static std::ostream* outputStream;

  static std::queue<Channel> channelsForMessages;
  static std::queue<std::string> messages;

  static std::string getDateTimeLogName(Params* params);
}; // StatusPrinter

#endif // _TRITONSORT_STATUSPRINTER_H
