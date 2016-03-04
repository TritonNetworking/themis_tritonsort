#ifndef THEMIS_REDIS_COMMAND_H
#define THEMIS_REDIS_COMMAND_H

#include <hiredis/hiredis.h>

#include "core/constants.h"

/**
   RedisCommand abstracts away a particular redis operation and reply. It
   accepts a format string with variable arguments. It automatically frees
   associated hiredis memory when it goes out of scope, so the user simply
   needs to create objects to use redis:

   RedisCommand command("GET foo");
   reply = command.reply();

   \warning commands must be at most 1023 characters after formatting due to
   internal memory issues.
 */
class RedisCommand {
public:
  /// Constructor
  /**
     \param format a printf format string followed by a variable number of
     arguments
   */
  RedisCommand(const char* format ...);

  /// Destructor
  ~RedisCommand();

  /// Get the formatted redis command.
  inline const char* command() {
    return _command;
  }

  /// Get the reply from the redis server.
  inline redisReply& reply() {
    return *(_reply);
  }

  /**
     Helper method to obtain a reply type string from an integer reply type

     \param replyType the integer reply type from hiredis

     \return the reply type string
   */
  static const char* replyTypeString(int replyType);

  /**
     Helper method to check reply types.

     \param reply the reply from the redis server

     \param expectedReplyType the integer reply type you were expecting from
     executing the command
   */
  static void assertReplyType(redisReply& reply, int expectedReplyType);

private:
  char _command[1024];
  redisReply* _reply;
};

#endif // THEMIS_REDIS_COMMAND_H
