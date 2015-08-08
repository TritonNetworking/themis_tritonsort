#include "core/RedisCommand.h"
#include "core/RedisConnection.h"
#include "core/TritonSortAssert.h"

RedisCommand::RedisCommand(const char* format ...) {
  // Write command string so we can print debug messages.
  va_list ap;
  va_start(ap, format);
  int charactersWritten = vsnprintf(_command, 1024, format, ap);
  ABORT_IF(charactersWritten == -1, "vsnprintf(%s, ...) failed; either memory "
           "allocation failed, or some other error occurred", format);
  ABORT_IF(charactersWritten >= 1024,
           "Redis command must be less than 1024 characters");
  va_end(ap);

  // Issue redis command.
  va_start(ap, format);
  _reply = RedisConnection::getReply(_command, format, ap);
  va_end(ap);

  if (_reply->type == REDIS_REPLY_ERROR) {
    std::string errorString;
    errorString.assign(_reply->str, _reply->len);

    ABORT("Redis command (%s) returned error from server : %s",
          _command, errorString.c_str());
  }
}

RedisCommand::~RedisCommand() {
  if (_reply != NULL) {
    freeReplyObject(_reply);
    _reply = NULL;
  }
}

const char* RedisCommand::replyTypeString(int replyType) {
  switch (replyType) {
  case REDIS_REPLY_STRING:
    return "string";
    break;
  case REDIS_REPLY_ARRAY:
    return "array";
    break;
  case REDIS_REPLY_INTEGER:
    return "integer";
    break;
  case REDIS_REPLY_NIL:
    return "nil";
    break;
  case REDIS_REPLY_STATUS:
    return "status";
    break;
  case REDIS_REPLY_ERROR:
    return "error";
    break;
  default:
    return "unknown";
    break;
  }
}

void RedisCommand::assertReplyType(
  redisReply& reply, int expectedReplyType) {
  ABORT_IF(reply.type != expectedReplyType, "Expected %s reply but got %s "
           "reply", replyTypeString(expectedReplyType),
           replyTypeString(reply.type));
}
