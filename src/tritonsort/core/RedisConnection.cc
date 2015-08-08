#include "core/Params.h"
#include "core/RedisCommand.h"
#include "core/RedisConnection.h"
#include "core/ScopedLock.h"

redisContext* RedisConnection::context = NULL;
pthread_mutex_t RedisConnection::mutex;

void RedisConnection::connect(Params& params) {
  pthread_mutex_init(&mutex, NULL);

  // First connect to redis.
  const std::string& hostname = params.get<std::string>("COORDINATOR.HOSTNAME");
  uint32_t port = params.get<uint32_t>("COORDINATOR.PORT");
  context = redisConnect(hostname.c_str(), port);
  ABORT_IF(context->err, "redisConnect(%s, %lu) failed: %s",
           hostname.c_str(), port, context->errstr);

  // Now select the appropriate database.
  RedisCommand selectCommand(
    "SELECT %llu", params.get<uint64_t>("COORDINATOR.DB"));
  redisReply& reply = selectCommand.reply();

  RedisCommand::assertReplyType(reply, REDIS_REPLY_STATUS);

  ABORT_IF(strncmp(reply.str, "OK", 2) != 0,
           "Redis command '%s' returned unexpected status '%s'",
           selectCommand.command(), reply.str);
}

void RedisConnection::disconnect() {
  pthread_mutex_lock(&mutex);
  if (context != NULL) {
    redisFree(context);
    context = NULL;
  }
  pthread_mutex_unlock(&mutex);

  pthread_mutex_destroy(&mutex);
}

redisReply* RedisConnection::getReply(
  const char* command, const char* format, va_list ap) {
  ScopedLock lock(&mutex);

  redisReply* reply =
    static_cast<redisReply*>(redisvCommand(context, format, ap));

  ABORT_IF(reply == NULL, "Redis command (%s) failed: %s", command,
           context->errstr);

  return reply;
}
