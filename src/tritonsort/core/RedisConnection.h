#ifndef THEMIS_REDIS_CONNECTION_H
#define THEMIS_REDIS_CONNECTION_H

#include <hiredis/hiredis.h>
#include <pthread.h>

class Params;

/**
   RedisConnection represents a single connection to redis to be used throughout
   all of Themis. Operations are serialized on a single mutex, so make sure to
   use a relatively small number of accesses to redis to avoid performance
   issues. To use, simply call connect() once and then create RedisCommand
   objects. To teardown, call disconnect().
 */
class RedisConnection {
public:
  /**
     Open a connection to the redis server specified by the following
     parameters:

     COORDINATOR.HOSTNAME - hostname of redis server
     COORDINATOR.PORT - port on which redis server is accepting connections
     COORDINATOR.DB - the redis database number to use

     \param params the global params object containing the redis server
     location parameters
   */
  static void connect(Params& params);

  /// Disconnect from redis.
  static void disconnect();

  /**
     Issue a command to redis and get a reply.

     \warning Although you can use this method directly, the best way to issue
     commands is with a RedisCommand object, which calls this method and
     performs some extra checks.

     \param command a pre-formatted command string that is only used for
     debugging purposes

     \param format a printf style format string representing the redis command

     \param ap a variable argument list for the format string

     \return the reply from the redis server
   */
  static redisReply* getReply(
    const char* command, const char* format, va_list ap);

private:
  static redisContext* context;

  static pthread_mutex_t mutex;
};

#endif // THEMIS_REDIS_CONNECTION_H
