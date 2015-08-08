#include <sys/time.h>
#include <stdint.h>
#include <stdlib.h>

uint64_t
utime() {
  struct timeval time;
  gettimeofday(&time, NULL);

  return (((uint64_t) time.tv_sec) * 1000000) + time.tv_usec;
}

void
initPacketPayload(uint8_t * buf, uint64_t len)
{
    uint64_t i;

    for (i = 0; i < len; i++) {
        buf[i] = i % 256;
    }
}

int
isPacketPayloadCorrect(uint8_t * buf, uint64_t len)
{
    uint64_t i;

    for (i = 0; i < len; i++) {
        if (buf[i] != i % 256) {
            return 0;
        }
    }
    return 1;
}
