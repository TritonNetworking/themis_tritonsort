#ifndef TCP_COMMON_H
#define TCP_COMMON_H

#include <stdint.h>

#define BASE_PORT 8000
#define STAT_BATCH_INTERVAL 10000
#define MAX_MACHINES 52

#define EXPERIMENT_DURATION 20

#define TCP_SEND_BUFLEN 8196
#define TCP_RECV_BUFLEN 8196

uint64_t utime();

void initPacketPayload(uint8_t * buf, uint64_t len);
int isPacketPayloadCorrect(uint8_t * buf, uint64_t len);

/* Threads */
void * connectThreadMain(void * port);
void * acceptThreadMain(void * port);
void * statThreadMain(void * ptr);
void * receiverThreadMain(void * port);
void * sendThreadMain(void * port);

#endif /* TCP_COMMON_H */
