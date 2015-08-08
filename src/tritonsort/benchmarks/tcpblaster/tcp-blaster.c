
/* Sample UDP server */

#define _GNU_SOURCE

#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <linux/net.h>
#include <assert.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <netinet/tcp.h>
#include <linux/sockios.h>

#include "tcp-common.h"

pthread_mutex_t statMutex = PTHREAD_MUTEX_INITIALIZER;
uint64_t bytes_received;
uint64_t * pernode_bytes_received;

int shouldStop;

char * machines[MAX_MACHINES];
int numMachines;

int clisocks[MAX_MACHINES];
int servsocks[MAX_MACHINES];

int main(int argc, char**argv)
{
   int i;
   pthread_t statThread, sendThread, recvThread,
             connectThread, acceptThread;

   /* initialize the statistics/globals */
   bytes_received = 0;
   shouldStop = 0;

   /* read in a list of machines involved */
   for (i = 1; i < argc; i++) {
      machines[i-1] = argv[i];
      numMachines++;
   }

   /* Initialize the per-node statistics */
   pernode_bytes_received = malloc(sizeof(uint64_t) * numMachines);
   memset(pernode_bytes_received, 0, numMachines * sizeof(uint64_t));

   /* create connect thread */
   if (pthread_create(&connectThread, NULL,
               connectThreadMain, NULL) != 0) {
       perror("pthread_create");
       exit(1);
   }

   /* create accept thread */
   if (pthread_create(&acceptThread, NULL,
               acceptThreadMain, NULL) != 0) {
       perror("pthread_create");
       exit(1);
   }

   /* wait for all the sockets are established */
   pthread_join(connectThread, NULL);
   pthread_join(acceptThread, NULL);

   printf("All sockets established, begin sending\n");
   fflush(stdout);

   /* create stat threads */
   if (pthread_create(&statThread, NULL,
            statThreadMain, NULL) != 0) {
     perror("pthread_create");
     exit(1);
   }

   /* create receiver thread */
   if (pthread_create(&recvThread, NULL,
               receiverThreadMain, NULL) != 0) {
       perror("pthread_create");
       exit(1);
   }

   /* create sender thread */
   if (pthread_create(&sendThread, NULL,
               sendThreadMain, NULL) != 0) {
       perror("pthread_create");
       exit(1);
   }

   /* wait for all the threads to complete */
   pthread_join(recvThread, NULL);
   pthread_join(sendThread, NULL);
   pthread_join(statThread, NULL);

   return 0;
}

void *
statThreadMain(void * ptr)
{
   int iter, i;
   float accum_rate;
   float * accum_pernode_rate;
   uint64_t accum_cwnd;

   accum_rate = 0;
   accum_pernode_rate = (float *) malloc(sizeof(uint64_t) * numMachines);
   memset(accum_pernode_rate, 0, sizeof(uint64_t) * numMachines);
   accum_cwnd = 0;

   printf("#rate bytesReceived\n");
   iter = 0;
   for (;;) {
      sleep(1);
      pthread_mutex_lock(&statMutex);
      printf("%.4g %ld\n",
              (bytes_received / 125000000.0),
              bytes_received);
      fflush(stdout);
      accum_rate += bytes_received / 125000000.0;
      for (i = 0; i < numMachines; i++) {
        accum_pernode_rate[i] = pernode_bytes_received[i] / 125000000.0;
      }
      bytes_received = 0;
      pthread_mutex_unlock(&statMutex);

#if 0
      for (i = 0; i < numMachines; i++) {
          int sendQ, recvQ;
          struct tcp_info tcpinfo;
          socklen_t len;

          assert(ioctl(clisocks[i], SIOCOUTQ, &sendQ) != -1);
          assert(ioctl(servsocks[i], SIOCINQ, &recvQ) != -1);
          assert(getsockopt(clisocks[i], IPPROTO_TCP, TCP_INFO,
                     &tcpinfo, &len) != -1);

          accum_cwnd += tcpinfo.tcpi_snd_cwnd;
          printf("->%d send:%d recv:%d cwnd:%d retrans:%d ss:%d\n", i,
                 sendQ, recvQ,
                 tcpinfo.tcpi_snd_cwnd,
                 tcpinfo.tcpi_retrans,
                 tcpinfo.tcpi_snd_ssthresh);
      }
#endif

      if (iter == EXPERIMENT_DURATION) {
        printf("Shutting down the experiment\n");
        printf("AVG: %g\n", (accum_rate / (1.0+EXPERIMENT_DURATION)));
        for (i = 0; i < numMachines; i++) {
            printf("NODE %d: %g\n", i, (accum_pernode_rate[i] / (1.0+EXPERIMENT_DURATION)));
        }
#if 0
        printf("CWNDAVG: %g\n", (accum_cwnd / (1.0*EXPERIMENT_DURATION*numMachines)));
#endif
        fflush(stdout);
        shouldStop = 1;
        break;
      }
      iter++;
   }

   return 0;
}

void *
connectThreadMain(void * nothing)
{
   int i, opt=1;
   struct sockaddr_in servAddr[MAX_MACHINES];

   for (i = 0; i < numMachines; i++) {
       if ((clisocks[i] = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
          perror("socket");
          exit(1);
       }
       setsockopt(clisocks[i], SOL_SOCKET, SO_REUSEADDR,
                  (char *)&opt, sizeof(opt));

       memset(&servAddr[i], 0, sizeof(servAddr[i]));
       servAddr[i].sin_family = AF_INET;
       servAddr[i].sin_addr.s_addr = inet_addr(machines[i]);
       servAddr[i].sin_port = htons(BASE_PORT);

       while (connect(clisocks[i], (struct sockaddr *) &servAddr[i],
                   sizeof(servAddr[i])) < 0) {
         perror("connect");
         sleep(1);
       }
       printf("Connected to %s\n", machines[i]);
   }
   fflush(stdout);

   return 0;
}

void *
acceptThreadMain(void * nothing)
{
    int serverSock, i, opt=1, rcvbuf;
    struct sockaddr_in servAddr;
    struct sockaddr_in cliAddr;
    unsigned int clientLen;

    if (numMachines >= 24) {
      rcvbuf = 16535;
    } else if (numMachines >= 16) {
      rcvbuf = 32768;
    } else if (numMachines >= 10) {
      rcvbuf = 131072;
    } else {
      rcvbuf = 524288;
    }

    if ((serverSock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("socket");
        exit(1);
    }
    setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR,
               (char *)&opt, sizeof(opt));
    setsockopt(serverSock, SOL_SOCKET, SO_RCVBUF,
               (char *)&rcvbuf, (int)sizeof(rcvbuf));

    memset(&servAddr, 0, sizeof(servAddr));
    servAddr.sin_family = AF_INET;
    servAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servAddr.sin_port = htons(BASE_PORT);

    if (bind(serverSock, (struct sockaddr *) &servAddr,
             sizeof(servAddr)) < 0) {
       perror("bind");
       exit(1);
    }

    if (listen(serverSock, numMachines) < 0) {
       perror("listen");
       exit(1);
    }

    for (i = 0; i < numMachines; i++) {
       clientLen = sizeof(cliAddr);
       if ((servsocks[i] = accept(serverSock, (struct sockaddr *) &cliAddr,
                              &clientLen)) < 0) {
          perror("accept");
          exit(1);
       }
       printf("got connection from %s\n", inet_ntoa(cliAddr.sin_addr));
    }

    return 0;
}

void *
sendThreadMain(void * ptr)
{
    int i, ret, iter;
    uint8_t buf[TCP_SEND_BUFLEN];

    initPacketPayload(buf, TCP_SEND_BUFLEN);
    assert(isPacketPayloadCorrect(buf, TCP_SEND_BUFLEN));

    iter = 0;
    for (;;) {
        for (i = 0; i < numMachines; i++) {
           ret = send(clisocks[i], buf, TCP_SEND_BUFLEN, MSG_DONTWAIT);
           if (ret < 0 && errno != EWOULDBLOCK && errno != EAGAIN) {
              perror("send");
              break;
           }
        }

        if (iter % STAT_BATCH_INTERVAL == 0) {
           if (shouldStop) {
              break;
           }
        }

        iter++;
    }

    return 0;
}

void *
receiverThreadMain(void * ptr)
{
    int i, iter;
    ssize_t ret;
    uint8_t buf[TCP_RECV_BUFLEN];
    uint64_t datReceived;
    uint64_t * datPerNodeReceived;

    datPerNodeReceived = (uint64_t *) malloc(sizeof(uint64_t) * numMachines);
    memset(datPerNodeReceived, 0, sizeof(uint64_t) * numMachines);
    
    iter = 0;
    datReceived = 0;
    for (;;) {
        for (i = 0; i < numMachines; i++) {
           ret = recv(servsocks[i], buf, TCP_RECV_BUFLEN, MSG_DONTWAIT);
           if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
              perror("recv");
              exit(1);
           }
           if (ret > 0) {
              datReceived += ret;
              datPerNodeReceived[i] += ret;
           }
        }

        if (iter % STAT_BATCH_INTERVAL == 0) {
            pthread_mutex_lock(&statMutex);
            bytes_received += datReceived;
            memcpy(pernode_bytes_received, datPerNodeReceived, sizeof(uint64_t) * numMachines);
            pthread_mutex_unlock(&statMutex);
            datReceived = 0;

            if (shouldStop) {
               break;
            }
        }
        iter++;
    }

    return 0;
}
