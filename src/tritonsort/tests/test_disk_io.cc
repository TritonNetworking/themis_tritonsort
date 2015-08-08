#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <algorithm>
#include <assert.h>
#include <core/Timer.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>

// DIRECTIOSIZE is the alignment of direct i/o writes
#define DIRECTIOSIZE 512

const int WRITETYPE_WRITE = 0;
const int WRITETYPE_WRITEV = 1;
const int IOTYPE_DIRECTIO = 2;
const int IOTYPE_BUFFERED = 3;

int * create_files(char * dir, int numlds, int iotype,
                   uint64_t maxfilesize, int fallocatesize);
void write_test(int * fds, int numlds, int writesize,
                int numpasses, int wsyscallsize);
void writev_test(int * fds, int numlds, int writesize, int numpasses);

int
main(int argc, char **argv)
{
  char test_dir[1024];
  int writetype, writesize, numlds, iotype, numpasses,
      wsyscallsize, fallocatesize;

  if (argc != 9) {
    fprintf(stderr,
            "Usage: %s <test_directory> <write|writev> <writesize> <numlds> "
            "<directio|nodirectio> <numpasses> <wsyscallsize> "
            "<fallocatesize>\n", argv[0]);
    exit(1);
  }

  // test_dir is where we should create our files
  memcpy(test_dir, argv[1], 1024);

  // write, or writev?
  if (!strcmp(argv[2], "write")) {
    writetype = WRITETYPE_WRITE;
  } else if (!strcmp(argv[2], "writev")) {
    writetype = WRITETYPE_WRITEV;
  } else {
    fprintf(stderr, "Unknown option: %s\n", argv[2]);
    exit(1);
  }

  // write size
  writesize = atoi(argv[3]);

  // number of logical disks
  numlds = atoi(argv[4]);

  // should we use direct i/o?
  if (!strcmp(argv[5], "directio")) {
    iotype = IOTYPE_DIRECTIO;
  } else if (!strcmp(argv[5], "nodirectio")) {
    iotype = IOTYPE_BUFFERED;
  } else {
    fprintf(stderr, "Unknown option: %s\n", argv[2]);
    exit(1);
  }

  numpasses = atoi(argv[6]);
  wsyscallsize = atoi(argv[7]);
  fallocatesize = atoi(argv[8]);

  const uint64_t maxfilesize = writesize * numpasses;
  const uint64_t total_data_written = maxfilesize * numlds;

  // print out parameters for posterity
  printf("test_dir: %s\n", test_dir);
  if (writetype == WRITETYPE_WRITE) {
    printf("write_type: write\n");
  } else {
    printf("write_type: writev\n");
  }
  printf("write_size: %d\n", writesize);
  printf("numlds: %d\n", numlds);
  if (iotype == IOTYPE_DIRECTIO) {
    printf("iotype: direct\n");
  } else {
    printf("iotype: buffered\n");
  }
  printf("total_data_written: %ld\n", total_data_written);
  printf("numpasses: %d\n", numpasses);
  printf("wsyscallsize/iovecsize: %d\n", wsyscallsize);
  printf("fallocatesize: %d\n", fallocatesize);

  // create the files we'll test over
  int * fds = create_files(test_dir, numlds, iotype,
                           maxfilesize, fallocatesize);
  assert(fds != 0);

  Timer totalWriteTime;
  totalWriteTime.start();
  if (writetype == WRITETYPE_WRITE) {
    write_test(fds, numlds, writesize, numpasses, wsyscallsize);
  } else if (writetype == WRITETYPE_WRITEV) {
    writev_test(fds, numlds, writesize, numpasses);
  } else {
    assert(false);
  }
  totalWriteTime.stop();
  printf("total_write_latency: %ld\n", totalWriteTime.getElapsed());

  // derived statistics

  // the 0.953... business converts from MiB to MB
  printf("write_bw_in_MBps: %g\n",
         ((total_data_written / totalWriteTime.getElapsed()) * 0.953674));

  delete [] fds;
}

int *
create_files(char * test_dir, int numlds, int iotype,
             uint64_t maxfilesize, int fallocatesize)
{
  int * fds = new int[numlds];

  for (int i = 0; i < numlds; i++) {
    char fn[1024];
    snprintf(fn, 1024, "%s/%d.dat", test_dir, i);

    int flags = O_CREAT | O_EXCL | O_WRONLY;

    if (iotype == IOTYPE_DIRECTIO) {
      flags = flags | O_DIRECT;
    }

    fds[i] = open(fn, flags, S_IRUSR | S_IWUSR | S_IRWXG | S_IRGRP);

    if (fds[i] == -1) {
      perror("open");
      exit(1);
    }

    // fallocate the file
    int ret = posix_fallocate(fds[i], 0, fallocatesize);
    if (ret != 0) {
      perror("posix_fallocate");
      exit(1);
    }
  }

  return fds;
}

void
write_test(int * fds, int numlds, int writesize,
           int numpasses, int wsyscallsize)
{
  char * data;
  int status = posix_memalign((void **)&data, DIRECTIOSIZE, writesize);

  if (status != 0) {
    printf("posix_memalign failed with error %d", status);
    exit(1);
  }

  for (int i = 0; i < numpasses; i++) {
    std::vector<int> ldlist;
    for (int ld = 0; ld < numlds; ld++) {
      ldlist.push_back(ld);
    }
    random_shuffle(ldlist.begin(), ldlist.end());

    for (std::vector<int>::iterator iter = ldlist.begin();
         iter != ldlist.end(); iter++) {
      int ld = *iter;
      assert(fds[ld] != -1);
      ssize_t remaining = writesize;

      while (remaining > DIRECTIOSIZE) {
        ssize_t alignedWriteSize = (remaining / DIRECTIOSIZE) * DIRECTIOSIZE;
        //printf("write(%d, 'ptr', %d)\n", fds[ld], (int)alignedWriteSize);
        if (alignedWriteSize > wsyscallsize) {
          alignedWriteSize = wsyscallsize;
        }
        ssize_t ret = write(fds[ld], data + (writesize - remaining),
                            alignedWriteSize);
        if (ret < 0) {
          perror("write");
          exit(1);
        }

        remaining -= ret;
      }
    }
  }

  delete [] data;

  // sync the data down to the disk
  for (int ld = 0; ld < numlds; ld++) {
    /*
      ret = posix_fadvise(fds[i], 0, 0, POSIX_FADV_NOREUSE);
      if (ret != 0) {
      perror("posix_fadvise");
      }
      ret = posix_fadvise(fds[i], 0, 0, POSIX_FADV_DONTNEED);
      if (ret != 0) {
      perror("posix_fadvise");
      }
    */
    if (fsync(fds[ld]) != 0) {
      perror("fsync");
    }
    if (close(fds[ld]) != 0) {
      perror("close");
    }
  }
}

void
writev_test(int * fds, int numlds, int writesize, int numpasses)
{
  char * data[1000];
  struct iovec ios[1000];

  for (int i = 0; i < 1000; i++) {
    int status = posix_memalign((void **)&data[i], DIRECTIOSIZE, writesize);

    if (status != 0) {
      printf("posix_memalign failed with error %d", status);
      exit(1);
    }

    ios[i].iov_base = data[i];
    ios[i].iov_len = writesize;
  }

  for (int i = 0; i < numpasses; i++) {

    std::vector<int> ldlist;
    for (int ld = 0; ld < numlds; ld++) {
      assert(fds[ld] != -1);
      ldlist.push_back(ld);
    }
    random_shuffle(ldlist.begin(), ldlist.end());

    for (std::vector<int>::iterator iter = ldlist.begin();
         iter != ldlist.end(); iter++) {
      int ld = *iter;

      ssize_t bytesWritten = writev(fds[ld], ios, 1000);

      if (bytesWritten == -1) {
        perror("writev");
        exit(1);
      }
    }
  }

  for (int i = 0; i < 1000; i++) {
    delete [] data[i];
  }

  // sync the data down to the disk
  for (int ld = 0; ld < numlds; ld++) {
    /*
      ret = posix_fadvise(fds[i], 0, 0, POSIX_FADV_NOREUSE);
      if (ret != 0) {
      perror("posix_fadvise");
      }
      ret = posix_fadvise(fds[i], 0, 0, POSIX_FADV_DONTNEED);
      if (ret != 0) {
      perror("posix_fadvise");
      }
    */
    if (fsync(fds[ld]) != 0) {
      perror("fsync");
    }
    if (close(fds[ld]) != 0) {
      perror("close");
    }
  }
}
