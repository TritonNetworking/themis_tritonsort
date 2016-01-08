/* gensort.c - Generator program for sort benchmarks.
 *
 * Special BSD version 1.5
 * Chris Nyberg <chris.nyberg@ordinal.com>
 *
 * Copyright (C) 2009 - 2016, Chris Nyberg
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in 
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the names of Chris Nyberg nor Ordinal Technology Corp
 *       may be used to endorse or promote products derived from this
 *       software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT
 * HOLDER> BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 * OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
char *Version = "1.5";

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rand16.h"
#include <zlib.h>   /* use crc32() function in zlib */

#if defined(SUMP_PUMP)
# include "sump.h"
# include <fcntl.h>

/* Number of records to be generated per output block */
# define BLK_RECS       100000

/* "instruction" structure that is passed to sump pump threads */
struct gen_instruct
{
    u16         starting_rec;   /* starting record number */
    u8          num_recs;       /* the number of records to generate */
};

#endif

#define REC_SIZE       100
#define SKEW_BYTES     6
#define HEX_DIGIT(x) ((x) >= 10 ? 'A' + (x) - 10 : '0' + (x))

/* Structure for a 10-deep queue of random numbers.  This queue is used
 * to cheaply create records where every byte is psuedo random, while
 * only creating one 128-bit number per record.  The 10-byte keys that
 * begin each record are generated using the top 10 bytes of a random
 * number (this is exactly the same as was done in the original gensort
 * program).  The next 90 bytes of each record are broken into 9 10-byte
 * parts.  Each part is generated using subsequent random numbers in the
 * queue xor'ed with a constant that is particular to that part. 
 */
#define QUEUE_SIZE      10
#define get_queue_rand(rq, index) (rq->rand[(index + rq->head_index) % QUEUE_SIZE])
typedef struct
{
    int         head_index;             /* index of head of queue in rand[] */
    u16         curr_rec_number;        /* current record number */
    u16         rand[QUEUE_SIZE];       /* circular queue of random numbers */
    int         skew_index;             /* index into key skews; approximately
                                         * log2(current_rec_number) + 1 */
} rand_queue;
#define RQ(rq, i) (rq->rand[rq->head_index + i - (rq->head_index + i >= QUEUE_SIZE ? QUEUE_SIZE : 0)])
#define ASSIGN_10_BYTES(rec_buf, rand) \
    (rec_buf)[0] = (rand.hi8 >> 56) & 0xFF; \
    (rec_buf)[1] = (rand.hi8 >> 48) & 0xFF; \
    (rec_buf)[2] = (rand.hi8 >> 40) & 0xFF; \
    (rec_buf)[3] = (rand.hi8 >> 32) & 0xFF; \
    (rec_buf)[4] = (rand.hi8 >> 24) & 0xFF; \
    (rec_buf)[5] = (rand.hi8 >> 16) & 0xFF; \
    (rec_buf)[6] = (rand.hi8 >>  8) & 0xFF; \
    (rec_buf)[7] = (rand.hi8 >>  0) & 0xFF; \
    (rec_buf)[8] = (rand.lo8 >> 56) & 0xFF; \
    (rec_buf)[9] = (rand.lo8 >> 48) & 0xFF

int         Print_checksum;     /* boolean to produce record checksum */
u16         Sum16;              /* record checksum */
void        (*Gen)(unsigned char *buf, rand_queue *rq); /* ptr to generator */
int         Skip_output;        /* boolean to skip output */

unsigned char Skew_binary[129][SKEW_BYTES] = {
    /*   0 */ { 0x4a, 0x69, 0x6d, 0x47, 0x72, 0x61 },
    /*   1 */ { 0x95, 0xe0, 0xe4, 0x82, 0x62, 0xb3 },
    /*   2 */ { 0x45, 0x97, 0x93, 0x53, 0xdb, 0xed },
    /*   3 */ { 0x88, 0x2a, 0x02, 0xc3, 0x15, 0x36 },
    /*   4 */ { 0x5c, 0x90, 0xab, 0x38, 0xae, 0x52 },
    /*   5 */ { 0x72, 0xdc, 0x0c, 0xa5, 0x1e, 0x33 },
    /*   6 */ { 0x10, 0x43, 0x1a, 0xf6, 0xa0, 0xd8 },
    /*   7 */ { 0x5e, 0xfc, 0x4a, 0xbf, 0xac, 0xa2 },
    /*   8 */ { 0x44, 0xf7, 0x8c, 0x8b, 0x40, 0xbf },
    /*   9 */ { 0x84, 0xc0, 0x99, 0x2f, 0x3b, 0x94 },
    /*  10 */ { 0xb3, 0xe9, 0x68, 0x9d, 0xe1, 0x6b },
    /*  11 */ { 0xf8, 0xf6, 0x42, 0x63, 0xfd, 0x0b },
    /*  12 */ { 0xda, 0x7a, 0x45, 0xa1, 0x82, 0xde },
    /*  13 */ { 0x9b, 0x6b, 0x48, 0x25, 0xe2, 0x51 },
    /*  14 */ { 0xdc, 0x68, 0x2a, 0x00, 0x64, 0x7e },
    /*  15 */ { 0xf2, 0x5b, 0xd1, 0x54, 0x39, 0xd1 },
    /*  16 */ { 0xf2, 0xfa, 0x42, 0xed, 0x18, 0x72 },
    /*  17 */ { 0x6a, 0x59, 0x45, 0x1b, 0xe8, 0xd0 },
    /*  18 */ { 0x27, 0x29, 0xb9, 0x77, 0x14, 0x71 },
    /*  19 */ { 0x87, 0x9b, 0x2f, 0xb7, 0xbb, 0x35 },
    /*  20 */ { 0x68, 0xd0, 0xcc, 0x3c, 0x19, 0x99 },
    /*  21 */ { 0x27, 0xd8, 0x08, 0x79, 0xd7, 0x9e },
    /*  22 */ { 0xb0, 0x79, 0x50, 0x11, 0xb7, 0x82 },
    /*  23 */ { 0x46, 0x4f, 0xb8, 0x4a, 0xb8, 0x48 },
    /*  24 */ { 0x21, 0xf0, 0x3e, 0xe8, 0xac, 0x41 },
    /*  25 */ { 0xe7, 0x96, 0x1c, 0x0d, 0x82, 0x7f },
    /*  26 */ { 0x84, 0xd9, 0x04, 0x45, 0x7a, 0x61 },
    /*  27 */ { 0x53, 0x59, 0xd3, 0x5d, 0xa8, 0x84 },
    /*  28 */ { 0x4e, 0x38, 0x54, 0x66, 0x52, 0x5c },
    /*  29 */ { 0x87, 0x0f, 0xa6, 0x45, 0x90, 0x11 },
    /*  30 */ { 0xff, 0x00, 0x46, 0x3a, 0xdf, 0xc8 },
    /*  31 */ { 0x89, 0xca, 0x67, 0xc2, 0x9c, 0x93 },
    /*  32 */ { 0x75, 0x50, 0x90, 0xc0, 0x17, 0x7d },
    /*  33 */ { 0xeb, 0x4d, 0x81, 0xa5, 0xc9, 0xea },
    /*  34 */ { 0x8a, 0x85, 0x68, 0xb3, 0x08, 0x6f },
    /*  35 */ { 0x5d, 0xa6, 0x9a, 0x3d, 0x86, 0x67 },
    /*  36 */ { 0x6a, 0x97, 0x43, 0x59, 0xea, 0xab },
    /*  37 */ { 0x63, 0xb6, 0x04, 0x4b, 0x8e, 0x78 },
    /*  38 */ { 0x33, 0x41, 0x49, 0x12, 0xcb, 0x67 },
    /*  39 */ { 0x22, 0x6d, 0xf2, 0xb7, 0x9c, 0x9b },
    /*  40 */ { 0x1e, 0x58, 0x39, 0x6c, 0x59, 0x9a },
    /*  41 */ { 0x4d, 0x67, 0x60, 0x91, 0xdc, 0xfe },
    /*  42 */ { 0xc9, 0x8f, 0x25, 0x9b, 0x15, 0x0d },
    /*  43 */ { 0xa8, 0x27, 0xdc, 0x9a, 0xff, 0x7e },
    /*  44 */ { 0x06, 0x96, 0xc9, 0xa1, 0xba, 0x3b },
    /*  45 */ { 0x6d, 0x16, 0xe3, 0x38, 0xd7, 0x77 },
    /*  46 */ { 0xac, 0x35, 0xa4, 0x3b, 0xa6, 0x62 },
    /*  47 */ { 0x7e, 0xe1, 0xe4, 0x00, 0x71, 0x63 },
    /*  48 */ { 0xa1, 0x6b, 0x6f, 0xa9, 0xf1, 0xea },
    /*  49 */ { 0x2c, 0xb7, 0xa1, 0xbb, 0x93, 0x62 },
    /*  50 */ { 0x2f, 0x4b, 0x08, 0x26, 0x7c, 0xe7 },
    /*  51 */ { 0x86, 0xd1, 0x92, 0xc5, 0x41, 0x84 },
    /*  52 */ { 0xf6, 0xe4, 0x14, 0x3f, 0xde, 0xaa },
    /*  53 */ { 0x45, 0x83, 0x69, 0xe8, 0x3c, 0xb9 },
    /*  54 */ { 0x6c, 0x15, 0xf7, 0x0f, 0x81, 0x76 },
    /*  55 */ { 0xc0, 0xb4, 0x87, 0x02, 0x6b, 0x7f },
    /*  56 */ { 0xae, 0x90, 0x31, 0xf8, 0x7d, 0x14 },
    /*  57 */ { 0x6b, 0x25, 0xdc, 0x59, 0xe0, 0x9e },
    /*  58 */ { 0x88, 0x38, 0x23, 0x62, 0x42, 0x4b },
    /*  59 */ { 0xaf, 0xb9, 0x6f, 0x95, 0xd3, 0x2b },
    /*  60 */ { 0xc1, 0xd4, 0xfc, 0xf5, 0x77, 0xdb },
    /*  61 */ { 0xc6, 0x8d, 0x66, 0xd1, 0x84, 0x53 },
    /*  62 */ { 0x74, 0xfe, 0x19, 0xdc, 0x52, 0x68 },
    /*  63 */ { 0x8b, 0x6a, 0xe0, 0x36, 0x71, 0x3b },
    /*  64 */ { 0x33, 0xd5, 0xb5, 0xb1, 0x5c, 0x70 },
    /*  65 */ { 0x5e, 0x46, 0xf5, 0x43, 0x2f, 0x2c },
    /*  66 */ { 0x26, 0x55, 0x46, 0x25, 0xdd, 0x68 },
    /*  67 */ { 0xf6, 0xed, 0xf4, 0xe3, 0xba, 0xfd },
    /*  68 */ { 0xcf, 0x9f, 0xb7, 0x8a, 0xa3, 0xca },
    /*  69 */ { 0x08, 0x14, 0x09, 0x8c, 0x2d, 0x9a },
    /*  70 */ { 0xea, 0x1c, 0xfc, 0x70, 0xfb, 0x3f },
    /*  71 */ { 0x68, 0xed, 0xe8, 0x28, 0xd4, 0xc5 },
    /*  72 */ { 0x86, 0x67, 0xc9, 0xb9, 0xbb, 0x8c },
    /*  73 */ { 0xe7, 0xaf, 0xa5, 0x12, 0x6f, 0x3d },
    /*  74 */ { 0xd0, 0x01, 0x02, 0xa1, 0xc5, 0x10 },
    /*  75 */ { 0xf9, 0x54, 0x9b, 0x14, 0x3a, 0x9e },
    /*  76 */ { 0xda, 0x0f, 0x75, 0x54, 0xe7, 0x9e },
    /*  77 */ { 0xca, 0x16, 0xea, 0x9b, 0x71, 0xf0 },
    /*  78 */ { 0xf9, 0x5a, 0x03, 0x5a, 0x6b, 0xe8 },
    /*  79 */ { 0xf3, 0xf0, 0x37, 0x8f, 0x70, 0x43 },
    /*  80 */ { 0xbb, 0x4d, 0x8a, 0x4f, 0xd7, 0x6c },
    /*  81 */ { 0xc9, 0x4a, 0x04, 0x75, 0x3d, 0xfc },
    /*  82 */ { 0x30, 0x9a, 0x89, 0x71, 0x88, 0x29 },
    /*  83 */ { 0xdd, 0xa5, 0x70, 0x75, 0xdf, 0x7a },
    /*  84 */ { 0xa6, 0x61, 0xcd, 0xc3, 0x16, 0x22 },
    /*  85 */ { 0xc5, 0x96, 0x93, 0x15, 0x25, 0x8c },
    /*  86 */ { 0x3a, 0x16, 0x93, 0xac, 0x95, 0x3b },
    /*  87 */ { 0xe9, 0x0e, 0x58, 0x7d, 0xf6, 0x9f },
    /*  88 */ { 0x8f, 0xc9, 0x47, 0x45, 0xb2, 0xfd },
    /*  89 */ { 0xa7, 0x6f, 0xd6, 0xfc, 0x71, 0x78 },
    /*  90 */ { 0x4c, 0x67, 0x4c, 0xe2, 0x3a, 0x86 },
    /*  91 */ { 0xf0, 0x05, 0xc4, 0x06, 0x15, 0x58 },
    /*  92 */ { 0x2a, 0x90, 0xa6, 0x7e, 0x8c, 0x6c },
    /*  93 */ { 0x5a, 0xdc, 0xee, 0x8c, 0xa7, 0x09 },
    /*  94 */ { 0xff, 0x81, 0xed, 0x50, 0xd5, 0x78 },
    /*  95 */ { 0xed, 0x44, 0x53, 0x6c, 0x44, 0x16 },
    /*  96 */ { 0x64, 0x8e, 0x48, 0x56, 0x64, 0x1a },
    /*  97 */ { 0xa4, 0x47, 0x3f, 0x64, 0xf9, 0xd0 },
    /*  98 */ { 0x6e, 0x45, 0xfb, 0x3d, 0x1c, 0x2c },
    /*  99 */ { 0x3c, 0xb4, 0x46, 0xe3, 0x07, 0x0c },
    /* 100 */ { 0x0a, 0x25, 0xa9, 0x9a, 0xf4, 0x39 },
    /* 101 */ { 0x2c, 0xb5, 0xa1, 0xdc, 0xef, 0x47 },
    /* 102 */ { 0x0e, 0x4d, 0x9c, 0xd4, 0x57, 0xae },
    /* 103 */ { 0x3b, 0x86, 0x6f, 0x4a, 0x1a, 0xef },
    /* 104 */ { 0x3e, 0x98, 0xbe, 0xe5, 0xfd, 0xf5 },
    /* 105 */ { 0x99, 0x9a, 0x6d, 0x40, 0xa4, 0x3f },
    /* 106 */ { 0xf7, 0xe8, 0xb4, 0x8b, 0xaa, 0xf9 },
    /* 107 */ { 0xef, 0xe5, 0x08, 0x20, 0x54, 0x1e },
    /* 108 */ { 0xf7, 0xd1, 0x98, 0x23, 0x53, 0x67 },
    /* 109 */ { 0x21, 0xa5, 0x8b, 0xdd, 0x20, 0x20 },
    /* 110 */ { 0xed, 0x59, 0xb7, 0x23, 0xb1, 0x6e },
    /* 111 */ { 0x20, 0xd1, 0xef, 0x94, 0x2f, 0x79 },
    /* 112 */ { 0x8f, 0x23, 0x46, 0xa3, 0x2a, 0xf7 },
    /* 113 */ { 0xb0, 0x98, 0x61, 0xcc, 0x8b, 0x8a },
    /* 114 */ { 0xb5, 0xe2, 0x63, 0x33, 0x3a, 0x0d },
    /* 115 */ { 0x63, 0xc1, 0xb7, 0xe7, 0x2b, 0x41 },
    /* 116 */ { 0xaf, 0x90, 0x85, 0x9b, 0x1c, 0xa9 },
    /* 117 */ { 0x9a, 0x52, 0x5e, 0x2f, 0x33, 0xbf },
    /* 118 */ { 0xc2, 0x83, 0xea, 0x63, 0xf3, 0x00 },
    /* 119 */ { 0x02, 0x0d, 0xe5, 0x60, 0x00, 0xf6 },
    /* 120 */ { 0x55, 0xcf, 0xe9, 0xd4, 0x3d, 0x64 },
    /* 121 */ { 0xb5, 0xd7, 0x69, 0x82, 0x36, 0x39 },
    /* 122 */ { 0xe6, 0x29, 0xca, 0xb5, 0x3c, 0xa1 },
    /* 123 */ { 0x9c, 0xbf, 0xeb, 0x07, 0x9d, 0xde },
    /* 124 */ { 0xa0, 0xba, 0x1e, 0xd1, 0xea, 0x79 },
    /* 125 */ { 0x0b, 0xe5, 0x49, 0xa5, 0x12, 0xd3 },
    /* 126 */ { 0x78, 0x70, 0xde, 0x1f, 0xc5, 0x61 },
    /* 127 */ { 0x98, 0xa2, 0x54, 0x2f, 0xd2, 0x3d },
    /* 128 */ { 0xe1, 0xdc, 0x46, 0xb6, 0x45, 0xc4 } };

unsigned char Skew_ascii[129][SKEW_BYTES] = {
    /*   0 */ { 'A', 's', 'f', 'A', 'G', 'H' },
    /*   1 */ { '~', 's', 'H', 'd', '0', 'j' },
    /*   2 */ { 'u', 'I', '^', 'E', 'Y', 'm' },
    /*   3 */ { 'Q', ')', 'J', 'N', ')', 'R' },
    /*   4 */ { 'o', '4', 'F', 'o', 'B', 'k' },
    /*   5 */ { '*', '}', '-', 'W', 'z', '1' },
    /*   6 */ { '0', 'f', 's', 's', 'x', '}' },
    /*   7 */ { 'm', 'z', '4', 'V', 'C', 'N' },
    /*   8 */ { 'm', 'y', '+', '=', '5', 'r' },
    /*   9 */ { '5', 'H', 'A', '\\', 'z', '%' },
    /*  10 */  { '`', 'P', 'k', 'X', 'Q', '<' },
    /*  11 */  { 'G', 'L', 'S', 'n', 'l', 'm' },
    /*  12 */  { 's', 'w', 'B', 'z', 'Q', '#' },
    /*  13 */  { '9', 'S', 'C', '<', 'z', ' ' },
    /*  14 */  { 'o', '7', '~', 'd', 'r', 's' },
    /*  15 */  { 'K', '~', '%', 'q', '4', ']' },
    /*  16 */  { 'g', 'j', '<', 'm', '5', 'S' },
    /*  17 */  { '&', '~', '2', 'C', 'h', '{' },
    /*  18 */  { 'B', ']', 'a', 'z', '\'', '~' },
    /*  19 */  { ',', '2', 'K', ',', 'D', 'K' },
    /*  20 */  { '}', '[', 'v', '>', 'z', 'y' },
    /*  21 */  { 'k', 'X', 'r', 'G', '^', 'e' },
    /*  22 */  { 'v', '%', '^', 'R', 'G', '^' },
    /*  23 */  { 'v', 'i', '@', '>', '8', 'j' },
    /*  24 */  { '}', 'd', 'A', '7', '*', '<' },
    /*  25 */  { '>', 'Y', '$', 'K', 'c', ')' },
    /*  26 */  { 'u', 'F', '?', 'T', ';', '*' },
    /*  27 */  { '~', '%', '.', '@', '@', ',' },
    /*  28 */  { 'J', 'P', 'r', 'A', 'K', 'c' },
    /*  29 */  { '*', 'i', '@', 'l', 'F', '3' },
    /*  30 */  { '9', 'U', 'H', '>', '%', 'F' },
    /*  31 */  { '-', '^', '1', '~', '=', 'a' },
    /*  32 */  { '.', '\\', 'K', 'q', 'T', '&' },
    /*  33 */  { '9', 'R', 't', 's', 'P', 'a' },
    /*  34 */  { 'F', 'q', 'y', '~', ']', 'O' },
    /*  35 */  { 'W', 'j', '-', '%', '?', 'e' },
    /*  36 */  { 'R', 'v', ']', 'Q', 'H', 'o' },
    /*  37 */  { '1', ' ', 'e', '!', 'o', 'g' },
    /*  38 */  { 'N', ',', 'g', 'k', '>', '3' },
    /*  39 */  { 's', '.', '7', '2', 'Y', '.' },
    /*  40 */  { 'j', '*', '0', '`', 'g', 'c' },
    /*  41 */  { 'R', '+', 'S', 'h', '=', 'K' },
    /*  42 */  { 'x', ']', 'w', ']', '=', 'D' },
    /*  43 */  { 'm', 'a', 'U', '!', 'U', 'o' },
    /*  44 */  { 's', 'm', 'N', '$', 'i', ' ' },
    /*  45 */  { 'T', '}', '#', ' ', '`', '9' },
    /*  46 */  { 'G', '2', 'o', 'M', 'V', 'H' },
    /*  47 */  { '(', 't', 'v', '1', '\'', '>' },
    /*  48 */  { 'R', 'E', '%', 'Z', '{', 'o' },
    /*  49 */  { '\'', '%', ':', 'Y', 'Y', '?' },
    /*  50 */  { 'N', '(', 'J', 'u', 'G', '%' },
    /*  51 */  { '`', 'E', '$', ',', 'x', 'R' },
    /*  52 */  { 'j', 'z', 'u', '$', 'F', 'n' },
    /*  53 */  { '|', 'x', 'a', 'P', 'O', '\\' },
    /*  54 */  { 'G', 'C', 'U', '4', 'g', 'f' },
    /*  55 */  { 'J', 'U', '9', 's', '0', '3' },
    /*  56 */  { 'D', '@', 'v', '9', '"', '"' },
    /*  57 */  { 'O', 'f', 'f', 'B', '8', '(' },
    /*  58 */  { '-', '_', ')', ' ', 'R', '\'' },
    /*  59 */  { '\'', 'z', '4', 'P', '|', 'n' },
    /*  60 */  { '[', '!', 'N', 'b', 'x', '&' },
    /*  61 */  { 'p', '?', '"', '\\', '\\', '6' },
    /*  62 */  { '+', '2', 'A', '^', 'x', 'P' },
    /*  63 */  { '-', 'c', 'u', 'J', 'v', 'y' },
    /*  64 */  { 'i', 'x', '^', 'T', '<', 'S' },
    /*  65 */  { 'E', 'v', 't', 'L', '^', '`' },
    /*  66 */  { '\\', 'E', '=', 'u', 'j', '=' },
    /*  67 */  { 'C', 'U', '7', 'l', '~', '"' },
    /*  68 */  { ',', 'v', 'W', 'e', '=', '|' },
    /*  69 */  { 'k', '|', 'M', '.', 'e', 'l' },
    /*  70 */  { 'u', '<', '#', 'x', '_', 'G' },
    /*  71 */  { 'K', ' ', '&', '>', 'Q', '-' },
    /*  72 */  { 'E', '(', 'g', '$', '5', '-' },
    /*  73 */  { '~', '\\', 'p', 't', 's', 'n' },
    /*  74 */  { 'Y', 'h', 'l', 'P', 'k', '`' },
    /*  75 */  { 'W', 'B', 'z', 'u', 'l', ',' },
    /*  76 */  { 'q', ')', 'e', 'p', 'J', 's' },
    /*  77 */  { '`', 'W', 'V', 'k', 'W', '&' },
    /*  78 */  { 'H', 'X', 'g', '#', 'Z', '^' },
    /*  79 */  { '-', '&', '8', '$', 'l', 'c' },
    /*  80 */  { '^', 'S', 'p', 'q', 'w', 'o' },
    /*  81 */  { 'P', '-', '6', 'Y', 'b', 'w' },
    /*  82 */  { 'a', '`', ':', ';', 'y', 'd' },
    /*  83 */  { 'd', 'O', ']', 'G', '~', '-' },
    /*  84 */  { 'W', 'M', '^', ';', 'r', 'v' },
    /*  85 */  { '-', 'W', 'w', 'U', '4', 'B' },
    /*  86 */  { 'y', '\'', 'w', '/', 'P', 'Z' },
    /*  87 */  { '#', 'j', 'A', '`', 'l', '-' },
    /*  88 */  { ':', '!', 'W', '.', '=', 'v' },
    /*  89 */  { '"', '!', 'r', '4', 'u', 'g' },
    /*  90 */  { 'O', '?', '(', '|', '_', 'd' },
    /*  91 */  { 'x', 'w', 'W', 'R', 'j', 'k' },
    /*  92 */  { 'T', 'e', ' ', '(', '9', '@' },
    /*  93 */  { '(', 'c', 'b', '6', '2', '1' },
    /*  94 */  { 'h', 'g', '5', 'x', ' ', 's' },
    /*  95 */  { 'w', '5', 'n', 'n', '`', 'w' },
    /*  96 */  { 'q', 'e', 'Q', 'N', 'o', 'B' },
    /*  97 */  { '6', 'n', '>', 'i', 'M', 'b' },
    /*  98 */  { 'u', 'G', 'z', '7', 'S', '=' },
    /*  99 */  { 'r', 'd', ')', 'x', '<', '>' },
    /* 100 */  { 'x', 'E', 'w', 'a', '^', ':' },
    /* 101 */  { '\'', ':', ']', '$', '}', 'b' },
    /* 102 */  { 'v', 'Z', 'l', '"', 'J', 'i' },
    /* 103 */  { 'o', '%', ',', '!', 'f', 'S' },
    /* 104 */  { '*', '+', 'o', '@', '+', 't' },
    /* 105 */  { '(', 'C', 'o', 'K', 'U', 'h' },
    /* 106 */  { 'I', '>', ':', '9', 'J', 'Q' },
    /* 107 */  { 'e', 'S', 'z', 'U', '!', 'X' },
    /* 108 */  { '[', 'Q', 'Z', '{', 'a', '&' },
    /* 109 */  { '?', ':', 'Y', 'p', '4', '%' },
    /* 110 */  { '\'', '_', '>', '/', ' ', '4' },
    /* 111 */  { 'K', 'r', 'N', 'W', '.', '!' },
    /* 112 */  { 'j', '1', '7', 'a', '3', 'a' },
    /* 113 */  { ')', 'o', '-', 'Q', 'L', 'H' },
    /* 114 */  { 'a', 'l', ';', ':', 'O', 'E' },
    /* 115 */  { 'I', '@', 'L', '*', '`', 'H' },
    /* 116 */  { '<', '!', 'p', 'S', 'W', 'F' },
    /* 117 */  { '1', 'w', '@', 'T', 'Z', 'u' },
    /* 118 */  { 'V', 'h', '%', '|', 'K', 'i' },
    /* 119 */  { 'h', '{', 'y', 'd', 'A', 'P' },
    /* 120 */  { ']', 'V', '9', 'h', 'C', 'o' },
    /* 121 */  { '.', 'T', 'K', '[', '[', 'l' },
    /* 122 */  { 'F', 'k', 't', 'H', 's', 'd' },
    /* 123 */  { 'K', 'i', '|', '$', '-', 'M' },
    /* 124 */  { '\\', 'g', 'W', 'C', '3', '[' },
    /* 125 */  { '.', '"', '^', '}', ',', ' ' },
    /* 126 */  { 'k', ' ', 'V', 'C', '?', 'n' },
    /* 127 */  { '}', ']', 'K', 'k', '&', ':' },
    /* 128 */  { '1', '2', 'N', 'S', 'K', '|' } };
    
/* init_rand_queue - initialize a queue of random numbers 
 */
void init_rand_queue(rand_queue *rq, u16 starting_rec_number)
{
    int         i;
    
    rq->head_index = 0;
    rq->curr_rec_number = starting_rec_number;
    rq->rand[0] = next_rand(skip_ahead_rand(rq->curr_rec_number));
    for (i = 1; i < QUEUE_SIZE; i++)
        rq->rand[i] = next_rand(rq->rand[i - 1]);
    rq->skew_index = 0;
}


/* bump_queue - progress random queue to next random number and record number.
 */
void bump_queue(rand_queue *rq)
{
    int tail_index;

    /* head_index is the head of the queue.  find the tail index. */
    tail_index = rq->head_index - 1;
    if (tail_index < 0)
        tail_index = QUEUE_SIZE - 1;

    /* make a new tail entry where the current head is */
    rq->rand[rq->head_index] = next_rand(rq->rand[tail_index]);

    /* bump the head_index to make a new head of the queue */
    if (++rq->head_index == QUEUE_SIZE)
        rq->head_index = 0;

    /* bump the current record number */
    if (++rq->curr_rec_number.lo8 == 0)
        ++rq->curr_rec_number.hi8;
}


/* gen_rec - generate a "binary" record suitable for all sort
 *              benchmarks *except* PennySort.
 */
void gen_rec(unsigned char *rec_buf, rand_queue *rq)
{
    u16         rand;
    
    /* generate the 10-byte key using the high 10 bytes of the 1st 128-bit
     * random number
     */
    rand = RQ(rq, 0);
    ASSIGN_10_BYTES(rec_buf + 0, rand);

    /* generate next 10 bytes using 2nd random number, xor with a constant
     * that is specific to the 2nd ten bytes.  This could allow a
     * a rogue entrant to compress the sort input 10 : 1 using these
     * same xor constants, but this would violate the sort contest rules.
     * The psuedo-random records should make ineffective any inadvertent
     * compression at the hardware or lower levels of the network protocols.
     */
    rand = RQ(rq, 1);
    rand.hi8 ^= 0xF0E8E4E2E1D8D4D2LL;   rand.lo8 ^= 0xD1CC000000000000LL;
    ASSIGN_10_BYTES(rec_buf + 10, rand);

    /* get next 10 bytes using 3rd random number, xor with specific constant
     */
    rand = RQ(rq, 2);
    rand.hi8 ^= 0xCAC9C6C5C3B8B4B2LL;   rand.lo8 ^= 0xB1AC000000000000LL;
    ASSIGN_10_BYTES(rec_buf + 20, rand);

    /* get next 10 bytes using 4th random number, xor with specific constant
     */
    rand = RQ(rq, 3);
    rand.hi8 ^= 0xAAA9A6A5A39C9A99LL;   rand.lo8 ^= 0x9695000000000000LL;
    ASSIGN_10_BYTES(rec_buf + 30, rand);

    /* get next 10 bytes using 5th random number, xor with specific constant
     */
    rand = RQ(rq, 4);
    rand.hi8 ^= 0x938E8D8B87787472LL;   rand.lo8 ^= 0x716C000000000000LL;
    ASSIGN_10_BYTES(rec_buf + 40, rand);

    /* get next 10 bytes using 6th random number, xor with specific constant
     */
    rand = RQ(rq, 5);
    rand.hi8 ^= 0x6A696665635C5A59LL;   rand.lo8 ^= 0x5655000000000000LL;
    ASSIGN_10_BYTES(rec_buf + 50, rand);

    /* get next 10 bytes using 7th random number, xor with specific constant
     */
    rand = RQ(rq, 6);
    rand.hi8 ^= 0x534E4D4B473C3A39LL;   rand.lo8 ^= 0x3635000000000000LL;
    ASSIGN_10_BYTES(rec_buf + 60, rand);

    /* get next 10 bytes using 8th random number, xor with specific constant
     */
    rand = RQ(rq, 7);
    rand.hi8 ^= 0x332E2D2B271E1D1BLL;   rand.lo8 ^= 0x170F000000000000LL;
    ASSIGN_10_BYTES(rec_buf + 70, rand);

    /* get next 10 bytes using 9th random number, xor with specific constant
     */
    rand = RQ(rq, 8);
    rand.hi8 ^= 0xC8C4C2C198949291LL;   rand.lo8 ^= 0x8CE0000000000000LL;
    ASSIGN_10_BYTES(rec_buf + 80, rand);

    /* get last 10 bytes using 10th random number, xor with specific constant
     */
    rand = RQ(rq, 9);
    rand.hi8 ^= 0x170F332E2D2B271ELL;   rand.lo8 ^= 0x1D1B000000000000LL;
    ASSIGN_10_BYTES(rec_buf + 90, rand);
}


/* get_skew_index - get the skew index for the current record number.
 *                  The skew number is the number of bits of relevance
 *                  in the current record number, or roughly log2(rec_num).
 *                  Examples:
 *                             current_rec_number  -->   skew_index
 *                            hi8              lo8
 *                     0000000000000000 0000000000000000     0
 *                     0000000000000000 0000000000000001     1
 *                     0000000000000000 0000000000000002     2
 *                     0000000000000000 0000000000000003     2
 *                     0000000000000000 0000000000000004     3
 *                     0000000000000000 0000000000000005     3
 *                     0000000000000000 0000000000000006     3
 *                     0000000000000000 0000000000000007     3
 *                     0000000000000000 0000000000000008     4
 *                     0000000000000000 0000000000000009     4
 *                     0000000000000000 000000000000000A     4
 *                     0000000000000000 000000000000000B     4
 *                     0000000000000000 000000000000000C     4
 *                     0000000000000000 000000000000000D     4
 *                     0000000000000000 000000000000000E     4
 *                     0000000000000000 000000000000000F     4
 *                     0000000000000000 0000000000000010     5
 *                     0000000000000000 8000000000000000    64
 *                     0000000000000000 FFFFFFFFFFFFFFFF    64
 *                     0000000000000001 0000000000000000    65
 *                     0000000000000001 0000000000000001    65
 *                     8000000000000000 0000000000000000   128
 *                     FFFFFFFFFFFFFFFF FFFFFFFFFFFFFFFF   128
 */
int get_skew_index(rand_queue *rq)
{
    int         skew_index;

    /* Get last skew_index for previous record. Skew_index will be 0 if
     * this is the first record generated by this rand_queue.
     */
    skew_index = rq->skew_index;

    /* Given that either:
     * 1) this is first record generated from the rand_queue, and
     *    skew_index is 0; or
     * 2) this is not the first record generated from the rand_queue, and
     *    skew_index is the correct index of the previous record number,
     * then figure out what the skew_index is for the current record number.
     * In most cases (scenario 2 above), the skew_index for the current
     * record number is the same as for the previous record number, but we
     * need to verify that. In other scenario 2 cases, the skew_index will
     * need to be incremented by 1. In the scenario 1 case (first record
     * generated by this rand_queue), a linear search will need to be done
     * to find the correct skew_index (don't worry, it's logarithmic). 
     */
    if (rq->curr_rec_number.hi8 == 0)
    {
        if (rq->curr_rec_number.lo8 == 0)
            skew_index = 0;
        /* if highest order bit is set */
        else if (rq->curr_rec_number.lo8 & 0xF000000000000000LL)
            skew_index = 64;
        else
        {
            /* while 2**(skew_index) <= lo8 */
            while (((u8)0x1 << skew_index) <= rq->curr_rec_number.lo8)
                skew_index++;
        }
    }
    else
    {
        /* if highest order bit is set */
        if (rq->curr_rec_number.hi8 & 0xF000000000000000LL)
            skew_index = 128;
        else
        {
            if (skew_index < 64)
                skew_index = 64;
            /* while 2**(skew_index - 64) <= hi8 */
            while (((u8)0x1 << (skew_index - 64)) <= rq->curr_rec_number.hi8)
                skew_index++;
        }
    }
    /* remember index to speed up index calculation for next possible record */
    rq->skew_index = skew_index; 
    return (skew_index);
}


/* gen_skewed_rec - generate a "binary" skewed record suitable for 
 *                  alternate Daytona skewed data test.
 */
void gen_skewed_rec(unsigned char *rec_buf, rand_queue *rq)
{
    int                 skew_index;
    u16                 rand;
    unsigned char       mask;
    unsigned char       *skew_bytes;
    int                 skew_bits;
    
    /* first generate non-skewed record */
    gen_rec(rec_buf, rq);

    /* get skew index for current record */
    skew_index = get_skew_index(rq);

    /* use skew_index to get a pointer to the potential SKEW_BYTES key bytes
     * which may replace the high-order key bytes just generated by gen_rec().
     */
    skew_bytes = Skew_binary[skew_index];
    
    rand = RQ(rq, 0);

    /* get an index in the inclusive range 0 - 48 from 6 bits in lo8 */
    skew_bits = (int)(rand.lo8 >> 32) & 0x3F;
    if (skew_bits > 8 * SKEW_BYTES)
        skew_bits = 0;  /* each rec has 16/64 = .25 chance to not be skewed */

    /* replace the high-order "skew_bits" bits in record key with bits from
     * the skew bytes.
     */
    while (skew_bits >= 8) /* while there is a whole byte to replace */
    {
        *rec_buf = *skew_bytes;
        skew_bits -= 8;
        rec_buf++;
        skew_bytes++;
    }
    if (skew_bits > 0)
    {
        /* We know that skew_bits is at least 1 and no more than 7.
         * Replace the high-order "skew_bits" bits with the same bits from
         * the byte pointed to by skew_bytes. Leave the remain bits the same.
         */
        mask = (unsigned char)0xFF >> skew_bits;
        *rec_buf = (*skew_bytes & ~mask) | (*rec_buf & mask);
    }
}


/* gen_ascii_rec = generate an ascii record suitable for all sort
 *              benchmarks including PennySort.
 */
void gen_ascii_rec(unsigned char *rec_buf, rand_queue *rq)
{
    int         i;
    u16         rand = rq->rand[rq->head_index];
    u16         rec_number = rq->curr_rec_number;
    u8          temp;
    
    /* generate the 10-byte ascii key using mostly the high 64 bits.
     */
    temp = rand.hi8;
    rec_buf[0] = (unsigned char)(' ' + (temp % 95));
    temp /= 95;
    rec_buf[1] = (unsigned char)(' ' + (temp % 95));
    temp /= 95;
    rec_buf[2] = (unsigned char)(' ' + (temp % 95));
    temp /= 95;
    rec_buf[3] = (unsigned char)(' ' + (temp % 95));
    temp /= 95;
    rec_buf[4] = (unsigned char)(' ' + (temp % 95));
    temp /= 95;
    rec_buf[5] = (unsigned char)(' ' + (temp % 95));
    temp /= 95;
    rec_buf[6] = (unsigned char)(' ' + (temp % 95));
    temp /= 95;
    rec_buf[7] = (unsigned char)(' ' + (temp % 95));
    temp = rand.lo8;
    rec_buf[8] = (unsigned char)(' ' + (temp % 95));
    temp /= 95;
    rec_buf[9] = (unsigned char)(' ' + (temp % 95));
    temp /= 95;

    /* add 2 bytes of "break" */
    rec_buf[10] = ' ';
    rec_buf[11] = ' ';
    
    /* convert the 128-bit record number to 32 bits of ascii hexadecimal
     * as the next 32 bytes of the record.
     */
    for (i = 0; i < 16; i++)
        rec_buf[12 + i] =
            (unsigned char)(HEX_DIGIT((rec_number.hi8 >> (60 - 4 * i)) & 0xF));
    for (i = 0; i < 16; i++)
        rec_buf[28 + i] =
            (unsigned char)(HEX_DIGIT((rec_number.lo8 >> (60 - 4 * i)) & 0xF));

    /* add 2 bytes of "break" data */
    rec_buf[44] = ' ';
    rec_buf[45] = ' ';

    /* add 52 bytes of filler based on low 48 bits of random number */
    rec_buf[46] = rec_buf[47] = rec_buf[48] = rec_buf[49] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >> 48) & 0xF));
    rec_buf[50] = rec_buf[51] = rec_buf[52] = rec_buf[53] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >> 44) & 0xF));
    rec_buf[54] = rec_buf[55] = rec_buf[56] = rec_buf[57] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >> 40) & 0xF));
    rec_buf[58] = rec_buf[59] = rec_buf[60] = rec_buf[61] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >> 36) & 0xF));
    rec_buf[62] = rec_buf[63] = rec_buf[64] = rec_buf[65] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >> 32) & 0xF));
    rec_buf[66] = rec_buf[67] = rec_buf[68] = rec_buf[69] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >> 28) & 0xF));
    rec_buf[70] = rec_buf[71] = rec_buf[72] = rec_buf[73] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >> 24) & 0xF));
    rec_buf[74] = rec_buf[75] = rec_buf[76] = rec_buf[77] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >> 20) & 0xF));
    rec_buf[78] = rec_buf[79] = rec_buf[80] = rec_buf[81] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >> 16) & 0xF));
    rec_buf[82] = rec_buf[83] = rec_buf[84] = rec_buf[85] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >> 12) & 0xF));
    rec_buf[86] = rec_buf[87] = rec_buf[88] = rec_buf[89] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >>  8) & 0xF));
    rec_buf[90] = rec_buf[91] = rec_buf[92] = rec_buf[93] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >>  4) & 0xF));
    rec_buf[94] = rec_buf[95] = rec_buf[96] = rec_buf[97] = 
        (unsigned char)(HEX_DIGIT((rand.lo8 >>  0) & 0xF));

    /* add 2 bytes of "break" data */
    rec_buf[98] = '\r'; /* nice for Windows */
    rec_buf[99] = '\n';
}


/* gen_ascii_skewed_rec = generate an ascii skewed record suitable for
 *                        alternate Daytona skewed data test.
 */
void gen_ascii_skewed_rec(unsigned char *rec_buf, rand_queue *rq)
{
    int                 skew_index;
    u16                 rand;
    unsigned char       *skew_bytes;
    int                 skew_count;

    /* generate non-skewed record */
    gen_ascii_rec(rec_buf, rq);

    /* get skew index for current record */
    skew_index = get_skew_index(rq);

    /* use skew_index to get the SKEW_BYTES potential skew bytes for the 
     * high-order key bytes.
     */
    skew_bytes = Skew_ascii[skew_index];
    
    rand = RQ(rq, 0);

    /* get random number in the inclusive range 0 - SKEW_BYTES from
     * 3 bits in lo8.
     */
    skew_count = (int)(rand.lo8 >> 32) & 0x7;
    if (skew_count > SKEW_BYTES)
        skew_count = 0; /* each rec has 2/8 = .25 chance of being non-skewed */

    /* replace the high-order "skew_count" bytes of record key with bytes from
     * the skew bytes.
     */
    while (skew_count > 0) /* while there is another byte to replace */
    {
        *rec_buf = *skew_bytes;
        skew_count--;
        rec_buf++;
        skew_bytes++;
    }
}


#if defined(SUMP_PUMP)
/* gen_block -  pump function that reads a task instruction from the
 *              process's main thread.  The instruction consists of the
 *              beginning record number and the number of records to be
 *              generated.  The records are written to the task output.
 *              The sump pump infrastructure will run multiple instances
 *              of this function in parallel, and concatenate their
 *              outputs to form the output of the sump pump.
 */
int gen_block(sp_task_t t, void *unused)
{
    struct gen_instruct *ip;
    struct gen_instruct instruct;
    u8                  j;
    u16                 temp16 = {0LL, 0LL};
    u16                 sum16 = {0LL, 0LL};
    unsigned char       rec_buf[100];
    rand_queue          rq;

    /* read instruction from the thread's input */
    if (pfunc_get_rec(t, &ip) != sizeof(instruct))
        return (pfunc_error(t, "sp_get_rec() error"));
    instruct = *ip;
        
    init_rand_queue(&rq, instruct.starting_rec);
    
    for (j = 0; j < instruct.num_recs; j++)
    {
        (*Gen)(rec_buf, &rq);
        if (Print_checksum)
        {
            temp16.lo8 = crc32(0, rec_buf, REC_SIZE);
            sum16 = add16(sum16, temp16);
        }
        if (!Skip_output)
            pfunc_write(t, 0, rec_buf, REC_SIZE);
        bump_queue(&rq);
    }

    /* add the checksum for the block of records just generated to
     * the global checksum.
     *
     * this could be done without a mutex by outputing the checksum
     * to a second output stream that is read by the main thread,
     * but this way is much simpler.
     */
    pfunc_mutex_lock(t);
    Sum16 = add16(Sum16, sum16);  
    pfunc_mutex_unlock(t);
    
    return (SP_OK);
}
#endif


static char usage_str[] =
    "Gensort Sort Input Generator\n"
    "\n"
    "usage: gensort [-a] [-c] [-bSTARTING_REC_NUM] "
#if defined(SUMP_PUMP)
    "[-tN] NUM_RECS FILE_NAME[,opts]\n"
#else
    "NUM_RECS FILE_NAME\n"
#endif
    "-a        Generate ascii records required for PennySort or JouleSort.\n"
    "          These records are also an alternative input for the other\n"
    "          sort benchmarks.  Without this flag, binary records will be\n"
    "          generated that contain the highest density of randomness in\n"
    "          the 10-byte key.\n"
    "-c        Calculate the sum of the crc32 checksums of each of the\n"
    "          generated records and send it to standard error.\n"
    "-bN       Set the beginning record generated to N. By default the\n"
    "          first record generated is record 0.\n"
    "-s        Generate input records with skewed keys. If used with -a\n"
    "          option, then skewed ascii records are generated.\n"
#if defined(SUMP_PUMP)
    "-tN       Use N internal program threads to generate the records.\n"
#endif
    "NUM_RECS  The number of sequential records to generate.\n"
#if defined(SUMP_PUMP)
    "FILE_NAME[,opts] The name of the file to write the records to.\n"
    "          File options may immediately follow the file name:\n"
    "          ,buf           Use buffered and synchronous file writes,\n"
    "                         instead of the default direct and asynchronous\n"
    "                         writes.\n"
    "          ,dir           Use direct and asynchronous file writes.\n"
    "                         The is the default.\n"
    "          ,trans=N[k,m,g] Sets the file write request size in bytes,\n"
    "                         kilobytes, megabytes or gigabytes.\n"
    "          ,count=N       Sets the maximum number of simultaneous\n"
    "                         asynchronous write requests allowed.\n"
#else
    "FILE_NAME The name of the file to write the records to.\n"
#endif
    "\n"
    "Example 1 - to generate 1000000 ascii records starting at record 0 to\n"
    "the file named \"pennyinput\":\n"
    "    gensort -a 1000000 pennyinput\n"
    "\n"
    "Example 2 - to generate 1000 binary records beginning with record 2000\n"
    "to the file named \"partition2\":\n"
    "    gensort -b2000 1000 partition2\n"
    "\n"
    "Copyright (C) 2009 - 2016, Chris Nyberg\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions\n"
    "are met:\n"
    "    * Redistributions of source code must retain the above copyright\n"
    "      notice, this list of conditions and the following disclaimer.\n"
    "    * Redistributions in binary form must reproduce the above copyright\n"
    "      notice, this list of conditions and the following disclaimer in \n"
    "      the documentation and/or other materials provided with the\n"
    "      distribution.\n"
    "    * Neither the names of Chris Nyberg nor Ordinal Technology Corp\n"
    "      may be used to endorse or promote products derived from this\n"
    "      software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS\n"
    "\"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT\n"
    "LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR\n"
    "A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT\n"
    "HOLDER> BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,\n"
    "EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,\n"
    "PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR\n"
    "PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY\n"
    "OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE\n"
    "OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    ;

void usage(void)
{
    fprintf(stderr, usage_str);
    fprintf(stderr, "\nVersion %s, cvs $Revision: 1.14 $\n", Version);
#if defined(SUMP_PUMP)
    fprintf(stderr, "SUMP Pump version %s\n", sp_get_version());
#endif
    exit(1);
}


int main(int argc, char *argv[])
{
    u8                  j;                      /* should be a u16 someday */
    u16                 starting_rec_number;
    u16                 num_recs;
    unsigned char       rec_buf[REC_SIZE];
    FILE                *out;
    u16                 temp16 = {0LL, 0LL};
    char                sumbuf[U16_ASCII_BUF_SIZE];
    rand_queue          rq;
#if defined(SUMP_PUMP)
    int                 number_threads = 0;
    int                 ret;
    sp_t                sp_gen;         /* handle for sump pump */
    u8                  blk_recs;
    struct gen_instruct instruct;
#endif
    
    starting_rec_number.hi8 = 0;
    starting_rec_number.lo8 = 0;
    Gen = gen_rec;

    while (argc > 1 && argv[1][0] == '-')
    {
        if (argv[1][1] == 'a')
        {
            if (Gen == gen_rec)
                Gen = gen_ascii_rec;
            else if (Gen == gen_skewed_rec)
                Gen = gen_ascii_skewed_rec;
        }
        else if (argv[1][1] == 'b')
            starting_rec_number = dec_to_u16(argv[1] + 2);
        else if (argv[1][1] == 'c')
            Print_checksum = 1;
        else if (argv[1][1] == 's')
        {
            if (Gen == gen_rec)
                Gen = gen_skewed_rec;
            else if (Gen == gen_ascii_rec)
                Gen = gen_ascii_skewed_rec;
        }
#if defined(SUMP_PUMP)
        else if (argv[1][1] == 't')
            number_threads = atoi(argv[1] + 2);
#endif
        else
            usage();
        argc--;
        argv++;
    }
    if (argc != 3)
        usage();
    num_recs = dec_to_u16(argv[1]);
    Skip_output = (strcmp(argv[2], "/dev/null") == 0);

#if defined(SUMP_PUMP)
    if (number_threads != 1)
    {
        /* start a sump pump to generate records using the gen_block()
         * function.  Making the input buffer size the size of an
         * instruction structure insures that each sump pump task executes
         * exactly one instruction.  The output buffer size will hold the
         * maximum number of records that will be generated per instruction.
         */
        ret = sp_start(&sp_gen, gen_block,
                       "-IN_BUF_SIZE=%d -REC_SIZE=%d -OUT_BUF_SIZE[0]=%d "
                       "-OUT_FILE[0]=%s -THREADS=%d",
                       sizeof(struct gen_instruct),  /* input buf size */
                       sizeof(struct gen_instruct),  /* input record size */
                       BLK_RECS * REC_SIZE,          /* output buf size */
                       argv[2],                      /* file name */
                       number_threads);
        if (ret)
        {
            fprintf(stderr, "sp_start failed: %s\n",
                    sp_get_error_string(sp_gen, ret));
            return (ret);
        }

        /* Feed generate instruction structures to the sump pump threads.
         * Each instruction struct will handled by a sump pump thread
         * executing the gen_block() pump function.
         */
        instruct.starting_rec = starting_rec_number;
        for (j = 0; (j * BLK_RECS) < num_recs.lo8; j++)
        {
            /* set starting rec and number of recs to generate */
            /* instruct.starting_rec.lo8 = (j * BLK_RECS); */
            blk_recs = num_recs.lo8 - (j * BLK_RECS);
            if (blk_recs > BLK_RECS)
                blk_recs = BLK_RECS;
            instruct.num_recs = blk_recs;
            if (sp_write_input(sp_gen, &instruct, sizeof(instruct)) !=
                sizeof(instruct))
            {
                fprintf(stderr, "sp_write_input: %s\n",
                        sp_get_error_string(sp_gen, SP_WRITE_ERROR)), exit(1);
            }
            instruct.starting_rec.lo8 += BLK_RECS;
        }
        /* write EOF to sump pump so it will wind down */
        if (sp_write_input(sp_gen, NULL, 0) != 0)
            fprintf(stderr, "sp_write_input(0): %s\n",
                    sp_get_error_string(sp_gen, SP_WRITE_ERROR)), exit(1); 

        /* wait for sump pump to finish */
        if ((ret = sp_wait(sp_gen)) != SP_OK)
            fprintf(stderr, "sp_wait: %s\n",
                    sp_get_error_string(sp_gen, ret)), exit(1); 
    }
    else
#endif
    {
        /* use just this single thread */
        
        if ((out = fopen(argv[2], "w")) == NULL)
        {
            perror(argv[2]);
            exit(1);
        }
        
        init_rand_queue(&rq, starting_rec_number);
    
        for (j = 0; j < num_recs.lo8; j++)
        {
            (*Gen)(rec_buf, &rq);
            if (Print_checksum)
            {
                temp16.lo8 = crc32(0, rec_buf, REC_SIZE);
                Sum16 = add16(Sum16, temp16);
            }
            if (!Skip_output)
                fwrite(rec_buf, REC_SIZE, 1, out);
            bump_queue(&rq);
        }
    }
    
    if (Print_checksum)
        fprintf(stderr, "%s\n", u16_to_hex(Sum16, sumbuf));
    return (0);
}
