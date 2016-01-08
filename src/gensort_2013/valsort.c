/* valsort.c - Sort output data validator
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
#include <zlib.h>   /* use crc32() function */
#include "rand16.h"

#if defined(SUMP_PUMP)
# include <fcntl.h>
# include "sump.h"
#endif  /* end defined(SUMP_PUMP) */

#define REC_SIZE 100
#define SUM_SIZE (sizeof(struct summary))

#if defined(_WIN32)
# define strcasecmp  _stricmp
#endif

/* Comparison routine, either memcmp() or strcasecmp() */
int     (*Compare)(const unsigned char *a, const unsigned char *b, size_t n) =
    (int (*)(const unsigned char *a, const unsigned char *b, size_t n))memcmp;
int     Read_summary;    /* Read a file of partition summaries, not records */
int     Quiet;           /* Quiet mode, don't output diagnostic information */
int     Multithreaded;   /* boolean indicating if a sump pump is being used */

/* struct used to summarize a partition of sort output
 */
struct summary
{
    u16             first_unordered;     /* index of first unordered record,
                                          * or 0 if no unordered records */
    u16             unordered_count;     /* total number of unordered records*/
    u16             rec_count;           /* total number of records */
    u16             dup_count;           /* total number of duplicate keys */
    u16             checksum;            /* checksum of all records */
    unsigned char   first_rec[REC_SIZE]; /* first record */
    unsigned char   last_rec[REC_SIZE];  /* last record */
};

struct summary Summary;


/* next_rec - get the next record to be validated
 *
 */
unsigned char *next_rec(void *in, unsigned char *rec_buf, struct summary *sum)
{
    int                 read_size;
    unsigned char       *rec = NULL;
    u16                 temp16 = {0LL, 0LL};

#if defined(SUMP_PUMP)
    if (Multithreaded)
    {
        /* get the record from the sump pump infrastructure */
        read_size = (int)pfunc_get_rec(in, &rec);
    }
    else
#endif
    {
        read_size = fread(rec_buf, 1, REC_SIZE, in);
        rec = rec_buf;
    }
    
    if (read_size == REC_SIZE)
    {
        temp16.lo8 = crc32(0, rec, REC_SIZE);
        sum->checksum = add16(sum->checksum, temp16);
    }
    else if (read_size == 0)
        return (NULL);
    else if (read_size < 0)
    {
        fprintf(stderr, "record read error\n");
        exit(1);
    }
    else
    {
        fprintf(stderr, "partial record found at end\n");
        exit(1);
    }
    return (rec);
}


/* summarize_records - summarize the validity of a sequence of records.
 */
int summarize_records(void *in, void *unused)
{
    struct summary      *sum;
    int                 diff;
    u16                 one = {0LL, 1LL};
    unsigned char       *rec;
    unsigned char       rec_buf[REC_SIZE];
    unsigned char       prev[REC_SIZE];
    char                sumbuf[U16_ASCII_BUF_SIZE];
#if defined(SUMP_PUMP)
    struct summary      local_summary;

    if (Multithreaded)
    {
        sum = &local_summary;
        memset(sum, 0, sizeof(struct summary));
    }
    else
#endif
        sum = &Summary;

    if ((rec = next_rec(in, rec_buf, sum)) == NULL)
    {
        fprintf(stderr, "there must be at least one record to be validated\n");
        exit(1);
    }
    memcpy(sum->first_rec, rec, REC_SIZE);
    memcpy(prev, rec, REC_SIZE);
    sum->rec_count = add16(sum->rec_count, one);
    
    while ((rec = next_rec(in, rec_buf, sum)) != NULL)
    {
        /* make sure the record key is equal to or more than the
         * previous key
         */
        diff = (*Compare)(prev, rec, 10);
        if (diff == 0)
            sum->dup_count = add16(sum->dup_count, one);
        else if (diff > 0)
        {
            if (sum->first_unordered.hi8 == 0 &&
                sum->first_unordered.lo8 == 0)
            {
                sum->first_unordered = sum->rec_count;
                if (!Multithreaded && !Quiet)
                    fprintf(stderr, "First unordered record is record %s\n",
                            u16_to_dec(sum->first_unordered, sumbuf));
            }
            sum->unordered_count = add16(sum->unordered_count, one);
        }

        sum->rec_count = add16(sum->rec_count, one);
        memcpy(prev, rec, REC_SIZE);
    }
    memcpy(sum->last_rec, prev, REC_SIZE);  /* set last record for summary */

#if defined(SUMP_PUMP)
    if (Multithreaded)
    {
        pfunc_write(in, 0, sum, SUM_SIZE);
        return (SP_OK);
    }
    else
#endif
        return (0);
}


/* next_sum - get the next partition summary
 */
int next_sum(void *in, struct summary *sum)
{
    int                 ret;

#if defined(SUMP_PUMP)
    if (Multithreaded)
        ret = (int)sp_read_output(in, 0, sum, SUM_SIZE);  /* from sump pump */
    else
#endif
        ret = fread(sum, 1, SUM_SIZE, in);           /* get from file */
    
    if (ret == 0)
        return (0);
    else if (ret < 0)
    {
        fprintf(stderr, "summary read error\n");
        exit(1);
    }
    else if (ret != SUM_SIZE)
    {
        fprintf(stderr, "partial partition summary found at end\n");
        exit(1);
    }
    return (ret);
}


/* sum_summaries - validate a sequence of partition summaries
 */
void sum_summaries(void *in)
{
    int                 diff;
    u16                 one = {0LL, 1LL};
    unsigned char       prev[REC_SIZE];
    char                sumbuf[U16_ASCII_BUF_SIZE];
    struct summary      local_sum;

    if (next_sum(in, &local_sum) == 0)
    {
        fprintf(stderr, "there must be at least one record to be validated\n");
        exit(1);
    }
    memcpy(&Summary, &local_sum, SUM_SIZE);
    memcpy(prev, Summary.last_rec, REC_SIZE);
    if (Summary.first_unordered.hi8 != 0 ||
        Summary.first_unordered.lo8 != 0)
    {
        if (!Quiet)
            fprintf(stderr, "First unordered record is record %s\n",
                    u16_to_dec(Summary.first_unordered, sumbuf));
    }
    
    while (next_sum(in, &local_sum))
    {
        /* make sure the record key is equal to or more than the
         * previous key
         */
        diff = (*Compare)(prev, local_sum.first_rec, 10);
        if (diff == 0)
            Summary.dup_count = add16(Summary.dup_count, one);
        else if (diff > 0)
        {
            if (Summary.first_unordered.hi8 == 0 &&
                Summary.first_unordered.lo8 == 0)
            {
                if (!Quiet)
                    fprintf(stderr, "First unordered record is record %s\n",
                            u16_to_dec(Summary.rec_count, sumbuf));
                Summary.first_unordered = Summary.rec_count;
            }
            Summary.unordered_count = add16(Summary.unordered_count, one);
        }

        if ((Summary.first_unordered.hi8 == 0 &&
             Summary.first_unordered.lo8 == 0) &&
            !(local_sum.first_unordered.hi8 == 0 &&
              local_sum.first_unordered.lo8 == 0))
        {
            Summary.first_unordered =
                add16(Summary.rec_count, local_sum.first_unordered);
            if (!Quiet)
                fprintf(stderr, "First unordered record is record %s\n",
                        u16_to_dec(Summary.first_unordered, sumbuf));
        }

        Summary.unordered_count =
            add16(Summary.unordered_count, local_sum.unordered_count);
        Summary.rec_count = add16(Summary.rec_count, local_sum.rec_count);
        Summary.dup_count = add16(Summary.dup_count, local_sum.dup_count);
        Summary.checksum = add16(Summary.checksum, local_sum.checksum);
        memcpy(prev, local_sum.last_rec, REC_SIZE);
    }
    memcpy(Summary.last_rec, prev, REC_SIZE); /* get last rec of last summary*/
}


/* get_input_fp - get input file pointer by opening input file for reading
 */
FILE *get_input_fp(char *filename)
{
    FILE        *in;
    
    if ((in = fopen(filename, "r")) == NULL)
    {
        perror(filename);
        exit(1);
    }
    return (in);
}


static char usage_str[] =
    "Valsort Sort Output Validator\n"
    "\n"
    "usage: valsort [-i] [-q] "
#if defined(SUMP_PUMP)
    "[-tN] [-o SUMMARY_FILE] [-s] FILE_NAME[,opts]\n"
#else
    "[-o SUMMARY_FILE] [-s] FILE_NAME\n"
#endif
    "-i        Use case insensitive ascii comparisons (optional for PennySort).\n"
    "          Case sensitive ascii or binary keys are assumed by default.\n"
    "-q        Quiet mode, don't output diagnostic text.\n"
    "-o SUMMARY_FILE  Output a summary of the validated records. This method\n"
    "          can be used to validate partitioned sort outputs separately.\n"
    "          The contents of the separate summary files can then be\n"
    "          concatenated into a single file that can be checked using\n"
    "          the valsort program with the -s flag.\n"
    "-s        The file to validate contains partition summaries instead of\n"
    "          sorted records.\n"
#if defined(SUMP_PUMP)
    "-tN       Use N internal program threads to validate the records.\n"
    "FILE_NAME[,opts] The name of the sort output file or the partition\n"
    "          summaries file to validate. File options may immediately\n"
    "          follow the file name:\n"
    "          ,buf           Use buffered and synchronous file reads,\n"
    "                         instead of the default direct and asynchronous\n"
    "                         reads.\n"
    "          ,dir           Use direct and asynchronous file reads.\n"
    "                         The is the default.\n"
    "          ,trans=N[k,m,g] Sets the file read request size in bytes,\n"
    "                         kilobytes, megabytes or gigabytes.\n"
    "          ,count=N       Sets the maximum number of simultaneous\n"
    "                         asynchronous read requests allowed.\n"
#else
    "FILE_NAME The name of the sort output file or the partition summaries\n"
    "          file to validate.\n"
#endif
    "\n"
    "Example 1 - to validate the sorted order of a single sort output file:\n"
    "    valsort sortoutputfile\n"
    "\n"
    "Example 2 - to validate the sorted order of output that has been\n"
    "partitioned into 4 output files: out0.dat, out1.dat, out2.dat and out3.dat:\n"
    "    valsort -o out0.sum out0.dat\n"
    "    valsort -o out1.sum out1.dat\n"
    "    valsort -o out2.sum out2.dat\n"
    "    valsort -o out3.sum out3.dat\n"
    "    cat out0.sum out1.sum out2.sum out3.sum > all.sum\n"
    "    valsort -s all.sum\n"
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
    FILE                *in;
    char                sumbuf[U16_ASCII_BUF_SIZE];
    FILE                *out = NULL;
#if defined(SUMP_PUMP)
    int                 number_threads = 0;
    int                 ret;
    sp_t                sp_val;         /* handle for sump pump */
#endif
    
    while (argc > 1 && argv[1][0] == '-')
    {
        if (argv[1][1] == 'i')
            Compare =
                (int (*)(const unsigned char *a, const unsigned char *b, size_t n))strcasecmp;
        else if (argv[1][1] == 'o')
        {
            if (argc < 4 || argv[2][0] == '-')
                usage();
            if ((out = fopen(argv[2], "w")) == NULL)
            {
                perror(argv[2]);
                exit(1);
            }
            argc--;
            argv++;
        }
        else if (argv[1][1] == 'q')
            Quiet = 1;
        else if (argv[1][1] == 's')
            Read_summary = 1;
#if defined(SUMP_PUMP)
        else if (argv[1][1] == 't')
            number_threads = atoi(argv[1] + 2);
#endif
        else
            usage();
        argc--;
        argv++;
    }
    if (argc != 2)
        usage();

    /* if we are validating output partition summaries
     */
    if (Read_summary)
    {
        in = get_input_fp(argv[1]);
        sum_summaries(in);
    }
    /* else we are validating a file with records in sorted order */
#if defined(SUMP_PUMP)
    else if (number_threads != 1)
    {
        Multithreaded = 1;
        
        /* start a sump pump to validate the correct order of records.
         * the sump pump infrastructure will break the file being
         * validated into separate blocks that will be summarized by
         * separate threads.  this thread will then validate the
         * summaries.
         */
        ret = sp_start(&sp_val, (sp_pump_t)summarize_records,
                       "-IN_FILE=%s -IN_BUF_SIZE=%d -REC_SIZE=%d "
                       "-OUT_BUF_SIZE[0]=%d -THREADS=%d",
                       argv[1],
                       4 * 1024 * 1024,              /* input buf size */
                       REC_SIZE,                     /* input record size */
                       sizeof(struct summary),       /* output buf size */
                       number_threads);
        if (ret)
        {
            fprintf(stderr, "sp_start failed: %s\n",
                    sp_get_error_string(sp_val, ret));
            return (ret);
        }

        /* sum the summaries that are the output of the sump pump */
        sum_summaries(sp_val);
    }
#endif
    else
    {
        /* else validate the order of records with using only main thread */
        in = get_input_fp(argv[1]);
        summarize_records(in, NULL);
    }

    /* if requested ("-o"), output a summary file */
    if (out != NULL)
        fwrite(&Summary, SUM_SIZE, 1, out);

    if (!Quiet)
    {
        fprintf(stderr, "Records: %s\n",
                u16_to_dec(Summary.rec_count, sumbuf));
        fprintf(stderr, "Checksum: %s\n",
                u16_to_hex(Summary.checksum, sumbuf));
        if (Summary.unordered_count.hi8 | Summary.unordered_count.lo8)
        {
            fprintf(stderr, "ERROR - there are %s unordered records\n",
                    u16_to_dec(Summary.unordered_count, sumbuf));
        }
        else
        {
            fprintf(stderr, "Duplicate keys: %s\n",
                    u16_to_dec(Summary.dup_count, sumbuf));
            fprintf(stderr, "SUCCESS - all records are in order\n");
        }
    }
    
    /* return non-zero if there are any unordered records */
    return (Summary.unordered_count.hi8 | Summary.unordered_count.lo8) ? 1 : 0;
}
