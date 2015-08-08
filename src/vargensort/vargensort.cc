#include <boost/lexical_cast.hpp>
#include <gflags/gflags.h>
#include <gsl/gsl_randist.h>
#include <gsl/gsl_rng.h>
#include <iostream>
#include <limits.h>
#include <list>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "core/Utils.h"
#include "vargensort.h"

DEFINE_bool(debug, false, "Debug (print key/value lengths in ASCII)");
DEFINE_bool(overwrite, false, "overwrite file if it already exists");
DEFINE_uint64(minKeyLen, 0, "minimum key length");
DEFINE_uint64(maxKeyLen, UINT_MAX, "maximum key length allowed");
DEFINE_uint64(minValLen, 0, "minimum value length");
DEFINE_uint64(maxValLen, UINT_MAX, "maximum value length allowed");
DEFINE_double(pareto_a, -1.0, "'a'/'alpha' paremeter for the Pareto "
                              "distribution");
DEFINE_double(pareto_b, -1.0, "'b'/'xmin' paremeter for the Pareto "
                              "distribution");
/// \TODO(MC): gflags truncates this string. Find a more succinct help string.
DEFINE_string(largeTuples, "",
              "Specify a set of large tuples to inject using the format: "
              "location_1,keylen_1,vallen_1,location_2,keylen_2,vallen_2,... "
              "Tuples must be provided in location order.");
std::list<uint64_t> largeTupleLocations;
std::list<uint64_t> largeTupleKeyLengths;
std::list<uint64_t> largeTupleValueLengths;

int main(int argc, char** argv)
{
    FILE* fp;

    google::SetUsageMessage("This command generates key-value pairs");
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc != 4) {
        fprintf(stderr, "Usage: %s <outputfile> <method> <numbytes>\n",
                argv[0]);
        exit(1);
    }
    char* outfn = argv[1];
    char* method = argv[2];
    uint64_t numbytes = getNumBytesFromCmdLineOrDie(argv[3]);

    validateCommandLineArguments(method);

    /* Fail if output file already exists and we're not overwriting */
    if (!FLAGS_overwrite && file_exists(outfn)) {
        fprintf(stderr, "%s exists and 'overwrite' not specified, skipping\n",
                outfn);
        exit(0);
    }

    /* create global write buffer */
    writeBufferOffset = 0;
    writeBuffer = (uint8_t*) malloc(WRITEBUFFER_LEN);

    /* Create output file */
    if ((fp = fopen(outfn, "w")) == NULL) {
        perror("fopen");
        exit(1);
    }

    /* Fill with tuples */
    bool status;
    if (!strcmp(method, "pareto")) {
        status = fillWithParetoTuples(fp, numbytes,
                                      FLAGS_pareto_a, FLAGS_pareto_b);
    } else {
        fprintf(stderr, "Unknown method: %s\n", method);
        exit(1);
    }

    /* Flush and close the output file */
    if (writeBufferOffset > 0) {
        if (fwrite(writeBuffer, writeBufferOffset, 1, fp) == 0) {
            fprintf(stderr, "Error writing tuples to file\n");
            exit(1);
        }
    }
    if (fclose(fp) == EOF) {
        perror("fclose");
        exit(1);
    }

    return status ? 0 : 1;
}

uint64_t utime() {
    struct timeval time;
    gettimeofday(&time, NULL);

    return (((uint64_t) time.tv_sec) * 1000000) + time.tv_usec;
}

uint64_t getNumBytesFromCmdLineOrDie(char* numstr)
{
    uint64_t ret;
    char* badCharPtr;

    if (!numstr || strlen(numstr) == 0) {
        fprintf(stderr, "Invalid number of bytes argument\n");
        exit(1);
    }
    ret = (uint64_t) strtoll(numstr, &badCharPtr, 0);
    if (*badCharPtr != '\0') {
        fprintf(stderr, "Invalid number of bytes argument: %s\n", numstr);
        exit(1);
    }

    return ret;
}

void validateCommandLineArguments(char* method)
{
    if (!strcmp(method, "pareto")) {
        if (FLAGS_pareto_a == -1) {
            fprintf(stderr, "Must set 'pareto_a' parameter\n");
            exit(1);
        }
        if (FLAGS_pareto_b == -1) {
            fprintf(stderr, "Must set 'pareto_b' parameter\n");
            exit(1);
        }
    }
    if (FLAGS_minKeyLen > FLAGS_maxKeyLen) {
        fprintf(stderr, "minKeyLen must be smaller than maxKeyLen\n");
        exit(1);
    }
    if (FLAGS_minValLen > FLAGS_maxValLen) {
        fprintf(stderr, "minValLen must be smaller than maxValLen\n");
        exit(1);
    }

    // Tokenize largeTuples on commas.
    StringList tokens;
    parseCommaDelimitedList<std::string, StringList>(tokens, FLAGS_largeTuples);
    uint32_t tokenIndex = 0;
    // Distribute tokens into the three lists based on position
    std::list<uint64_t>* lists[] = {
      &largeTupleLocations, &largeTupleKeyLengths, &largeTupleValueLengths};
    while (!tokens.empty()) {
      lists[tokenIndex]->push_back(
        boost::lexical_cast<uint64_t>(tokens.front()));
      tokens.pop_front();
      // Move to the next list.
      tokenIndex = (tokenIndex + 1) % 3;
    }
    if (tokenIndex != 0) {
      std::cerr << "Got wrong number of tokens for largeTuples parameter. "
                << "Must provide triples of location,key_len,val_len."
                << std::endl;
      exit(1);
    }
}

inline bool file_exists(const char* filename)
{
    return !access(filename, F_OK);
}

tuple* tuple_alloc()
{
    tuple* t;
    t = (tuple*) malloc(sizeof(tuple));
    memset(t, 0, sizeof(tuple));
    return t;
}

void tuple_free(tuple* t)
{
    if (t) {
        if (t->key) {
            free(t->key);
        }
        if (t->val) {
            free(t->val);
        }
        free(t);
    }
}

void fillUniformRandom(gsl_rng* rnd, uint8_t* dst, uint64_t len)
{
    /* Note that the type signature and man page for gsl_rng_uniform_int
     * says that you can return an unsigned long int.  But it lies!
     * you can actually only recover 'unsigned int' actual random bytes */
    uint64_t offset = 0;
    while (offset < len) {
        unsigned int rndint = (unsigned int) gsl_rng_uniform_int(rnd, UINT_MAX);
        size_t amtToCopy = std::min(sizeof(unsigned int), len - offset);
        memcpy(dst + offset, &rndint, amtToCopy);
        offset += amtToCopy;
    }
}

void instantiateTuple(gsl_rng* rnd, tuple* t,
                      uint64_t keylen, uint64_t vallen)
{
    /* make our buffers bigger if necessary */
    if (keylen > t->keycapacity) {
        if (t->key) {
            free(t->key);
        }
        t->keycapacity = sizeof(uint8_t) * keylen;
        t->key = (uint8_t*) malloc(t->keycapacity);
    }

    if (vallen > t->valcapacity) {
        if (t->val) {
            free(t->val);
        }
        t->valcapacity = sizeof(uint8_t) * vallen;
        t->val = (uint8_t*) malloc(t->valcapacity);
    }

    /* fill key and value with random data */
    fillUniformRandom(rnd, t->key, keylen);
    t->keylen = keylen;
    fillUniformRandom(rnd, t->val, vallen);
    t->vallen = vallen;
}

void emitTuple(FILE* fp, tuple* t)
{
    if (FLAGS_debug) {
        emitTupleKeyValueLengthInASCII(fp, t);
    } else {
        emitTupleBinary(fp, t);
    }
}

void emitTupleKeyValueLengthInASCII(FILE* fp, tuple* t)
{
    fprintf(fp, "%u %u\n", t->keylen, t->vallen);
}

void emitTupleBinary(FILE* fp, tuple* t)
{
    const uint64_t tupleLen = 4 + 4 + t->keylen + t->vallen;
    const uint64_t bufferRemaining = WRITEBUFFER_LEN - writeBufferOffset;

    /* if there isn't enough space in the buffer, let's write it out */
    if (bufferRemaining < tupleLen && writeBufferOffset > 0) {
        if (fwrite(writeBuffer, writeBufferOffset, 1, fp) == 0) {
            fprintf(stderr, "Error writing tuples to file\n");
            exit(1);
        }
        writeBufferOffset = 0;
    }

    // If the tuple doesn't fit in the write buffer, just write it directly.
    if (tupleLen > WRITEBUFFER_LEN) {
      // Write the header.
      if (fwrite(&t->keylen, sizeof(t->keylen), 1, fp) != 1) {
        std::cerr << "Error writing key length to file" << std::endl;
      }
      if (fwrite(&t->vallen, sizeof(t->vallen), 1, fp) != 1) {
        std::cerr << "Error writing value length to file" << std::endl;
      }
      // Write the key.
      if (fwrite(t->key, t->keylen, 1, fp) != 1) {
        std::cerr << "Error writing key to file" << std::endl;
      }
      // Write the value.
      if (fwrite(t->val, t->vallen, 1, fp) != 1) {
        std::cerr << "Error writing value to file" << std::endl;
      }
    } else {
      /* Buffer the tuple to our buffer */
      memcpy(writeBuffer + writeBufferOffset, &t->keylen, 4);
      memcpy(writeBuffer + writeBufferOffset + 4, &t->vallen, 4);
      memcpy(writeBuffer + writeBufferOffset + 4 + 4, t->key, t->keylen);
      memcpy(writeBuffer + writeBufferOffset + 4 + 4 + t->keylen,
             t->val, t->vallen);
      writeBufferOffset += tupleLen;
    }
}

bool fillWithParetoTuples(FILE* fp, uint64_t numbytes, double a, double b)
{
    const gsl_rng_type* T;
    gsl_rng* rnd;
    tuple* t;

    t = tuple_alloc();

    // create a generator chosen by the environment variable GSL_RNG_TYPE
    gsl_rng_env_setup();
    T = gsl_rng_default;
    rnd = gsl_rng_alloc (T);
    gsl_rng_set(rnd, getpid() ^ utime());

    uint64_t bytesGenerated = 0;
    uint64_t tuplesGenerated = 0;
    // Ensure that you always have enough room for a minimally sized tuple
    // before continuing.
    uint64_t minTupleLength = sizeof(uint32_t) + sizeof(uint32_t) +
      FLAGS_minKeyLen + FLAGS_minValLen;
    while (bytesGenerated + minTupleLength < numbytes) {
        unsigned int keylen = gsl_ran_pareto(rnd, a, b);
        unsigned int vallen = gsl_ran_pareto(rnd, a, b);

        /* check user-specified tuple limits */
        if (keylen > FLAGS_maxKeyLen) {
            keylen = FLAGS_maxKeyLen;
        }
        if (keylen < FLAGS_minKeyLen) {
            keylen = FLAGS_minKeyLen;
        }
        if (vallen > FLAGS_maxValLen) {
            vallen = FLAGS_maxValLen;
        }
        if (vallen < FLAGS_minValLen) {
            vallen = FLAGS_minValLen;
        }

        if (!largeTupleLocations.empty() &&
            tuplesGenerated == largeTupleLocations.front()) {
          // Inject the next large tuple.
          keylen = largeTupleKeyLengths.front();
          vallen = largeTupleValueLengths.front();
          largeTupleLocations.pop_front();
          largeTupleKeyLengths.pop_front();
          largeTupleValueLengths.pop_front();
        }

        instantiateTuple(rnd, t, keylen, vallen);
        emitTuple(fp, t);

        bytesGenerated += (4 + 4 + keylen + vallen);
        ++tuplesGenerated;
    }

    tuple_free(t);
    gsl_rng_free (rnd);

    return true;
}
