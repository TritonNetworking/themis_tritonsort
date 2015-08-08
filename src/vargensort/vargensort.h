#ifndef VARGENSORT_H
#define VARGENSORT_H

typedef struct tuple_t {
    uint32_t keylen;
    uint32_t vallen;
    uint8_t* key;
    uint8_t* val;
    uint32_t keycapacity;
    uint32_t valcapacity;
} tuple;

#define WRITEBUFFER_LEN 16*1024*1024
uint8_t* writeBuffer;
uint64_t writeBufferOffset;

uint64_t getNumBytesFromCmdLineOrDie(char*);
void validateCommandLineArguments(char*);
bool file_exists(const char*);
bool fillWithParetoTuples(FILE*, uint64_t, double, double);
tuple* tuple_alloc();
void tuple_free(tuple*);
void emitTuple(FILE*, tuple*);
void emitTupleKeyValueLengthInASCII(FILE*, tuple*);
void emitTupleBinary(FILE*, tuple*);
void fillUniformRandom(gsl_rng*, uint8_t*, uint64_t);
void instantiateTuple(gsl_rng*, tuple*, uint64_t, uint64_t);

uint64_t utime();

#endif // VARGENSORT_H
