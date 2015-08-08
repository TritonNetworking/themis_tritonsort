#ifndef _TRITONSORT_ALIGNMENT_UTILS_H
#define _TRITONSORT_ALIGNMENT_UTILS_H

#include <stdint.h>
#include "config.h"

uint64_t roundUp(uint64_t size, uint64_t memAlignment);
uint8_t* align(uint8_t* buf, uint64_t memAlignment);

#endif //_TRITONSORT_ALIGNMENT_UTILS_H
