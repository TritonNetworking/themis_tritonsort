#ifndef MAPRED_BUFFER_COMBINER_IMPLS_H
#define MAPRED_BUFFER_COMBINER_IMPLS_H

#include "BufferCombiner.h"
#include "core/ImplementationList.h"

class BufferCombinerImpls : public ImplementationList {
public:
  BufferCombinerImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(BufferCombiner, "BufferCombiner");
  }
};

#endif // MAPRED_BUFFER_COMBINER_IMPLS_H
