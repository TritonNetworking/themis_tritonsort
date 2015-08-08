#ifndef MAPRED_BOUNDARY_DESERIALIZER_IMPLS_H
#define MAPRED_BOUNDARY_DESERIALIZER_IMPLS_H

#include "BoundaryDeserializer.h"
#include "core/ImplementationList.h"

class BoundaryDeserializerImpls : public ImplementationList {
public:
  BoundaryDeserializerImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(BoundaryDeserializer, "BoundaryDeserializer");
  }
};

#endif // MAPRED_BOUNDARY_DESERIALIZER_IMPLS_H
