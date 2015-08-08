#ifndef MAPRED_BOUNDARY_SCANNER_IMPLS_H
#define MAPRED_BOUNDARY_SCANNER_IMPLS_H

#include "BoundaryScanner.h"
#include "core/ImplementationList.h"

class BoundaryScannerImpls : public ImplementationList {
public:
  BoundaryScannerImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(BoundaryScanner, "BoundaryScanner");
  }
};

#endif // MAPRED_BOUNDARY_SCANNER_IMPLS_H
