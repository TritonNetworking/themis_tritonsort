#ifndef TRITONSORT_COMMON_WORKERS_ITEM_HOLDER_IMPLS_H
#define TRITONSORT_COMMON_WORKERS_ITEM_HOLDER_IMPLS_H

#include "ItemHolder.h"
#include "core/ImplementationList.h"

/// \sa ImplementationList
template <typename T> class ItemHolderImpls : public ImplementationList {
public:
  /// ItemHolder implementations
  ItemHolderImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(ItemHolder<T>, "ItemHolder");
  }
};

#endif // TRITONSORT_COMMON_WORKERS_ITEM_HOLDER_IMPLS_H
