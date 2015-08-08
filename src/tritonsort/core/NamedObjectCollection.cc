#include "core/NamedObjectCollection.h"

void NamedObjectCollection::add(
  const std::string& scope, const std::string& name, void* objectPtr) {

  scopedObjectMap[scope][name] = objectPtr;
}

void NamedObjectCollection::add(const std::string& name, void* objectPtr) {
  globalScopeObjectMap[name] = objectPtr;
}
