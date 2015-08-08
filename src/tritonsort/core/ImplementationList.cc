#include "ImplementationList.h"

ImplementationList::ImplementationList() {
}

ImplementationList::~ImplementationList() {
  for (std::map<std::string, ImplementationList*>::iterator
         iter = nestedImplementationLists.begin();
       iter != nestedImplementationLists.end(); iter++) {
    delete iter->second;
  }
}

const ImplementationList& ImplementationList::getStageImplList(
  const std::string& nestedStageName) const {

  ImplListMapConstIter iter = nestedImplementationLists.find(nestedStageName);

  ABORT_IF(iter == nestedImplementationLists.end(), "Can't find a registered "
           "implementation list for '%s'", nestedStageName.c_str());

  return *(iter->second);
}

WorkerFactoryMethod ImplementationList::getImplFactoryMethod(
  const std::string& implName) const {

  std::map<std::string, WorkerFactoryMethod>::const_iterator iter =
    factoryMethods.find(implName);

  ABORT_IF(iter == factoryMethods.end(), "Can't find a registered "
           "implementation named '%s'", implName.c_str());

  return iter->second;
}

void ImplementationList::__addImplementation(
  const std::string& className, WorkerFactoryMethod factoryMethod) {

  std::map<std::string, WorkerFactoryMethod>::const_iterator iter =
    factoryMethods.find(className);

  ABORT_IF(iter != factoryMethods.end(), "An implementation named '%s' is "
           "already registered; can't have more than one implementation with "
           "the same name", className.c_str());

  factoryMethods[className] = factoryMethod;
}

void ImplementationList::__addImplList(const std::string& stageName,
                                       ImplementationList* implList) {
  nestedImplementationLists[stageName] = implList;
}
