#ifndef TRITONSORT_IMPLEMENTATION_LIST_H
#define TRITONSORT_IMPLEMENTATION_LIST_H

#include <string>
#include <map>

#include "core/MemoryAllocatorInterface.h"
#include "core/NamedObjectCollection.h"
#include "core/Params.h"
#include "core/BaseWorker.h"

typedef BaseWorker* (*WorkerFactoryMethod)(
  const std::string& phaseName, const std::string& stageName, uint64_t id,
  Params& params, MemoryAllocatorInterface& memoryAllocator,
  NamedObjectCollection& dependencies);

#define ADD_IMPLEMENTATION(_className_, _implNameString_)        \
  __addImplementation(_implNameString_, &_className_::newInstance)

#define ADD_STAGE_IMPLS(_implListSubclassName_, _stageNameString_)      \
  __addImplList(_stageNameString_, new _implListSubclassName_())

/**
   A list of implementations of a given stage or a list of stages for an
   application. Used by factories to construct workers.

   When subclassing this method, you should _only_ override the constructor and
   provide a list of implementations with ADD_IMPLEMENTATION and
   ADD_STAGE_IMPLS macros. This base class will take care of the rest.
 */
class ImplementationList {
public:
  /// Constructor
  ImplementationList();

  /// Destructor
  virtual ~ImplementationList();

  /// Get the implementation list for the given stage
  /**
     \param stageName the name of the stage
     \return an ImplementationList giving the implementations for the stage
   */
  const ImplementationList& getStageImplList(const std::string& stageName)
    const;

  /// Get the factory method for a given implementation of the stage
  /**
     \param implName the name of the implementation
   */
  WorkerFactoryMethod getImplFactoryMethod(const std::string& implName)
    const;

protected:
  /// \cond PRIVATE
  void __addImplementation(const std::string& className,
                         WorkerFactoryMethod factoryMethod);

  void __addImplList(const std::string& stageName,
                     ImplementationList* implList);
  /// \endcond
private:
  typedef std::map<std::string, ImplementationList*> ImplListMap;
  typedef ImplListMap::const_iterator ImplListMapConstIter;

  std::map<std::string, WorkerFactoryMethod> factoryMethods;

  ImplListMap nestedImplementationLists;

}; // ImplementationList

#endif // TRITONSORT_IMPLEMENTATION_LIST_H
