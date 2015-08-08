#ifndef TRITONSORT_NAMED_OBJECT_COLLECTION_H
#define TRITONSORT_NAMED_OBJECT_COLLECTION_H

#include <map>
#include <string>

#include "core/TritonSortAssert.h"

/**
   NamedObjectCollection provides a map whose keys are strings and whose values
   can be pointers to any object.
 */
class NamedObjectCollection {
public:
  /// Add an object to the collection
  /**
     \param name the object's name

     \param objectPtr a pointer to the object
   */
  void add(const std::string& name, void* objectPtr);

  /// Add an object to the collection
  /**
     \param name the object's name

     \param objectPtr a pointer to the object
   */
  void add(const std::string& scope, const std::string& name, void* objectPtr);


  /// Retrieve an object from the collection, specifying its type and giving
  /// its scope
  /**
     If the specified scope does not exist or the name does not exist in the
     specified scope, it will be searched for in the global scope. If it is not
     found in the global scope, execution will abort.

     Uses static_cast to cast the object up to a T. Assumes (unfortunately)
     that the caller knows what they're doing when requesting the specific type
     of the object.

     \param scope the scope in which to first look for the object
     \param name the object's name

     \return a pointer to the object
   */
  template <typename T> T* get(const std::string& scope,
                               const std::string& name) const {
    T* objPtr = getScopedObject<T>(scope, name);

    ABORT_IF(objPtr == NULL, "Named object '%s' is not in this collection, "
             "either in scope '%s' or in global scope", name.c_str(),
             scope.c_str());

    return objPtr;
  }

  /// Retrieve an object from the collection, specifying its type
  /**
     If the object is not in the collection, execution will abort.

     Uses static_cast to cast the object up to a T. Assumes (unfortunately)
     that the caller knows what they're doing when requesting the specific type
     of the object.

     \param name the object's name

     \return a pointer to the object
   */
  template <typename T> T* get(const std::string& name) const {
    T* objPtr = getObjectFromMap<T>(globalScopeObjectMap, name);

    ABORT_IF(objPtr == NULL, "Named object '%s' is not in "
             "this collection", name.c_str());

    return objPtr;
  }

  /**
     Check if the given object is in the collection

     \param name the object's name

     \return if the object appears in the collection
  */
  template <typename T> bool contains(const std::string& name) const {
    return (getObjectFromMap<T>(globalScopeObjectMap, name) != NULL);
  }

  /**
     \param scope the scope in which to first look for the object
     \param name the object's name

     \return if the object appears in the collection
   */
  template <typename T> bool contains(const std::string& scope,
                                      const std::string& name) const {
    return (getScopedObject<T>(scope, name) != NULL);
  }

private:
  typedef std::map<std::string, void*> ObjectPtrMap;
  typedef ObjectPtrMap::const_iterator ObjectPtrMapIter;
  typedef std::map<std::string, ObjectPtrMap> ScopedObjectPtrMap;
  typedef ScopedObjectPtrMap::const_iterator ScopedObjectPtrMapIter;

  ScopedObjectPtrMap scopedObjectMap;
  ObjectPtrMap globalScopeObjectMap;

  template <typename T> T* getScopedObject(const std::string& scope,
                                           const std::string& name) const {
    ScopedObjectPtrMapIter scopeIter = scopedObjectMap.find(scope);

    T* objPtr = NULL;

    if (scopeIter != scopedObjectMap.end()) {
      const ObjectPtrMap& objMap = scopeIter->second;

      objPtr = getObjectFromMap<T>(objMap, name);
    }

    if (objPtr == NULL) {
      objPtr = getObjectFromMap<T>(globalScopeObjectMap, name);
    }

    return objPtr;
  }

  template <typename T> T* getObjectFromMap(const ObjectPtrMap& ptrMap,
                                            const std::string& name) const {
    ObjectPtrMapIter iter = ptrMap.find(name);

    T* objPtr = NULL;

    if (iter != ptrMap.end()) {
      void* objectVoidPtr = iter->second;

      objPtr = static_cast<T*>(objectVoidPtr);
    }

    return objPtr;
  }
};


#endif // TRITONSORT_NAMED_OBJECT_COLLECTION_H
