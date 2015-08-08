#ifndef _TRITONSORT_CONSTANTS_H
#define _TRITONSORT_CONSTANTS_H

#include <stdint.h>
#include <string>
#include <list>
#include <map>
#include <vector>

typedef std::vector<std::string> StringVector;
typedef std::list<std::string> StringList;
typedef std::map<std::string, std::string> StringMap;

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
// From http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml
// #Copy_Constructors

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#endif //_TRITONSORT_CONSTANTS_H
